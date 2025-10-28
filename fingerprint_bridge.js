/**
 * fingerprint_bridge.js  — Render 서버 ↔ 로컬 장비 브릿지
 *
 * 기능 요약
 *  - 시리얼 포트를 통해 아두이노 지문 센서를 제어하고 결과를 Render 서버로 전달
 *  - Render 백엔드와의 WebSocket 연결을 유지하며 TAB 단말 명령을 중계
 *  - AUTO_IDENTIFY=1 설정 시 기존처럼 연속 Identify 루프 유지, 기본값은 필요 시에만 스캔
 *
 * 필요한 패키지: serialport, @serialport/parser-readline, ws, dotenv
 */

const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');

require('dotenv').config({ path: path.join(__dirname, '.env') });

const { SerialPort } = require('serialport');
const { ReadlineParser } = require('@serialport/parser-readline');
const WebSocket = require('ws');

function normalizeBackendWsUrl(raw) {
  if (!raw) return '';
  try {
    const url = new URL(raw);
    url.pathname = '/ws';
    url.search = url.search || '';
    return url.toString().replace(/\/$/, '');
  } catch (err) {
    if (/^wss?:\/\//i.test(raw)) {
      const [base, query] = raw.split('?');
      const trimmed = base.replace(/\/+$/, '');
      const withPath = /\/ws$/i.test(trimmed) ? trimmed : `${trimmed}/ws`;
      return query ? `${withPath}?${query}` : withPath;
    }
    return raw;
  }
}

const PORT_HINT      = process.env.FINGERPRINT_PORT || 'auto';
const BAUD           = Number(process.env.FINGERPRINT_BAUD || 115200);
const AUTO_IDENTIFY  = (process.env.AUTO_IDENTIFY || '0') === '1';
const IDENTIFY_BACKOFF_MS = Number(process.env.IDENTIFY_BACKOFF_MS || 300);

const BACKEND_WS_URL = normalizeBackendWsUrl(
  process.env.RENDER_WSS_URL
  || process.env.RENDER_WS_URL
  || process.env.RENDER_FP_WS_URL
  || process.env.RENDER_FP_URL
  || ''
);
const FORWARD_TOKEN  = process.env.RENDER_FP_TOKEN || '';
const FP_SITE        = process.env.FP_SITE || 'default';

const DEBUG_WS       = (process.env.DEBUG_WS || '') === '1';
const DEBUG_WS_PORT  = Number(process.env.DEBUG_WS_PORT || 8787);

const DEFAULT_LED_ON = { mode: 'breathing', color: 'blue', speed: 18 };
const DEFAULT_LED_OFF = { mode: 'off' };
const DEFAULT_ENROLL_TIMEOUT_MS = Number(process.env.ENROLL_TIMEOUT_MS || 70000);
const DEFAULT_DELETE_TIMEOUT_MS = Number(process.env.DELETE_TIMEOUT_MS || 18000);
const DEFAULT_CLEAR_TIMEOUT_MS = Number(process.env.CLEAR_TIMEOUT_MS || 25000);
const DEFAULT_COUNT_TIMEOUT_MS = Number(process.env.COUNT_TIMEOUT_MS || 8000);

const PYTHON_BIN = process.env.PYTHON_BIN || (process.platform === 'win32' ? 'python' : 'python3');
const DEFAULT_ROBOT_SCRIPT = path.join(__dirname, '..', 'AAMS_ROBOT+RL+VIS', 'mission_controller_with_vision.py');
const ROBOT_SCRIPT = process.env.ROBOT_SCRIPT || DEFAULT_ROBOT_SCRIPT;
const ROBOT_DISABLED = (process.env.ROBOT_DISABLED || '') === '1';
const ROBOT_FORWARD_URL = process.env.ROBOT_FORWARD_URL || process.env.RENDER_ROBOT_URL || '';
const ROBOT_FORWARD_TOKEN = process.env.ROBOT_FORWARD_TOKEN || process.env.RENDER_ROBOT_TOKEN || FORWARD_TOKEN || '';


function log(...args){ console.log('[fp-bridge]', ...args); }
function warn(...args){ console.warn('[fp-bridge]', ...args); }
const sleep = (ms)=>new Promise((resolve)=>setTimeout(resolve, ms));

let serial = null;
let parser = null;
let lastGoodPath = null;
let reconnecting = false;
let identifyLoopRunning = false;
let closedByUs = false;

let manualSession = null;
let manualSessionCounter = 0;
let manualIdentifyRequested = false;
let manualIdentifyDeadline = 0;
let activeCommand = null;

const forwardStatus = { enabled: !!BACKEND_WS_URL, lastOkAt: 0, lastErrorAt: 0 };
const robotForwardStatus = { enabled: true, lastOkAt: 0, lastErrorAt: 0 };
const ledState = { mode: null, color: null, speed: null, cycles: null, ok: null, pending: false, lastCommandAt: 0 };
let lastIdentifyEvent = null;
let lastIdentifyAt = 0;
let lastSerialEventAt = 0;

let robotJobCounter = 0;
const ROBOT_HISTORY_LIMIT = 10;
const robotState = {
  enabled: !ROBOT_DISABLED,
  python: PYTHON_BIN,
  script: path.resolve(ROBOT_SCRIPT),
  scriptExists: false,
  active: null,
  last: null,
  history: [],
  lastEventAt: 0
};

const activeJobsByRequestId = new Map();

const timeNow = () => Date.now();

function scheduleReconnect() {
  if (reconnectTimer) return;
  if (!BACKEND_WS_URL) return;
  const delay = Math.min(reconnectDelayMs, 30000);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    reconnectDelayMs = Math.min(reconnectDelayMs * 1.5, 30000);
    connectToBackend();
  }, delay);
}

function flushBackendQueue() {
  if (!backendAuthenticated) return;
  if (!backendWs || backendWs.readyState !== WebSocket.OPEN) return;
  while (backendQueue.length) {
    const payload = backendQueue.shift();
    try {
      backendWs.send(JSON.stringify(payload));
      forwardStatus.lastOkAt = timeNow();
    } catch (err) {
      forwardStatus.lastErrorAt = timeNow();
      warn('backend send failed:', err?.message || err);
      backendQueue.unshift(payload);
      try { backendWs.close(); } catch (_) {}
      break;
    }
  }
}

function sendToBackend(message) {
  if (!message || typeof message !== 'object') return;
  backendQueue.push({ site: FP_SITE, ...message });
  if (backendWs && backendWs.readyState === WebSocket.OPEN && backendAuthenticated) {
    flushBackendQueue();
  } else {
    connectToBackend();
  }
}

async function handleBackendMessage(ws, data) {
  const text = typeof data === 'string' ? data : data?.toString?.();
  if (!text) return;
  let message;
  try {
    message = JSON.parse(text);
  } catch (err) {
    warn('invalid backend message:', err?.message || err);
    return;
  }
  const type = message?.type;
  if (!type) return;

  if (type === 'AUTH_ACK') {
    if (message.role === 'bridge') {
      backendAuthenticated = true;
      reconnectDelayMs = 2000;
      forwardStatus.lastOkAt = timeNow();
      flushBackendQueue();
    }
    return;
  }

  if (type === 'PING') {
    try {
      ws.send(JSON.stringify({ type: 'PONG', site: FP_SITE, ts: timeNow() }));
    } catch (err) {
      warn('failed to reply pong:', err?.message || err);
    }
    return;
  }

  if (type === 'FP_START_REQUEST') {
    try {
      const session = startManualIdentify({
        timeoutMs: message.timeoutMs || message.timeout_ms || message.payload?.timeoutMs,
        led: message.led || message.payload?.led,
        ledOff: message.ledOff || message.payload?.ledOff,
        site: message.site || FP_SITE
      });
      identifyLoop();
      sendToBackend({ type: 'FP_SESSION_STARTED', session: session ? {
        id: session.id,
        requestedAt: session.requestedAt,
        deadline: session.deadline
      } : null });
    } catch (err) {
      warn('fp start failed:', err?.message || err);
      sendToBackend({ type: 'FP_SESSION_ERROR', error: err?.message || 'start_failed' });
    }
    return;
  }

  if (type === 'FP_STOP_REQUEST') {
    try {
      const reason = message.reason || 'manual';
      const turnOffLed = message.turnOffLed !== false;
      const ledOverride = message.led || message.ledOff || null;
      const session = stopManualIdentify(reason, { turnOffLed, ledOverride });
      sendToBackend({ type: 'FP_SESSION_STOPPED', session: session ? {
        id: session.id,
        active: session.active,
        reason: session.reason,
        stoppedAt: session.stoppedAt || timeNow()
      } : null });
    } catch (err) {
      warn('fp stop failed:', err?.message || err);
      sendToBackend({ type: 'FP_SESSION_ERROR', error: err?.message || 'stop_failed' });
    }
    return;
  }

  if (type === 'LED_COMMAND') {
    try {
      const ok = applyLedCommand(message.command || message.payload || message);
      sendToBackend({ type: 'LED_STATUS', ok, led: { ...ledState } });
    } catch (err) {
      warn('led command failed:', err?.message || err);
      sendToBackend({ type: 'LED_STATUS', ok: false, error: err?.message || 'led_failed', led: { ...ledState } });
    }
    return;
  }

  if (type === 'ROBOT_EXECUTE') {
    const relayRequestId = message.requestId || message.request_id || null;
    try {
      const job = startRobotJob(message.payload || {});
      const basePayload = { type: 'ROBOT_EVENT', requestId: relayRequestId, job: sanitizeRobotJob(job, { includePayload: true }) };
      sendToBackend(basePayload);
      waitForRobotJob(job, { timeoutMs: Number(message.timeoutMs || message.timeout_ms || 0) || 120000 })
        .then((result) => {
          sendToBackend({ type: 'ROBOT_EVENT', requestId: relayRequestId, job: result, final: true });
        })
        .catch((err) => {
          const snapshot = err?.job || sanitizeRobotJob(job, { includePayload: true });
      sendToBackend({ type: 'ROBOT_EVENT', requestId: relayRequestId, error: err?.message || 'robot_failed' });
        });
    } catch (err) {
      warn('robot execute failed:', err?.message || err);
      sendToBackend({ type: 'ROBOT_EVENT', error: err?.message || 'robot_failed' });
    }
    return;
  }

  if (type === 'ROBOT_INTERACTION') {
    const requestId = message.requestId || message.request_id || null;
    const action = message.action || message.command || 'resume';
    const token = message.token || null;
    const stage = message.stage || null;
    const job = requestId ? activeJobsByRequestId.get(String(requestId)) : null;
    if (!job || !job.process || job.process.killed) {
      warn('robot interaction ignored: job not found', { requestId, action });
      sendToBackend({
        type: 'ROBOT_EVENT',
        requestId,
        error: 'interaction_job_missing',
        action,
        stage
      });
      return;
    }

    let resolvedToken = token;
    if (!resolvedToken && stage && job.interactions instanceof Map) {
      resolvedToken = job.interactions.get(stage) || null;
    }
    if (!resolvedToken && job.pendingTokens instanceof Map && job.pendingTokens.size === 1) {
      const [[pendingToken]] = job.pendingTokens.entries();
      resolvedToken = pendingToken;
    }

    if (!resolvedToken) {
      warn('robot interaction token not found', { requestId, action, stage });
      sendToBackend({
        type: 'ROBOT_EVENT',
        requestId,
        error: 'interaction_token_missing',
        action,
        stage
      });
      return;
    }

    const commandPayload = {
      command: action,
      token: resolvedToken,
      stage: stage || job.stage || 'await_user',
      meta: message.meta || null
    };

    try {
      job.process.stdin.write(`${JSON.stringify(commandPayload)}\n`);
      job.logs?.push?.({ type: 'stdin', text: JSON.stringify(commandPayload), at: timeNow() });
      forwardRobotEvent(job, {
        status: 'progress',
        stage: job.stage,
        message: `interaction:${action}`,
        progress: { event: 'interaction', action, stage: commandPayload.stage, token: resolvedToken },
        meta: { interaction: commandPayload }
      });
    } catch (err) {
      warn('failed to forward interaction', err?.message || err);
      sendToBackend({
        type: 'ROBOT_EVENT',
        requestId,
        error: 'interaction_write_failed',
        message: err?.message || String(err),
        action,
        stage
      });
    }
    return;
  }

  if (type === 'FP_STATUS_REQUEST') {
    sendToBackend({ type: 'FP_STATUS', serial: {
      connected: !!(serial && serial.isOpen),
      path: serial?.path || lastGoodPath || null
    },
    manual: manualSession ? { id: manualSession.id, active: manualSession.active, deadline: manualSession.deadline } : null,
    led: { ...ledState } });
    return;
  }

  if (type === 'FP_ENROLL_REQUEST') {
    const requestId = message.requestId || null;
    const sensorId = message.sensorId ?? message.sensor_id ?? message.payload?.sensorId ?? message.payload?.sensor_id ?? message.id;
    try {
      const led = message.led ?? message.payload?.led ?? DEFAULT_LED_ON;
      const ledOff = message.ledOff ?? message.payload?.ledOff ?? DEFAULT_LED_OFF;
      const timeoutMs = message.timeoutMs ?? message.timeout_ms ?? message.payload?.timeoutMs ?? message.payload?.timeout_ms;
      const result = await enrollFingerprint({ sensorId, timeoutMs, led, ledOff });
      sendToBackend({
        type: 'FP_ENROLL_RESULT',
        ok: true,
        result,
        payload: result,
        sensorId: result?.id ?? (Number(sensorId) || null),
        requestId
      });
    } catch (err) {
      warn('fp enroll request failed:', err?.message || err);
      sendToBackend({
        type: 'FP_ENROLL_RESULT',
        ok: false,
        error: err?.message || 'enroll_failed',
        code: err?.statusCode || err?.status || 500,
        payload: err?.payload || null,
        sensorId: Number(sensorId) || null,
        requestId
      });
    }
    return;
  }

  if (type === 'FP_DELETE_REQUEST') {
    const requestId = message.requestId || null;
    const sensorId = message.sensorId ?? message.sensor_id ?? message.id ?? message.payload?.sensorId ?? message.payload?.sensor_id;
    const allowMissing = message.allowMissing ?? message.allow_missing ?? message.payload?.allowMissing ?? false;
    try {
      const result = await deleteFingerprint({ sensorId, allowMissing, timeoutMs: message.timeoutMs ?? message.timeout_ms });
      sendToBackend({
        type: 'FP_DELETE_RESULT',
        ok: true,
        result,
        payload: result,
        sensorId: Number(sensorId) || null,
        requestId
      });
    } catch (err) {
      warn('fp delete request failed:', err?.message || err);
      sendToBackend({
        type: 'FP_DELETE_RESULT',
        ok: false,
        error: err?.message || 'delete_failed',
        code: err?.statusCode || err?.status || 500,
        payload: err?.payload || null,
        sensorId: Number(sensorId) || null,
        requestId
      });
    }
    return;
  }

  if (type === 'FP_CLEAR_REQUEST') {
    const requestId = message.requestId || null;
    try {
      const result = await clearFingerprints({ timeoutMs: message.timeoutMs ?? message.timeout_ms });
      sendToBackend({ type: 'FP_CLEAR_RESULT', ok: true, result, payload: result, requestId });
    } catch (err) {
      warn('fp clear request failed:', err?.message || err);
      sendToBackend({
        type: 'FP_CLEAR_RESULT',
        ok: false,
        error: err?.message || 'clear_failed',
        code: err?.statusCode || err?.status || 500,
        payload: err?.payload || null,
        requestId
      });
    }
    return;
  }

  if (type === 'FP_COUNT_REQUEST') {
    const requestId = message.requestId || null;
    try {
      const result = await countFingerprints({ timeoutMs: message.timeoutMs ?? message.timeout_ms });
      const count = result?.count ?? result?.result?.count ?? result?.result;
      sendToBackend({ type: 'FP_COUNT_RESULT', ok: true, count, result, payload: result, requestId });
    } catch (err) {
      warn('fp count request failed:', err?.message || err);
      sendToBackend({
        type: 'FP_COUNT_RESULT',
        ok: false,
        error: err?.message || 'count_failed',
        code: err?.statusCode || err?.status || 500,
        payload: err?.payload || null,
        requestId
      });
    }
    return;
  }

  if (type === 'FP_HEALTH_REQUEST') {
    const requestId = message.requestId || null;
    try {
      const status = buildHealthPayload();
      sendToBackend({ type: 'FP_HEALTH', ok: true, status, payload: status, requestId });
    } catch (err) {
      warn('fp health request failed:', err?.message || err);
      sendToBackend({
        type: 'FP_HEALTH_ERROR',
        ok: false,
        error: err?.message || 'health_failed',
        code: err?.statusCode || err?.status || 500,
        requestId
      });
    }
    return;
  }
}

function connectToBackend() {
  if (!BACKEND_WS_URL) {
    warn('RENDER_WSS_URL not configured; backend relay disabled');
    return;
  }
  if (backendConnecting) return;
  if (backendWs && backendWs.readyState === WebSocket.OPEN) {
    if (!backendAuthenticated) {
      try {
        backendWs.send(JSON.stringify({ type: 'AUTH_BRIDGE', site: FP_SITE, token: FORWARD_TOKEN || undefined }));
      } catch (err) {
        warn('failed to send auth message:', err?.message || err);
      }
    }
    return;
  }

  backendConnecting = true;
  const ws = new WebSocket(BACKEND_WS_URL);
  backendWs = ws;
  backendAuthenticated = false;

  ws.on('open', () => {
    backendConnecting = false;
    reconnectDelayMs = 2000;
    try {
      ws.send(JSON.stringify({ type: 'AUTH_BRIDGE', site: FP_SITE, token: FORWARD_TOKEN || undefined }));
    } catch (err) {
      warn('failed to send auth:', err?.message || err);
    }
    forwardStatus.lastOkAt = timeNow();
    flushBackendQueue();
  });

  ws.on('message', (data) => {
    Promise.resolve(handleBackendMessage(ws, data)).catch((err) => {
      warn('backend message handler failed:', err?.message || err);
    });
  });

  ws.on('ping', () => {
    try {
      ws.pong();
      forwardStatus.lastOkAt = timeNow();
    } catch (err) {
      warn('failed to reply backend ping:', err?.message || err);
    }
  });

  ws.on('pong', () => {
    forwardStatus.lastOkAt = timeNow();
  });

  ws.on('close', () => {
    backendConnecting = false;
    backendAuthenticated = false;
    if (backendWs === ws) {
      backendWs = null;
    }
    forwardStatus.lastErrorAt = timeNow();
    scheduleReconnect();
  });

  ws.on('error', (err) => {
    warn('backend ws error:', err?.message || err);
  });

  ws.on('unexpected-response', (_req, res) => {
    backendConnecting = false;
    backendAuthenticated = false;
    const status = res?.statusCode;
    const statusMessage = res?.statusMessage;
    warn('backend ws unexpected response:', status, statusMessage);
    try { res?.resume?.(); } catch (_) {}
    forwardStatus.lastErrorAt = timeNow();
    scheduleReconnect();
  });
}
let backendWs = null;
let backendAuthenticated = false;
let backendConnecting = false;
let reconnectTimer = null;
let reconnectDelayMs = 2000;
const backendQueue = [];

function httpError(status, message){
  const err = new Error(message || 'error');
  err.statusCode = status;
  return err;
}

function cleanObject(obj){
  if (!obj || typeof obj !== 'object') return obj;
  if (Array.isArray(obj)){
    for (let i = obj.length - 1; i >= 0; i -= 1){
      const item = obj[i];
      if (item && typeof item === 'object'){
        cleanObject(item);
        if (item && typeof item === 'object' && !Object.keys(item).length){
          obj.splice(i, 1);
          continue;
        }
      }
      if (item === null || item === undefined || (typeof item === 'string' && item.trim() === '')){
        obj.splice(i, 1);
      }
    }
    return obj;
  }
  for (const key of Object.keys(obj)){
    const value = obj[key];
    if (value === null || value === undefined){
      delete obj[key];
      continue;
    }
    if (typeof value === 'string'){
      const trimmed = value.trim();
      if (!trimmed){
        delete obj[key];
        continue;
      }
      obj[key] = trimmed;
      continue;
    }
    if (Array.isArray(value)){
      cleanObject(value);
      if (!value.length){
        delete obj[key];
      }
      continue;
    }
    if (typeof value === 'object'){
      cleanObject(value);
      if (!Object.keys(value).length){
        delete obj[key];
      }
    }
  }
  return obj;
}

function normalizeFlag(value){
  if (value === null || value === undefined) return undefined;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string'){
    const trimmed = value.trim().toLowerCase();
    if (!trimmed) return undefined;
    if (['true', '1', 'yes', 'y', 'on'].includes(trimmed)) return true;
    if (['false', '0', 'no', 'n', 'off'].includes(trimmed)) return false;
  }
  return Boolean(value);
}

function createToken(prefix = 'stage'){
  const suffix = Math.random().toString(16).slice(2, 8);
  return `${prefix}-${Date.now().toString(36)}-${suffix}`;
}

function resolveDirection(payload = {}, summary = null){
  const direct = payload.direction || payload.mode || payload.type;
  const fromSummary = summary && summary.action ? summary.action : null;
  const candidate = (direct || fromSummary || '').toString().toLowerCase();
  if (['return', 'incoming', 'in', '입고', '불입'].includes(candidate)) return 'in';
  if (['dispatch', 'issue', 'out', '불출'].includes(candidate)) return 'out';
  return fromSummary === 'return' ? 'in' : 'out';
}

function resolveLocker(payload = {}, summary = null){
  return payload.locker
    || payload.mission
    || payload.missionLabel
    || payload.mission_label
    || (summary && summary.locker)
    || null;
}

function parseMissionNumber(raw, fallback = 1){
  if (raw === null || raw === undefined) return fallback;
  if (typeof raw === 'number' && Number.isFinite(raw)) {
    const num = Math.round(raw);
    if (num === 1 || num === 2) return num;
    if (num > 2) return ((num - 1) % 2) + 1;
  }
  const text = String(raw).trim();
  if (!text) return fallback;
  const match = text.match(/(\d+)/);
  if (match) {
    const num = Number.parseInt(match[1], 10);
    if (Number.isFinite(num)) {
      if (num === 1 || num === 2) return num;
      if (num > 2) return ((num - 1) % 2) + 1;
    }
  }
  if (/2/.test(text)) return 2;
  return fallback;
}

function buildRobotScriptArgs(payload = {}, summary = null){
  const args = ['--bridge-mode', '--auto'];
  const direction = resolveDirection(payload, summary);
  args.push('--direction', direction === 'in' ? 'in' : 'out');

  const locker = resolveLocker(payload, summary);
  if (locker) {
    args.push('--mission-label', String(locker));
  }
  const missionSource = payload.mission ?? locker ?? null;
  const missionNumber = parseMissionNumber(missionSource, 1);
  args.push('--mission', String(missionNumber));

  const firearmSerial = payload.firearmSerial
    || payload.expectedQr
    || payload.expected_qr
    || (summary && summary.firearmCode)
    || null;
  if (firearmSerial) {
    args.push('--expected-qr', String(firearmSerial));
  }

  const includesAmmo = Object.prototype.hasOwnProperty.call(payload, 'includesAmmo')
    ? normalizeFlag(payload.includesAmmo)
    : (summary ? !!summary.includes?.ammo : undefined);
  if (includesAmmo) {
    args.push('--with-mag');
  }

  if (payload.requestId || payload.request_id) {
    args.push('--request-id', String(payload.requestId ?? payload.request_id));
  }

  if (payload.site || payload.site_id) {
    args.push('--site', String(payload.site || payload.site_id));
  }

  return args;
}

function summarizeRobotPayload(payload = {}){
  if (!payload || typeof payload !== 'object') return null;

  const bridgePayload = (payload.bridgePayload && typeof payload.bridgePayload === 'object')
    ? payload.bridgePayload
    : null;
  const merged = bridgePayload ? { ...payload, ...bridgePayload } : payload;

  const dispatch = (merged.dispatch && typeof merged.dispatch === 'object') ? merged.dispatch : {};

  const includesInput = (merged.includes && typeof merged.includes === 'object') ? merged.includes : {};
  const includesDispatch = (dispatch.includes && typeof dispatch.includes === 'object') ? dispatch.includes : {};

  let firearmIncluded = Object.prototype.hasOwnProperty.call(includesInput, 'firearm')
    ? normalizeFlag(includesInput.firearm)
    : Object.prototype.hasOwnProperty.call(includesDispatch, 'firearm')
      ? normalizeFlag(includesDispatch.firearm)
      : undefined;

  if (firearmIncluded === undefined) {
    if (Object.prototype.hasOwnProperty.call(merged, 'includesFirearm')) {
      firearmIncluded = normalizeFlag(merged.includesFirearm);
    }
  }

  if (firearmIncluded === undefined) {
    firearmIncluded = !!(merged.firearm || dispatch.firearm);
  }

  let ammoIncluded = Object.prototype.hasOwnProperty.call(includesInput, 'ammo')
    ? normalizeFlag(includesInput.ammo)
    : Object.prototype.hasOwnProperty.call(includesDispatch, 'ammo')
      ? normalizeFlag(includesDispatch.ammo)
      : undefined;

  if (ammoIncluded === undefined) {
    if (Object.prototype.hasOwnProperty.call(merged, 'includesAmmo')) {
      ammoIncluded = normalizeFlag(merged.includesAmmo);
    }
  }

  if (ammoIncluded === undefined) {
    ammoIncluded = (Array.isArray(merged.ammo) && merged.ammo.length > 0)
      || (Array.isArray(dispatch.ammo) && dispatch.ammo.length > 0);
  }

  const payloadItems = Array.isArray(merged.items) ? merged.items : [];
  if (firearmIncluded === undefined) {
    firearmIncluded = payloadItems.some((item) => String(item?.item_type || item?.type || '').toUpperCase() === 'FIREARM');
  }
  if (ammoIncluded === undefined) {
    ammoIncluded = payloadItems.some((item) => String(item?.item_type || item?.type || '').toUpperCase() === 'AMMO');
  }

  const firearmHas = !!firearmIncluded;
  const ammoHas = !!ammoIncluded;

  let action = String(merged.mode || '').toLowerCase();
  if (!action && merged.direction) {
    action = String(merged.direction).toLowerCase();
  }
  if (['firearm_and_ammo', 'firearm_only', 'ammo_only', 'dispatch', 'issue', 'out', '불출'].includes(action)) {
    action = 'dispatch';
  } else if (['return', 'incoming', 'in', '입고', '불입'].includes(action)) {
    action = 'return';
  }
  const typeRaw = String(merged.type || merged.request_type || '').toUpperCase();
  if (!action && merged.direction) {
    action = ['in', '입고', '불입'].includes(String(merged.direction).toLowerCase()) ? 'return' : 'dispatch';
  }
  if (!action){
    if (typeRaw === 'RETURN') action = 'return';
    else if (typeRaw === 'DISPATCH' || typeRaw === 'ISSUE') action = 'dispatch';
  }
  if (!action){
    action = firearmHas || ammoHas ? 'dispatch' : 'unknown';
  }

  const firearm = merged.firearm || dispatch.firearm || {};
  const firearmCode = merged.firearmSerial
    || firearm.code
    || firearm.firearm_number
    || firearm.serial
    || firearm.weapon_code
    || merged.weapon_code
    || merged.weaponCode
    || null;

  const locker = merged.locker
    || merged.mission
    || merged.mission_label
    || firearm.locker
    || firearm.locker_code
    || firearm.lockerCode
    || dispatch.locker
    || dispatch.location
    || merged.storage
    || merged.storage_code
    || (merged.request && (merged.request.locker || merged.request.storage_locker))
    || null;

  const ammoItems = Array.isArray(merged.ammo) && merged.ammo.length
    ? merged.ammo
    : (Array.isArray(dispatch.ammo) ? dispatch.ammo : []);
  const ammoSummaryParts = [];
  let ammoCount = 0;
  for (const item of ammoItems){
    if (!item || typeof item !== 'object') continue;
    const caliber = (item.caliber || item.type || item.name || item.label || '').toString().trim();
    const qtyRaw = item.qty ?? item.quantity ?? item.amount ?? item.count;
    const qty = Number(qtyRaw);
    if (Number.isFinite(qty)){
      ammoCount += qty;
    }
    const label = [caliber || null, Number.isFinite(qty) ? `×${qty}` : null]
      .filter(Boolean)
      .join('');
    if (label){
      ammoSummaryParts.push(label);
    }
  }
  const ammoCountValue = Number.isFinite(ammoCount) ? ammoCount : null;
  const hasAmmoSummary = ammoSummaryParts.length > 0;

  const includesLabel = firearmHas && ammoHas
    ? '총기+탄약'
    : (firearmHas ? '총기' : (ammoHas ? '탄약' : '기타'));

  const summary = {
    requestId: merged.requestId ?? merged.request_id ?? null,
    action,
    actionLabel: action === 'return' ? '불입' : (action === 'dispatch' ? '불출' : '장비'),
    type: typeRaw || null,
    includes: { firearm: firearmHas, ammo: ammoHas, label: includesLabel },
    firearmCode,
    ammoSummary: hasAmmoSummary ? ammoSummaryParts.join(', ') : null,
    ammoCount: hasAmmoSummary ? ammoCountValue : null,
    locker,
    site: merged.site_id || merged.site || null,
    purpose: merged.purpose || null,
    location: merged.location
      || dispatch.location
      || (merged.request && merged.request.location)
      || null
  };

  cleanObject(summary);
  return Object.keys(summary).length ? summary : null;
}

function buildRobotPayloadSnapshot(payload = {}){
  if (!payload || typeof payload !== 'object') return null;
  const dispatch = (payload.dispatch && typeof payload.dispatch === 'object') ? payload.dispatch : {};
  const includes = dispatch.includes || payload.includes || null;
  const firearm = dispatch.firearm || payload.firearm || null;
  const ammoList = Array.isArray(dispatch.ammo) && dispatch.ammo.length
    ? dispatch.ammo
    : (Array.isArray(payload.ammo) ? payload.ammo : []);

  const ammoPreview = ammoList.slice(0, 5).map(item => cleanObject({
    id: item?.id || item?.ammo_id || null,
    name: item?.name || item?.label || item?.category || item?.caliber || null,
    qty: item?.qty ?? item?.quantity ?? item?.count ?? null
  })).filter(Boolean);

  return cleanObject({
    requestId: payload.requestId ?? payload.request_id ?? null,
    executionEventId: payload.executionEventId ?? payload.eventId ?? null,
    type: payload.type || dispatch.type || null,
    mode: payload.mode || dispatch.mode || null,
    site: payload.site || dispatch.site_id || payload.site_id || null,
    includes,
    firearm: firearm ? cleanObject({
      id: firearm.id || firearm.firearm_id || null,
      code: firearm.code || firearm.firearm_number || firearm.serial || null,
      locker: firearm.locker || firearm.storage_locker || null,
      slot: firearm.slot || null
    }) : null,
    ammo: ammoPreview.length ? ammoPreview : null,
    location: payload.location
      || dispatch.location
      || (payload.request && payload.request.location)
      || null,
    purpose: dispatch.purpose || payload.purpose || null,
    simulate: dispatch.simulate || payload.simulate || null
  });
}

function updateRobotScriptState(){
  try {
    robotState.scriptExists = robotState.script ? fs.existsSync(robotState.script) : false;
  } catch (err) {
    robotState.scriptExists = false;
  }
}

updateRobotScriptState();


async function listCandidates(){
  const ports = await SerialPort.list();
  const preferred = [];
  const normal = [];
  for (const p of ports){
    const manufacturer = (p.manufacturer || '').toLowerCase();
    const vendor = (p.vendorId || '').toLowerCase();
    const isUsb = manufacturer.includes('arduino') || manufacturer.includes('wch') || manufacturer.includes('silicon labs') || manufacturer.includes('ftdi') || vendor === '2341' || vendor === '1a86' || vendor === '10c4' || vendor === '0403';
    (isUsb ? preferred : normal).push(p);
  }
  const ordered = [];
  if (lastGoodPath){
    const all = [...preferred, ...normal];
    const found = all.find(p => p.path === lastGoodPath);
    if (found) ordered.push(found);
  }
  for (const p of [...preferred, ...normal]){
    if (!ordered.find(x => x.path === p.path)) ordered.push(p);
  }
  return ordered;
}

async function tryOpen(path){
  const port = new SerialPort({ path, baudRate: BAUD, autoOpen: false });
  const lineParser = port.pipe(new ReadlineParser({ delimiter: '\n' }));
  await new Promise((resolve, reject) => port.open(err => err ? reject(err) : resolve()));
  log(`opened serial ${path}@${BAUD}`);
  return { port, lineParser };
}

async function findAndOpen(){
  if (PORT_HINT !== 'auto'){
    try { return await tryOpen(PORT_HINT); }
    catch (err) { warn('explicit port failed:', err.message || err); }
  }
  const candidates = await listCandidates();
  for (const entry of candidates){
    try { return await tryOpen(entry.path); }
    catch (err) { warn('candidate open failed:', entry.path, err.message || err); }
  }
  throw new Error('no usable serial port found');
}

function writeSerial(obj){
  if (!serial || !serial.isOpen) return false;
  try {
    serial.write(JSON.stringify(obj) + '\n');
    return true;
  } catch (err) {
    warn('serial write failed:', err.message || err);
    return false;
  }
}

function ensureSerialReady(){
  if (!serial || !serial.isOpen || !parser){
    throw httpError(503, 'serial_not_ready');
  }
}

function beginCommand(type, meta = {}){
  if (activeCommand && !activeCommand.done){
    throw httpError(409, 'command_in_progress');
  }
  activeCommand = {
    type,
    startedAt: timeNow(),
    meta: { ...meta },
    done: false
  };
  return activeCommand;
}

function finishCommand(entry, { result = null, error = null } = {}){
  if (!entry) return;
  entry.done = true;
  if (result) entry.result = result;
  if (error) entry.error = error.message || String(error);
  if (activeCommand === entry){
    activeCommand = null;
  }
}

function waitForCommandResult({ expectedType, timeoutMs = 20000, onStage }){
  ensureSerialReady();
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(httpError(504, 'sensor_timeout'));
    }, timeoutMs);

    const handler = (obj) => {
      if (settled || !obj) return;
      if (obj.stage && typeof onStage === 'function'){
        try { onStage(obj); } catch (_) {}
      }
      if (obj.ok === false){
        settled = true;
        clearTimeout(timer);
        cleanup();
        const err = httpError(502, obj.error || 'sensor_error');
        err.payload = obj;
        reject(err);
        return;
      }
      if (expectedType && obj.type !== expectedType){
        return;
      }
      settled = true;
      clearTimeout(timer);
      cleanup();
      resolve(obj);
    };

    const cleanup = () => { parser?.off('dataLine', handler); };
    parser?.on('dataLine', handler);
  });
}

async function runSensorCommand({ command, payload = {}, expectedType, timeoutMs, onStage }){
  ensureSerialReady();
  const wrote = writeSerial({ cmd: command, ...payload });
  if (!wrote){
    throw httpError(503, 'serial_write_failed');
  }
  const result = await waitForCommandResult({ expectedType, timeoutMs, onStage });
  return result;
}

async function enrollFingerprint({ sensorId, timeoutMs, led, ledOff }){
  const id = Number(sensorId || 0);
  if (!Number.isInteger(id) || id <= 0){
    throw httpError(400, 'bad_sensor_id');
  }
  const entry = beginCommand('enroll', { sensorId: id });
  const ledOffCommand = ledOff === false ? null : (ledOff ? normalizeLedCommand(ledOff) : DEFAULT_LED_OFF);
  try {
    const commandTimeout = Math.max(5000, Number(timeoutMs) || DEFAULT_ENROLL_TIMEOUT_MS);
    if (led && led !== false) {
      try { applyLedCommand(led); }
      catch (err) { warn('led command failed before enroll:', err?.message || err); }
    }
    const result = await runSensorCommand({
      command: 'enroll',
      payload: { id },
      expectedType: 'enroll',
      timeoutMs: commandTimeout
    });
    finishCommand(entry, { result });
    if (ledOffCommand) {
      try { applyLedCommand(ledOffCommand); }
      catch (err) { warn('led command failed after enroll:', err?.message || err); }
    }
    return result;
  } catch (err) {
    finishCommand(entry, { error: err });
    if (ledOffCommand) {
      try { applyLedCommand(ledOffCommand); }
      catch (ledErr) { warn('led command failed after enroll error:', ledErr?.message || ledErr); }
    }
    throw err;
  }
}

async function deleteFingerprint({ sensorId, allowMissing = false, timeoutMs }){
  const id = Number(sensorId || 0);
  if (!Number.isInteger(id) || id <= 0){
    throw httpError(400, 'bad_sensor_id');
  }
  const entry = beginCommand('delete', { sensorId: id });
  try {
    const result = await runSensorCommand({
      command: 'delete',
      payload: { id },
      expectedType: 'delete',
      timeoutMs: Math.max(4000, Number(timeoutMs) || DEFAULT_DELETE_TIMEOUT_MS)
    });
    finishCommand(entry, { result });
    return result;
  } catch (err) {
    finishCommand(entry, { error: err });
    if (allowMissing && err?.payload?.error === 'delete_failed') {
      return { ok: true, skipped: true, id };
    }
    throw err;
  }
}

async function clearFingerprints({ timeoutMs } = {}){
  const entry = beginCommand('clear', {});
  try {
    const result = await runSensorCommand({
      command: 'clear',
      expectedType: 'clear',
      timeoutMs: Math.max(4000, Number(timeoutMs) || DEFAULT_CLEAR_TIMEOUT_MS)
    });
    finishCommand(entry, { result });
    return result;
  } catch (err) {
    finishCommand(entry, { error: err });
    throw err;
  }
}

async function countFingerprints({ timeoutMs } = {}){
  const entry = beginCommand('count', {});
  try {
    const result = await runSensorCommand({
      command: 'count',
      expectedType: 'count',
      timeoutMs: Math.max(2000, Number(timeoutMs) || DEFAULT_COUNT_TIMEOUT_MS)
    });
    finishCommand(entry, { result });
    return result;
  } catch (err) {
    finishCommand(entry, { error: err });
    throw err;
  }
}

function forwardToRender(obj) {
  if (!BACKEND_WS_URL) return;
  sendToBackend({ type: 'FP_EVENT', payload: obj });
}

let wsServer = null;
function setupDebugWS(){
  if (!DEBUG_WS) return;
  wsServer = new WebSocket.Server({ port: DEBUG_WS_PORT });
  wsServer.on('connection', ws => {
    ws.send(JSON.stringify({ hello: 'fp-bridge', version: 'bridge-2.0' }));
    ws.on('message', buf => {
      let obj;
      try { obj = JSON.parse(String(buf)); }
      catch { return; }
      writeSerial(obj);
    });
  });
  log(`debug ws on ws://localhost:${DEBUG_WS_PORT}`);
}

function wsBroadcast(obj){
  if (!wsServer) return;
  const payload = JSON.stringify(obj);
  wsServer.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN){
      client.send(payload);
    }
  });
}

function normalizeLedCommand(cmd){
  if (!cmd || typeof cmd !== 'object') return null;
  const out = {};
  if (cmd.mode) out.mode = String(cmd.mode);
  else if (cmd.state) out.mode = String(cmd.state);
  if (cmd.color) out.color = String(cmd.color);
  if (cmd.speed !== undefined) out.speed = Number(cmd.speed);
  else if (cmd.brightness !== undefined) out.speed = Number(cmd.brightness);
  if (cmd.cycles !== undefined) out.cycles = Number(cmd.cycles);
  return out;
}

function applyLedCommand(command){
  const payload = normalizeLedCommand(command);
  if (!payload) return false;
  const ok = writeSerial({ cmd: 'led', ...payload });
  ledState.mode = payload.mode || ledState.mode;
  ledState.color = payload.color || ledState.color;
  if (payload.speed !== undefined && !Number.isNaN(payload.speed)) ledState.speed = payload.speed;
  if (payload.cycles !== undefined && !Number.isNaN(payload.cycles)) ledState.cycles = payload.cycles;
  ledState.lastCommandAt = timeNow();
  ledState.pending = true;
  if (!ok){
    ledState.ok = false;
    ledState.pending = false;
  }
  return ok;
}

function sanitizeRobotJob(job, { includePayload = false } = {}){
  if (!job) return null;
  const base = {
    id: job.id,
    requestId: job.requestId ?? null,
    eventId: job.eventId ?? null,
    status: job.status,
    stage: job.stage || null,
    message: job.message || null,
    error: job.error || null,
    site: job.site || FP_SITE,
    startedAt: job.startedAt || null,
    finishedAt: job.finishedAt || null,
    mode: job.mode || null,
    progress: job.progress || null
  };
  if (job.summary) {
    base.summary = job.summary;
  }
  if (job.payloadPreview) {
    base.payloadPreview = job.payloadPreview;
  }
  if (includePayload) {
    base.payload = job.payload;
  }
  if (job.result) {
    base.result = job.result;
  }
  return base;
}

function recordRobotHistory(job){
  const snapshot = sanitizeRobotJob(job);
  if (!snapshot) return;
  robotState.last = snapshot;
  robotState.history.unshift(snapshot);
  if (robotState.history.length > ROBOT_HISTORY_LIMIT) {
    robotState.history.length = ROBOT_HISTORY_LIMIT;
  }
}

function forwardRobotEvent(job, update = {}){
  const payload = {
    channel: 'robot',
    site: job?.site || FP_SITE,
    job: {
      id: job?.id || null,
      requestId: job?.requestId ?? null,
      eventId: job?.eventId ?? null,
      status: update.status || job?.status || null,
      stage: update.stage || job?.stage || null,
      message: update.message || job?.message || null,
      progress: update.progress || job?.progress || null,
      mode: job?.mode || null,
      summary: job?.summary || null,
      payload: job?.payloadPreview || null,
      timestamp: timeNow(),
      meta: update.meta || job?.payloadPreview || null
    }
  };
  sendToBackend({ type: 'ROBOT_EVENT', site: payload.site, job: payload.job, channel: payload.channel, requestId: job?.requestId ?? null });
}

function finalizeRobotJob(job, status, info = {}){
  if (!job || job.finishedAt) return;
  job.status = status === 'succeeded' ? 'succeeded' : status === 'success' ? 'succeeded' : 'failed';
  job.finishedAt = timeNow();
  if (info.stage) job.stage = info.stage;
  if (info.message) job.message = info.message;
  if (info.mode) job.mode = info.mode;
  if (info.progress) job.progress = info.progress;
  if (info.result) job.result = info.result;
  if (job.status === 'failed' && info.message && !job.error) {
    job.error = info.message;
  }
  if (info.error) job.error = info.error;
  log('robot job finished', {
    jobId: job.id,
    status: job.status,
    stage: job.stage,
    message: job.message,
    error: job.error || null,
    summary: job.summary || null
  });
  robotState.active = null;
  robotState.lastEventAt = job.finishedAt;
  activeJobsByRequestId.delete(String(job.requestId));
  if (job.process && job.process.stdin && !job.process.stdin.destroyed) {
    try { job.process.stdin.end(); }
    catch (_) {}
  }
  recordRobotHistory(job);
  forwardRobotEvent(job, {
    status: job.status === 'succeeded' ? 'success' : 'error',
    stage: job.stage,
    message: job.message,
    progress: job.progress,
    meta: info.result || info.meta || info
  });
  if (Array.isArray(job.waiters) && job.waiters.length) {
    const snapshot = sanitizeRobotJob(job, { includePayload: true });
    const waiters = job.waiters.splice(0);
    job.waiters = [];
    waiters.forEach(({ resolve }) => {
      try { resolve(snapshot); }
      catch (_) {}
    });
  }
}

function handleRobotStdout(job, line){
  if (!job || !line) return;
  let obj = null;
  try { obj = JSON.parse(line); }
  catch (err) {
    job.logs?.push?.({ type: 'stdout', text: line, at: timeNow(), error: err.message || String(err) });
    return;
  }
  if (obj.event === 'progress'){
    job.stage = obj.stage || job.stage;
    job.message = obj.message || job.message;
    job.mode = obj.mode || job.mode;
    job.progress = obj;
    if (obj.token && job.pendingTokens) {
      job.pendingTokens.delete(obj.token);
    }
    if (obj.stage && job.interactions && job.interactions.has(obj.stage)) {
      job.interactions.delete(obj.stage);
    }
    robotState.lastEventAt = timeNow();
    log('robot progress', {
      jobId: job.id,
      stage: job.stage,
      message: job.message,
      summary: job.summary || null
    });
    forwardRobotEvent(job, {
      status: 'progress',
      stage: job.stage,
      message: job.message,
      progress: obj,
      meta: obj
    });
    return;
  }
  if (obj.event === 'complete'){
    job.stage = obj.stage || job.stage;
    job.message = obj.message || job.message;
    job.mode = obj.mode || job.mode;
    job.result = obj;
    log('robot complete', {
      jobId: job.id,
      status: obj.status,
      stage: job.stage,
      message: job.message,
      summary: job.summary || null
    });
    finalizeRobotJob(job, obj.status === 'success' ? 'succeeded' : 'failed', {
      stage: job.stage,
      message: job.message,
      mode: job.mode,
      progress: obj,
      result: obj,
      error: obj.status === 'success' ? null : (obj.message || obj.error || 'robot_error')
    });
    return;
  }
  if (obj.event === 'await_user'){
    const token = obj.token || createToken(obj.stage || 'await');
    obj.token = token;
    job.stage = obj.stage || job.stage || 'await_user';
    job.message = obj.message || job.message || '사용자 입력 대기';
    job.mode = obj.mode || job.mode;
    job.progress = obj;
    job.pendingTokens.set(token, obj);
    if (obj.stage) {
      job.interactions.set(obj.stage, token);
    }
    robotState.lastEventAt = timeNow();
    forwardRobotEvent(job, {
      status: 'progress',
      stage: job.stage,
      message: job.message,
      progress: obj,
      meta: obj
    });
    return;
  }
  if (obj.event === 'await_user_done') {
    const token = obj.token;
    if (token && job.pendingTokens) {
      job.pendingTokens.delete(token);
    }
    if (obj.stage && job.interactions) {
      job.interactions.delete(obj.stage);
    }
    job.stage = obj.stage || job.stage;
    job.message = obj.message || job.message;
    job.mode = obj.mode || job.mode;
    job.progress = obj;
    robotState.lastEventAt = timeNow();
    forwardRobotEvent(job, {
      status: 'progress',
      stage: job.stage,
      message: job.message,
      progress: obj,
      meta: obj
    });
    return;
  }
  if (obj.event === 'log') {
    job.logs?.push?.({ type: 'log', text: obj.message || '', at: timeNow(), level: obj.level || 'info', stage: obj.stage || null });
    job.stage = obj.stage || job.stage;
    job.message = obj.message || job.message;
    job.progress = obj;
    forwardRobotEvent(job, {
      status: 'progress',
      stage: job.stage,
      message: job.message,
      progress: obj,
      meta: obj
    });
    return;
  }
  if (obj.event === 'lockdown') {
    job.stage = obj.stage || 'lockdown';
    job.message = obj.message || '시스템 락다운';
    job.mode = obj.mode || job.mode;
    job.progress = obj;
    job.error = obj.reason || job.message;
    finalizeRobotJob(job, 'failed', {
      stage: job.stage,
      message: job.message,
      mode: job.mode,
      progress: obj,
      error: obj.reason || obj.message || 'lockdown',
      result: { lockdown: obj }
    });
    return;
  }
  job.logs?.push?.({ type: 'stdout', text: line, at: timeNow() });
}

function extractForwardConfig(raw){
  if (raw && typeof raw === 'object') {
    const url = typeof raw.url === 'string' ? raw.url.trim() : null;
    const token = typeof raw.token === 'string' ? raw.token.trim() : null;
    if (url) {
      return { url, token: token || null };
    }
  }
  if (ROBOT_FORWARD_URL) {
    return { url: ROBOT_FORWARD_URL, token: ROBOT_FORWARD_TOKEN || null };
  }
  return null;
}

function startRobotJob(payload = {}){
  if (!robotState.enabled){
    const err = new Error('robot_disabled');
    err.statusCode = 503;
    throw err;
  }
  updateRobotScriptState();
  if (!robotState.scriptExists){
    const err = new Error('robot_script_missing');
    err.statusCode = 503;
    throw err;
  }
  if (robotState.active && !robotState.active.finishedAt){
    const err = new Error('robot_busy');
    err.statusCode = 409;
    throw err;
  }
    const requestId = payload.requestId ?? payload.request_id ?? null;
  if (!requestId){
    const err = new Error('missing_request_id');
    err.statusCode = 400;
    throw err;
  }

  const jobId = (++robotJobCounter);
  const summary = summarizeRobotPayload(payload);
  const scriptArgs = buildRobotScriptArgs(payload, summary);
  const site = payload.site || payload.site_id || payload.dispatch?.site_id || FP_SITE;
  const payloadPreview = buildRobotPayloadSnapshot(payload);
  const forward = extractForwardConfig(payload.forward);
  const job = {
    id: jobId,
    requestId,
    eventId: payload.eventId ?? payload.executionEventId ?? payload.execution_event_id ?? null,
    payload,
    payloadPreview,
    summary: summary || null,
    site,
    status: 'starting',
    stage: 'starting',
    createdAt: timeNow(),
    logs: [],
    forward,
    waiters: [],
    interactions: new Map(),
    pendingTokens: new Map()
  };

    if (summary){
    job.mode = job.mode || summary.action || job.mode;
    if (!job.message){
      const parts = [summary.actionLabel, summary.includes?.label].filter(Boolean);
      job.message = parts.length ? `${parts.join(' ')} 준비` : '장비 명령 준비';
    }
  }

  const env = {
    ...process.env,
    PYTHONIOENCODING: 'utf-8',
    PYTHONUNBUFFERED: '1'
  };
  const scriptCwd = path.dirname(robotState.script);
  const resolvedCwd = scriptCwd && scriptCwd !== '.' && fs.existsSync(scriptCwd) ? scriptCwd : undefined;
  const proc = spawn(
    robotState.python || PYTHON_BIN,
    [robotState.script, ...scriptArgs],
    {
      env,
      cwd: resolvedCwd,
    }
  );
  if (proc.stdin && typeof proc.stdin.setDefaultEncoding === 'function') {
    proc.stdin.setDefaultEncoding('utf8');
  }
  job.process = proc;
  job.startedAt = timeNow();
  job.status = 'running';
  robotState.active = job;
  robotState.lastEventAt = job.startedAt;
  activeJobsByRequestId.set(String(requestId), job);

  log('robot job accepted', {
    jobId,
    requestId: job.requestId,
    action: summary?.actionLabel || summary?.action || payload.mode || payload.type || 'unknown',
    includes: summary?.includes || null,
    firearm: summary?.firearmCode || null,
    ammo: summary?.ammoSummary || null,
    site: job.site,
    python: robotState.python,
    script: robotState.script,
    args: scriptArgs
  });

  forwardRobotEvent(job, { status: 'accepted', stage: job.stage, message: 'job accepted' });

  let stdoutBuffer = '';
  proc.stdout.setEncoding('utf8');
  proc.stdout.on('data', chunk => {
    stdoutBuffer += String(chunk || '');
    let idx;
    while ((idx = stdoutBuffer.indexOf('\n')) >= 0){
      const line = stdoutBuffer.slice(0, idx).trim();
      stdoutBuffer = stdoutBuffer.slice(idx + 1);
      if (line) handleRobotStdout(job, line);
    }
  });
  proc.stdout.on('close', () => {
    const remaining = stdoutBuffer.trim();
    if (remaining) handleRobotStdout(job, remaining);
    stdoutBuffer = '';
  });

  proc.stderr.setEncoding('utf8');
  proc.stderr.on('data', chunk => {
    const text = String(chunk || '');
    job.logs.push({ type: 'stderr', text, at: timeNow() });
  });

  proc.on('error', err => {
    job.logs.push({ type: 'error', message: err.message || String(err), at: timeNow() });
    finalizeRobotJob(job, 'failed', { message: err.message || '프로세스 오류', error: err.message || String(err) });
  });

  proc.on('close', code => {
    if (job.finishedAt) return;
    const message = code === 0 ? '모의 스크립트 종료' : `스크립트 종료 코드 ${code}`;
    finalizeRobotJob(job, code === 0 ? 'succeeded' : 'failed', {
      message,
      meta: { exitCode: code }
    });
  });

  return job;
}

function waitForRobotJob(job, { timeoutMs = 120000 } = {}) {
  if (!job) {
    return Promise.reject(new Error('job_missing'));
  }
  if (job.finishedAt) {
    return Promise.resolve(sanitizeRobotJob(job, { includePayload: true }));
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      const snapshot = sanitizeRobotJob(job, { includePayload: true });
      reject(Object.assign(new Error('robot_timeout'), { job: snapshot }));
    }, timeoutMs);
    const resolver = resolve;
    job.waiters.push({
      resolve(value) {
        clearTimeout(timer);
        resolver(value);
      }
    });
  });
}


function manualIdentifyActive(){
  if (!manualIdentifyRequested) return false;
  if (manualIdentifyDeadline && timeNow() > manualIdentifyDeadline){
    stopManualIdentify('timeout', { turnOffLed: true });
    return false;
  }
  return true;
}

function shouldIdentify(){
  const active = AUTO_IDENTIFY || manualIdentifyActive();
  if (!active) return false;
  if (activeCommand && !activeCommand.done) return false;
  return true;
}

function startManualIdentify(options = {}){
  if (manualSession && manualSession.active){
    stopManualIdentify('replaced', { turnOffLed: false });
  }
  manualSessionCounter += 1;
  const startAt = timeNow();
  const timeoutMs = Math.max(3000, Number(options.timeoutMs) || 60000);
  const ledOn = options.led === false ? null : (normalizeLedCommand(options.led) || DEFAULT_LED_ON);
  const ledOff = options.ledOff === false ? null : (normalizeLedCommand(options.ledOff || options.onStopLed) || DEFAULT_LED_OFF);

  manualSession = {
    id: manualSessionCounter,
    requestedAt: startAt,
    deadline: startAt + timeoutMs,
    options: { ledOn, ledOff, site: options.site || null },
    active: true,
    reason: null
  };

  manualIdentifyRequested = true;
  manualIdentifyDeadline = manualSession.deadline;

  if (ledOn) applyLedCommand(ledOn);

  return manualSession;
}

function stopManualIdentify(reason = 'manual_stop', { turnOffLed = true, ledOverride = null } = {}){
  if (manualSession){
    manualSession.active = false;
    manualSession.reason = reason;
    manualSession.stoppedAt = timeNow();
  }
  manualIdentifyRequested = false;
  manualIdentifyDeadline = 0;

  const target = ledOverride ? normalizeLedCommand(ledOverride) : (manualSession?.options?.ledOff || DEFAULT_LED_OFF);
  if (turnOffLed && target){
    applyLedCommand(target);
  }
  return manualSession;
}


function waitForIdentifyResult(timeoutMs){
  return new Promise(resolve => {
    let done = false;
    const timer = setTimeout(() => {
      if (!done){
        done = true;
        cleanup();
        resolve(null);
      }
    }, timeoutMs);

    const handler = (obj) => {
      if (done) return;
      const ok = obj && obj.ok === true && obj.type === 'identify';
      const err = obj && obj.ok === false && (
        obj.error === 'timeout_or_no_finger' ||
        obj.error === 'image2tz_failed' ||
        obj.error === 'search_error' ||
        obj.error === 'no_match'
      );
      if (ok || err){
        done = true;
        clearTimeout(timer);
        cleanup();
        resolve(obj);
      }
    };

    const cleanup = () => parser?.off('dataLine', handler);
    parser?.on('dataLine', handler);
  });
}

async function identifyLoop(){
  if (identifyLoopRunning) return;
  identifyLoopRunning = true;
  while (!closedByUs){
    try {
      if (!serial || !serial.isOpen){
        await sleep(250);
        continue;
      }
      if (!shouldIdentify()){
        await sleep(120);
        continue;
      }
      const wrote = writeSerial({ cmd: 'identify' });
      if (!wrote){
        await sleep(400);
        continue;
      }
      await waitForIdentifyResult(9000);
      await sleep(IDENTIFY_BACKOFF_MS);
    } catch (err) {
      warn('identify loop error:', err.message || err);
      await sleep(600);
    }
  }
  identifyLoopRunning = false;
}


async function openAndWire(){
  const { port, lineParser } = await findAndOpen();
  serial = port;
  parser = lineParser;
  lastGoodPath = port.path;

  try { serial.write('{"cmd":"open"}\n'); } catch (err) { warn('write open failed:', err.message || err); }

  parser.on('data', raw => {
    const line = String(raw || '').trim();
    if (!line) return;
    let obj;
    try { obj = JSON.parse(line); }
    catch { obj = { raw: line }; }
    parser.emit('dataLine', obj);
    lastSerialEventAt = timeNow();

    if (obj && obj.type === 'identify'){
      lastIdentifyEvent = { ...obj };
      lastIdentifyAt = timeNow();
      if (obj.ok) stopManualIdentify('matched', { turnOffLed: true });
    }
    if (obj && obj.type === 'led'){
      ledState.mode = obj.mode || obj.state || ledState.mode;
      if (obj.color) ledState.color = obj.color;
      if (obj.speed !== undefined) ledState.speed = obj.speed;
      if (obj.cycles !== undefined) ledState.cycles = obj.cycles;
      ledState.ok = obj.ok !== false;
      ledState.pending = false;
      ledState.lastCommandAt = timeNow();
    }
    if (obj && obj.error === 'led_failed'){
      ledState.ok = false;
      ledState.pending = false;
      ledState.lastCommandAt = timeNow();
    }

    wsBroadcast(obj);
    forwardToRender(obj);
    if (obj.type === 'identify' || obj.error) {
      log('sensor:', JSON.stringify(obj));
    }
  });

  serial.on('close', () => {
    if (closedByUs) return;
    warn('serial closed; reconnecting...');
    reconnect();
  });
  serial.on('error', err => {
    warn('serial error:', err.message || err);
    try { serial.close(); } catch (_) {}
  });
}

async function reconnect(){
  if (reconnecting) return;
  reconnecting = true;
  try {
    await sleep(1000);
    await openAndWire();
  } catch (err) {
    warn('reconnect fail:', err.message || err);
    reconnecting = false;
    await sleep(1500);
    return reconnect();
  }
  reconnecting = false;
}

function buildHealthPayload(){
  const manualActive = manualIdentifyActive();
  return {
    ok: true,
    time: timeNow(),
    serial: {
      connected: !!(serial && serial.isOpen),
      path: (serial && serial.path) || lastGoodPath || null,
      lastEventAt: lastSerialEventAt || null
    },
    identify: {
      auto: AUTO_IDENTIFY,
      running: identifyLoopRunning,
      manual: manualSession ? {
        active: manualActive,
        id: manualSession.id,
        requestedAt: manualSession.requestedAt,
        deadline: manualSession.deadline,
        reason: manualSession.reason || null
      } : { active: false },
      last: lastIdentifyEvent ? { ...lastIdentifyEvent, at: lastIdentifyAt } : null
    },
    led: { ...ledState },
    forward: { ...forwardStatus },
    command: activeCommand
      ? {
          active: true,
          type: activeCommand.type,
          startedAt: activeCommand.startedAt,
          meta: cleanObject({ ...(activeCommand.meta || {}) }) || null,
          result: activeCommand.result ? cleanObject({ ...(activeCommand.result || {}) }) : null,
          error: activeCommand.error || null
        }
      : { active: false },
    robot: {
      enabled: !!robotState.enabled,
      python: robotState.python || null,
      script: robotState.script || null,
      scriptExists: !!robotState.scriptExists,
      active: sanitizeRobotJob(robotState.active, { includePayload: false }),
      last: robotState.last || null,
      history: robotState.history || [],
      lastEventAt: robotState.lastEventAt || null,
      forward: { ...robotForwardStatus }
    }
  };
}

log('env:', {
  PORT_HINT,
  BAUD,
  AUTO_IDENTIFY,
  IDENTIFY_BACKOFF_MS,
  BACKEND_WS_URL: BACKEND_WS_URL ? '[set]' : '',
  FP_SITE,
  DEBUG_WS,
  DEBUG_WS_PORT,
  ROBOT_SCRIPT: robotState.script || '',
  PYTHON_BIN,
  ROBOT_ENABLED: robotState.enabled
});

setupDebugWS();
connectToBackend();
identifyLoop().catch(err => warn('identify loop exited:', err?.message || err));

(async () => {
  try {
    await openAndWire();
  } catch (err) {
    warn('initial open failed:', err.message || err);
    await reconnect();
  }

  process.on('SIGINT', async () => {
    closedByUs = true;
    try { serial?.isOpen && serial.close(); } catch (_) {}
    process.exit(0);
  });
})();
