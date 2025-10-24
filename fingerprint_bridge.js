/**
 * fingerprint_bridge.js  — Render 서버 ↔ 로컬 장비 브릿지
 *
 * 기능 요약
 *  - 시리얼 포트를 통해 아두이노 지문 센서를 제어하고 결과를 Render 서버로 전달
 *  - HTTP /health, /identify/start, /identify/stop, /led 엔드포인트를 제공하여
 *    TAB 단말에서 로컬 브릿지 상태 확인 및 지문 인식 세션을 온디맨드로 시작/종료
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
const http = require('http');
const { URL } = require('url');

const PORT_HINT      = process.env.FINGERPRINT_PORT || 'auto';
const BAUD           = Number(process.env.FINGERPRINT_BAUD || 115200);
const AUTO_IDENTIFY  = (process.env.AUTO_IDENTIFY || '0') === '1';
const IDENTIFY_BACKOFF_MS = Number(process.env.IDENTIFY_BACKOFF_MS || 300);

const FORWARD_URL    = process.env.RENDER_FP_URL || '';
const FORWARD_TOKEN  = process.env.RENDER_FP_TOKEN || '';
const FP_SITE        = process.env.FP_SITE || 'default';

const LOCAL_PORT     = Number(process.env.LOCAL_PORT || process.env.FP_LOCAL_PORT || 8790);

const DEBUG_WS       = (process.env.DEBUG_WS || '') === '1';
const DEBUG_WS_PORT  = Number(process.env.DEBUG_WS_PORT || 8787);

const DEFAULT_LED_ON = { mode: 'breathing', color: 'blue', speed: 18 };
const DEFAULT_LED_OFF = { mode: 'off' };

const PYTHON_BIN = process.env.PYTHON_BIN || (process.platform === 'win32' ? 'python' : 'python3');
const ROBOT_SCRIPT = process.env.ROBOT_SCRIPT || path.join(__dirname, 'robot_simulator.py');
const ROBOT_DISABLED = (process.env.ROBOT_DISABLED || '') === '1';
const ROBOT_FORWARD_URL = process.env.ROBOT_FORWARD_URL || process.env.RENDER_ROBOT_URL || FORWARD_URL || '';
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

const forwardStatus = { enabled: !!FORWARD_URL, lastOkAt: 0, lastErrorAt: 0 };
const robotForwardStatus = { enabled: !!ROBOT_FORWARD_URL, lastOkAt: 0, lastErrorAt: 0 };
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

const timeNow = () => Date.now();

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

function summarizeRobotPayload(payload = {}){
  if (!payload || typeof payload !== 'object') return null;

  const dispatch = (payload.dispatch && typeof payload.dispatch === 'object') ? payload.dispatch : {};

  const includesInput = (payload.includes && typeof payload.includes === 'object') ? payload.includes : {};
  const includesDispatch = (dispatch.includes && typeof dispatch.includes === 'object') ? dispatch.includes : {};

  let firearmIncluded = Object.prototype.hasOwnProperty.call(includesInput, 'firearm')
    ? normalizeFlag(includesInput.firearm)
    : Object.prototype.hasOwnProperty.call(includesDispatch, 'firearm')
      ? normalizeFlag(includesDispatch.firearm)
      : undefined;

  if (firearmIncluded === undefined) {
    firearmIncluded = !!(payload.firearm || dispatch.firearm);
  }

  let ammoIncluded = Object.prototype.hasOwnProperty.call(includesInput, 'ammo')
    ? normalizeFlag(includesInput.ammo)
    : Object.prototype.hasOwnProperty.call(includesDispatch, 'ammo')
      ? normalizeFlag(includesDispatch.ammo)
      : undefined;

  if (ammoIncluded === undefined) {
    ammoIncluded = (Array.isArray(payload.ammo) && payload.ammo.length > 0)
      || (Array.isArray(dispatch.ammo) && dispatch.ammo.length > 0);
  }
  
  const payloadItems = Array.isArray(payload.items) ? payload.items : [];
  if (firearmIncluded === undefined) {
    firearmIncluded = payloadItems.some((item) => String(item?.item_type || item?.type || '').toUpperCase() === 'FIREARM');
  }
  if (ammoIncluded === undefined) {
    ammoIncluded = payloadItems.some((item) => String(item?.item_type || item?.type || '').toUpperCase() === 'AMMO');
  }

  const firearmHas = !!firearmIncluded;
  const ammoHas = !!ammoIncluded;

  let action = String(payload.mode || '').toLowerCase();
  if (['firearm_and_ammo', 'firearm_only', 'ammo_only', 'dispatch', 'issue', 'out', '불출'].includes(action)) {
    action = 'dispatch';
  } else if (['return', 'incoming', 'in', '입고', '불입'].includes(action)) {
    action = 'return';
  }
  const typeRaw = String(payload.type || payload.request_type || '').toUpperCase();
  if (!action){
    if (typeRaw === 'RETURN') action = 'return';
    else if (typeRaw === 'DISPATCH' || typeRaw === 'ISSUE') action = 'dispatch';
  }
  if (!action){
    action = firearmHas || ammoHas ? 'dispatch' : 'unknown';
  }

  const firearm = payload.firearm || dispatch.firearm || {};
  const firearmCode = firearm.code
    || firearm.firearm_number
    || firearm.serial
    || firearm.weapon_code
    || payload.weapon_code
    || payload.weaponCode
    || null;

  const locker = firearm.locker
    || firearm.locker_code
    || firearm.lockerCode
    || dispatch.locker
    || dispatch.location
    || payload.locker
    || payload.storage
    || payload.storage_code
    || (payload.request && (payload.request.locker || payload.request.storage_locker))
    || null;

  const ammoItems = Array.isArray(payload.ammo) && payload.ammo.length
    ? payload.ammo
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
    requestId: payload.requestId ?? payload.request_id ?? null,
    action,
    actionLabel: action === 'return' ? '불입' : (action === 'dispatch' ? '불출' : '장비'),
    type: typeRaw || null,
    includes: { firearm: firearmHas, ammo: ammoHas, label: includesLabel },
    firearmCode,
    ammoSummary: hasAmmoSummary ? ammoSummaryParts.join(', ') : null,
    ammoCount: hasAmmoSummary ? ammoCountValue : null,
    locker,
    site: payload.site_id || payload.site || null,
    purpose: payload.purpose || null,
    location: payload.location
      || dispatch.location
      || (payload.request && payload.request.location)
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

async function forwardToRender(obj, { url = FORWARD_URL, token = FORWARD_TOKEN, statusRef = forwardStatus } = {}){
  if (!url) return;
  try {
    const headers = {
      'content-type': 'application/json'
    };
    if (token) {
      headers['x-fp-token'] = token;
      headers['x-robot-token'] = token;
    }
    const res = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify({ site: FP_SITE, data: obj })
    });
    if (!res.ok){
      if (statusRef) statusRef.lastErrorAt = timeNow();
      const text = await res.text().catch(() => '');
      warn('forward failed:', res.status, res.statusText, text || '');
    } else {
      if (statusRef) statusRef.lastOkAt = timeNow();
    }
  } catch (err) {
    if (statusRef) statusRef.lastErrorAt = timeNow();
    warn('forward error:', err.message || err);
  }
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

function getForwardConfig(job){
  if (job?.forward && job.forward.url) {
    robotForwardStatus.enabled = true;
    return job.forward;
  }
  if (ROBOT_FORWARD_URL) {
    return { url: ROBOT_FORWARD_URL, token: ROBOT_FORWARD_TOKEN || null };
  }
  return null;
}

function forwardRobotEvent(job, update = {}){
  const forward = getForwardConfig(job);
  if (!forward || !forward.url) return;
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
  forwardToRender(payload, { url: forward.url, token: forward.token || null, statusRef: robotForwardStatus });
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
    waiters: []
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
  const proc = spawn(robotState.python || PYTHON_BIN, [robotState.script], { env });
  if (proc.stdin && typeof proc.stdin.setDefaultEncoding === 'function') {
    proc.stdin.setDefaultEncoding('utf8');
  }
  job.process = proc;
  job.startedAt = timeNow();
  job.status = 'running';
  robotState.active = job;
  robotState.lastEventAt = job.startedAt;

  log('robot job accepted', {
    jobId,
    requestId: job.requestId,
    action: summary?.actionLabel || summary?.action || payload.mode || payload.type || 'unknown',
    includes: summary?.includes || null,
    firearm: summary?.firearmCode || null,
    ammo: summary?.ammoSummary || null,
    site: job.site,
    python: robotState.python,
    script: robotState.script
  });

  forwardRobotEvent(job, { status: 'accepted', stage: job.stage, message: 'job accepted' });

  const input = JSON.stringify({
    ...payload,
    jobId,
    site,
    requestedAt: payload.requestedAt || job.createdAt
  });

  try {
    proc.stdin.write(input);
    proc.stdin.end();
  } catch (err) {
    try { proc.kill(); } catch (_) {}
    robotState.active = null;
    const error = new Error('robot_spawn_failed');
    error.cause = err;
    error.statusCode = 500;
    throw error;
  }

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

function applyCors(req, res){
  const origin = req?.headers?.origin;
  const varyParts = new Set();
  if (origin){
    res.setHeader('Access-Control-Allow-Origin', origin);
    varyParts.add('Origin');
  } else {
    res.setHeader('Access-Control-Allow-Origin', '*');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');

  const requestedHeaders = req?.headers?.['access-control-request-headers'];
  if (requestedHeaders){
    res.setHeader('Access-Control-Allow-Headers', requestedHeaders);
    varyParts.add('Access-Control-Request-Headers');
  } else {
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  }

  if (varyParts.size){
    res.setHeader('Vary', Array.from(varyParts).join(', '));
  }

  res.setHeader('Access-Control-Allow-Private-Network', 'true');
}

function sendJson(req, res, status, payload){
  applyCors(req, res);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload ?? {}));
}

async function readJson(req){
  const chunks = [];
  for await (const chunk of req){ chunks.push(chunk); }
  if (!chunks.length) return {};
  const raw = Buffer.concat(chunks).toString('utf8').trim();
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch (err) {
    const error = new Error('invalid_json');
    error.statusCode = 400;
    error.message = err.message || 'invalid_json';
    throw error;
  }
}

async function handleHttpRequest(req, res){
  applyCors(req, res);
  if (req.method === 'OPTIONS'){
    res.writeHead(204);
    res.end();
    return;
  }

  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  const pathname = url.pathname;

  try {
    if (req.method === 'GET' && pathname === '/health'){
      return sendJson(req, res, 200, buildHealthPayload());
    }

    if (req.method === 'POST' && pathname === '/identify/start'){
      const body = await readJson(req);
      const session = startManualIdentify({
        timeoutMs: body?.timeoutMs,
        led: body?.led,
        ledOff: body?.ledOff || body?.onStopLed,
        site: body?.site
      });
      identifyLoop();
      return sendJson(req, res, 200, {
        ok: true,
        session: {
          id: session.id,
          requestedAt: session.requestedAt,
          deadline: session.deadline
        },
        serial: {
          connected: !!(serial && serial.isOpen),
          path: (serial && serial.path) || lastGoodPath || null
        },
        led: { ...ledState }
      });
    }

    if (req.method === 'POST' && pathname === '/identify/stop'){
      const body = await readJson(req);
      const turnOffLed = body?.led === false ? false : true;
      const ledOverride = (turnOffLed && body && typeof body.led === 'object') ? body.led : null;
      const session = stopManualIdentify(body?.reason || 'manual_stop', { turnOffLed, ledOverride });
      return sendJson(req, res, 200, {
        ok: true,
        session: session ? {
          id: session.id,
          active: session.active,
          reason: session.reason || null,
          stoppedAt: session.stoppedAt || null
        } : null,
        led: { ...ledState }
      });
    }

    if (req.method === 'POST' && pathname === '/led'){
      const body = await readJson(req);
      const ok = applyLedCommand(body);
      return sendJson(req, res, ok ? 200 : 503, {
        ok,
        led: { ...ledState },
        serial: { connected: !!(serial && serial.isOpen) }
      });
    }
    if (req.method === 'POST' && pathname === '/enroll'){
      const body = await readJson(req);
      const id = Number.parseInt(body?.id, 10);
      if (!Number.isInteger(id) || id <= 0){
        throw httpError(400, 'missing_or_bad_id');
      }
      const timeoutMs = Math.max(10000, Number(body?.timeoutMs) || 45000);
      const entry = beginCommand('enroll', { id });
      const ledOnRaw = body?.led === false ? null : (normalizeLedCommand(body?.led) || DEFAULT_LED_ON);
      const ledOffRaw = body?.ledOff === false ? null : (normalizeLedCommand(body?.ledOff || body?.onStopLed) || DEFAULT_LED_OFF);
      const ledOnCmd = ledOnRaw ? { ...DEFAULT_LED_ON, ...ledOnRaw } : null;
      const ledOffCmd = ledOffRaw ? { ...DEFAULT_LED_OFF, ...ledOffRaw } : null;
      try {
        stopManualIdentify('command', { turnOffLed: false });
        if (ledOnCmd) applyLedCommand(ledOnCmd);
        const result = await runSensorCommand({
          command: 'enroll',
          payload: { id },
          expectedType: 'enroll',
          timeoutMs,
          onStage: (stage) => {
            entry.meta.stage = stage.stage || null;
            entry.meta.message = stage.msg || stage.message || null;
            entry.meta.updatedAt = timeNow();
          }
        });
        if (ledOffCmd) applyLedCommand(ledOffCmd);
        finishCommand(entry, { result });
        return sendJson(req, res, 200, { ok: true, result });
      } catch (err) {
        if (ledOffCmd) applyLedCommand(ledOffCmd);
        finishCommand(entry, { error: err });
        throw err;
      }
    }
    if (req.method === 'POST' && pathname === '/delete'){
      const body = await readJson(req);
      const id = Number.parseInt(body?.id, 10);
      if (!Number.isInteger(id) || id <= 0){
        throw httpError(400, 'missing_or_bad_id');
      }
      const allowMissing = body?.allowMissing === true || body?.allow_missing === true;
      const timeoutMs = Math.max(6000, Number(body?.timeoutMs) || 15000);
      const entry = beginCommand('delete', { id });
      try {
        stopManualIdentify('command', { turnOffLed: false });
        const result = await runSensorCommand({
          command: 'delete',
          payload: { id },
          expectedType: 'delete',
          timeoutMs
        });
        finishCommand(entry, { result });
        return sendJson(req, res, 200, { ok: true, result });
      } catch (err) {
        finishCommand(entry, { error: err });
        if (allowMissing && err?.payload?.error === 'delete_failed'){
          return sendJson(req, res, 200, { ok: true, skipped: true, reason: 'not_found' });
        }
        throw err;
      }
    }
    if (req.method === 'POST' && pathname === '/clear'){
      const body = await readJson(req);
      const timeoutMs = Math.max(8000, Number(body?.timeoutMs) || 20000);
      const entry = beginCommand('clear');
      try {
        stopManualIdentify('command', { turnOffLed: false });
        const result = await runSensorCommand({
          command: 'clear',
          expectedType: 'clear',
          timeoutMs
        });
        finishCommand(entry, { result });
        return sendJson(req, res, 200, { ok: true, result });
      } catch (err) {
        finishCommand(entry, { error: err });
        throw err;
      }
    }
    if (req.method === 'GET' && pathname === '/count'){
      const timeoutParam = url.searchParams.get('timeoutMs') || url.searchParams.get('timeout_ms');
      const timeoutMs = Math.max(5000, Number(timeoutParam) || 10000);
      const entry = beginCommand('count');
      try {
        stopManualIdentify('command', { turnOffLed: false });
        const result = await runSensorCommand({
          command: 'count',
          expectedType: 'count',
          timeoutMs
        });
        finishCommand(entry, { result });
        return sendJson(req, res, 200, { ok: true, result });
      } catch (err) {
        finishCommand(entry, { error: err });
        throw err;
      }
    }
    if (req.method === 'POST' && pathname === '/robot/execute'){
      const body = await readJson(req);
      const job = startRobotJob(body || {});
      try {
        const result = await waitForRobotJob(job, {
          timeoutMs: Number(body?.timeoutMs || body?.timeout_ms || 0) || 120000
        });
        const status = String(result?.status || '').toLowerCase();
        const success = status === 'succeeded';
        return sendJson(req, res, success ? 200 : 500, {
          ok: success,
          job: result,
          robot: {
            enabled: robotState.enabled,
            script: robotState.script,
            python: robotState.python
          }
        });
      } catch (err) {
        const statusCode = err?.statusCode || 504;
        const snapshot = err?.job || sanitizeRobotJob(job, { includePayload: true });
        return sendJson(req, res, statusCode, {
          ok: false,
          error: err?.message || 'robot_failed',
          job: snapshot,
          robot: {
            enabled: robotState.enabled,
            script: robotState.script,
            python: robotState.python
          }
        });
      }
    }
    return sendJson(req, res, 404, { ok: false, error: 'not_found' });
  } catch (err) {
    const status = err?.statusCode || 500;
    return sendJson(req, res, status, { ok: false, error: err.message || 'server_error' });
  }
}

function startHttpServer(){
  const server = http.createServer(handleHttpRequest);
  server.listen(LOCAL_PORT, '0.0.0.0', () => {
    log(`local HTTP bridge listening on http://0.0.0.0:${LOCAL_PORT}`);
  });
  server.on('error', err => warn('http server error:', err.message || err));
}

log('env:', {
  PORT_HINT,
  BAUD,
  AUTO_IDENTIFY,
  IDENTIFY_BACKOFF_MS,
  FORWARD_URL: FORWARD_URL ? '[set]' : '',
  FP_SITE,
  LOCAL_PORT,
  DEBUG_WS,
  DEBUG_WS_PORT,
  ROBOT_SCRIPT: robotState.script || '',
  PYTHON_BIN,
  ROBOT_FORWARD_URL: ROBOT_FORWARD_URL ? '[set]' : '',
  ROBOT_ENABLED: robotState.enabled
});

setupDebugWS();
startHttpServer();
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
