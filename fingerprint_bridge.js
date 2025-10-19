/**
 * fingerprint_bridge.js  — POST/GET(Rendar) 아키텍처용 로컬 브릿지
 *
 * npm i serialport @serialport/parser-readline ws
 * node fingerprint_bridge.js
 *
 * ENV:
 *   FINGERPRINT_PORT=auto
 *   FINGERPRINT_BAUD=115200
 *   AUTO_IDENTIFY=1                # identify 자동 루프 (기본 1)
 *   IDENTIFY_BACKOFF_MS=300
 *   RENDER_FP_URL=https://<render>/api/fp/event
 *   RENDER_FP_TOKEN=<Render FP_SITE_TOKEN>
 *   FP_SITE=site-01
 *   DEBUG_WS=1                     # (옵션) ws://localhost:8787
 */
require('dotenv').config({ path: require('path').join(__dirname, '.env') });

const { SerialPort } = require('serialport');
const { ReadlineParser } = require('@serialport/parser-readline');
const WebSocket = require('ws');

const PORT_HINT     = process.env.FINGERPRINT_PORT || 'auto';
const BAUD          = Number(process.env.FINGERPRINT_BAUD || 115200);
const AUTO_IDENTIFY = (process.env.AUTO_IDENTIFY || '1') === '1';
const IDENTIFY_BACKOFF_MS = Number(process.env.IDENTIFY_BACKOFF_MS || 300);

const FORWARD_URL   = process.env.RENDER_FP_URL || '';
const FORWARD_TOKEN = process.env.RENDER_FP_TOKEN || '';
const FP_SITE       = process.env.FP_SITE || 'default';

const DEBUG_WS      = (process.env.DEBUG_WS || '') === '1';
const DEBUG_WS_PORT = Number(process.env.DEBUG_WS_PORT || 8787);

function log(...a){ console.log('[fp-bridge]', ...a); }
function warn(...a){ console.warn('[fp-bridge]', ...a); }
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));

let serial=null, parser=null, lastGoodPath=null, reconnecting=false, identifyRunning=false, closedByUs=false;

// ----- Serial discovery -----
async function listCandidates() {
  const ports = await SerialPort.list();
  const pref=[], normal=[];
  for (const p of ports) {
    const m=(p.manufacturer||'').toLowerCase();
    const v=(p.vendorId||'').toLowerCase();
    const usb = m.includes('arduino')||m.includes('silicon labs')||m.includes('wch')||m.includes('ftdi')||
                v==='2341'||v==='1a86'||v==='10c4'||v==='0403';
    (usb?pref:normal).push(p);
  }
  const all=[];
  if (lastGoodPath){
    const idx=[...pref,...normal].findIndex(x=>x.path===lastGoodPath);
    if (idx>=0) all.push([...pref,...normal][idx]);
  }
  for (const p of [...pref,...normal]) if (!all.find(x=>x.path===p.path)) all.push(p);
  return all;
}
async function tryOpen(path){
  const port=new SerialPort({path, baudRate:BAUD, autoOpen:false});
  const lineParser=port.pipe(new ReadlineParser({delimiter:'\n'}));
  await new Promise((res,rej)=>port.open(err=>err?rej(err):res()));
  log(`opened serial ${path}@${BAUD}`);
  return {port,lineParser};
}
async function findAndOpen(){
  if (PORT_HINT!=='auto'){ try{return await tryOpen(PORT_HINT);}catch(e){warn('explicit port failed:',e.message||e);} }
  const cands=await listCandidates();
  for (const c of cands){ try{ return await tryOpen(c.path);}catch{} }
  throw new Error('no usable serial port found');
}

// ----- Forward to Render -----
async function forwardToRender(obj) {
  if (!FORWARD_URL || !FORWARD_TOKEN) {
    console.warn('[fp-bridge] forward disabled: missing RENDER_FP_URL or RENDER_FP_TOKEN');
    return;
  }
  try {
    const resp = await fetch(FORWARD_URL, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-fp-token': FORWARD_TOKEN
      },
      body: JSON.stringify({ site: FP_SITE, data: obj })
    });
    if (!resp.ok) {
      console.warn('[fp-bridge] forward failed:', resp.status, resp.statusText);
      const text = await resp.text().catch(()=>null);
      if (text) console.warn('[fp-bridge] response:', text);
    } else {
      console.log('[fp-bridge] forwarded ok');
    }
  } catch (e) {
    console.warn('[fp-bridge] forward error:', e.message || e);
  }
}

// ----- Debug WS (optional) -----
let wsServer=null;
function setupDebugWS(){
  if(!DEBUG_WS) return;
  wsServer=new WebSocket.Server({port:DEBUG_WS_PORT});
  wsServer.on('connection',ws=>{
    ws.send(JSON.stringify({hello:'fp-bridge',version:'post-get-1.0'}));
    ws.on('message',buf=>{
      let obj; try{ obj=JSON.parse(String(buf)); }catch{return;}
      if(serial?.isOpen) serial.write(JSON.stringify(obj)+'\n');
    });
  });
  log(`debug ws on ws://localhost:${DEBUG_WS_PORT}`);
}
function wsBroadcast(obj){
  if(!wsServer) return;
  const s=JSON.stringify(obj);
  wsServer.clients.forEach(ws=>{ if(ws.readyState===WebSocket.OPEN) ws.send(s); });
}

// ----- Identify loop (hands-free kiosk) -----
function waitForIdentifyResult(timeoutMs){
  return new Promise(resolve=>{
    let done=false;
    const timer=setTimeout(()=>{ if(!done){ done=true; off(); resolve(null);} }, timeoutMs);
    const onLine=(o)=>{
      if(done) return;
      const ok = o && o.ok===true && o.type==='identify';
      const err= o && o.ok===false && (
        o.error==='timeout_or_no_finger' || o.error==='image2tz_failed' ||
        o.error==='search_error' || o.error==='no_match'
      );
      if(ok||err){ done=true; clearTimeout(timer); off(); resolve(o); }
    };
    const off=()=>parser?.off('dataLine',onLine);
    parser?.on('dataLine',onLine);
  });
}
async function identifyLoop(){
  if(!AUTO_IDENTIFY || identifyRunning) return;
  identifyRunning=true;
  try{
    while(serial?.isOpen){
      try{ serial.write('{"cmd":"identify"}\n'); }catch(e){ warn('write identify failed:',e.message||e); break; }
      await waitForIdentifyResult(9000); // 센서 타임아웃 고려
      await sleep(IDENTIFY_BACKOFF_MS);
    }
  }finally{ identifyRunning=false; }
}

function mask(t){ if(!t) return ''; return t.length<=6 ? '******' : t.slice(0,3)+'***'+t.slice(-3); }
log('env:',
  { PORT_HINT, BAUD, AUTO_IDENTIFY, IDENTIFY_BACKOFF_MS,
    FORWARD_URL, FORWARD_TOKEN: mask(FORWARD_TOKEN), FP_SITE,
    DEBUG_WS, DEBUG_WS_PORT });

// ----- Wire & Reconnect -----
async function openAndWire(){
  const {port,lineParser}=await findAndOpen();
  serial=port; parser=lineParser; lastGoodPath=port.path;

  try{ serial.write('{"cmd":"open"}\n'); }catch{}
  parser.on('data',raw=>{
    const line=String(raw||'').trim(); if(!line) return;
    let obj; try{ obj=JSON.parse(line); }catch{ obj={raw:line}; }
    parser.emit('dataLine',obj);
    wsBroadcast(obj);
    forwardToRender(obj);
    if(obj.type==='identify'||obj.error) log('sensor:', JSON.stringify(obj));
  });
  serial.on('close',()=>{ if(closedByUs) return; warn('serial closed; reconnecting...'); reconnect(); });
  serial.on('error',e=>{ warn('serial error:',e.message||e); try{serial.close();}catch{} });

  if(AUTO_IDENTIFY) identifyLoop().catch(()=>{});
}
async function reconnect(){
  if(reconnecting) return; reconnecting=true;
  try{ await sleep(1000); await openAndWire(); }
  catch(e){ warn('reconnect fail:',e.message||e); await sleep(1500); reconnecting=false; return reconnect(); }
  reconnecting=false;
}

// ----- Start -----
(async()=>{
  setupDebugWS();
  try{ await openAndWire(); }
  catch(e){ warn('initial open failed:',e.message||e); await reconnect(); }

  process.on('SIGINT',async()=>{ closedByUs=true; try{ serial?.isOpen && serial.close(); }catch{} process.exit(0); });
})();
