// Prueba automatizada de la página con Chromium headless + CDP.
// Uso: node tools/test-cdp.js [url]
const { spawn } = require('child_process');
const fs = require('fs');
const WebSocket = require('/home/gam/Descargas/Juegitopro/node_modules/ws');

const URL_PAGINA = process.argv[2] || 'http://localhost:3000';
const PORT = 9333 + (process.pid % 500);
const proc = spawn('/usr/bin/chromium', [
  '--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
  '--remote-debugging-port=' + PORT,
  '--user-data-dir=/tmp/juegitopro-cdp-' + Date.now(),
  '--window-size=1280,900',
  'about:blank'
], { stdio: ['ignore', 'pipe', 'pipe'] });

let wsUrl = null;
function buscarWs(chunk) {
  if (wsUrl) return;
  const m = chunk.toString().match(/DevTools listening on (ws:\/\/[^\s]+)/);
  if (m) wsUrl = m[1];
}
proc.stdout.on('data', buscarWs);
proc.stderr.on('data', buscarWs);

let ws, sessionId = null, nextId = 1;
const pending = new Map();
const logs = [], errors = [];

function send(method, params = {}, useSession = true) {
  const id = nextId++;
  const payload = { id, method, params };
  if (useSession && sessionId) payload.sessionId = sessionId;
  ws.send(JSON.stringify(payload));
  return new Promise(res => pending.set(id, res));
}

async function evaluate(expr) {
  const r = await send('Runtime.evaluate', { expression: expr, returnByValue: true, awaitPromise: true });
  if (r && r.exceptionDetails) {
    const d = r.exceptionDetails;
    return { error: (d.exception && d.exception.description) || d.text };
  }
  return { value: r && r.result ? r.result.value : undefined };
}

async function esperar(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  for (let i = 0; i < 80 && !wsUrl; i++) await esperar(250);
  if (!wsUrl) throw new Error('No se pudo obtener la URL de CDP');

  ws = new WebSocket(wsUrl, { maxPayload: 64 * 1024 * 1024 });
  await new Promise((res, rej) => { ws.on('open', res); ws.on('error', rej); });

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (m.id && pending.has(m.id)) { pending.get(m.id)(m.result); pending.delete(m.id); return; }
    if (m.method === 'Runtime.consoleAPICalled') {
      const txt = m.params.args.map(a => a.value !== undefined ? a.value : (a.description || a.type)).join(' ');
      logs.push('[' + m.params.type + '] ' + txt);
    }
    if (m.method === 'Runtime.exceptionThrown') {
      const d = m.params.exceptionDetails;
      errors.push((d.exception && d.exception.description) || d.text);
    }
  });

  const targets = await send('Target.getTargets', {}, false);
  const tlist = (targets && targets.result && targets.result.targetInfos) ? targets.result.targetInfos : ((targets && targets.targets) ? targets.targets : []);
  const page = tlist.find(t => t.type === 'page');
  if (!page) throw new Error('No hay target de página');
  const att = await send('Target.attachToTarget', { targetId: page.targetId, flatten: true }, false);
  sessionId = att.sessionId;

  await send('Runtime.enable');
  await send('Page.enable');
  await send('Log.enable');

  await send('Page.navigate', { url: URL_PAGINA });
  await esperar(4000);

  console.log('=== CONSOLA ===');
  logs.forEach(l => console.log(l));
  console.log('=== ERRORES JS ===');
  errors.forEach(e => console.log(e));
  if (!errors.length) console.log('(ninguno)');

  const st = await evaluate(`(() => ({
    url: location.href,
    hasCanvas: !!document.getElementById('gameCanvas'),
    hasROSTER: typeof ROSTER !== 'undefined',
    hasAnimate: typeof animate === 'function',
    gameRunning: typeof gameRunning !== 'undefined' ? gameRunning : null,
    FIXED_DT: typeof FIXED_DT !== 'undefined' ? FIXED_DT : null,
    MAX_STEPS: typeof MAX_STEPS_PER_FRAME !== 'undefined' ? MAX_STEPS_PER_FRAME : null,
    simTicks: typeof simTickCounter !== 'undefined' ? simTickCounter : null
  }))()`);
  console.log('=== ESTADO INICIAL ===');
  console.log(JSON.stringify(st.value || st, null, 2));

  fs.writeFileSync('/tmp/juegitopro-test/out.json', JSON.stringify({ logs, errors, state: st }, null, 2));
  ws.close();
  proc.kill();
  process.exit(0);
}

main().catch(e => { console.error('FALLO:', e.message); proc.kill(); process.exit(1); });
