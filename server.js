// ============================================================
//  JuegitoPro — servidor Express + MongoDB (Atlas) + Render
//  Cuentas (registro/login con bcrypt), leaderboard con rangos
//  y salas online (amistosas/ranked) vía WebSocket.
//
//  - Si MONGODB_URI está definida  -> persiste en MongoDB Atlas.
//  - Si no lo está / falla la conexión -> usa un store en MEMORIA
//    para poder probar en local sin crear la base.
// ============================================================
require('dotenv').config();

const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const http = require('http');
const crypto = require('crypto');
const path = require('path');
const bcrypt = require('bcryptjs');
const { WebSocketServer } = require('ws');
const { rateLimit } = require('express-rate-limit');

const app = express();
app.set('trust proxy', 1);

// Usar puerto 3000 para pruebas locales; Render y otros entornos usan variables de entorno (PORT).
const PORT = process.env.PORT || 3000;
const GAME_FILE = path.join(__dirname, 'index.html');

// ---- Constantes de validación ----
const USERNAME_RE = /^[A-Za-z0-9_]{3,16}$/;
const PASSWORD_MIN = 6;
const BCRYPT_COST = 10;
const VALID_RESULTS = new Set(['p1', 'p2', 'draw']);
const VALID_ROOM_MODES = new Set(['friendly', 'ranked']);

app.use(express.json({ limit: '10kb' }));
app.use(cors());

// ---- Utilidades ----
function normalizeUser(username) {
  if (typeof username !== 'string') return null;
  const u = username.trim();
  if (!USERNAME_RE.test(u)) return null;
  return u;
}

function sha256(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function newToken() {
  return crypto.randomBytes(32).toString('hex');
}

// Rango según victorias ranked (escala literal: noob 2, pro 4, hacker 8)
function rankFor(win) {
  const w = win || 0;
  return w >= 8 ? 'hacker' : w >= 4 ? 'pro' : w >= 2 ? 'noob' : 'novato';
}

// Renglón público de un usuario (winrate y rango calculados al leer)
function toPublicUser(d) {
  const total = d.totalRanked || 0;
  const wins = d.rankWins || 0;
  return {
    username: d.username,
    rank: rankFor(wins),
    rankWins: wins,
    rankLosses: d.rankLosses || 0,
    rankDraws: d.rankDraws || 0,
    totalRanked: total,
    winrate: total ? +((100 * wins) / total).toFixed(1) : 0,
    lastMatchAt: d.lastMatchAt,
  };
}

const sortUsers = (rows) =>
  rows.sort(
    (a, b) => b.rankWins - a.rankWins || b.totalRanked - a.totalRanked || a.username.localeCompare(b.username)
  );

// ---- Modelo de Mongoose ----
const userSchema = new mongoose.Schema({
  username:    { type: String, required: true, trim: true, maxlength: 16 },
  usernameKey: { type: String, required: true, unique: true, index: true }, // minúsculas (dedupe)
  passHash:    { type: String, required: true },
  tokenHashes: { type: [String], default: [], index: true },
  rankWins:    { type: Number, default: 0 },
  rankLosses:  { type: Number, default: 0 },
  rankDraws:   { type: Number, default: 0 },
  totalRanked: { type: Number, default: 0 },
  lastMatchAt: { type: Date, default: Date.now },
  createdAt:   { type: Date, default: Date.now },
});
const User = mongoose.model('User', userSchema);

const isDupKey = (err) => err && err.code === 11000;

// ---- Store MongoDB ----
function createMongoStore() {
  return {
    async register({ username, password }) {
      const passHash = await bcrypt.hash(password, BCRYPT_COST);
      try {
        const u = await User.create({
          username,
          usernameKey: username.toLowerCase(),
          passHash,
        });
        return toPublicUser(u.toObject());
      } catch (err) {
        if (isDupKey(err)) {
          const e = new Error('El nombre de usuario ya está en uso.');
          e.status = 409;
          throw e;
        }
        throw err;
      }
    },
    async login({ username, password }) {
      const u = await User.findOne({ usernameKey: username.toLowerCase() }).lean();
      if (!u) return null;
      const ok = await bcrypt.compare(password, u.passHash);
      return ok ? u : null;
    },
    async findUserByTokenHash(tokenHash) {
      const u = await User.findOne({ tokenHashes: tokenHash }).lean();
      return u || null;
    },
    async setToken(usernameKey, tokenHash) {
      await User.updateOne({ usernameKey }, { $push: { tokenHashes: { $each: [tokenHash], $slice: -5 } } });
    },
    async clearToken(usernameKey, tokenHash) {
      if (tokenHash) {
        await User.updateOne({ usernameKey }, { $pull: { tokenHashes: tokenHash } });
      } else {
        await User.updateOne({ usernameKey }, { $set: { tokenHashes: [] } });
      }
    },
    async recordRanked({ p1, p2, result }) {
      const incFor = (kind) =>
        kind === 'draw' ? { rankDraws: 1, totalRanked: 1 }
        : kind === 'win' ? { rankWins: 1, totalRanked: 1 }
        : { rankLosses: 1, totalRanked: 1 };
      const apply = async (name, kind) => {
        await User.updateOne(
          { usernameKey: name.toLowerCase() },
          { $set: { lastMatchAt: new Date() }, $inc: incFor(kind) }
        );
      };
      const kindFor = (side) => (result === 'draw' ? 'draw' : result === side ? 'win' : 'loss');
      await apply(p1, kindFor('p1'));
      await apply(p2, kindFor('p2'));
      const rows = await Promise.all([
        User.findOne({ usernameKey: p1.toLowerCase() }).lean(),
        User.findOne({ usernameKey: p2.toLowerCase() }).lean(),
      ]);
      return rows.map(toPublicUser);
    },
    async getLeaderboard(limit) {
      const docs = await User.find({})
        .sort({ rankWins: -1, totalRanked: -1, username: 1 })
        .limit(limit)
        .lean();
      return docs.map(toPublicUser);
    },
    async getPlayer(username) {
      const u = await User.findOne({ usernameKey: username.toLowerCase() }).lean();
      return u ? toPublicUser(u) : null;
    },
    async health() {
      return mongoose.connection.readyState === 1;
    },
  };
}

// ---- Store en memoria (respaldo, sin base de datos) ----
function createMemStore() {
  const users = new Map(); // usernameKey -> row

  const touch = (username) => {
    const key = username.toLowerCase();
    let row = users.get(key);
    if (!row) {
      row = {
        username,
        usernameKey: key,
        passHash: '',
        tokenHashes: [],
        rankWins: 0,
        rankLosses: 0,
        rankDraws: 0,
        totalRanked: 0,
        lastMatchAt: new Date(),
        createdAt: new Date(),
      };
      users.set(key, row);
    }
    return row;
  };

  return {
    async register({ username, password }) {
      const key = username.toLowerCase();
      if (users.has(key)) {
        const e = new Error('El nombre de usuario ya está en uso.');
        e.status = 409;
        throw e;
      }
      const row = touch(username);
      row.passHash = await bcrypt.hash(password, BCRYPT_COST);
      return toPublicUser(row);
    },
    async login({ username, password }) {
      const row = users.get(username.toLowerCase());
      if (!row) return null;
      const ok = await bcrypt.compare(password, row.passHash);
      return ok ? row : null;
    },
    async findUserByTokenHash(tokenHash) {
      for (const row of users.values()) if (row.tokenHashes && row.tokenHashes.includes(tokenHash)) return row;
      return null;
    },
    async setToken(usernameKey, tokenHash) {
      const row = touch(usernameKey);
      if (!row.tokenHashes) row.tokenHashes = [];
      row.tokenHashes.push(tokenHash);
      if (row.tokenHashes.length > 5) row.tokenHashes = row.tokenHashes.slice(-5);
    },
    async clearToken(usernameKey, tokenHash) {
      const row = users.get(usernameKey.toLowerCase());
      if (row) {
        if (tokenHash && row.tokenHashes) {
          row.tokenHashes = row.tokenHashes.filter(t => t !== tokenHash);
        } else {
          row.tokenHashes = [];
        }
      }
    },
    async recordRanked({ p1, p2, result }) {
      const apply = (name, kind) => {
        const row = touch(name);
        row.lastMatchAt = new Date();
        if (kind === 'draw') row.rankDraws++;
        else if (kind === 'win') row.rankWins++;
        else row.rankLosses++;
        row.totalRanked++;
      };
      apply(p1, result === 'draw' ? 'draw' : result === 'p1' ? 'win' : 'loss');
      apply(p2, result === 'draw' ? 'draw' : result === 'p1' ? 'loss' : 'win');
      return [toPublicUser(users.get(p1.toLowerCase())), toPublicUser(users.get(p2.toLowerCase()))];
    },
    async getLeaderboard(limit) {
      return sortUsers([...users.values()].map(toPublicUser)).slice(0, limit);
    },
    async getPlayer(username) {
      const row = users.get(username.toLowerCase());
      return row ? toPublicUser(row) : null;
    },
    async health() {
      return true;
    },
  };
}

// ---- Elección de store al arrancar ----
let store;

async function configureStore() {
  const uri = process.env.MONGODB_URI;
  if (!uri) {
    store = createMemStore();
    console.warn('[server] MONGODB_URI no definida → usando memoria.');
    return;
  }
  try {
    await mongoose.connect(uri, { serverSelectionTimeoutMS: 5000 });
    store = createMongoStore();
    console.log('[server] Conectado a MongoDB Atlas (persistente).');
  } catch (err) {
    store = createMemStore();
    console.error('[server] Error conectando a MongoDB:', err.message);
    console.warn('   Usando store en memoria hasta que MongoDB esté disponible.');
  }
}

// ---- Middleware de autenticación (Bearer token) ----
async function authRequired(req, res, next) {
  const h = req.headers.authorization || '';
  if (!h.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Token requerido.' });
  }
  const tokenHash = sha256(h.slice(7));
  const user = await store.findUserByTokenHash(tokenHash);
  if (!user) return res.status(401).json({ error: 'Sesión inválida o expirada.' });
  req.user = toPublicUser(user);
  req.usernameKey = user.usernameKey;
  req.tokenHash = tokenHash;
  next();
}

// ---- Rutas HTTP ----
const authLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 8,
  message: { error: 'Demasiados intentos. Intenta de nuevo en un momento.' },
});

async function startServer() {
  await configureStore();

  app.get('/health', async (req, res) => res.json({ status: 'ok', db: await store.health() }));

  // 1) Registro
  app.post('/api/auth/register', authLimiter, async (req, res) => {
    const body = req.body || {};
    const username = normalizeUser(body.username);
    const password = typeof body.password === 'string' ? body.password : '';
    if (!username) {
      return res.status(400).json({ error: 'Usuario inválido: 3-16 caracteres (letras, números o _).' });
    }
    if (password.length < PASSWORD_MIN) {
      return res.status(400).json({ error: 'La contraseña debe tener al menos ' + PASSWORD_MIN + ' caracteres.' });
    }
    try {
      await store.register({ username, password });
      const token = newToken();
      await store.setToken(username.toLowerCase(), sha256(token));
      const user = await store.getPlayer(username);
      res.status(201).json({ token, user });
    } catch (err) {
      if (err.status === 409) return res.status(409).json({ error: err.message });
      console.error('[api] Error registrando:', err);
      res.status(500).json({ error: 'No se pudo registrar.' });
    }
  });

  // 2) Login
  app.post('/api/auth/login', authLimiter, async (req, res) => {
    const body = req.body || {};
    const username = normalizeUser(body.username);
    const password = typeof body.password === 'string' ? body.password : '';
    if (!username || !password) {
      return res.status(400).json({ error: 'Usuario y contraseña requeridos.' });
    }
    const user = await store.login({ username, password });
    if (!user) return res.status(401).json({ error: 'Usuario o contraseña incorrectos.' });
    const token = newToken();
    await store.setToken(user.usernameKey, sha256(token));
    res.json({ token, user: toPublicUser(user) });
  });

  // 3) Sesión actual
  app.get('/api/auth/me', authRequired, (req, res) => res.json({ user: req.user }));

  // 4) Cerrar sesión
  app.post('/api/auth/logout', authRequired, async (req, res) => {
    await store.clearToken(req.usernameKey, req.tokenHash);
    res.json({ ok: true });
  });

  // 5) Ranking (público)
  app.get('/api/leaderboard', async (req, res) => {
    try {
      const limit = Math.min(100, Math.max(1, parseInt(req.query.limit, 10) || 20));
      res.json(await store.getLeaderboard(limit));
    } catch (err) {
      console.error('[api] Error leyendo ranking:', err);
      res.status(500).json({ error: 'No se pudo leer el ranking.' });
    }
  });

  // 6) Récord de un jugador (público)
  app.get('/api/player/:username', async (req, res) => {
    const u = normalizeUser(req.params.username);
    if (!u) return res.status(400).json({ error: 'Usuario inválido.' });
    try {
      const row = await store.getPlayer(u);
      if (!row) return res.status(404).json({ error: 'Jugador no encontrado.' });
      res.json(row);
    } catch (err) {
      res.status(500).json({ error: 'No se pudo leer al jugador.' });
    }
  });

  // 7) El juego en la raíz
  app.get('/', (req, res) => res.sendFile(GAME_FILE));

  // 404 para el resto de rutas /api
  app.use('/api', (req, res) => res.status(404).json({ error: 'Ruta no encontrada.' }));

  // Manejador de errores global
  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, next) => {
    console.error('[server] Error no controlado:', err);
    res.status(err.status || 500).json({ error: 'Error interno del servidor.' });
  });

  const httpServer = http.createServer(app);
  setupWebSocket(httpServer);

  httpServer.listen(PORT, () => {
    console.log(`Juego servido en  -> http://localhost:${PORT}`);
    console.log(`Leaderboard API   -> http://localhost:${PORT}/api/leaderboard`);
  });
}

// ============================================================
//  Salas online (relay de inputs) vía WebSocket
// ============================================================
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // sin 0/O/1/I
const ROOM_CODE_LEN = 6;

function setupWebSocket(httpServer) {
  const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
  const HEARTBEAT_INTERVAL = 25000;
  let heartbeatInterval;

  function startHeartbeat() {
    heartbeatInterval = setInterval(() => {
      wss.clients.forEach((client) => {
        if (!client.isAlive) return client.terminate();
        client.isAlive = false;
        client.ping();
      });
    }, HEARTBEAT_INTERVAL);
  }

  startHeartbeat();
  wss.on('close', () => clearInterval(heartbeatInterval));

  const rooms = new Map(); // code -> Room

  function genCode() {
    let code;
    do {
      code = '';
      for (let i = 0; i < ROOM_CODE_LEN; i++) {
        code += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
      }
    } while (rooms.has(code));
    return code;
  }

  const send = (ws, obj) => {
    if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
  };

  wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    ws.user = null; // { usernameKey, username, pub }
    ws.room = null; // Room | null

    function handleLeave(room, notifyPeer) {
      if (!room) return;
      rooms.delete(room.code);
      const host = room.host;
      const guest = room.guest;
      if (host) host.room = null;
      if (guest) guest.room = null;
      if (notifyPeer) {
        const peer = host === ws ? guest : host;
        send(peer, { t: 'opponent_left' });
      }
    }

    ws.on('message', async (data) => {
      // ---- Fase E: límite de tamaño y tasa ----
      const maxPayload = 32 * 1024; // 32 KB
      if (data.length > maxPayload) return send(ws, { t: 'err', msg: 'Mensaje demasiado grande.' });
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return send(ws, { t: 'err', msg: 'Mensaje inválido.' });
      }
      // Validación básica: debe ser objeto con tipo
      if (!msg || typeof msg !== 'object' || typeof msg.t !== 'string') return send(ws, { t: 'err', msg: 'Mensaje inválido.' });
      // Rate limit básico: máximo 60 mensajes/segundo por conexión (simple)
      const nowTs = Date.now();
      ws.msgTimestamps = ws.msgTimestamps || [];
      ws.msgTimestamps = ws.msgTimestamps.filter(ts => nowTs - ts < 1000);
      if (ws.msgTimestamps.length > 60) return send(ws, { t: 'err', msg: 'Demasiados mensajes.' });
      ws.msgTimestamps.push(nowTs);

      // ---- Auth (primer mensaje obligatorio) ----
      if (msg.t === 'auth') {
        const token = typeof msg.token === 'string' ? msg.token : '';
        const user = await store.findUserByTokenHash(sha256(token));
        if (!user) return send(ws, { t: 'err', msg: 'Sesión inválida. Inicia sesión.' });
        ws.user = { usernameKey: user.usernameKey, username: user.username, pub: toPublicUser(user) };
        return send(ws, { t: 'authed', user: ws.user.pub });
      }

      if (!ws.user) return send(ws, { t: 'err', msg: 'Autentícate primero.' });

      // ---- Crear sala ----
      if (msg.t === 'create') {
        if (ws.room) return send(ws, { t: 'err', msg: 'Ya estás en una sala.' });
        const mode = msg.mode;
        if (!VALID_ROOM_MODES.has(mode)) return send(ws, { t: 'err', msg: 'Modo inválido.' });
        const code = genCode();
        const room = {
          code,
          mode,
          host: ws,
          guest: null,
          readyCount: 0,
          hostChar: null,
          guestChar: null,
          started: false,
          recorded: false,
        };
        rooms.set(code, room);
        ws.room = room;
        return send(ws, { t: 'created', code, mode });
      }

      // ---- Unirse a sala ----
      if (msg.t === 'join') {
        if (ws.room) return send(ws, { t: 'err', msg: 'Ya estás en una sala.' });
        const code = typeof msg.code === 'string' ? msg.code.toUpperCase().trim() : '';
        const room = rooms.get(code);
        if (!room) return send(ws, { t: 'err', msg: 'Sala no encontrada.' });
        if (room.guest) return send(ws, { t: 'err', msg: 'La sala ya está llena.' });
        if (room.started) return send(ws, { t: 'err', msg: 'La partida ya empezó.' });
        if (room.host === ws) return send(ws, { t: 'err', msg: 'Esa es tu propia sala.' });
        room.guest = ws;
        ws.room = room;
        const hostPub = room.host.user.pub;
        const guestPub = room.guest.user.pub;
        send(room.host, { t: 'opponent_joined', opponent: guestPub });
        send(room.host, { t: 'match_found', mode: room.mode, side: 'p1', self: hostPub, opponent: guestPub });
        send(room.guest, { t: 'match_found', mode: room.mode, side: 'p2', self: guestPub, opponent: hostPub });
        return;
      }

      const room = ws.room;

      // ---- Listo para empezar ----
      if (msg.t === 'ready') {
        if (!room) return send(ws, { t: 'err', msg: 'No estás en una sala.' });
        if (!room.guest || room.started) return send(ws, { t: 'err', msg: 'Aún no se puede listo.' });
        const char = typeof msg.char === 'string' && msg.char.trim() ? msg.char.trim().slice(0, 24) : null;
        if (!char) return send(ws, { t: 'err', msg: 'Selecciona un personaje.' });
        if (ws === room.host && !room.hostChar) {
          room.hostChar = char;
          room.readyCount++;
        } else if (ws === room.guest && !room.guestChar) {
          room.guestChar = char;
          room.readyCount++;
        } else {
          return send(ws, { t: 'err', msg: 'Ya enviaste tu personaje.' });
        }
        if (room.readyCount === 2) {
          room.started = true;
          const oppForHost = { ...room.guest.user.pub, char: room.guestChar };
          const oppForGuest = { ...room.host.user.pub, char: room.hostChar };
          const stageName = room.host && room.host.user ? (room.host.user.username ? 'arena' : 'arena') : 'arena';
          const stageMsg = { t: 'start', mode: room.mode, side: 'p1', self: room.host.user.pub, opponent: oppForHost, stage: 'arena' };
          send(room.host, stageMsg);
          send(room.guest, { t: 'start', mode: room.mode, side: 'p2', self: room.guest.user.pub, opponent: oppForGuest, stage: 'arena' });
        }
        return;
      }

      // ---- Relay de inputs (solo durante la partida) ----
      if (msg.t === 'in' || msg.t === 'act') {
        if (!room || !room.started || !room.guest) return;
        const peer = ws === room.host ? room.guest : room.host;
        const relayed = { t: msg.t };
        if (msg.t === 'in') {
          const m = Number(msg.m);
          if (!Number.isInteger(m) || m < 0 || m > 0xffff) return send(ws, { t: 'err', msg: 'Input inválido.' });
          relayed.m = m;
        } else {
          if (!['ki', 'ult', 'fly', 'awaken', 'dashL', 'dashR'].includes(msg.a)) {
            return send(ws, { t: 'err', msg: 'Acción inválida.' });
          }
          relayed.a = msg.a;
        }
        return send(peer, relayed);
      }

      // ---- Resultado (solo el host registra) ----
      if (msg.t === 'end') {
        if (!room || !room.guest) return send(ws, { t: 'err', msg: 'Partida no activa.' });
        if (room.recorded) return;
        if (ws !== room.host) return; // el guest no puntúa
        if (!VALID_RESULTS.has(msg.result)) return send(ws, { t: 'err', msg: 'Resultado inválido.' });
        room.recorded = true;
        try {
          if (room.mode === 'ranked') {
            const rows = await store.recordRanked({
              p1: room.host.user.username,
              p2: room.guest.user.username,
              result: msg.result,
            });
            send(room.host, { t: 'ranked_result', result: msg.result, myRow: rows[0], oppRow: rows[1], recorded: true });
            send(room.guest, { t: 'ranked_result', result: msg.result, myRow: rows[1], oppRow: rows[0], recorded: true });
          } else {
            const notify = { t: 'ranked_result', result: msg.result, recorded: false, friendly: true };
            send(room.host, notify);
            send(room.guest, notify);
          }
        } catch (err) {
          console.error('[ws] Error registrando resultado:', err);
          send(ws, { t: 'err', msg: 'No se pudo guardar el resultado.' });
        }
        handleLeave(room, false); // los dos ya están notificados, no mandar opponent_left
        return;
      }

      // ---- Loaded (coordinación de inicio) ----
      if (msg.t === 'loaded') {
        if (!room || !room.started) return send(ws, { t: 'err', msg: 'No hay partida activa.' });
        if (!room.gotLoaded) room.gotLoaded = new Set();
        room.gotLoaded.add(ws === room.host ? 'host' : 'guest');
        if (room.gotLoaded.has('host') && room.gotLoaded.has('guest')) {
          send(room.host, { t: 'go' });
          send(room.guest, { t: 'go' });
        } else {
          const peer = ws === room.host ? room.guest : room.host;
          if (peer) send(peer, { t: 'loaded', from: ws === room.host ? 'host' : 'guest' });
        }
        return;
      }

      // ---- Snapshots del host (solo host envía) ----
      if (msg.t === 'snap') {
        if (!room || !room.started || !room.guest) return;
        if (ws !== room.host) return send(ws, { t: 'err', msg: 'Solo el host envía snapshots.' });
        const snap = msg.s;
        if (!snap || typeof snap !== 'object') return send(ws, { t: 'err', msg: 'Snapshot inválido.' });
        // Validación básica de rango
        if (Array.isArray(snap.pos)) {
          for (const p of snap.pos) {
            if (typeof p.x !== 'number' || typeof p.y !== 'number' || p.x < -100 || p.x > 2000 || p.y < -100 || p.y > 2000) return send(ws, { t: 'err', msg: 'Posición fuera de rango.' });
          }
        }
        return send(room.guest, { t: 'snap', s: snap, ts: msg.ts || Date.now() });
      }

      // ---- Over (fin del juego por host) ----
      if (msg.t === 'over') {
        if (!room || !room.guest) return send(ws, { t: 'err', msg: 'No hay partida activa.' });
        if (ws !== room.host) return send(ws, { t: 'err', msg: 'Solo host declara fin.' });
        if (!VALID_RESULTS.has(msg.result)) return send(ws, { t: 'err', msg: 'Resultado inválido.' });
        return send(room.guest, { t: 'over', result: msg.result });
      }

      // ---- Abandonar ----
      if (msg.t === 'leave') {
        handleLeave(room, true);
        return;
      }

      return send(ws, { t: 'err', msg: 'Mensaje desconocido.' });
    });

    ws.on('close', () => handleLeave(ws.room, true));
  });
}

startServer();