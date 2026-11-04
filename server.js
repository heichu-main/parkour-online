const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 10000);
const INDEX = path.join(__dirname, 'index.html');
const ADMIN_ID = 'P-MW2H4MHO-NRQLO3BP';
const BALANCES_FILE = path.join(__dirname, 'player-balances.json');
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' }); return res.end('ok'); }
  if (req.url === '/' || req.url.startsWith('/?')) { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' }); return fs.createReadStream(INDEX).pipe(res); }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('Not found');
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 32 * 1024 * 1024 });
const players = new Map();
let fallbackId = 1;
let balances = {};
try { balances = JSON.parse(fs.readFileSync(BALANCES_FILE, 'utf8')); if (!balances || typeof balances !== 'object' || Array.isArray(balances)) balances = {}; } catch { balances = {}; }
function saveBalances() { try { fs.writeFileSync(BALANCES_FILE, JSON.stringify(balances, null, 2)); } catch (e) { console.error('Could not save balances:', e.message); } }
function send(ws, obj) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
function broadcast(obj, except = null) { const packet = JSON.stringify(obj); for (const ws of wss.clients) if (ws !== except && ws.readyState === WebSocket.OPEN) ws.send(packet); }
function cleanText(value, max, fallback = '') { return String(value ?? '').replace(/[<>\\]/g, '').trim().slice(0, max) || fallback; }
function isAdmin(record) { return !!record && record.clientId === ADMIN_ID; }
function onlineList() { return [...players.values()].filter(p => p.ready).map(p => ({ id: p.clientId, nick: p.nick || 'Игрок' })); }
function refreshAdminLists() { for (const [ws, p] of players) if (p.ready && isAdmin(p)) send(ws, { type: 'admin-player-list', players: onlineList() }); }
function broadcastProfile(record) {
  if (record.state) {
    record.state.name = record.nick; record.state.tagText = record.tagText; record.state.tagColor = record.tagColor;
    broadcast({ type: 'peer-data', id: record.clientId, data: record.state });
  }
  refreshAdminLists();
}
function sendWelcome(ws, record) {
  const snapshot = [];
  for (const p of players.values()) if (p !== record && p.ready) snapshot.push({ id: p.clientId, state: p.state, appearance: p.appearance });
  send(ws, { type: 'welcome', id: record.clientId, coins: Number.isFinite(balances[record.clientId]) ? balances[record.clientId] : null, players: snapshot });
  refreshAdminLists();
}

wss.on('connection', (ws) => {
  const record = { clientId: null, ready: false, state: null, appearance: null, nick: 'Игрок', tagText: '', tagColor: '#ffd54a', joinedAt: Date.now() };
  players.set(ws, record);
  ws.on('message', (raw) => {
    let data; try { data = JSON.parse(raw.toString()); } catch { return; }
    if (!data || typeof data.type !== 'string') return;

    if (data.type === 'hello' && !record.ready) {
      let requested = String(data.clientId || '');
      if (!/^P-[A-Z0-9]{4,32}-[A-Z0-9]{4,32}$/.test(requested)) requested = `P-SERVER-${fallbackId++}`;
      // Prevent two live sockets from controlling the same client identity.
      for (const [otherWs, other] of players) if (otherWs !== ws && other.ready && other.clientId === requested) { try { otherWs.close(4002, 'Duplicate client ID'); } catch {} }
      record.clientId = requested; record.ready = true;
      record.nick = cleanText(data.nick, 20, 'Игрок');
      record.tagText = cleanText(data.tagText, 28, '');
      record.tagColor = /^#[0-9a-fA-F]{6}$/.test(String(data.tagColor || '')) ? data.tagColor : '#ffd54a';
      if (!Number.isFinite(balances[requested])) balances[requested] = Math.max(0, Math.floor(Number(data.initialCoins) || 0));
      saveBalances(); sendWelcome(ws, record); return;
    }
    if (!record.ready) return;

    if (data.type === 'state') {
      record.state = data;
      record.state.name = record.nick; record.state.tagText = record.tagText; record.state.tagColor = record.tagColor;
      broadcast({ type: 'peer-data', id: record.clientId, data: record.state }, ws);
      return;
    }
    if (data.type === 'appearance') { record.appearance = Array.isArray(data.parts) ? data.parts : null; broadcast({ type: 'peer-data', id: record.clientId, data: { type: 'appearance', parts: record.appearance } }, ws); return; }
    if (data.type === 'profile') {
      record.nick = cleanText(data.nick, 20, 'Игрок'); record.tagText = cleanText(data.tagText, 28, '');
      record.tagColor = /^#[0-9a-fA-F]{6}$/.test(String(data.tagColor || '')) ? data.tagColor : '#ffd54a';
      broadcastProfile(record); send(ws, { type: 'admin-result', message: 'Профиль обновлён.' }); return;
    }
    if (data.type === 'admin-list') {
      if (!isAdmin(record)) return send(ws, { type: 'admin-result', message: 'Нет прав администратора.' });
      send(ws, { type: 'admin-player-list', players: onlineList() }); return;
    }
    if (data.type === 'admin-coins') {
      if (!isAdmin(record)) return send(ws, { type: 'admin-result', message: 'Нет прав администратора.' });
      const targetId = String(data.targetId || ''); const amount = Number(data.amount);
      const targetEntry = [...players.entries()].find(([, p]) => p.ready && p.clientId === targetId);
      if (!targetEntry) return send(ws, { type: 'admin-result', message: 'Игрок уже не на сервере.' });
      if (!Number.isSafeInteger(amount) || amount === 0 || Math.abs(amount) > 1000000000) return send(ws, { type: 'admin-result', message: 'Некорректная сумма.' });
      const next = Math.max(0, Math.min(2147483647, (Number(balances[targetId]) || 0) + amount));
      balances[targetId] = next; saveBalances(); send(targetEntry[0], { type: 'coin-update', coins: next });
      send(ws, { type: 'admin-result', message: `${amount > 0 ? 'Выдано' : 'Снято'} ${Math.abs(amount)} монет. Новый баланс: ${next}.` }); return;
    }
    if (data.type === 'admin-kick') {
      if (!isAdmin(record)) return send(ws, { type: 'admin-result', message: 'Нет прав администратора.' });
      const targetId = String(data.targetId || '');
      const targetEntry = [...players.entries()].find(([, p]) => p.ready && p.clientId === targetId);
      if (!targetEntry) return send(ws, { type: 'admin-result', message: 'Игрок уже не на сервере.' });
      if (targetId === record.clientId) return send(ws, { type: 'admin-result', message: 'Нельзя кикнуть самого себя.' });
      send(targetEntry[0], { type: 'kicked' });
      setTimeout(() => { try { targetEntry[0].close(4001, 'Kicked by administrator'); } catch {} }, 80);
      send(ws, { type: 'admin-result', message: `Игрок ${targetId} отключён от сервера.` }); return;
    }
  });

  ws.on('close', () => {
    players.delete(ws);
    if (record.ready) { broadcast({ type: 'peer-left', id: record.clientId }); refreshAdminLists(); }
  });
  ws.on('error', () => {});
});
server.listen(PORT, '0.0.0.0', () => console.log(`Parkour server listening on 0.0.0.0:${PORT}`));
