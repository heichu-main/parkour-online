const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 10000);
const INDEX = path.join(__dirname, 'index.html');
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    return res.end('ok');
  }
  if (req.url === '/' || req.url.startsWith('/?')) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    return fs.createReadStream(INDEX).pipe(res);
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 32 * 1024 * 1024 });
const players = new Map();
let nextId = 1;
function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}
function broadcast(obj, except = null) {
  const packet = JSON.stringify(obj);
  for (const ws of wss.clients) {
    if (ws !== except && ws.readyState === WebSocket.OPEN) ws.send(packet);
  }
}

wss.on('connection', (ws) => {
  const id = `player-${nextId++}`;
  const record = { id, state: null, appearance: null, joinedAt: Date.now() };
  players.set(ws, record);
  const snapshot = [];
  for (const [otherWs, p] of players) {
    if (otherWs !== ws) snapshot.push({ id: p.id, state: p.state, appearance: p.appearance });
  }
  send(ws, { type: 'welcome', id, players: snapshot });

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw.toString()); } catch { return; }
    if (!data || typeof data.type !== 'string') return;
    if (data.type === 'state') record.state = data;
    else if (data.type === 'appearance') record.appearance = data.parts;
    else return;
    // Server assigns the sender ID. Clients cannot choose another player's ID.
    broadcast({ type: 'peer-data', id, data }, ws);
  });

  ws.on('close', () => {
    players.delete(ws);
    broadcast({ type: 'peer-left', id });
  });
  ws.on('error', () => {});
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Parkour server listening on 0.0.0.0:${PORT}`);
});
