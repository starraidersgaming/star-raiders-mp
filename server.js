/**
 * Star Raiders — realtime sector sync server (zero dependencies, Node 18+)
 *
 * Rooms are per areaIndex. Host = first nick sorted. Tick ~20 Hz.
 * Env: PORT=8787  TICK_HZ=20
 */
const http = require("http");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 8787);
const TICK_MS = Math.max(16, Math.round(1000 / Number(process.env.TICK_HZ || 20)));

/** @type {Map<number, SectorRoom>} */
const rooms = new Map();

function wsAcceptKey(key) {
  return crypto
    .createHash("sha1")
    .update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
    .digest("base64");
}

function wsSend(socket, obj) {
  if (!socket || socket.destroyed || !socket.__srOpen) return;
  const json = typeof obj === "string" ? obj : JSON.stringify(obj);
  const payload = Buffer.from(json, "utf8");
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x81;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeUInt32BE(0, 2);
    header.writeUInt32BE(len, 6);
  }
  try {
    socket.write(Buffer.concat([header, payload]));
  } catch (_) {}
}

function wsClose(socket, code = 1000) {
  if (!socket || socket.destroyed) return;
  try {
    const buf = Buffer.alloc(4);
    buf[0] = 0x88;
    buf[1] = 2;
    buf.writeUInt16BE(code, 2);
    socket.write(buf);
  } catch (_) {}
  try {
    socket.end();
  } catch (_) {}
}

/** Parse one or more WebSocket frames from a buffer. Returns { messages, rest } */
function wsConsume(buf) {
  const messages = [];
  let offset = 0;
  while (offset + 2 <= buf.length) {
    const b0 = buf[offset];
    const b1 = buf[offset + 1];
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f;
    let hdr = 2;
    if (len === 126) {
      if (offset + 4 > buf.length) break;
      len = buf.readUInt16BE(offset + 2);
      hdr = 4;
    } else if (len === 127) {
      if (offset + 10 > buf.length) break;
      len = Number(buf.readBigUInt64BE(offset + 2));
      hdr = 10;
    }
    const maskLen = masked ? 4 : 0;
    if (offset + hdr + maskLen + len > buf.length) break;
    let payload = buf.subarray(offset + hdr + maskLen, offset + hdr + maskLen + len);
    if (masked) {
      const mask = buf.subarray(offset + hdr, offset + hdr + 4);
      const decoded = Buffer.alloc(payload.length);
      for (let i = 0; i < payload.length; i++) decoded[i] = payload[i] ^ mask[i % 4];
      payload = decoded;
    }
    offset += hdr + maskLen + len;
    if (opcode === 0x8) {
      messages.push({ type: "close" });
    } else if (opcode === 0x9) {
      messages.push({ type: "ping", data: payload });
    } else if (opcode === 0x1 || opcode === 0x2) {
      messages.push({ type: "msg", data: payload.toString("utf8") });
    }
  }
  return { messages, rest: buf.subarray(offset) };
}

class SectorRoom {
  constructor(areaIndex) {
    this.areaIndex = areaIndex;
    this.clients = new Map();
    this.host = null;
    this.enemies = null;
    this.kills = {};
    this.lastEnemyAt = 0;
  }

  pickHost() {
    const nicks = [...this.clients.keys()].sort();
    this.host = nicks[0] || null;
    for (const c of this.clients.values()) {
      c.isHost = c.nick === this.host;
      wsSend(c.socket, { t: "host", host: this.host, youAreHost: c.isHost });
    }
  }

  broadcast(msg, exceptNick = null) {
    const raw = JSON.stringify(msg);
    for (const [nick, c] of this.clients) {
      if (nick === exceptNick) continue;
      wsSend(c.socket, raw);
    }
  }

  snapshotPlayers() {
    const out = {};
    for (const [nick, c] of this.clients) out[nick] = { nick, ...c.state };
    return out;
  }

  join(socket, msg) {
    const nick = String(msg.nick || "").slice(0, 24);
    if (!nick) {
      wsSend(socket, { t: "err", m: "nick required" });
      return null;
    }
    const prev = this.clients.get(nick);
    if (prev && prev.socket !== socket) {
      wsClose(prev.socket, 4000);
      this.clients.delete(nick);
    }
    const client = {
      socket,
      nick,
      isHost: false,
      state: {
        x: Number(msg.x) || 0,
        y: Number(msg.y) || 0,
        vx: Number(msg.vx) || 0,
        vy: Number(msg.vy) || 0,
        angle: Number(msg.angle) || 0,
        shipIndex: msg.shipIndex | 0,
        hp: Math.round(msg.hp || 0),
        maxHp: Math.round(msg.maxHp || 0),
        shield: Math.round(msg.shield || 0),
        maxShield: Math.round(msg.maxShield || 0),
        drones: msg.drones | 0,
        inHangar: !!msg.inHangar,
        level: msg.level | 0,
        hasBetaBadge: !!msg.hasBetaBadge,
        isRankOne: !!msg.isRankOne,
        isGM: !!msg.isGM,
        isMod: !!msg.isMod,
        killPoints: msg.killPoints | 0,
        lastActive: Date.now()
      }
    };
    this.clients.set(nick, client);
    socket.__srNick = nick;
    socket.__srArea = this.areaIndex;
    this.pickHost();
    wsSend(socket, {
      t: "welcome",
      areaIndex: this.areaIndex,
      host: this.host,
      youAreHost: client.isHost,
      players: this.snapshotPlayers()
    });
    this.broadcast({ t: "join", nick, player: { nick, ...client.state } }, nick);
    if (this.enemies) {
      wsSend(socket, {
        t: "enemies",
        updatedAt: this.lastEnemyAt,
        host: this.host,
        areaIndex: this.areaIndex,
        enemies: this.enemies,
        kills: this.kills
      });
    }
    return client;
  }

  leave(nick) {
    if (!this.clients.has(nick)) return;
    this.clients.delete(nick);
    this.broadcast({ t: "leave", nick });
    if (!this.clients.size) {
      rooms.delete(this.areaIndex);
      return;
    }
    if (this.host === nick) {
      this.enemies = null;
      this.kills = {};
      this.pickHost();
    }
  }

  onState(nick, msg) {
    const c = this.clients.get(nick);
    if (!c) return;
    const s = c.state;
    if (Number.isFinite(msg.x)) s.x = msg.x;
    if (Number.isFinite(msg.y)) s.y = msg.y;
    if (Number.isFinite(msg.vx)) s.vx = msg.vx;
    if (Number.isFinite(msg.vy)) s.vy = msg.vy;
    if (Number.isFinite(msg.angle)) s.angle = msg.angle;
    if (msg.shipIndex != null) s.shipIndex = msg.shipIndex | 0;
    if (msg.hp != null) s.hp = Math.round(msg.hp);
    if (msg.maxHp != null) s.maxHp = Math.round(msg.maxHp);
    if (msg.shield != null) s.shield = Math.round(msg.shield);
    if (msg.maxShield != null) s.maxShield = Math.round(msg.maxShield);
    if (msg.drones != null) s.drones = msg.drones | 0;
    if (msg.inHangar != null) s.inHangar = !!msg.inHangar;
    if (msg.level != null) s.level = msg.level | 0;
    s.lastActive = Date.now();
  }

  onShot(nick, msg) {
    this.broadcast(
      {
        t: "shot",
        nick,
        id: msg.id,
        x: msg.x,
        y: msg.y,
        a: msg.a,
        asset: msg.asset || "laser_player",
        vx: msg.vx,
        vy: msg.vy,
        t: Date.now()
      },
      nick
    );
  }

  onHit(nick, msg) {
    const payload = {
      t: "hit",
      by: nick,
      syncId: msg.syncId,
      hp: Math.max(0, Number(msg.hp) || 0),
      dmg: Math.max(0, Number(msg.dmg) || 0),
      kill: !!msg.kill,
      x: msg.x,
      y: msg.y,
      ts: Date.now()
    };
    if (payload.kill && payload.syncId) this.kills[payload.syncId] = Date.now();
    this.broadcast(payload, null);
  }

  onEnemies(nick, msg) {
    const c = this.clients.get(nick);
    if (!c || !c.isHost) return;
    this.enemies = msg.enemies && typeof msg.enemies === "object" ? msg.enemies : {};
    if (msg.kills && typeof msg.kills === "object") this.kills = { ...this.kills, ...msg.kills };
    const now = Date.now();
    for (const [id, kt] of Object.entries(this.kills)) if (now - kt > 3000) delete this.kills[id];
    this.lastEnemyAt = now;
    this.broadcast(
      {
        t: "enemies",
        updatedAt: now,
        host: this.host,
        areaIndex: this.areaIndex,
        enemies: this.enemies,
        kills: this.kills
      },
      nick
    );
  }
}

function getRoom(areaIndex) {
  const a = areaIndex | 0;
  let room = rooms.get(a);
  if (!room) {
    room = new SectorRoom(a);
    rooms.set(a, room);
  }
  return room;
}

function handleMessage(socket, raw) {
  let msg;
  try {
    msg = JSON.parse(raw);
  } catch (_) {
    return;
  }
  if (!msg || typeof msg !== "object") return;

  let nick = socket.__srNick;
  let area = socket.__srArea;

  if (msg.t === "join") {
    if (nick != null && area != null) {
      const old = rooms.get(area);
      old && old.leave(nick);
    }
    area = msg.areaIndex | 0;
    const room = getRoom(area);
    const client = room.join(socket, msg);
    if (client) {
      nick = client.nick;
    }
    return;
  }

  if (nick == null || area == null) return;
  const room = rooms.get(area);
  if (!room) return;

  if (msg.t === "state") room.onState(nick, msg);
  else if (msg.t === "shot") room.onShot(nick, msg);
  else if (msg.t === "hit") room.onHit(nick, msg);
  else if (msg.t === "enemies") room.onEnemies(nick, msg);
  else if (msg.t === "switch") {
    const nextArea = msg.areaIndex | 0;
    if (nextArea === area) return;
    room.leave(nick);
    area = nextArea;
    const next = getRoom(area);
    next.join(socket, { ...msg, nick, areaIndex: area });
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
    res.end(
      JSON.stringify({
        ok: true,
        rooms: rooms.size,
        players: [...rooms.values()].reduce((n, r) => n + r.clients.size, 0)
      })
    );
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Star Raiders MP — connect via WebSocket\n");
});

server.on("upgrade", (req, socket, head) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) {
    socket.destroy();
    return;
  }
  const headers = [
    "HTTP/1.1 101 Switching Protocols",
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Accept: ${wsAcceptKey(key)}`,
    "",
    ""
  ].join("\r\n");
  socket.write(headers);
  socket.__srOpen = true;
  socket.__srBuf = Buffer.alloc(0);
  if (head && head.length) socket.__srBuf = Buffer.concat([socket.__srBuf, head]);

  socket.on("data", (chunk) => {
    socket.__srBuf = Buffer.concat([socket.__srBuf, chunk]);
    const { messages, rest } = wsConsume(socket.__srBuf);
    socket.__srBuf = rest;
    for (const m of messages) {
      if (m.type === "close") {
        const nick = socket.__srNick;
        const area = socket.__srArea;
        if (nick != null && area != null) {
          const room = rooms.get(area);
          room && room.leave(nick);
        }
        wsClose(socket);
        return;
      }
      if (m.type === "ping") {
        // pong
        if (!socket.destroyed && socket.__srOpen) {
          const len = m.data.length;
          const hdr = Buffer.alloc(2);
          hdr[0] = 0x8a;
          hdr[1] = len;
          socket.write(Buffer.concat([hdr, m.data]));
        }
      } else if (m.type === "msg") handleMessage(socket, m.data);
    }
  });

  socket.on("close", () => {
    const nick = socket.__srNick;
    const area = socket.__srArea;
    if (nick != null && area != null) {
      const room = rooms.get(area);
      room && room.leave(nick);
    }
  });

  socket.on("error", () => {});
});

setInterval(() => {
  for (const room of rooms.values()) {
    if (room.clients.size < 1) continue;
    room.broadcast({ t: "players", players: room.snapshotPlayers(), host: room.host });
  }
}, TICK_MS);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[Star Raiders MP] ws://0.0.0.0:${PORT}  tick=${TICK_MS}ms  (no npm deps)`);
});
