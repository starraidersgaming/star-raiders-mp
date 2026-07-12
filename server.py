#!/usr/bin/env python3
"""
Star Raiders — realtime sector sync server (Python 3.10+, stdlib only)

Rooms are per areaIndex. Host = first nick sorted. Tick ~30 Hz.
Env: PORT=8787  TICK_HZ=30

  py -3 server.py
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import select
import socket
import struct
import threading
import time
from typing import Any, Dict, Optional

PORT = int(os.environ.get("PORT", "8787"))
TICK_HZ = float(os.environ.get("TICK_HZ", "30"))
TICK_MS = max(0.016, 1.0 / TICK_HZ)

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def ws_accept(key: str) -> str:
    return base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()


def ws_encode(text: str) -> bytes:
    payload = text.encode("utf8")
    n = len(payload)
    if n < 126:
        header = bytes([0x81, n])
    elif n < 65536:
        header = bytes([0x81, 126]) + struct.pack("!H", n)
    else:
        header = bytes([0x81, 127]) + struct.pack("!Q", n)
    return header + payload


def ws_decode_frames(buf: bytearray):
    messages = []
    while len(buf) >= 2:
        b0, b1 = buf[0], buf[1]
        opcode = b0 & 0x0F
        masked = (b1 & 0x80) != 0
        length = b1 & 0x7F
        hdr = 2
        if length == 126:
            if len(buf) < 4:
                break
            length = struct.unpack("!H", buf[2:4])[0]
            hdr = 4
        elif length == 127:
            if len(buf) < 10:
                break
            length = struct.unpack("!Q", buf[2:10])[0]
            hdr = 10
        mask_len = 4 if masked else 0
        total = hdr + mask_len + length
        if len(buf) < total:
            break
        payload = bytes(buf[hdr + mask_len : total])
        if masked:
            mask = buf[hdr : hdr + 4]
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        del buf[:total]
        if opcode == 0x8:
            messages.append(("close", b""))
        elif opcode == 0x9:
            messages.append(("ping", payload))
        elif opcode in (0x1, 0x2):
            messages.append(("msg", payload.decode("utf8", errors="ignore")))
    return messages


class Client:
    __slots__ = ("sock", "nick", "is_host", "state", "buf", "alive")

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.nick: Optional[str] = None
        self.is_host = False
        self.state: Dict[str, Any] = {}
        self.buf = bytearray()
        self.alive = True

    def send(self, obj: Any) -> None:
        if not self.alive:
            return
        raw = obj if isinstance(obj, str) else json.dumps(obj, separators=(",", ":"))
        try:
            self.sock.sendall(ws_encode(raw))
        except OSError:
            self.alive = False


class SectorRoom:
    def __init__(self, area_index: int):
        self.area_index = area_index
        self.clients: Dict[str, Client] = {}
        self.host: Optional[str] = None
        self.enemies: Optional[dict] = None
        self.kills: Dict[str, float] = {}
        self.last_enemy_at = 0.0

    def pick_host(self) -> None:
        nicks = sorted(self.clients.keys())
        self.host = nicks[0] if nicks else None
        for c in self.clients.values():
            c.is_host = c.nick == self.host
            c.send({"t": "host", "host": self.host, "youAreHost": c.is_host})

    def broadcast(self, msg: Any, except_nick: Optional[str] = None) -> None:
        raw = json.dumps(msg, separators=(",", ":"))
        for nick, c in list(self.clients.items()):
            if nick == except_nick:
                continue
            c.send(raw)

    def snapshot_players(self) -> dict:
        out = {}
        for nick, c in self.clients.items():
            out[nick] = {"nick": nick, **c.state}
        return out

    def join(self, client: Client, msg: dict) -> Optional[Client]:
        nick = str(msg.get("nick") or "")[:24]
        if not nick:
            client.send({"t": "err", "m": "nick required"})
            return None
        prev = self.clients.get(nick)
        if prev and prev is not client:
            prev.alive = False
            try:
                prev.sock.close()
            except OSError:
                pass
            self.clients.pop(nick, None)
        client.nick = nick
        client.state = {
            "x": float(msg.get("x") or 0),
            "y": float(msg.get("y") or 0),
            "vx": float(msg.get("vx") or 0),
            "vy": float(msg.get("vy") or 0),
            "angle": float(msg.get("angle") or 0),
            "shipIndex": int(msg.get("shipIndex") or 0),
            "hp": int(msg.get("hp") or 0),
            "maxHp": int(msg.get("maxHp") or 0),
            "shield": int(msg.get("shield") or 0),
            "maxShield": int(msg.get("maxShield") or 0),
            "drones": int(msg.get("drones") or 0),
            "inHangar": bool(msg.get("inHangar")),
            "level": int(msg.get("level") or 0),
            "hasBetaBadge": bool(msg.get("hasBetaBadge")),
            "isRankOne": bool(msg.get("isRankOne")),
            "isGM": bool(msg.get("isGM")),
            "isMod": bool(msg.get("isMod")),
            "killPoints": int(msg.get("killPoints") or 0),
            "lastActive": time.time() * 1000,
        }
        self.clients[nick] = client
        self.pick_host()
        client.send(
            {
                "t": "welcome",
                "areaIndex": self.area_index,
                "host": self.host,
                "youAreHost": client.is_host,
                "players": self.snapshot_players(),
            }
        )
        self.broadcast({"t": "join", "nick": nick, "player": {"nick": nick, **client.state}}, nick)
        if self.enemies is not None:
            client.send(
                {
                    "t": "enemies",
                    "updatedAt": self.last_enemy_at,
                    "host": self.host,
                    "areaIndex": self.area_index,
                    "enemies": self.enemies,
                    "kills": self.kills,
                }
            )
        return client

    def leave(self, nick: str) -> None:
        if nick not in self.clients:
            return
        self.clients.pop(nick, None)
        self.broadcast({"t": "leave", "nick": nick})
        if not self.clients:
            rooms.pop(self.area_index, None)
            return
        if self.host == nick:
            self.enemies = None
            self.kills = {}
            self.pick_host()

    def on_state(self, nick: str, msg: dict) -> None:
        c = self.clients.get(nick)
        if not c:
            return
        s = c.state
        for k in ("x", "y", "vx", "vy", "angle"):
            if k in msg and isinstance(msg[k], (int, float)):
                s[k] = float(msg[k])
        for k in ("shipIndex", "hp", "maxHp", "shield", "maxShield", "drones", "level"):
            if k in msg and msg[k] is not None:
                s[k] = int(msg[k])
        if "inHangar" in msg:
            s["inHangar"] = bool(msg["inHangar"])
        for k in ("isGM", "isMod", "hasBetaBadge", "isRankOne"):
            if k in msg:
                s[k] = bool(msg[k])
        if "killPoints" in msg and msg["killPoints"] is not None:
            try:
                s["killPoints"] = int(msg["killPoints"])
            except (TypeError, ValueError):
                pass
        s["lastActive"] = time.time() * 1000
        # Relay immediately so remote ships don't wait on the room tick (avoids freeze/hitch)
        self.broadcast(
            {
                "t": "state",
                "nick": nick,
                "x": s["x"],
                "y": s["y"],
                "vx": s["vx"],
                "vy": s["vy"],
                "angle": s["angle"],
                "shipIndex": s["shipIndex"],
                "hp": s["hp"],
                "maxHp": s["maxHp"],
                "shield": s["shield"],
                "maxShield": s["maxShield"],
                "drones": s["drones"],
                "inHangar": s["inHangar"],
                "isGM": s.get("isGM", False),
                "isMod": s.get("isMod", False),
                "hasBetaBadge": s.get("hasBetaBadge", False),
                "isRankOne": s.get("isRankOne", False),
                "killPoints": s.get("killPoints", 0),
            },
            nick,
        )

    def on_shot(self, nick: str, msg: dict) -> None:
        self.broadcast(
            {
                "t": "shot",
                "nick": nick,
                "id": msg.get("id"),
                "x": msg.get("x"),
                "y": msg.get("y"),
                "a": msg.get("a"),
                "asset": msg.get("asset") or "laser_player",
                "vx": msg.get("vx"),
                "vy": msg.get("vy"),
                "ts": int(time.time() * 1000),
            },
            nick,
        )

    def on_hit(self, nick: str, msg: dict) -> None:
        sync_id = msg.get("syncId")
        kill = bool(msg.get("kill"))
        payload = {
            "t": "hit",
            "by": nick,
            "syncId": sync_id,
            "hp": max(0, float(msg.get("hp") or 0)),
            "dmg": max(0, float(msg.get("dmg") or 0)),
            "kill": kill,
            "x": msg.get("x"),
            "y": msg.get("y"),
            "ts": int(time.time() * 1000),
        }
        if kill and sync_id:
            self.kills[str(sync_id)] = time.time() * 1000
        self.broadcast(payload, None)

    def on_enemies(self, nick: str, msg: dict) -> None:
        c = self.clients.get(nick)
        if not c or not c.is_host:
            return
        enemies = msg.get("enemies")
        self.enemies = enemies if isinstance(enemies, dict) else {}
        kills = msg.get("kills")
        if isinstance(kills, dict):
            self.kills.update(kills)
        now = time.time() * 1000
        self.kills = {k: v for k, v in self.kills.items() if now - float(v) < 3000}
        self.last_enemy_at = now
        self.broadcast(
            {
                "t": "enemies",
                "updatedAt": now,
                "host": self.host,
                "areaIndex": self.area_index,
                "enemies": self.enemies,
                "kills": self.kills,
            },
            nick,
        )


rooms: Dict[int, SectorRoom] = {}
# sock -> (client, area)
conn_meta: Dict[socket.socket, tuple] = {}
lock = threading.Lock()


def get_room(area: int) -> SectorRoom:
    area = int(area)
    if area not in rooms:
        rooms[area] = SectorRoom(area)
    return rooms[area]


def handle_message(client: Client, area: Optional[int], raw: str) -> Optional[int]:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return area
    if not isinstance(msg, dict):
        return area
    t = msg.get("t")
    with lock:
        if t == "join":
            if client.nick is not None and area is not None and area in rooms:
                rooms[area].leave(client.nick)
            area = int(msg.get("areaIndex") or 0)
            room = get_room(area)
            room.join(client, msg)
            return area

        if client.nick is None or area is None:
            return area
        room = rooms.get(area)
        if not room:
            return area
        nick = client.nick
        if t == "state":
            room.on_state(nick, msg)
        elif t == "shot":
            room.on_shot(nick, msg)
        elif t == "hit":
            room.on_hit(nick, msg)
        elif t == "enemies":
            room.on_enemies(nick, msg)
        elif t == "switch":
            next_area = int(msg.get("areaIndex") or 0)
            if next_area != area:
                room.leave(nick)
                area = next_area
                get_room(area).join(client, {**msg, "nick": nick, "areaIndex": area})
    return area


def client_thread(sock: socket.socket) -> None:
    client = Client(sock)
    area: Optional[int] = None
    conn_meta[sock] = (client, area)
    try:
        while client.alive:
            r, _, _ = select.select([sock], [], [], 0.5)
            if not r:
                continue
            try:
                data = sock.recv(65536)
            except OSError:
                break
            if not data:
                break
            client.buf.extend(data)
            for kind, payload in ws_decode_frames(client.buf):
                if kind == "close":
                    client.alive = False
                    break
                if kind == "ping":
                    try:
                        sock.sendall(bytes([0x8A, len(payload)]) + payload)
                    except OSError:
                        client.alive = False
                elif kind == "msg":
                    area = handle_message(client, area, payload)
                    conn_meta[sock] = (client, area)
    finally:
        with lock:
            if client.nick is not None and area is not None and area in rooms:
                rooms[area].leave(client.nick)
        conn_meta.pop(sock, None)
        try:
            sock.close()
        except OSError:
            pass


def tick_loop() -> None:
    while True:
        time.sleep(TICK_MS)
        with lock:
            for room in list(rooms.values()):
                if not room.clients:
                    continue
                room.broadcast({"t": "players", "players": room.snapshot_players(), "host": room.host})


def accept_ws(sock: socket.socket, req: bytes) -> bool:
    try:
        text = req.decode("utf8", errors="ignore")
        lines = text.split("\r\n")
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        key = headers.get("sec-websocket-key")
        if not key:
            return False
        resp = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {ws_accept(key)}\r\n"
            "\r\n"
        )
        sock.sendall(resp.encode())
        return True
    except OSError:
        return False


def handle_http(sock: socket.socket, req: bytes) -> None:
    path = "/"
    try:
        line = req.decode("utf8", errors="ignore").split("\r\n", 1)[0]
        parts = line.split(" ")
        if len(parts) >= 2:
            path = parts[1]
    except Exception:
        pass
    if path.startswith("/health"):
        with lock:
            players = sum(len(r.clients) for r in rooms.values())
            body = json.dumps({"ok": True, "rooms": len(rooms), "players": players})
        resp = (
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            f"Access-Control-Allow-Origin: *\r\nContent-Length: {len(body)}\r\n\r\n{body}"
        )
    else:
        body = "Star Raiders MP — connect via WebSocket\n"
        resp = (
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
            f"Content-Length: {len(body)}\r\n\r\n{body}"
        )
    try:
        sock.sendall(resp.encode())
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def main() -> None:
    threading.Thread(target=tick_loop, daemon=True).start()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", PORT))
    srv.listen(64)
    print(f"[Star Raiders MP] ws://0.0.0.0:{PORT}  tick={TICK_MS*1000:.0f}ms  (Python stdlib)")
    while True:
        conn, _addr = srv.accept()
        conn.settimeout(10)
        try:
            data = conn.recv(4096)
        except OSError:
            conn.close()
            continue
        if not data:
            conn.close()
            continue
        head = data.decode("utf8", errors="ignore").lower()
        if "upgrade: websocket" in head:
            conn.settimeout(None)
            if accept_ws(conn, data):
                threading.Thread(target=client_thread, args=(conn,), daemon=True).start()
            else:
                conn.close()
        else:
            handle_http(conn, data)


if __name__ == "__main__":
    main()
