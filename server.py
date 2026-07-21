#!/usr/bin/env python3
"""
Star Raiders — realtime sector sync server (Python 3.10+, stdlib only)

Rooms are per areaIndex.
Enemies are SERVER-OWNED: the server spawns/ticks/broadcasts them.
Clients only mirror snapshots and send hit events.
Env: PORT=8787  ENEMY_HZ=12

  py -3 server.py
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import random
import select
import socket
import struct
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

PORT = int(os.environ.get("PORT", "8787"))
ENEMY_HZ = float(os.environ.get("ENEMY_HZ", "12"))
ENEMY_DT = max(0.05, 1.0 / ENEMY_HZ)
PLAYERS_HZ = 4.0
PLAYERS_DT = 1.0 / PLAYERS_HZ

WORLD = 6000.0
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# Minimal enemy catalog (index -> stats). Matches client ft[] basics.
ENEMY_TYPES = [
    {"t": 0, "h": 40, "s": 100, "d": 10, "xp": 15, "c": 10, "r": 20},
    {"t": 1, "h": 150, "s": 60, "d": 25, "xp": 45, "c": 30, "r": 30},
    {"t": 2, "h": 400, "s": 120, "d": 60, "xp": 100, "c": 60, "r": 25},
    {"t": 3, "h": 1000, "s": 80, "d": 150, "xp": 250, "c": 150, "r": 40},
    {"t": 4, "h": 2500, "s": 140, "d": 350, "xp": 600, "c": 350, "r": 30},
    {"t": 5, "h": 6000, "s": 90, "d": 600, "xp": 1200, "c": 700, "r": 50},
    {"t": 6, "h": 12000, "s": 150, "d": 1000, "xp": 2500, "c": 1500, "r": 22},
    {"t": 7, "h": 20000, "s": 70, "d": 1500, "xp": 4000, "c": 2500, "r": 45},
]


def enemy_cap(area: int) -> int:
    return 50 if area == 0 else 36


def type_for_area(area: int) -> dict:
    if area <= 0:
        return ENEMY_TYPES[0] if random.random() < 0.7 else ENEMY_TYPES[1]
    idx = min(len(ENEMY_TYPES) - 1, max(0, area))
    # Prefer sector-tier and next tier
    a = ENEMY_TYPES[min(idx, len(ENEMY_TYPES) - 1)]
    b = ENEMY_TYPES[min(idx + 1, len(ENEMY_TYPES) - 1)]
    return a if random.random() < 0.6 else b


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
        self.host: Optional[str] = None  # legacy sticky host (debris only)
        # Server-owned enemies: id -> live entity
        self.enemy_ents: Dict[str, dict] = {}
        self.enemies: Dict[str, dict] = {}  # serialized snap
        self.enemy_shots: list = []
        self.kills: Dict[str, float] = {}
        self.last_enemy_at = 0.0
        self.last_enemy_seq = 0
        self.next_enemy_id = 1
        self.debris: Optional[dict] = None
        self.rocks: dict = {}
        self.debris_kills: Dict[str, float] = {}
        self.last_debris_at = 0.0
        self._spawned = False

    def pick_host(self) -> None:
        # Sticky host kept for debris/legacy only — enemies are server-owned.
        if self.host and self.host in self.clients:
            for c in self.clients.values():
                c.is_host = c.nick == self.host
            return
        nicks = sorted(self.clients.keys())
        self.host = nicks[0] if nicks else None
        for c in self.clients.values():
            c.is_host = c.nick == self.host
            c.send(
                {
                    "t": "host",
                    "host": self.host,
                    "youAreHost": c.is_host,
                    "serverEnemies": True,
                }
            )

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

    def serialize_enemy(self, e: dict) -> dict:
        ang = float(e.get("ang") or e.get("w") or 1.0)
        wand = float(e.get("w") or ang)
        if abs(ang) < 0.05:
            ang = wand if abs(wand) >= 0.05 else random.random() * math.pi * 2
        if abs(wand) < 0.05:
            wand = ang
        return {
            "t": int(e["t"]),
            "x": round(float(e["x"]), 1),
            "y": round(float(e["y"]), 1),
            "h": int(max(0, e["h"])),
            "m": int(e["m"]),
            "ang": round(ang, 2),
            "a": round(ang, 2),
            "g": 1 if e.get("g") else 0,
            "w": round(wand, 2),
            "r": int(e["r"]),
            "s": int(e["s"]),
            "d": int(e["d"]),
            "c": int(e["c"]),
            "xp": int(e["xp"]),
        }

    def rebuild_enemy_snap(self) -> None:
        snap = {}
        for sid, e in self.enemy_ents.items():
            if e.get("h", 0) <= 0:
                continue
            snap[sid] = self.serialize_enemy(e)
        self.enemies = snap
        self.last_enemy_seq += 1
        self.last_enemy_at = time.time() * 1000

    def enemy_payload(self) -> dict:
        now = time.time() * 1000
        self.kills = {k: v for k, v in self.kills.items() if now - float(v) < 3000}
        return {
            "t": "enemies",
            "updatedAt": self.last_enemy_at or now,
            "seq": self.last_enemy_seq or None,
            "host": "SERVER",
            "serverEnemies": True,
            "areaIndex": self.area_index,
            "enemies": self.enemies,
            "kills": self.kills,
            "shots": self.enemy_shots[-40:],
            "full": 1,
        }

    def broadcast_enemies(self) -> None:
        if not self.clients:
            return
        self.rebuild_enemy_snap()
        self.broadcast(self.enemy_payload(), None)

    def ensure_enemies(self) -> None:
        if self._spawned and self.enemy_ents:
            return
        self._spawned = True
        self.seed_enemies()

    def seed_enemies(self) -> None:
        cap = enemy_cap(self.area_index)
        need = max(0, cap - len(self.enemy_ents))
        for _ in range(need):
            self.spawn_one()
        self.rebuild_enemy_snap()

    def spawn_one(self) -> None:
        typ = type_for_area(self.area_index)
        # Spread across map; avoid center spawn clump
        x = random.uniform(200, WORLD - 200)
        y = random.uniform(200, WORLD - 200)
        if self.area_index == 0:
            # Keep clear of typical base near center
            for _ in range(8):
                if math.hypot(x - WORLD / 2, y - WORLD / 2) > 700:
                    break
                x = random.uniform(200, WORLD - 200)
                y = random.uniform(200, WORLD - 200)
        wand = random.random() * math.pi * 2
        if abs(wand) < 0.2:
            wand = random.uniform(0.5, math.pi * 2 - 0.5)
        sid = f"s{self.area_index}_{self.next_enemy_id}"
        self.next_enemy_id += 1
        self.enemy_ents[sid] = {
            "id": sid,
            "t": typ["t"],
            "x": x,
            "y": y,
            "h": typ["h"],
            "m": typ["h"],
            "ang": wand,
            "w": wand,
            "g": 0,
            "r": typ["r"],
            "s": typ["s"],
            "d": typ["d"],
            "c": typ["c"],
            "xp": typ["xp"],
            "aggro_until": 0.0,
            "retarget": 0.0,
        }

    def nearest_pilot(self, x: float, y: float) -> Optional[Tuple[str, float, float, float]]:
        best = None
        best_d = 1e18
        for nick, c in self.clients.items():
            s = c.state
            px = float(s.get("x") or 0)
            py = float(s.get("y") or 0)
            d = math.hypot(px - x, py - y)
            if d < best_d:
                best_d = d
                best = (nick, px, py, d)
        return best

    def tick_enemies(self, dt: float) -> None:
        if not self.clients:
            return
        self.ensure_enemies()
        now = time.time()
        margin = 80.0
        dead_ids: List[str] = []
        for sid, e in list(self.enemy_ents.items()):
            if e["h"] <= 0:
                dead_ids.append(sid)
                continue
            # Never allow east-stuck heading
            if abs(float(e.get("w") or 0)) < 0.05:
                e["w"] = random.uniform(0.4, math.pi * 2 - 0.4)
            if abs(float(e.get("ang") or 0)) < 0.05:
                e["ang"] = e["w"]

            pilot = self.nearest_pilot(e["x"], e["y"])
            aggro = e.get("aggro_until", 0) > now
            if pilot and pilot[3] < 550:
                aggro = True
                e["aggro_until"] = now + 8.0
            e["g"] = 1 if aggro else 0

            speed = float(e["s"])
            if aggro and pilot:
                tx, ty = pilot[1], pilot[2]
                desired = math.atan2(ty - e["y"], tx - e["x"])
                e["w"] = desired
                e["ang"] = desired
                # Keep some range
                dist = pilot[3]
                if dist > 160:
                    e["x"] += math.cos(desired) * speed * dt
                    e["y"] += math.sin(desired) * speed * dt
                elif dist < 90:
                    e["x"] -= math.cos(desired) * speed * 0.55 * dt
                    e["y"] -= math.sin(desired) * speed * 0.55 * dt
                else:
                    # strafe
                    e["x"] += math.cos(desired + math.pi / 2) * speed * 0.35 * dt
                    e["y"] += math.sin(desired + math.pi / 2) * speed * 0.35 * dt
            else:
                # Patrol on wander heading (not lagging angle)
                if random.random() < 0.35 * dt:
                    e["w"] = float(e["w"]) + (random.random() - 0.5) * 2.2
                if random.random() < 0.08 * dt:
                    e["w"] = random.random() * math.pi * 2
                e["ang"] = e["w"]
                e["x"] += math.cos(e["w"]) * speed * 0.3 * dt
                e["y"] += math.sin(e["w"]) * speed * 0.3 * dt

            # Soft bounds — steer inward
            if e["x"] < margin or e["x"] > WORLD - margin or e["y"] < margin or e["y"] > WORLD - margin:
                e["w"] = math.atan2(WORLD / 2 - e["y"], WORLD / 2 - e["x"]) + (random.random() - 0.5) * 0.6
                e["ang"] = e["w"]
            e["x"] = max(40.0, min(WORLD - 40.0, e["x"]))
            e["y"] = max(40.0, min(WORLD - 40.0, e["y"]))

        for sid in dead_ids:
            self.enemy_ents.pop(sid, None)
            self.kills[sid] = time.time() * 1000

        # Respawn toward cap
        cap = enemy_cap(self.area_index)
        while len(self.enemy_ents) < cap:
            self.spawn_one()

        self.broadcast_enemies()

    def apply_hit(self, nick: str, msg: dict) -> None:
        sync_id = str(msg.get("syncId") or "")
        kind = msg.get("kind")
        dmg = max(0.0, float(msg.get("dmg") or 0))
        # Debris / rocks still relayed (legacy)
        if sync_id.startswith("d") or kind in ("debris", "rockCollect"):
            payload = {
                "t": "hit",
                "by": nick,
                "syncId": sync_id,
                "hp": max(0, float(msg.get("hp") or 0)),
                "dmg": dmg,
                "kill": bool(msg.get("kill")),
                "kind": kind,
                "x": msg.get("x"),
                "y": msg.get("y"),
                "ts": int(time.time() * 1000),
            }
            if payload["kill"] and sync_id.startswith("d"):
                self.debris_kills[sync_id] = time.time() * 1000
            self.broadcast(payload, None)
            return

        e = self.enemy_ents.get(sync_id)
        if not e:
            # Unknown id — still ack so clients don't soft-lock FX
            self.broadcast(
                {
                    "t": "hit",
                    "by": nick,
                    "syncId": sync_id,
                    "hp": 0,
                    "dmg": dmg,
                    "kill": True,
                    "kind": kind,
                    "x": msg.get("x"),
                    "y": msg.get("y"),
                    "ts": int(time.time() * 1000),
                },
                None,
            )
            return

        e["h"] = max(0.0, float(e["h"]) - dmg)
        e["g"] = 1
        e["aggro_until"] = time.time() + 10.0
        kill = e["h"] <= 0
        if kill:
            self.kills[sync_id] = time.time() * 1000
            self.enemy_ents.pop(sync_id, None)
        self.broadcast(
            {
                "t": "hit",
                "by": nick,
                "syncId": sync_id,
                "hp": int(e["h"]) if not kill else 0,
                "dmg": dmg,
                "kill": kill,
                "kind": kind,
                "x": e.get("x", msg.get("x")),
                "y": e.get("y", msg.get("y")),
                "ts": int(time.time() * 1000),
            },
            None,
        )

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
            "activeTitle": str(msg.get("activeTitle") or "")[:48] or None,
            "lastActive": time.time() * 1000,
        }
        self.clients[nick] = client
        if not self.host or self.host not in self.clients:
            self.pick_host()
        else:
            client.is_host = client.nick == self.host
        self.ensure_enemies()
        self.rebuild_enemy_snap()
        client.send(
            {
                "t": "welcome",
                "areaIndex": self.area_index,
                "host": self.host,
                "youAreHost": client.is_host,
                "serverEnemies": True,
                "players": self.snapshot_players(),
            }
        )
        self.broadcast({"t": "join", "nick": nick, "player": {"nick": nick, **client.state}}, nick)
        # Always send server enemy world to joiner
        client.send(self.enemy_payload())
        if self.debris is not None:
            client.send(
                {
                    "t": "debris",
                    "updatedAt": self.last_debris_at,
                    "host": self.host,
                    "areaIndex": self.area_index,
                    "debris": self.debris,
                    "kills": self.debris_kills,
                    "rocks": self.rocks,
                    "full": 1,
                }
            )
        return client

    def leave(self, nick: str) -> None:
        if nick not in self.clients:
            return
        self.clients.pop(nick, None)
        self.broadcast({"t": "leave", "nick": nick})
        if not self.clients:
            # Keep room enemies briefly? Drop room entirely — respawn on next join.
            rooms.pop(self.area_index, None)
            return
        if self.host == nick:
            # Do NOT wipe server enemies on host leave
            self.debris = None
            self.rocks = {}
            self.debris_kills = {}
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
        if "activeTitle" in msg:
            title = str(msg.get("activeTitle") or "").strip()[:48]
            s["activeTitle"] = title or None
        s["lastActive"] = time.time() * 1000
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
                "activeTitle": s.get("activeTitle"),
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

    def on_enemies(self, nick: str, msg: dict) -> None:
        # Clients no longer own enemies — ignore publishes (compat with old builds).
        return

    def on_debris(self, nick: str, msg: dict) -> None:
        c = self.clients.get(nick)
        if not c or not c.is_host:
            return
        debris = msg.get("debris")
        self.debris = debris if isinstance(debris, dict) else {}
        rocks = msg.get("rocks")
        self.rocks = rocks if isinstance(rocks, dict) else {}
        kills = msg.get("kills")
        if isinstance(kills, dict):
            self.debris_kills.update(kills)
        now = time.time() * 1000
        self.debris_kills = {k: v for k, v in self.debris_kills.items() if now - float(v) < 3000}
        self.last_debris_at = now
        self.broadcast(
            {
                "t": "debris",
                "updatedAt": now,
                "host": self.host,
                "areaIndex": self.area_index,
                "debris": self.debris,
                "kills": self.debris_kills,
                "rocks": self.rocks,
                "full": 1,
            },
            nick,
        )


rooms: Dict[int, SectorRoom] = {}
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
            room.apply_hit(nick, msg)
        elif t == "enemies":
            room.on_enemies(nick, msg)
        elif t == "debris":
            room.on_debris(nick, msg)
        elif t == "syncPlease":
            room.ensure_enemies()
            room.rebuild_enemy_snap()
            room.clients[nick].send(room.enemy_payload())
            if room.debris is not None:
                room.clients[nick].send(
                    {
                        "t": "debris",
                        "updatedAt": room.last_debris_at,
                        "host": room.host,
                        "areaIndex": room.area_index,
                        "debris": room.debris,
                        "kills": room.debris_kills,
                        "rocks": room.rocks,
                        "full": 1,
                    }
                )
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
    last_enemy = 0.0
    last_players = 0.0
    while True:
        time.sleep(0.02)
        now = time.time()
        with lock:
            if now - last_enemy >= ENEMY_DT:
                last_enemy = now
                for room in list(rooms.values()):
                    if room.clients:
                        room.tick_enemies(ENEMY_DT)
            if now - last_players >= PLAYERS_DT:
                last_players = now
                for room in list(rooms.values()):
                    if room.clients:
                        room.broadcast(
                            {
                                "t": "players",
                                "players": room.snapshot_players(),
                                "host": room.host,
                                "serverEnemies": True,
                            }
                        )


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
            enemies = sum(len(r.enemy_ents) for r in rooms.values())
            body = json.dumps(
                {"ok": True, "rooms": len(rooms), "players": players, "enemies": enemies, "serverEnemies": True}
            )
        resp = (
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            f"Access-Control-Allow-Origin: *\r\nContent-Length: {len(body)}\r\n\r\n{body}"
        )
    else:
        body = "Star Raiders MP — server-owned enemies — connect via WebSocket\n"
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
    print(
        f"[Star Raiders MP] ws://0.0.0.0:{PORT}  enemyHz={ENEMY_HZ}  SERVER-OWNED ENEMIES"
    )
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
