#!/usr/bin/env python3
"""
Star Raiders — realtime sector sync server (Python 3.10+, stdlib only)

Rooms are per areaIndex.
SERVER-OWNED: enemies (incl. bosses/minions), asteroids/debris, ore rocks, loot boxes.
Clients mirror snapshots and send hit / rockCollect intents.
Env: PORT=8787  ENEMY_HZ=20

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
ENEMY_HZ = float(os.environ.get("ENEMY_HZ", "20"))
ENEMY_DT = max(1.0 / 30.0, 1.0 / max(1.0, ENEMY_HZ))
DEBRIS_HZ = float(os.environ.get("DEBRIS_HZ", "8"))
DEBRIS_DT = max(1.0 / 15.0, 1.0 / max(1.0, DEBRIS_HZ))
PLAYERS_HZ = 4.0
PLAYERS_DT = 1.0 / PLAYERS_HZ

WORLD = 6000.0
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_HIT_DMG = 25000.0
MAX_HITS_PER_SEC = 28
ASTEROID_MAX_HP = 200.0

# Full client ft[] parity (radius = size/2). Boss=14, minion=15.
ENEMY_TYPES = [
    {"t": 0, "h": 40, "s": 100, "d": 10, "xp": 15, "c": 10, "r": 20, "fr": 0.5},
    {"t": 1, "h": 150, "s": 60, "d": 25, "xp": 45, "c": 30, "r": 30, "fr": 1.0},
    {"t": 2, "h": 400, "s": 120, "d": 60, "xp": 100, "c": 60, "r": 25, "fr": 1.2},
    {"t": 3, "h": 1000, "s": 80, "d": 150, "xp": 250, "c": 150, "r": 40, "fr": 1.5},
    {"t": 4, "h": 2500, "s": 140, "d": 350, "xp": 600, "c": 350, "r": 30, "fr": 2.0},
    {"t": 5, "h": 6000, "s": 90, "d": 600, "xp": 1200, "c": 700, "r": 50, "fr": 2.2},
    {"t": 6, "h": 12000, "s": 150, "d": 1000, "xp": 2500, "c": 1500, "r": 22, "fr": 2.5},
    {"t": 7, "h": 22000, "s": 110, "d": 1500, "xp": 4500, "c": 2800, "r": 32, "fr": 2.8},
    {"t": 8, "h": 45000, "s": 160, "d": 2500, "xp": 8000, "c": 5000, "r": 27, "fr": 3.0},
    {"t": 9, "h": 90000, "s": 85, "d": 4500, "xp": 18000, "c": 10000, "r": 45, "fr": 3.5},
    {"t": 10, "h": 180000, "s": 170, "d": 8000, "xp": 35000, "c": 20000, "r": 35, "fr": 4.0},
    {"t": 11, "h": 350000, "s": 100, "d": 15000, "xp": 75000, "c": 40000, "r": 55, "fr": 4.5},
    {"t": 12, "h": 750000, "s": 200, "d": 25000, "xp": 150000, "c": 80000, "r": 30, "fr": 5.0},
    {"t": 13, "h": 1500000, "s": 120, "d": 40000, "xp": 300000, "c": 150000, "r": 60, "fr": 6.0},
    {"t": 14, "h": 120000, "s": 45, "d": 1200, "xp": 15000, "c": 10000, "r": 150, "fr": 3.5, "boss": 1},
    {"t": 15, "h": 3000, "s": 220, "d": 300, "xp": 200, "c": 100, "r": 30, "fr": 2.5, "minion": 1},
]
BOSS_TYPE = ENEMY_TYPES[14]
MINION_TYPE = ENEMY_TYPES[15]
ORE_TYPES = (("iron", 2), ("gold", 3), ("crystal", 4))
BOSS_ZONES = frozenset((13, 14, 15))
# Visual area index for debris counts in boss zones (matches client Fe.baseVisualIndex)
BOSS_VISUAL = {13: 3, 14: 6, 15: 9}


def is_boss_zone(area: int) -> bool:
    return int(area) in BOSS_ZONES


def enemy_cap(area: int) -> int:
    if is_boss_zone(area):
        return 0  # boss + minions managed separately
    return 75 if area == 0 else 55


def type_for_area(area: int) -> dict:
    if area <= 0:
        return ENEMY_TYPES[0] if random.random() < 0.7 else ENEMY_TYPES[1]
    # Match client: min(12, area) and next tier (not bosses)
    de = min(12, max(0, area))
    a = ENEMY_TYPES[de]
    b = ENEMY_TYPES[min(12, de + 1)]
    return a if random.random() < 0.6 else b


def debris_count_for_area(area: int) -> int:
    if is_boss_zone(area):
        return 0
    o = area
    if o == 0:
        return 80
    if o in (2, 7, 10, 11):
        return 10
    return 40


def clamp_dmg(raw: float) -> float:
    if not math.isfinite(raw):
        return 0.0
    return max(0.0, min(MAX_HIT_DMG, float(raw)))


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
    __slots__ = ("sock", "nick", "is_host", "state", "buf", "alive", "hit_times")

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.nick: Optional[str] = None
        self.is_host = False
        self.state: Dict[str, Any] = {}
        self.buf = bytearray()
        self.alive = True
        self.hit_times: List[float] = []

    def send(self, obj: Any) -> None:
        if not self.alive:
            return
        raw = obj if isinstance(obj, str) else json.dumps(obj, separators=(",", ":"))
        try:
            self.sock.sendall(ws_encode(raw))
        except OSError:
            self.alive = False

    def allow_hit(self) -> bool:
        now = time.time()
        self.hit_times = [t for t in self.hit_times if now - t < 1.0]
        if len(self.hit_times) >= MAX_HITS_PER_SEC:
            return False
        self.hit_times.append(now)
        return True


class SectorRoom:
    def __init__(self, area_index: int):
        self.area_index = area_index
        self.clients: Dict[str, Client] = {}
        self.host: Optional[str] = None  # legacy UI only
        self.enemy_ents: Dict[str, dict] = {}
        self.enemies: Dict[str, dict] = {}
        self.enemy_shots: list = []
        self.kills: Dict[str, float] = {}
        self.last_enemy_at = 0.0
        self.last_enemy_seq = 0
        self.next_enemy_id = 1
        # (ready_at, avoid_x, avoid_y)
        self.respawn_queue: List[Tuple[float, float, float]] = []
        self.recent_deaths: List[Tuple[float, float, float]] = []
        self.last_boss_kill = 0.0
        self.debris_ents: Dict[str, dict] = {}
        self.rock_ents: Dict[str, dict] = {}
        self.loot_ents: Dict[str, dict] = {}
        self.debris: Dict[str, dict] = {}
        self.rocks: Dict[str, dict] = {}
        self.loot: Dict[str, dict] = {}
        self.debris_kills: Dict[str, float] = {}
        self.loot_kills: Dict[str, float] = {}
        self.last_debris_at = 0.0
        self.last_debris_seq = 0
        self.next_debris_id = 1
        self.next_rock_id = 1
        self.next_loot_id = 1
        self._spawned = False
        self._debris_spawned = False
        self.pending_collect: Dict[str, float] = {}

    def pick_host(self) -> None:
        # Legacy sticky host for old clients only — world is server-owned.
        if self.host and self.host in self.clients:
            for c in self.clients.values():
                c.is_host = c.nick == self.host
            return
        nicks = sorted(self.clients.keys())
        self.host = nicks[0] if nicks else None
        for c in self.clients.values():
            c.is_host = c.nick == self.host
            c.send(self._host_msg(c))

    def _host_msg(self, c: Client) -> dict:
        return {
            "t": "host",
            "host": self.host,
            "youAreHost": c.is_host,
            "serverEnemies": True,
            "serverDebris": True,
        }

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

    # ── enemies ──────────────────────────────────────────────

    def serialize_enemy(self, e: dict) -> dict:
        ang = float(e.get("ang") or e.get("w") or 1.0)
        wand = float(e.get("w") or ang)
        if abs(ang) < 0.05:
            ang = wand if abs(wand) >= 0.05 else random.random() * math.pi * 2
        if abs(wand) < 0.05:
            wand = ang
        out = {
            "t": int(e["t"]),
            "x": round(float(e["x"]), 1),
            "y": round(float(e["y"]), 1),
            "vx": round(float(e.get("vx") or 0), 1),
            "vy": round(float(e.get("vy") or 0), 1),
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
        ds = int(e.get("drones_to_spawn") or 0)
        if ds:
            out["ds"] = ds
        return out

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
            "serverDebris": True,
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
        if is_boss_zone(self.area_index):
            self.ensure_boss()
            self._spawned = True
            return
        # Seed once only — refills use delayed respawn queue (no same-spot instant reset).
        if self._spawned:
            return
        self._spawned = True
        self.seed_enemies()

    def ensure_boss(self) -> None:
        now = time.time()
        has_boss = any(int(e.get("t") or 0) == 14 for e in self.enemy_ents.values())
        if has_boss:
            return
        if self.last_boss_kill and now - self.last_boss_kill < 30.0:
            return
        self.spawn_typed(BOSS_TYPE, WORLD / 2, WORLD / 2 - 400)

    def seed_enemies(self) -> None:
        cap = enemy_cap(self.area_index)
        need = max(0, cap - len(self.enemy_ents))
        for _ in range(need):
            self.spawn_one()
        self.rebuild_enemy_snap()

    def spawn_typed(self, typ: dict, x: float, y: float, *, ang: Optional[float] = None) -> str:
        wand = ang if ang is not None else random.random() * math.pi * 2
        if abs(wand) < 0.2:
            wand = random.uniform(0.5, math.pi * 2 - 0.5)
        sid = f"s{self.area_index}_{self.next_enemy_id}"
        self.next_enemy_id += 1
        self.enemy_ents[sid] = {
            "id": sid,
            "t": typ["t"],
            "x": float(x),
            "y": float(y),
            "vx": 0.0,
            "vy": 0.0,
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
            "fr": float(typ.get("fr") or 1.0),
            "aggro_until": 0.0,
            "retarget": 0.0,
            "last_fire": 0.0,
            "drones_to_spawn": 0,
            "last_drone_spawn": 0.0,
            "boss": 1 if typ.get("boss") else 0,
            "minion": 1 if typ.get("minion") else 0,
        }
        return sid

    def _spawn_point_ok(self, x: float, y: float, avoid_x: Optional[float], avoid_y: Optional[float]) -> bool:
        if self.area_index == 0 and math.hypot(x - WORLD / 2, y - WORLD / 2) < 700:
            return False
        if avoid_x is not None and avoid_y is not None and math.hypot(x - avoid_x, y - avoid_y) < 900:
            return False
        now = time.time()
        self.recent_deaths = [d for d in self.recent_deaths if d[2] > now]
        for dx, dy, _u in self.recent_deaths:
            if math.hypot(x - dx, y - dy) < 750:
                return False
        for c in self.clients.values():
            s = c.state
            if math.hypot(x - float(s.get("x") or 0), y - float(s.get("y") or 0)) < 550:
                return False
        for e in self.enemy_ents.values():
            if math.hypot(x - float(e["x"]), y - float(e["y"])) < 220:
                return False
        return True

    def spawn_one(self, avoid_x: Optional[float] = None, avoid_y: Optional[float] = None) -> None:
        typ = type_for_area(self.area_index)
        x = random.uniform(200, WORLD - 200)
        y = random.uniform(200, WORLD - 200)
        for _ in range(28):
            cx = random.uniform(200, WORLD - 200)
            cy = random.uniform(200, WORLD - 200)
            if self._spawn_point_ok(cx, cy, avoid_x, avoid_y):
                x, y = cx, cy
                break
        self.spawn_typed(typ, x, y)

    def spawn_minion(self, boss: dict) -> None:
        side = 1 if random.random() < 0.5 else -1
        ye = float(boss["ang"]) + math.pi / 2 * side
        x = float(boss["x"]) + math.cos(ye) * (float(boss["r"]) + 30)
        y = float(boss["y"]) + math.sin(ye) * (float(boss["r"]) + 30)
        sid = self.spawn_typed(MINION_TYPE, x, y, ang=ye)
        e = self.enemy_ents[sid]
        e["g"] = 1
        e["aggro_until"] = time.time() + 20.0

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
            if abs(float(e.get("w") or 0)) < 0.05:
                e["w"] = random.uniform(0.4, math.pi * 2 - 0.4)
            if abs(float(e.get("ang") or 0)) < 0.05:
                e["ang"] = e["w"]

            ox, oy = float(e["x"]), float(e["y"])
            pilot = self.nearest_pilot(e["x"], e["y"])
            aggro = float(e.get("aggro_until") or 0) > now
            e["g"] = 1 if aggro else 0
            speed = float(e["s"])
            is_boss = int(e.get("t") or 0) == 14

            if is_boss and int(e.get("drones_to_spawn") or 0) > 0:
                if now - float(e.get("last_drone_spawn") or 0) >= 2.5:
                    e["last_drone_spawn"] = now
                    e["drones_to_spawn"] = max(0, int(e["drones_to_spawn"]) - 2)
                    self.spawn_minion(e)
                    self.spawn_minion(e)

            if aggro and pilot:
                tx, ty = pilot[1], pilot[2]
                desired = math.atan2(ty - e["y"], tx - e["x"])
                e["w"] = desired
                e["ang"] = desired
                dist = pilot[3]
                if dist > 160:
                    e["x"] += math.cos(desired) * speed * dt
                    e["y"] += math.sin(desired) * speed * dt
                elif dist < 90:
                    e["x"] -= math.cos(desired) * speed * 0.55 * dt
                    e["y"] -= math.sin(desired) * speed * 0.55 * dt
                else:
                    e["x"] += math.cos(desired + math.pi / 2) * speed * 0.35 * dt
                    e["y"] += math.sin(desired + math.pi / 2) * speed * 0.35 * dt
            else:
                if random.random() < 0.35 * dt:
                    e["w"] = float(e["w"]) + (random.random() - 0.5) * 2.2
                if random.random() < 0.08 * dt:
                    e["w"] = random.random() * math.pi * 2
                e["ang"] = e["w"]
                e["x"] += math.cos(e["w"]) * speed * 0.3 * dt
                e["y"] += math.sin(e["w"]) * speed * 0.3 * dt

            if e["x"] < margin or e["x"] > WORLD - margin or e["y"] < margin or e["y"] > WORLD - margin:
                e["w"] = math.atan2(WORLD / 2 - e["y"], WORLD / 2 - e["x"]) + (random.random() - 0.5) * 0.6
                e["ang"] = e["w"]
            e["x"] = max(40.0, min(WORLD - 40.0, e["x"]))
            e["y"] = max(40.0, min(WORLD - 40.0, e["y"]))
            if dt > 1e-6:
                e["vx"] = (float(e["x"]) - ox) / dt
                e["vy"] = (float(e["y"]) - oy) / dt
            else:
                e["vx"] = 0.0
                e["vy"] = 0.0

            if aggro and pilot and pilot[3] < 480:
                fr = max(0.35, float(e.get("fr") or 1.0))
                if now - float(e.get("last_fire") or 0) >= 1.0 / fr:
                    e["last_fire"] = now
                    aim = math.atan2(pilot[2] - e["y"], pilot[1] - e["x"])
                    if random.random() < 0.18:
                        aim += (1 if random.random() < 0.5 else -1) * (0.22 + random.random() * 0.18)
                    spd = 400.0
                    self._shot_seq = getattr(self, "_shot_seq", 0) + 1
                    self.enemy_shots.append(
                        {
                            "id": f"{self.area_index}:SERVER:{int(now*1000)}:{self._shot_seq}",
                            "areaIndex": self.area_index,
                            "x": float(e["x"]),
                            "y": float(e["y"]),
                            "vx": math.cos(aim) * spd,
                            "vy": math.sin(aim) * spd,
                            "a": aim,
                            "damage": int(e["d"]),
                            "assetId": "laser_enemy",
                            "createdAt": now * 1000,
                        }
                    )
                    if len(self.enemy_shots) > 80:
                        self.enemy_shots = self.enemy_shots[-80:]

        for sid in dead_ids:
            self.enemy_ents.pop(sid, None)

        # Delayed respawns (normal sectors only) — away from death spot
        if not is_boss_zone(self.area_index):
            cap = enemy_cap(self.area_index)
            self.respawn_queue.sort(key=lambda item: item[0] if isinstance(item, (list, tuple)) else item)
            while self.respawn_queue and len(self.enemy_ents) < cap:
                item = self.respawn_queue[0]
                ready = item[0] if isinstance(item, (list, tuple)) else item
                if ready > now:
                    break
                self.respawn_queue.pop(0)
                ax = item[1] if isinstance(item, (list, tuple)) and len(item) > 1 else None
                ay = item[2] if isinstance(item, (list, tuple)) and len(item) > 2 else None
                self.spawn_one(ax, ay)
            if len(self.enemy_ents) >= cap:
                self.respawn_queue = [
                    item for item in self.respawn_queue
                    if (item[0] if isinstance(item, (list, tuple)) else item) > now
                ]
        else:
            self.ensure_boss()

        now_ms = time.time() * 1000
        self.enemy_shots = [sh for sh in self.enemy_shots if now_ms - float(sh.get("createdAt") or 0) < 3500]
        self.broadcast_enemies()

    # ── debris / rocks ───────────────────────────────────────

    def ensure_debris(self) -> None:
        if self._debris_spawned and (self.debris_ents or self.loot_ents or is_boss_zone(self.area_index)):
            return
        self._debris_spawned = True
        self.seed_debris()
        self.seed_rocks()
        self.seed_loot()
        self.rebuild_debris_snap()

    def seed_debris(self) -> None:
        need = debris_count_for_area(self.area_index)
        assets = ["asteroid_1", "asteroid_2", "space_debris"]
        area = self.area_index
        for _ in range(need):
            if area in (0, 4):
                asset = assets[random.randint(0, 1)]
            else:
                asset = assets[random.randint(0, 2)]
            mineable = asset in ("asteroid_1", "asteroid_2")
            sid = f"d{self.area_index}_S_{self.next_debris_id}"
            self.next_debris_id += 1
            self.debris_ents[sid] = {
                "id": sid,
                "x": random.uniform(0, WORLD),
                "y": random.uniform(0, WORLD),
                "vx": (random.random() - 0.5) * 10,
                "vy": (random.random() - 0.5) * 10,
                "a": random.random() * math.pi * 2,
                "rs": (random.random() - 0.5) * 0.5,
                "sc": 0.5 + random.random() * 1.5,
                "asset": asset,
                "h": ASTEROID_MAX_HP if mineable else 0,
                "m": ASTEROID_MAX_HP if mineable else 0,
                "mn": 1 if mineable else 0,
            }

    def seed_rocks(self) -> None:
        # Match sticky-host setupRocks: 60 free-floating ore (also in boss zones).
        for _ in range(60):
            self.spawn_rock(
                random.uniform(0, WORLD),
                random.uniform(0, WORLD),
                (random.random() - 0.5) * 5,
                (random.random() - 0.5) * 5,
            )

    def spawn_rock(self, x: float, y: float, vx: float = 0.0, vy: float = 0.0, scale: Optional[float] = None) -> str:
        typ = random.choice(ORE_TYPES)
        sid = f"r{self.area_index}_S_{self.next_rock_id}"
        self.next_rock_id += 1
        sc = scale if scale is not None else (0.4 + random.random() * 0.4)
        self.rock_ents[sid] = {
            "id": sid,
            "x": float(x),
            "y": float(y),
            "vx": float(vx),
            "vy": float(vy),
            "a": random.random() * math.pi * 2,
            "rs": (random.random() - 0.5) * 0.5,
            "sc": sc,
            "t": typ[0],
            "v": typ[1],
        }
        return sid

    def ore_yield_for(self, d: dict) -> int:
        sc = float(d.get("sc") or 1)
        # Rough match client getAsteroidOreYield scale mapping ~1–12
        n = int(1 + sc * 4 + random.random() * 4)
        return max(1, min(12, n))

    def serialize_debris(self, d: dict) -> dict:
        return {
            "x": round(float(d["x"]), 1),
            "y": round(float(d["y"]), 1),
            "vx": round(float(d.get("vx") or 0), 2),
            "vy": round(float(d.get("vy") or 0), 2),
            "a": round(float(d.get("a") or 0), 2),
            "rs": round(float(d.get("rs") or 0), 3),
            "sc": round(float(d.get("sc") or 1), 2),
            "id": d.get("asset") or "asteroid_1",
            "h": int(max(0, d.get("h") or 0)),
            "m": int(max(0, d.get("m") or 0)),
            "mn": 1 if d.get("mn") else 0,
        }

    def serialize_rock(self, r: dict) -> dict:
        return {
            "x": round(float(r["x"]), 1),
            "y": round(float(r["y"]), 1),
            "vx": round(float(r.get("vx") or 0), 2),
            "vy": round(float(r.get("vy") or 0), 2),
            "a": round(float(r.get("a") or 0), 2),
            "rs": round(float(r.get("rs") or 0), 3),
            "sc": round(float(r.get("sc") or 1), 2),
            "t": r.get("t") or "iron",
            "v": int(r.get("v") or 2),
        }

    def seed_loot(self) -> None:
        # Match client setupLootBoxes: always 12 supply crates per sector.
        for _ in range(12):
            sid = f"l{self.area_index}_S_{self.next_loot_id}"
            self.next_loot_id += 1
            ang = (random.random() - 0.5) * 0.4
            self.loot_ents[sid] = {
                "id": sid,
                "x": random.uniform(0, WORLD),
                "y": random.uniform(0, WORLD),
                "vx": (random.random() - 0.5) * 2.5,
                "vy": (random.random() - 0.5) * 2.5,
                "a": ang,
                "ba": ang,
                "rs": 0.6 + random.random() * 0.5,
                "sc": 1.05 + random.random() * 0.25,
                "pu": random.random() * math.pi * 2,
                "r": 16,
            }

    def serialize_loot(self, b: dict) -> dict:
        return {
            "x": round(float(b["x"]), 1),
            "y": round(float(b["y"]), 1),
            "vx": round(float(b.get("vx") or 0), 2),
            "vy": round(float(b.get("vy") or 0), 2),
            "a": round(float(b.get("a") or 0), 2),
            "ba": round(float(b.get("ba") or 0), 2),
            "rs": round(float(b.get("rs") or 0), 3),
            "sc": round(float(b.get("sc") or 1), 2),
            "pu": round(float(b.get("pu") or 0), 2),
            "r": int(b.get("r") or 16),
        }

    def rebuild_debris_snap(self) -> None:
        self.debris = {sid: self.serialize_debris(d) for sid, d in self.debris_ents.items()}
        self.rocks = {sid: self.serialize_rock(r) for sid, r in self.rock_ents.items()}
        self.loot = {sid: self.serialize_loot(b) for sid, b in self.loot_ents.items()}
        self.last_debris_seq += 1
        self.last_debris_at = time.time() * 1000

    def debris_payload(self) -> dict:
        now = time.time() * 1000
        self.debris_kills = {k: v for k, v in self.debris_kills.items() if now - float(v) < 3000}
        self.loot_kills = {k: v for k, v in self.loot_kills.items() if now - float(v) < 3000}
        return {
            "t": "debris",
            "updatedAt": self.last_debris_at or now,
            "seq": self.last_debris_seq or None,
            "host": "SERVER",
            "serverDebris": True,
            "serverLoot": True,
            "areaIndex": self.area_index,
            "debris": self.debris,
            "kills": self.debris_kills,
            "rocks": self.rocks,
            "loot": self.loot,
            "lootKills": self.loot_kills,
            "full": 1,
        }

    def broadcast_debris(self) -> None:
        if not self.clients:
            return
        self.rebuild_debris_snap()
        self.broadcast(self.debris_payload(), None)

    def tick_debris(self, dt: float) -> None:
        if not self.clients:
            return
        self.ensure_debris()
        for d in self.debris_ents.values():
            d["x"] = float(d["x"]) + float(d.get("vx") or 0) * dt
            d["y"] = float(d["y"]) + float(d.get("vy") or 0) * dt
            d["a"] = float(d.get("a") or 0) + float(d.get("rs") or 0) * dt
            if d["x"] < -100:
                d["x"] += WORLD + 200
            elif d["x"] > WORLD + 100:
                d["x"] -= WORLD + 200
            if d["y"] < -100:
                d["y"] += WORLD + 200
            elif d["y"] > WORLD + 100:
                d["y"] -= WORLD + 200
        for r in self.rock_ents.values():
            r["x"] = float(r["x"]) + float(r.get("vx") or 0) * dt
            r["y"] = float(r["y"]) + float(r.get("vy") or 0) * dt
            r["a"] = float(r.get("a") or 0) + float(r.get("rs") or 0) * dt
            if r["x"] < -100:
                r["x"] += WORLD + 200
            elif r["x"] > WORLD + 100:
                r["x"] -= WORLD + 200
            if r["y"] < -100:
                r["y"] += WORLD + 200
            elif r["y"] > WORLD + 100:
                r["y"] -= WORLD + 200
        for b in self.loot_ents.values():
            b["x"] = float(b["x"]) + float(b.get("vx") or 0) * dt
            b["y"] = float(b["y"]) + float(b.get("vy") or 0) * dt
            b["pu"] = float(b.get("pu") or 0) + float(b.get("rs") or 0.8) * dt
            b["a"] = float(b.get("ba") or 0) + math.sin(float(b["pu"])) * 0.28
            if b["x"] < -40:
                b["x"] += WORLD + 80
            elif b["x"] > WORLD + 40:
                b["x"] -= WORLD + 80
            if b["y"] < -40:
                b["y"] += WORLD + 80
            elif b["y"] > WORLD + 40:
                b["y"] -= WORLD + 80
        # prune stale collect locks
        now = time.time()
        self.pending_collect = {k: t for k, t in self.pending_collect.items() if now - t < 2.0}
        self.broadcast_debris()

    def roll_loot_reward(self) -> dict:
        roll = random.random()
        if roll < 0.45:
            amt = random.randint(250, 1000)
            return {"type": "credits", "amt": amt, "label": f"+{amt} CR", "color": "#fbbf24"}
        if roll < 0.73:
            amt = random.randint(25, 50)
            return {"type": "ammo_x2", "amt": amt, "label": f"+{amt} x2 AMMO", "color": "#67e8f9"}
        if roll < 0.89:
            amt = random.randint(1, 1000)
            return {"type": "xp", "amt": amt, "label": f"+{amt} XP", "color": "#86efac"}
        if roll < 0.97:
            amt = random.randint(25, 50)
            return {"type": "ammo_x3", "amt": amt, "label": f"+{amt} x3 AMMO", "color": "#c084fc"}
        amt = random.randint(1, 5)
        return {"type": "merits", "amt": amt, "label": f"+{amt} MR", "color": "#fde68a"}

    def apply_loot_collect(self, nick: str, sync_id: str, msg: dict) -> None:
        self.ensure_debris()
        now = time.time()
        if sync_id in self.pending_collect and now - self.pending_collect[sync_id] < 1.2:
            return
        box = self.loot_ents.get(sync_id)
        if not box:
            return
        self.pending_collect[sync_id] = now
        self.loot_ents.pop(sync_id, None)
        self.loot_kills[sync_id] = now * 1000
        reward = self.roll_loot_reward()
        self.broadcast(
            {
                "t": "hit",
                "by": nick,
                "syncId": sync_id,
                "hp": 0,
                "dmg": 0,
                "kill": True,
                "kind": "lootCollect",
                "x": box.get("x", msg.get("x")),
                "y": box.get("y", msg.get("y")),
                "reward": reward,
                "ts": int(now * 1000),
            },
            None,
        )
        self.broadcast_debris()

    def destroy_asteroid(self, sid: str, d: dict) -> None:
        self.debris_ents.pop(sid, None)
        self.debris_kills[sid] = time.time() * 1000
        n = self.ore_yield_for(d)
        for _ in range(n):
            self.spawn_rock(
                float(d["x"]) + (random.random() - 0.5) * 70,
                float(d["y"]) + (random.random() - 0.5) * 70,
                (random.random() - 0.5) * 80,
                (random.random() - 0.5) * 80,
                0.35 + random.random() * 0.25,
            )

    def spawn_enemy_ore(self, x: float, y: float, count: int) -> None:
        for _ in range(max(0, count)):
            self.spawn_rock(
                x + (random.random() - 0.5) * 40,
                y + (random.random() - 0.5) * 40,
                (random.random() - 0.5) * 60,
                (random.random() - 0.5) * 60,
            )

    # ── hits ─────────────────────────────────────────────────

    def apply_hit(self, nick: str, msg: dict) -> None:
        c = self.clients.get(nick)
        if not c or not c.allow_hit():
            return
        sync_id = str(msg.get("syncId") or "")
        kind = msg.get("kind")
        dmg = clamp_dmg(float(msg.get("dmg") or 0))

        if kind == "lootCollect" or sync_id.startswith("l"):
            self.apply_loot_collect(nick, sync_id, msg)
            return

        if kind == "rockCollect" or sync_id.startswith("r"):
            self.apply_rock_collect(nick, sync_id, msg)
            return

        if kind == "debris" or sync_id.startswith("d"):
            self.apply_debris_hit(nick, sync_id, msg, dmg)
            return

        e = self.enemy_ents.get(sync_id)
        if not e:
            # Unknown — ignore (do not fake-kill)
            return
        if dmg <= 0:
            return
        e["h"] = max(0.0, float(e["h"]) - dmg)
        e["g"] = 1
        e["aggro_until"] = time.time() + 14.0
        if int(e.get("t") or 0) == 14 and not e.get("drones_armed"):
            e["drones_armed"] = 1
            e["drones_to_spawn"] = 12
        kill = e["h"] <= 0
        hp_left = 0 if kill else int(e["h"])
        ex, ey = float(e.get("x") or 0), float(e.get("y") or 0)
        if kill:
            was_boss = int(e.get("t") or 0) == 14
            self.kills[sync_id] = time.time() * 1000
            self.enemy_ents.pop(sync_id, None)
            self.recent_deaths.append((ex, ey, time.time() + 12.0))
            if was_boss:
                self.last_boss_kill = time.time()
            elif not is_boss_zone(self.area_index):
                self.respawn_queue.append((time.time() + 5.0, ex, ey))
            # Ore drops from kills (server-owned)
            self.spawn_enemy_ore(ex, ey, random.randint(1, 3))
            self.broadcast_debris()
        self.broadcast(
            {
                "t": "hit",
                "by": nick,
                "syncId": sync_id,
                "hp": hp_left,
                "dmg": dmg,
                "kill": kill,
                "kind": kind,
                "x": ex,
                "y": ey,
                "ts": int(time.time() * 1000),
            },
            None,
        )
        self.broadcast_enemies()

    def apply_debris_hit(self, nick: str, sync_id: str, msg: dict, dmg: float) -> None:
        self.ensure_debris()
        d = self.debris_ents.get(sync_id)
        if not d or not d.get("mn"):
            return
        # Prefer explicit dmg; else infer from client reported hp (mining path).
        if dmg <= 0:
            reported = msg.get("hp")
            if reported is not None and math.isfinite(float(reported)):
                dmg = clamp_dmg(max(0.0, float(d["h"]) - float(reported)))
            else:
                dmg = 0.0
        # Mining ticks are small; shots can be larger — still clamp.
        dmg = min(dmg, MAX_HIT_DMG)
        if dmg <= 0 and not msg.get("kill"):
            return
        if msg.get("kill") and dmg <= 0:
            dmg = float(d["h"])
        d["h"] = max(0.0, float(d["h"]) - dmg)
        kill = d["h"] <= 0 or bool(msg.get("kill"))
        if kill:
            d["h"] = 0
            self.destroy_asteroid(sync_id, d)
        hp_left = 0 if kill else int(d["h"])
        self.broadcast(
            {
                "t": "hit",
                "by": nick,
                "syncId": sync_id,
                "hp": hp_left,
                "dmg": dmg,
                "kill": kill,
                "kind": "debris",
                "x": d.get("x", msg.get("x")),
                "y": d.get("y", msg.get("y")),
                "ts": int(time.time() * 1000),
            },
            None,
        )
        self.broadcast_debris()

    def apply_rock_collect(self, nick: str, sync_id: str, msg: dict) -> None:
        self.ensure_debris()
        now = time.time()
        if sync_id in self.pending_collect and now - self.pending_collect[sync_id] < 1.2:
            return
        rock = self.rock_ents.get(sync_id)
        if not rock:
            return
        self.pending_collect[sync_id] = now
        self.rock_ents.pop(sync_id, None)
        payload = {
            "t": "hit",
            "by": nick,
            "syncId": sync_id,
            "hp": 1,
            "dmg": 0,
            "kill": False,
            "kind": "rockCollect",
            "x": rock.get("x", msg.get("x")),
            "y": rock.get("y", msg.get("y")),
            "ts": int(time.time() * 1000),
        }
        if msg.get("label") is not None:
            payload["label"] = msg.get("label")
        self.broadcast(payload, None)
        self.broadcast_debris()

    # ── join / leave / relay ─────────────────────────────────

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
            "palLevel": int(msg.get("palLevel") or msg.get("pl") or 0),
            "palX": msg.get("palX") if msg.get("palX") is not None else msg.get("px"),
            "palY": msg.get("palY") if msg.get("palY") is not None else msg.get("py"),
            "palAngle": msg.get("palAngle") if msg.get("palAngle") is not None else msg.get("pa"),
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
        self.ensure_debris()
        self.rebuild_enemy_snap()
        self.rebuild_debris_snap()
        client.send(
            {
                "t": "welcome",
                "areaIndex": self.area_index,
                "host": self.host,
                "youAreHost": client.is_host,
                "serverEnemies": True,
                "serverDebris": True,
                "players": self.snapshot_players(),
            }
        )
        self.broadcast({"t": "join", "nick": nick, "player": {"nick": nick, **client.state}}, nick)
        client.send(self.enemy_payload())
        client.send(self.debris_payload())
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
            # World persists — only reassign legacy host flag.
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
        if "clanTag" in msg:
            tag = str(msg.get("clanTag") or "").strip()[:8]
            s["clanTag"] = tag or None
        dl = msg.get("droneLevels", msg.get("dl"))
        if isinstance(dl, list):
            s["droneLevels"] = [max(1, min(3, int(x) if x is not None else 1)) for x in dl[:16]]
        # P.A.L. pose (accept long or compact keys)
        pl = msg.get("palLevel", msg.get("pl"))
        if pl is not None:
            try:
                s["palLevel"] = max(0, min(4, int(pl)))
            except (TypeError, ValueError):
                pass
            if int(s.get("palLevel") or 0) <= 0:
                s["palX"] = None
                s["palY"] = None
                s["palAngle"] = None
        px = msg.get("palX", msg.get("px"))
        py = msg.get("palY", msg.get("py"))
        pa = msg.get("palAngle", msg.get("pa"))
        if isinstance(px, (int, float)) and isinstance(py, (int, float)):
            s["palX"] = float(px)
            s["palY"] = float(py)
        if isinstance(pa, (int, float)):
            s["palAngle"] = float(pa)
        s["lastActive"] = time.time() * 1000
        out = {
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
            "palLevel": int(s.get("palLevel") or 0),
        }
        if isinstance(s.get("palX"), (int, float)) and isinstance(s.get("palY"), (int, float)):
            out["palX"] = float(s["palX"])
            out["palY"] = float(s["palY"])
            out["px"] = out["palX"]
            out["py"] = out["palY"]
        if isinstance(s.get("palAngle"), (int, float)):
            out["palAngle"] = float(s["palAngle"])
            out["pa"] = out["palAngle"]
        out["pl"] = out["palLevel"]
        if s.get("clanTag"):
            out["clanTag"] = s["clanTag"]
        if isinstance(s.get("droneLevels"), list):
            out["droneLevels"] = s["droneLevels"]
            out["dl"] = s["droneLevels"]
        self.broadcast(out, nick)

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
        return

    def on_debris(self, nick: str, msg: dict) -> None:
        # Clients no longer own debris — ignore publishes.
        return


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
            room.ensure_debris()
            room.rebuild_enemy_snap()
            room.rebuild_debris_snap()
            room.clients[nick].send(room.enemy_payload())
            room.clients[nick].send(room.debris_payload())
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
    last_debris = 0.0
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
            if now - last_debris >= DEBRIS_DT:
                last_debris = now
                for room in list(rooms.values()):
                    if room.clients:
                        room.tick_debris(DEBRIS_DT)
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
                                "serverDebris": True,
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
            debris = sum(len(r.debris_ents) for r in rooms.values())
            rocks = sum(len(r.rock_ents) for r in rooms.values())
            loot = sum(len(r.loot_ents) for r in rooms.values())
            body = json.dumps(
                {
                    "ok": True,
                    "rooms": len(rooms),
                    "players": players,
                    "enemies": enemies,
                    "debris": debris,
                    "rocks": rocks,
                    "loot": loot,
                    "serverEnemies": True,
                    "serverDebris": True,
                    "serverLoot": True,
                }
            )
        resp = (
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            f"Access-Control-Allow-Origin: *\r\nContent-Length: {len(body)}\r\n\r\n{body}"
        )
    else:
        body = "Star Raiders MP — server-owned world — connect via WebSocket\n"
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
        f"[Star Raiders MP] ws://0.0.0.0:{PORT}  enemyHz={ENEMY_HZ}  debrisHz={DEBRIS_HZ}  "
        "SERVER-OWNED ENEMIES+DEBRIS"
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
