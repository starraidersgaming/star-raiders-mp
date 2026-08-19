#!/usr/bin/env python3
"""
Sector Rift — realtime sector sync server (Python 3.10+, stdlib only)

Rooms are per areaIndex.
SERVER-OWNED: enemies (incl. bosses/minions), asteroids/debris, ore rocks, loot boxes.
Clients mirror snapshots and send hit / rockCollect intents.
Env: PORT=8787  ENEMY_HZ=12  DEBRIS_HZ=5

  py -3 server.py
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import random
import re
import select
import smtplib
import socket
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Tuple

PORT = int(os.environ.get("PORT", "8787"))
# Slightly lower defaults cut full-snap bandwidth; clients dead-reckon between ticks.
ENEMY_HZ = float(os.environ.get("ENEMY_HZ", "12"))
ENEMY_DT = max(1.0 / 30.0, 1.0 / max(1.0, ENEMY_HZ))
DEBRIS_HZ = float(os.environ.get("DEBRIS_HZ", "5"))
DEBRIS_DT = max(1.0 / 15.0, 1.0 / max(1.0, DEBRIS_HZ))
PLAYERS_HZ = 4.0
PLAYERS_DT = 1.0 / PLAYERS_HZ

WORLD = 12000.0
BASE_X = WORLD / 2
BASE_Y = WORLD / 2
BASE_RADIUS = 250.0
# Client activePortal uses portal.radius + 100.
PORTAL_RADIUS = 60.0
PORTAL_INSET = 360.0
PORTAL_SAFE_PAD = 100.0
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_HIT_DMG = 25000.0
# Separate buckets so mining (~12.5 Hz) is not starved by laser spam.
MAX_COMBAT_HITS_PER_SEC = 48
MAX_MINE_HITS_PER_SEC = 20
ASTEROID_MAX_HP = 200.0
# Match client getFireConeRange() — enemies shoot/chase on the same 400 cone as players.
# In-range: face + fire immediately, random strafe while staying inside the cone.
# Out of range: chase. Never kite past the cone edge.
PLAYER_FIRE_CONE = 400.0
ENEMY_TOO_CLOSE = 100.0
BOSS_TOO_CLOSE = 140.0
MINION_TOO_CLOSE = 90.0
ENEMY_FLEE_HP = 0.25
# Legacy names kept for any leftover refs; combat uses PLAYER_FIRE_CONE now.
ENEMY_HOLD_MIN = 100.0
ENEMY_HOLD_MAX = 400.0
ENEMY_FIRE_MAX = 400.0
BOSS_HOLD_MIN = 140.0
BOSS_HOLD_MAX = 400.0
BOSS_FIRE_MAX = 400.0
MINION_HOLD_MIN = 90.0
MINION_HOLD_MAX = 400.0
MINION_FIRE_MAX = 400.0
# Bump to force reseed of supply crates to the shared deterministic layout.
LOOT_LAYOUT_VER = 1
FIREBASE_DB = os.environ.get("FIREBASE_DB", "https://star-raiders-659bb-default-rtdb.firebaseio.com").rstrip("/")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
MAIL_FROM = os.environ.get("MAIL_FROM", "Sector Rift <noreply@sectorrift.com>").strip()
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp-mail.outlook.com").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "JumpingGoblinStudios@Outlook.com").strip()
SMTP_PASS = os.environ.get("SMTP_PASS", "").strip()
_recover_last: Dict[str, float] = {}
_recover_lock = threading.Lock()


def area_loot_rnd(area_index: int):
    """Same LCG as client setupLootBoxes / setupStars-style seeding."""
    seed = ((((int(area_index) + 1) * 2654435761) ^ 0xC0A7E001) & 0xFFFFFFFF)

    def rnd() -> float:
        nonlocal seed
        seed = (seed * 1664525 + 1013904223) & 0xFFFFFFFF
        return seed / 4294967296.0

    return rnd

# Full client ft[] parity (radius = size/2). Boss=14, minion=15.
ENEMY_TYPES = [
    {"t": 0, "h": 40, "s": 100, "d": 10, "xp": 15, "c": 10, "r": 20, "fr": 0.5},
    {"t": 1, "h": 150, "s": 60, "d": 25, "xp": 45, "c": 30, "r": 30, "fr": 1.0},
    {"t": 2, "h": 400, "s": 120, "d": 60, "xp": 100, "c": 60, "r": 25, "fr": 1.2},
    {"t": 3, "h": 1000, "s": 80, "d": 150, "xp": 250, "c": 150, "r": 40, "fr": 1.5},
    {"t": 4, "h": 2500, "s": 140, "d": 350, "xp": 600, "c": 350, "r": 30, "fr": 2.0},
    {"t": 5, "h": 6000, "s": 90, "d": 600, "xp": 1200, "c": 700, "r": 50, "fr": 2.2},
    {"t": 6, "h": 12000, "s": 150, "d": 1000, "xp": 2500, "c": 1500, "r": 48, "fr": 2.5},
    {"t": 7, "h": 22000, "s": 110, "d": 1500, "xp": 4500, "c": 2800, "r": 46, "fr": 2.8},
    {"t": 8, "h": 45000, "s": 160, "d": 2500, "xp": 8000, "c": 5000, "r": 50, "fr": 3.0},
    {"t": 9, "h": 90000, "s": 85, "d": 4500, "xp": 18000, "c": 10000, "r": 60, "fr": 3.5},
    {"t": 10, "h": 180000, "s": 170, "d": 8000, "xp": 35000, "c": 20000, "r": 55, "fr": 4.0},
    {"t": 11, "h": 350000, "s": 100, "d": 15000, "xp": 75000, "c": 40000, "r": 60, "fr": 4.5},
    {"t": 12, "h": 750000, "s": 200, "d": 25000, "xp": 150000, "c": 80000, "r": 20, "fr": 5.0},
    {"t": 13, "h": 1500000, "s": 120, "d": 40000, "xp": 300000, "c": 150000, "r": 70, "fr": 6.0},
    {"t": 14, "h": 200000, "s": 45, "d": 60, "xp": 15000, "c": 10000, "r": 150, "fr": 0.7, "boss": 1},
    {"t": 15, "h": 1200, "s": 220, "d": 30, "xp": 200, "c": 100, "r": 30, "fr": 1.0, "minion": 1},
]
BOSS_TYPE = ENEMY_TYPES[14]
MINION_TYPE = ENEMY_TYPES[15]
BOSS_LOOT_SLOTS = ("laser", "shield", "engine", "accuracy", "firerate", "bot", "capacitor")
BOSS_LOOT_COUNT = 3
BOSS_LOOT_SHARE_PCT = 0.05


def roll_boss_rarity() -> str:
    le = random.random()
    if le < 0.04:
        return "Legendary"
    if le < 0.14:
        return "Epic"
    if le < 0.32:
        return "Rare"
    if le < 0.58:
        return "Uncommon"
    return "Common"


def assign_boss_loot(nicks: List[str]) -> Dict[str, List[str]]:
    seen_sets: set = set()
    out: Dict[str, List[str]] = {}
    slots_all = list(BOSS_LOOT_SLOTS)
    for nick in nicks:
        key = str(nick or "").upper()
        if not key or key in out:
            continue
        pieces: List[str] = []
        for _attempt in range(40):
            slots = slots_all[:]
            random.shuffle(slots)
            picked = slots[:BOSS_LOOT_COUNT]
            pieces = [f"{slot}_{roll_boss_rarity().lower()}" for slot in picked]
            sig = tuple(sorted(pieces))
            if sig and sig not in seen_sets:
                seen_sets.add(sig)
                break
        out[key] = pieces
    return out


def boss_loot_eligible(dmg_by: dict, max_hp: float, fallback: str = "") -> List[str]:
    hp = max(1.0, float(max_hp) or 1.0)
    need = BOSS_LOOT_SHARE_PCT * hp
    scored = []
    for raw, dmg in (dmg_by or {}).items():
        nick = str(raw or "").upper()
        amt = float(dmg or 0)
        if nick and amt >= need:
            scored.append((amt, nick))
    scored.sort(reverse=True)
    out = []
    seen = set()
    for _amt, nick in scored:
        if nick not in seen:
            seen.add(nick)
            out.append(nick)
    if not out:
        fb = str(fallback or "").upper()
        if fb:
            out.append(fb)
    return out
ORE_TYPES = (("iron", 2), ("gold", 3), ("crystal", 4))
BOSS_ZONES = frozenset((13, 14, 15))
# Visual area index for debris counts in boss zones (matches client Fe.baseVisualIndex)
BOSS_VISUAL = {13: 3, 14: 6, 15: 9}


def is_boss_zone(area: int) -> bool:
    return int(area) in BOSS_ZONES


def area_portals(area: int) -> List[Tuple[float, float, float]]:
    """Portal centers + radii matching client setupPortals() (safe-zone combat cancel)."""
    a = int(area)
    portals: List[Tuple[float, float, float]] = []
    if is_boss_zone(a):
        portals.append((PORTAL_INSET, WORLD - PORTAL_INSET, PORTAL_RADIUS))
        return portals
    if a > 0:
        portals.append((PORTAL_INSET, WORLD - PORTAL_INSET, PORTAL_RADIUS))
    if a < 12:
        portals.append((WORLD - PORTAL_INSET, PORTAL_INSET, PORTAL_RADIUS))
    boss_gate = {3: 13, 6: 14, 9: 15}
    if a in boss_gate:
        r = 85.0 if boss_gate[a] == 13 else PORTAL_RADIUS
        portals.append((WORLD / 2, WORLD / 2, r))
    return portals


def point_in_safe_zone(area: int, x: float, y: float) -> bool:
    """Home Base (sector 0) or any portal ring — pilots here are combat-safe."""
    a = int(area)
    if a == 0 and math.hypot(float(x) - BASE_X, float(y) - BASE_Y) < BASE_RADIUS:
        return True
    pad = PORTAL_SAFE_PAD
    for px, py, pr in area_portals(a):
        if math.hypot(float(x) - px, float(y) - py) < pr + pad:
            return True
    return False


def enemy_safe_clearance(area: int, x: float, y: float, enemy_r: float = 20.0) -> Optional[Tuple[float, float, float]]:
    """If (x,y) is inside a safe bubble (+ hull), return (cx, cy, edge_r) to push to."""
    a = int(area)
    er = max(12.0, float(enemy_r or 20.0))
    cushion = 40.0
    if a == 0:
        d = math.hypot(float(x) - BASE_X, float(y) - BASE_Y)
        edge = BASE_RADIUS + er + cushion
        if d < edge:
            return (BASE_X, BASE_Y, edge)
    for px, py, pr in area_portals(a):
        edge = float(pr) + PORTAL_SAFE_PAD + er + cushion
        d = math.hypot(float(x) - px, float(y) - py)
        if d < edge:
            return (px, py, edge)
    return None


def nearest_safe_zone(
    area: int, x: float, y: float, enemy_r: float = 20.0
) -> Optional[Tuple[float, float, float, float]]:
    """Nearest safe bubble. Returns (cx, cy, edge, dist) or None."""
    a = int(area)
    er = max(12.0, float(enemy_r or 20.0))
    cushion = 40.0
    best = None
    best_d = 1e18
    zones = []
    if a == 0:
        zones.append((BASE_X, BASE_Y, BASE_RADIUS + er + cushion))
    for px, py, pr in area_portals(a):
        zones.append((px, py, float(pr) + PORTAL_SAFE_PAD + er + cushion))
    for cx, cy, edge in zones:
        d = math.hypot(float(x) - cx, float(y) - cy)
        if d < best_d:
            best_d = d
            best = (cx, cy, edge, max(d, 1e-3))
    return best


def push_xy_from_safe(area: int, x: float, y: float, enemy_r: float = 20.0) -> Tuple[float, float, bool]:
    """Push a point onto the rim of any overlapping safe zone. Returns (x, y, pushed)."""
    hit = enemy_safe_clearance(area, x, y, enemy_r)
    if not hit:
        return float(x), float(y), False
    cx, cy, edge = hit
    dx = float(x) - cx
    dy = float(y) - cy
    d = math.hypot(dx, dy)
    if d < 1e-3:
        ang = random.random() * math.pi * 2
        return cx + math.cos(ang) * (edge + 10.0), cy + math.sin(ang) * (edge + 10.0), True
    s = (edge + 10.0) / d
    return cx + dx * s, cy + dy * s, True


def update_safe_redirect(
    e: dict,
    area: int,
    heading: float,
) -> Optional[float]:
    """
    Hard peel-away from safe zones with hysteresis.
    Returns escape heading while redirect is active, else None.
    """
    er = float(e.get("r") or 20.0)
    zone = nearest_safe_zone(area, float(e["x"]), float(e["y"]), er)
    if not zone:
        e["safe_redirect"] = 0
        e["safe_side"] = 0.0
        e.pop("safe_w", None)
        return None
    cx, cy, edge, d = zone
    hit_pad = 90.0
    clear_pad = 220.0
    inside = d < edge
    near = d < edge + hit_pad
    clear = d > edge + clear_pad
    active = int(e.get("safe_redirect") or 0) == 1
    if active:
        if clear:
            e["safe_redirect"] = 0
            e["safe_side"] = 0.0
            e.pop("safe_w", None)
            return None
    elif not (inside or near):
        return None
    else:
        e["safe_redirect"] = 1

    out_x = (float(e["x"]) - cx) / d
    out_y = (float(e["y"]) - cy) / d
    if inside:
        rim = edge + 10.0
        e["x"] = cx + out_x * rim
        e["y"] = cy + out_y * rim
        e["aggro_until"] = 0.0
        e["strafe_dir"] = 0.0
        e["g"] = 0

    head = float(e.get("safe_w") or heading)
    hx = math.cos(head)
    hy = math.sin(head)
    side = float(e.get("safe_side") or 0.0)
    if side not in (1.0, -1.0):
        d_a = hx * (-out_y) + hy * out_x
        d_b = hx * out_y + hy * (-out_x)
        side = 1.0 if d_a >= d_b else -1.0
        e["safe_side"] = side
    # Lock escape heading on first engage — straight peel, not an orbiting curve.
    if e.get("safe_w") is None or not math.isfinite(float(e.get("safe_w"))):
        t_x = side * (-out_y)
        t_y = side * out_x
        escape = math.atan2(out_y * 0.88 + t_y * 0.35, out_x * 0.88 + t_x * 0.35)
        e["safe_w"] = escape
    else:
        escape = float(e["safe_w"])
    return escape


def enemy_cap(area: int) -> int:
    if is_boss_zone(area):
        return 0  # boss + minions managed separately
    return 150 if area == 0 else 110


def type_for_area(area: int) -> dict:
    if area <= 0:
        return ENEMY_TYPES[0] if random.random() < 0.7 else ENEMY_TYPES[1]
    # Match client: min(12, area) and next tier (13 = Core Sovereign; not bosses 14/15)
    de = min(12, max(0, area))
    a = ENEMY_TYPES[de]
    b = ENEMY_TYPES[min(13, de + 1)]
    return a if random.random() < 0.6 else b


def debris_count_for_area(area: int) -> int:
    if is_boss_zone(area):
        return 0
    o = area
    if o == 0:
        return 160
    if o in (2, 7, 10, 11):
        return 20
    return 80


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
    __slots__ = ("sock", "nick", "is_host", "state", "buf", "alive", "combat_hit_times", "mine_hit_times")

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.nick: Optional[str] = None
        self.is_host = False
        self.state: Dict[str, Any] = {}
        self.buf = bytearray()
        self.alive = True
        self.combat_hit_times: List[float] = []
        self.mine_hit_times: List[float] = []

    def send(self, obj: Any) -> None:
        if not self.alive:
            return
        raw = obj if isinstance(obj, str) else json.dumps(obj, separators=(",", ":"))
        try:
            self.sock.sendall(ws_encode(raw))
        except OSError:
            self.alive = False

    def allow_hit(self, mine: bool = False) -> bool:
        now = time.time()
        bucket = self.mine_hit_times if mine else self.combat_hit_times
        limit = MAX_MINE_HITS_PER_SEC if mine else MAX_COMBAT_HITS_PER_SEC
        keep = [t for t in bucket if now - t < 1.0]
        if mine:
            self.mine_hit_times = keep
        else:
            self.combat_hit_times = keep
        if len(keep) >= limit:
            return False
        keep.append(now)
        return True


class SectorRoom:
    def __init__(self, area_index: int):
        self.area_index = area_index
        # Unique per room instance so clients drop seq watermarks after empty-room recycle.
        self.room_epoch = int(time.time() * 1000) ^ ((int(area_index) + 1) * 10007)
        self.clients: Dict[str, Client] = {}
        self.host: Optional[str] = None  # legacy UI only
        self.enemy_ents: Dict[str, dict] = {}
        self.enemies: Dict[str, dict] = {}
        self.enemy_shots: list = []
        self.kills: Dict[str, float] = {}
        # sid -> (serialized corpse with h=0, expire_at). Older mobile clients need
        # an explicit hp=0 row in the enemies map to drop the sprite.
        self.kill_corpses: Dict[str, Tuple[dict, float]] = {}
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
        self._loot_layout_ver = 0
        self._spawned = False
        self._debris_spawned = False
        self.pending_collect: Dict[str, float] = {}
        # Pre-encoded broadcasts flushed outside the global lock (avoids sendall stalls).
        self._outbox: List[Tuple["Client", str]] = []
        # nick -> last syncPlease wall time (rate-limit full snap replies)
        self._sync_please_at: Dict[str, float] = {}
        # Keep empty rooms briefly so a reconnect does not reseed a brand-new world.
        self._empty_since: Optional[float] = None

    def pick_host(self) -> None:
        # NEVER hand world authority to a phone/PC. Sticky-host on mobile was
        # keeping a local enemy sim so kills worked on desktop but not on device.
        self.host = "SERVER"
        for c in self.clients.values():
            was = c.is_host
            c.is_host = False
            if was:
                c.send(self._host_msg(c))

    def _host_msg(self, c: Client) -> dict:
        return {
            "t": "host",
            "host": "SERVER",
            "youAreHost": False,
            "serverEnemies": True,
            "serverDebris": True,
            "serverLoot": True,
        }

    def broadcast(self, msg: Any, except_nick: Optional[str] = None) -> None:
        raw = json.dumps(msg, separators=(",", ":"))
        for nick, c in list(self.clients.items()):
            if nick == except_nick:
                continue
            self._outbox.append((c, raw))

    def flush_outbox(self) -> None:
        box = self._outbox
        if not box:
            return
        self._outbox = []
        for c, raw in box:
            if c and c.alive:
                c.send(raw)

    def snapshot_players(self) -> dict:
        out = {}
        for nick, c in self.clients.items():
            out[nick] = {"nick": nick, **c.state}
        return out

    # ── enemies ──────────────────────────────────────────────

    def serialize_enemy(self, e: dict) -> dict:
        # Do not use `or` on angles — 0.0 is a valid heading (east).
        ang_raw = e.get("ang")
        if ang_raw is None:
            ang_raw = e.get("w")
        try:
            ang = float(ang_raw)
        except (TypeError, ValueError):
            ang = 0.0
        wand_raw = e.get("w")
        if wand_raw is None:
            wand_raw = ang
        try:
            wand = float(wand_raw)
        except (TypeError, ValueError):
            wand = ang
        if not math.isfinite(ang):
            ang = wand if math.isfinite(wand) else random.random() * math.pi * 2
        if not math.isfinite(wand):
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
        now = time.time()
        self.kill_corpses = {
            sid: pair for sid, pair in self.kill_corpses.items() if pair[1] > now
        }
        # Inject hp=0 corpses so older clients that ignore `kills` still drop sprites.
        for sid, (corpse, _) in self.kill_corpses.items():
            if sid not in snap:
                snap[sid] = corpse
        self.enemies = snap
        self.last_enemy_seq += 1
        self.last_enemy_at = time.time() * 1000

    def enemy_payload(self) -> dict:
        now = time.time() * 1000
        # Keep kill tombstones long enough that late/stale snaps cannot resurrect foes.
        self.kills = {k: v for k, v in self.kills.items() if now - float(v) < 15000}
        # Only recent bolts — clients already dedupe by shot id; resending 40 every tick was heavy.
        shot_cut = now - 700.0
        recent_shots = [
            sh for sh in self.enemy_shots if float(sh.get("createdAt") or 0) >= shot_cut
        ][-16:]
        return {
            "t": "enemies",
            "updatedAt": self.last_enemy_at or now,
            "seq": self.last_enemy_seq or None,
            "roomEpoch": self.room_epoch,
            "host": "SERVER",
            "youAreHost": False,
            "serverEnemies": True,
            "serverDebris": True,
            "serverLoot": True,
            "areaIndex": self.area_index,
            "enemies": self.enemies,
            "kills": self.kills,
            "shots": recent_shots,
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
            "dmg_by": {},
        }
        return sid

    def _spawn_point_ok(self, x: float, y: float, avoid_x: Optional[float], avoid_y: Optional[float]) -> bool:
        # Keep spawns outside Home Base / portal safe bubbles (plus a little margin).
        if enemy_safe_clearance(self.area_index, x, y, 40.0) is not None:
            return False
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

    def nearest_pilot(self, x: float, y: float, *, combat: bool = False) -> Optional[Tuple[str, float, float, float]]:
        best = None
        best_d = 1e18
        for nick, c in self.clients.items():
            s = c.state
            px = float(s.get("x") or 0)
            py = float(s.get("y") or 0)
            if combat and point_in_safe_zone(self.area_index, px, py):
                continue
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
            # 0 is a valid heading (east) — do not rewrite near-zero angles.
            if not math.isfinite(float(e.get("w") or 0)):
                e["w"] = random.uniform(0.4, math.pi * 2 - 0.4)
            if not math.isfinite(float(e.get("ang") or 0)):
                e["ang"] = e["w"]

            ox, oy = float(e["x"]), float(e["y"])
            # Combat ignores pilots inside Home Base / portal safe zones.
            pilot = self.nearest_pilot(e["x"], e["y"], combat=True)
            aggro = float(e.get("aggro_until") or 0) > now
            # No valid combat target (everyone safe / gone) → drop chase and resume patrol.
            if aggro and not pilot:
                e["aggro_until"] = 0.0
                aggro = False
                e["strafe_dir"] = 0.0
            e["g"] = 1 if aggro else 0
            speed = float(e["s"])
            is_boss = int(e.get("t") or 0) == 14

            if is_boss and int(e.get("drones_to_spawn") or 0) > 0:
                if now - float(e.get("last_drone_spawn") or 0) >= 2.5:
                    e["last_drone_spawn"] = now
                    e["drones_to_spawn"] = max(0, int(e["drones_to_spawn"]) - 2)
                    self.spawn_minion(e)
                    self.spawn_minion(e)

            typ_i = int(e.get("t") or 0)
            is_minion = typ_i == 15
            fire_max = PLAYER_FIRE_CONE

            # Hard safe-zone peel takes priority — no chase-into-bubble then bounce.
            escape = update_safe_redirect(e, self.area_index, float(e.get("w") or 0.0))
            if escape is not None:
                peel = 0.55 if is_boss else 0.45
                e["x"] += math.cos(escape) * speed * peel * dt
                e["y"] += math.sin(escape) * speed * peel * dt
                e["w"] = escape
                e["ang"] = escape
                e["aggro_until"] = 0.0
                e["strafe_dir"] = 0.0
                e["g"] = 0
                aggro = False
                nx, ny, pushed = push_xy_from_safe(self.area_index, e["x"], e["y"], float(e.get("r") or 20.0))
                if pushed:
                    e["x"], e["y"] = nx, ny
            elif aggro and pilot:
                tx, ty = pilot[1], pilot[2]
                face = math.atan2(ty - e["y"], tx - e["x"])
                # Snap nose onto the pilot immediately and open fire the same tick.
                e["w"] = face
                e["ang"] = face
                dist = pilot[3]
                if dist > fire_max:
                    # Player left cone range — chase back into it.
                    move = face
                    scale = 1.0
                else:
                    # In cone: random lateral strafe, stay inside fire range.
                    side = float(e.get("strafe_dir") or 0)
                    if side == 0.0:
                        side = 1.0 if (hash(sid) & 1) else -1.0
                        e["strafe_dir"] = side
                    if random.random() < 0.35 * dt:
                        side = -side
                        e["strafe_dir"] = side
                    move = face + side * math.pi / 2
                    scale = 0.28 if is_minion else (0.22 if not is_boss else 0.2)
                    if dist > fire_max * 0.88:
                        # Near cone edge — bias inward so they do not drift out of range.
                        move = math.atan2(
                            math.sin(move) * 0.55 + math.sin(face) * 0.9,
                            math.cos(move) * 0.55 + math.cos(face) * 0.9,
                        )
                        scale = 0.35
                    elif dist < (140.0 if is_boss else 85.0):
                        move = math.atan2(
                            math.sin(move) * 0.4 - math.sin(face) * 0.85,
                            math.cos(move) * 0.4 - math.cos(face) * 0.85,
                        )
                        scale = 0.3
                e["x"] += math.cos(move) * speed * scale * dt
                e["y"] += math.sin(move) * speed * scale * dt
                # Keep facing the pilot after the strafe step.
                face = math.atan2(ty - e["y"], tx - e["x"])
                e["w"] = face
                e["ang"] = face
                nx, ny, pushed = push_xy_from_safe(self.area_index, e["x"], e["y"], float(e.get("r") or 20.0))
                if pushed:
                    e["x"], e["y"] = nx, ny
                    escape2 = update_safe_redirect(e, self.area_index, float(e.get("w") or 0.0))
                    if escape2 is not None:
                        e["w"] = escape2
                        e["ang"] = escape2
                        e["aggro_until"] = 0.0
                        e["strafe_dir"] = 0.0
                        e["g"] = 0
                        aggro = False
            else:
                if random.random() < 0.35 * dt:
                    e["w"] = float(e["w"]) + (random.random() - 0.5) * 2.2
                if random.random() < 0.08 * dt:
                    e["w"] = random.random() * math.pi * 2
                e["ang"] = e["w"]
                e["x"] += math.cos(e["w"]) * speed * 0.3 * dt
                e["y"] += math.sin(e["w"]) * speed * 0.3 * dt
                nx, ny, pushed = push_xy_from_safe(self.area_index, e["x"], e["y"], float(e.get("r") or 20.0))
                if pushed:
                    e["x"], e["y"] = nx, ny
                    escape2 = update_safe_redirect(e, self.area_index, float(e.get("w") or 0.0))
                    if escape2 is not None:
                        e["w"] = escape2
                        e["ang"] = escape2

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

            # Shoot inside the player fire cone while facing the pilot.
            if aggro and pilot and 90.0 <= pilot[3] <= fire_max:
                fr = max(0.35, float(e.get("fr") or 1.0))
                if now - float(e.get("last_fire") or 0) >= 1.0 / fr:
                    e["last_fire"] = now
                    # Miss % only — clients home non-miss bolts so strafing cannot dodge.
                    will_miss = random.random() < 0.18
                    aim = math.atan2(pilot[2] - e["y"], pilot[1] - e["x"])
                    e["ang"] = aim
                    e["w"] = aim
                    if will_miss:
                        aim += (1 if random.random() < 0.5 else -1) * (0.22 + random.random() * 0.18)
                    spd = 400.0
                    nose = max(12.0, float(e.get("r") or 20.0) * 0.9)
                    self._shot_seq = getattr(self, "_shot_seq", 0) + 1
                    self.enemy_shots.append(
                        {
                            "id": f"{self.area_index}:SERVER:{int(now*1000)}:{self._shot_seq}",
                            "areaIndex": self.area_index,
                            "sourceEnemyId": sid,
                            "targetNick": pilot[0],
                            "x": float(e["x"]) + math.cos(aim) * nose,
                            "y": float(e["y"]) + math.sin(aim) * nose,
                            "vx": math.cos(aim) * spd,
                            "vy": math.sin(aim) * spd,
                            "a": aim,
                            "angle": aim,
                            "damage": int(e["d"]),
                            "willMiss": 1 if will_miss else 0,
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
        if not self._debris_spawned:
            self._debris_spawned = True
            self.seed_debris()
            self.seed_rocks()
            self.seed_loot()
            self.rebuild_debris_snap()
            return
        # Redeploy: switch rooms onto the shared deterministic crate layout.
        if self._loot_layout_ver < LOOT_LAYOUT_VER:
            self.seed_loot()
            self.rebuild_debris_snap()
            return
        # Long-lived rooms: refill crates if the sector was emptied.
        if not self.loot_ents:
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
        # Match sticky-host setupRocks: 120 free-floating ore (also in boss zones).
        for _ in range(120):
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
        # Keep fractional HP — int() truncation made 0.1..0.9 publish as 0 and
        # clients treated still-living asteroids as dead (explode loop while mining).
        h = max(0.0, float(d.get("h") or 0))
        m = max(0.0, float(d.get("m") or 0))
        return {
            "x": round(float(d["x"]), 1),
            "y": round(float(d["y"]), 1),
            "vx": round(float(d.get("vx") or 0), 2),
            "vy": round(float(d.get("vy") or 0), 2),
            "a": round(float(d.get("a") or 0), 2),
            "rs": round(float(d.get("rs") or 0), 3),
            "sc": round(float(d.get("sc") or 1), 2),
            "id": d.get("asset") or "asteroid_1",
            "h": round(h, 2),
            "m": round(m, 2) if m > 0 else int(ASTEROID_MAX_HP),
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
        # Deterministic 12 crates per area — identical on every client even before snaps.
        rnd = area_loot_rnd(self.area_index)
        self.loot_ents.clear()
        self.next_loot_id = 13
        self._loot_layout_ver = LOOT_LAYOUT_VER
        for i in range(1, 13):
            sid = f"l{self.area_index}_S_{i}"
            ang = (rnd() - 0.5) * 0.4
            self.loot_ents[sid] = {
                "id": sid,
                "x": rnd() * WORLD,
                "y": rnd() * WORLD,
                "vx": (rnd() - 0.5) * 2.5,
                "vy": (rnd() - 0.5) * 2.5,
                "a": ang,
                "ba": ang,
                "rs": 0.6 + rnd() * 0.5,
                "sc": 1.05 + rnd() * 0.25,
                "pu": rnd() * math.pi * 2,
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
        self.debris = {}
        for sid, d in self.debris_ents.items():
            # Never publish dead/dying-truncated asteroids — clients used to boom-loop on h==0 rows.
            if d.get("mn") and float(d.get("h") or 0) <= 0.001:
                continue
            self.debris[sid] = self.serialize_debris(d)
        self.rocks = {sid: self.serialize_rock(r) for sid, r in self.rock_ents.items()}
        self.loot = {sid: self.serialize_loot(b) for sid, b in self.loot_ents.items()}
        self.last_debris_seq += 1
        self.last_debris_at = time.time() * 1000

    def debris_payload(self) -> dict:
        now = time.time() * 1000
        self.debris_kills = {k: v for k, v in self.debris_kills.items() if now - float(v) < 15000}
        self.loot_kills = {k: v for k, v in self.loot_kills.items() if now - float(v) < 15000}
        return {
            "t": "debris",
            "updatedAt": self.last_debris_at or now,
            "seq": self.last_debris_seq or None,
            "roomEpoch": self.room_epoch,
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
        # World crates: smaller payouts so flying boxes aren't a free economy.
        roll = random.random()
        if roll < 0.5:
            amt = random.randint(80, 350)
            return {"type": "credits", "amt": amt, "label": f"+{amt} CR", "color": "#fbbf24"}
        if roll < 0.74:
            amt = random.randint(10, 25)
            return {"type": "ammo_x2", "amt": amt, "label": f"+{amt} x2 AMMO", "color": "#67e8f9"}
        if roll < 0.9:
            amt = random.randint(1, 400)
            return {"type": "xp", "amt": amt, "label": f"+{amt} XP", "color": "#86efac"}
        if roll < 0.97:
            amt = random.randint(8, 20)
            return {"type": "ammo_x3", "amt": amt, "label": f"+{amt} x3 AMMO", "color": "#c084fc"}
        amt = random.randint(1, 2)
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
        # Hit event removes the crate on clients; next debris tick covers residual state.

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
        if not c:
            return
        sync_id = str(msg.get("syncId") or "")
        kind = msg.get("kind")
        dmg = clamp_dmg(float(msg.get("dmg") or 0))
        # Mining ticks use a separate budget from lasers / collects.
        is_mine = kind == "debris" or (sync_id.startswith("d") and kind not in ("rockCollect", "lootCollect"))
        if not c.allow_hit(mine=is_mine):
            return

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
        who = str(nick or "").upper()
        dmg_by = e.get("dmg_by")
        if not isinstance(dmg_by, dict):
            dmg_by = {}
            e["dmg_by"] = dmg_by
        dmg_by[who] = float(dmg_by.get(who) or 0) + float(dmg)
        # First damaging pilot owns the kill credit (non-squad / attribution).
        if not e.get("first_hit_by"):
            e["first_hit_by"] = nick
        if int(e.get("t") or 0) == 14 and not e.get("drones_armed"):
            e["drones_armed"] = 1
            e["drones_to_spawn"] = 6
        kill = e["h"] <= 0
        hp_left = 0 if kill else int(e["h"])
        ex, ey = float(e.get("x") or 0), float(e.get("y") or 0)
        credit_by = str(e.get("first_hit_by") or who)
        kill_type = int(e.get("t") or 0)
        kill_xp = int(e.get("xp") or 0)
        kill_credits = int(e.get("c") or 0)
        boss_loot = None
        if kill:
            was_boss = kill_type == 14
            if was_boss:
                elig = boss_loot_eligible(e.get("dmg_by") or {}, e.get("m") or 0, credit_by)
                boss_loot = assign_boss_loot(elig)
            self.kills[sync_id] = time.time() * 1000
            corpse = self.serialize_enemy(e)
            corpse["h"] = 0
            corpse["g"] = 0
            self.kill_corpses[sync_id] = (corpse, time.time() + 20.0)
            self.enemy_ents.pop(sync_id, None)
            self.recent_deaths.append((ex, ey, time.time() + 12.0))
            if was_boss:
                self.last_boss_kill = time.time()
            elif not is_boss_zone(self.area_index):
                self.respawn_queue.append((time.time() + 8.0, ex, ey))
            # Ore drops from kills (server-owned)
            self.spawn_enemy_ore(ex, ey, random.randint(1, 3))
        hit_msg = {
            "t": "hit",
            "by": credit_by if kill else nick,
            "finisher": nick if kill else None,
            "syncId": sync_id,
            "hp": hp_left,
            "dmg": dmg,
            "kill": kill,
            "dead": kill,
            "remove": kill,
            "kind": kind or "enemy",
            "x": ex,
            "y": ey,
            "ts": int(time.time() * 1000),
        }
        if kill:
            hit_msg["typeIndex"] = kill_type
            hit_msg["xp"] = kill_xp
            hit_msg["credits"] = kill_credits
            if boss_loot:
                hit_msg["bossLoot"] = boss_loot
        self.broadcast(hit_msg, None)
        # One enemy snap is enough (hit + corpse row). Ore appears on the next debris tick.
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
        # Fractional HP while alive — never send 0 unless kill (int trunc caused fake deaths).
        hp_left = 0 if kill else round(float(d["h"]), 2)
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
        # Full debris snap on kill only — mining ticks use hit HP; 8 Hz tick covers motion.
        if kill:
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
            "typeId": rock.get("t") or rock.get("typeId") or "iron",
            "value": int(rock.get("v") if rock.get("v") is not None else rock.get("value") or 2),
            "ts": int(time.time() * 1000),
        }
        if msg.get("label") is not None:
            payload["label"] = msg.get("label")
        # Hit event removes the rock on clients; next debris tick covers residual state.
        self.broadcast(payload, None)

    # ── join / leave / relay ─────────────────────────────────

    def join(self, client: Client, msg: dict) -> Optional[Client]:
        nick = str(msg.get("nick") or "")[:24]
        if not nick:
            client.send({"t": "err", "m": "nick required"})
            return None
        prev = self.clients.get(nick)
        if prev and prev is not client:
            if prev.alive:
                try:
                    client.send({"t": "kicked", "reason": "session", "m": "already logged in"})
                except Exception:
                    pass
                client.alive = False
                try:
                    client.sock.close()
                except OSError:
                    pass
                return None
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
            "isVIP": bool(msg.get("isVIP") or msg.get("isVip")),
            "killPoints": int(msg.get("killPoints") or 0),
            "activeTitle": str(msg.get("activeTitle") or "")[:48] or None,
            "lastActive": time.time() * 1000,
        }
        self.clients[nick] = client
        client.is_host = False
        self._empty_since = None
        self.pick_host()
        self.ensure_enemies()
        self.ensure_debris()
        self.rebuild_enemy_snap()
        self.rebuild_debris_snap()
        client.send(
            {
                "t": "welcome",
                "areaIndex": self.area_index,
                "roomEpoch": self.room_epoch,
                "host": "SERVER",
                "youAreHost": False,
                "serverEnemies": True,
                "serverDebris": True,
                "serverLoot": True,
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
        self._sync_please_at.pop(nick, None)
        self.broadcast({"t": "leave", "nick": nick})
        if not self.clients:
            # Do not destroy immediately — reconnecting players were getting a fresh
            # random enemy field (looked like a full reshuffle on startup).
            self._empty_since = time.time()
            return
        self._empty_since = None
        # World always stays on SERVER — refresh host flags for remaining clients.
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
        for k in ("isGM", "isMod", "isVIP", "hasBetaBadge", "isRankOne"):
            if k in msg:
                s[k] = bool(msg[k])
        if "isVip" in msg and "isVIP" not in msg:
            s["isVIP"] = bool(msg["isVip"])
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
        # Mining laser target — only update when the client sent mine keys (omit ≠ clear).
        if "mineId" in msg or "mid" in msg:
            mid = msg.get("mineId", msg.get("mid"))
            if mid is None or mid == "" or mid is False:
                s["mineId"] = None
                s["mineX"] = None
                s["mineY"] = None
            else:
                s["mineId"] = str(mid)[:48]
                mx = msg.get("mineX", msg.get("mx"))
                my = msg.get("mineY", msg.get("my"))
                if isinstance(mx, (int, float)) and isinstance(my, (int, float)):
                    s["mineX"] = float(mx)
                    s["mineY"] = float(my)
                else:
                    s["mineX"] = None
                    s["mineY"] = None
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
            "isVIP": s.get("isVIP", False),
            "hasBetaBadge": s.get("hasBetaBadge", False),
            "isRankOne": s.get("isRankOne", False),
            "killPoints": s.get("killPoints", 0),
            "activeTitle": s.get("activeTitle"),
            "palLevel": int(s.get("palLevel") or 0),
            "mineId": s.get("mineId"),
            "mid": s.get("mineId"),
        }
        if isinstance(s.get("mineX"), (int, float)) and isinstance(s.get("mineY"), (int, float)):
            out["mineX"] = float(s["mineX"])
            out["mineY"] = float(s["mineY"])
            out["mx"] = out["mineX"]
            out["my"] = out["mineY"]
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
# One live socket per callsign across all sectors.
live_nicks: Dict[str, Tuple["Client", int]] = {}
lock = threading.Lock()


def nick_key(nick: str) -> str:
    return str(nick or "").upper()


def occupy_live_nick(key: str, client: Client, area: int) -> bool:
    prev = live_nicks.get(key)
    if prev:
        old_client, old_area = prev
        if old_client is client:
            live_nicks[key] = (client, area)
            return True
        if old_client.alive:
            try:
                client.send({"t": "kicked", "reason": "session", "m": "already logged in"})
            except Exception:
                pass
            client.alive = False
            try:
                client.sock.close()
            except OSError:
                pass
            return False
        if old_client.nick and old_area in rooms:
            rooms[old_area].leave(old_client.nick)
        live_nicks.pop(key, None)
    live_nicks[key] = (client, area)
    return True


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
    flush_rooms: List[SectorRoom] = []
    with lock:
        if t == "join":
            if client.nick is not None and area is not None and area in rooms:
                old = rooms[area]
                old.leave(client.nick)
                flush_rooms.append(old)
            nick = str(msg.get("nick") or "")[:24]
            key = nick_key(nick)
            area = int(msg.get("areaIndex") or 0)
            if key and not occupy_live_nick(key, client, area):
                return area
            room = get_room(area)
            if not room.join(client, msg):
                cur = live_nicks.get(key) if key else None
                if cur and cur[0] is client:
                    live_nicks.pop(key, None)
                return area
            flush_rooms.append(room)
        elif client.nick is None or area is None:
            pass
        else:
            room = rooms.get(area)
            if room:
                flush_rooms.append(room)
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
                    # Rate-limit + queue via outbox. Rebuilding + sendall under the lock
                    # starved tick_enemies when clients spammed syncPlease on roster ticks.
                    now_sp = time.time()
                    last_sp = float(room._sync_please_at.get(nick) or 0.0)
                    if now_sp - last_sp >= 3.5:
                        room._sync_please_at[nick] = now_sp
                        room.ensure_enemies()
                        room.ensure_debris()
                        # Resend cached snaps — do not bump seq / rebuild unless empty.
                        if not room.enemies:
                            room.rebuild_enemy_snap()
                        if not room.debris and not room.rocks and not room.loot:
                            room.rebuild_debris_snap()
                        c_sp = room.clients.get(nick)
                        if c_sp:
                            room._outbox.append(
                                (c_sp, json.dumps(room.enemy_payload(), separators=(",", ":")))
                            )
                            room._outbox.append(
                                (c_sp, json.dumps(room.debris_payload(), separators=(",", ":")))
                            )
                elif t == "switch":
                    next_area = int(msg.get("areaIndex") or 0)
                    if next_area != area:
                        room.leave(nick)
                        area = next_area
                        nxt = get_room(area)
                        if nxt.join(client, {**msg, "nick": nick, "areaIndex": area}):
                            flush_rooms.append(nxt)
                            live_nicks[nick_key(nick)] = (client, area)
    # Send queued snapshots outside the lock so other rooms/clients are not stalled.
    seen = set()
    for room in flush_rooms:
        rid = id(room)
        if rid in seen:
            continue
        seen.add(rid)
        room.flush_outbox()
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
        left_room = None
        with lock:
            if client.nick is not None and area is not None and area in rooms:
                left_room = rooms[area]
                left_room.leave(client.nick)
            key = nick_key(client.nick or "")
            cur = live_nicks.get(key) if key else None
            if cur and cur[0] is client:
                live_nicks.pop(key, None)
        if left_room:
            left_room.flush_outbox()
        conn_meta.pop(sock, None)
        try:
            sock.close()
        except OSError:
            pass


# Empty sector rooms linger so a reconnect keeps the same enemy field.
EMPTY_ROOM_GRACE_SEC = 90.0


def tick_loop() -> None:
    last_enemy = 0.0
    last_debris = 0.0
    last_players = 0.0
    last_prune = 0.0
    while True:
        time.sleep(0.02)
        now = time.time()
        flush_rooms: List[SectorRoom] = []
        with lock:
            if now - last_prune >= 5.0:
                last_prune = now
                for area_idx, room in list(rooms.items()):
                    if room.clients:
                        room._empty_since = None
                        continue
                    empty_at = room._empty_since
                    if empty_at is None:
                        room._empty_since = now
                        continue
                    if now - float(empty_at) >= EMPTY_ROOM_GRACE_SEC:
                        rooms.pop(area_idx, None)
            if now - last_enemy >= ENEMY_DT:
                last_enemy = now
                for room in list(rooms.values()):
                    if room.clients:
                        try:
                            room.tick_enemies(ENEMY_DT)
                            flush_rooms.append(room)
                        except Exception as err:
                            # One bad room must not kill the whole enemy sim loop.
                            print(f"[SR-MP] tick_enemies area={room.area_index}: {err}")
            if now - last_debris >= DEBRIS_DT:
                last_debris = now
                for room in list(rooms.values()):
                    if room.clients:
                        room.tick_debris(DEBRIS_DT)
                        flush_rooms.append(room)
            if now - last_players >= PLAYERS_DT:
                last_players = now
                for room in list(rooms.values()):
                    if room.clients:
                        room.broadcast(
                            {
                                "t": "players",
                                "players": room.snapshot_players(),
                                "host": "SERVER",
                                "youAreHost": False,
                                "serverEnemies": True,
                                "serverDebris": True,
                                "serverLoot": True,
                            }
                        )
                        flush_rooms.append(room)
        seen = set()
        for room in flush_rooms:
            rid = id(room)
            if rid in seen:
                continue
            seen.add(rid)
            room.flush_outbox()


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


def recover_email_key(email: str) -> str:
    return re.sub(r"[.#$\[\]/]", ",", str(email or "").strip().lower())


def recover_hash(email: str, code: str) -> str:
    return hashlib.sha256(f"sr-recover:{email}:{code}".encode("utf8")).hexdigest()


def firebase_get(path: str):
    url = f"{FIREBASE_DB}/{path}.json"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=8) as resp:
        raw = resp.read().decode("utf8") or "null"
    return json.loads(raw)


def firebase_set(path: str, value) -> None:
    data = json.dumps(value).encode("utf8")
    req = urllib.request.Request(
        f"{FIREBASE_DB}/{path}.json",
        data=data,
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=8) as resp:
        resp.read()


def send_recover_email(to: str, code: str, callsign: str) -> None:
    subject = "Sector Rift password recovery"
    text = (
        f"Pilot {callsign},\n\nYour Sector Rift recovery code is {code}.\n"
        "It expires in 15 minutes.\n\nIf you did not request this, you can ignore this email."
    )
    if not RESEND_API_KEY:
        err = RuntimeError("not_configured")
        err.code = "not_configured"
        raise err
    payload = json.dumps(
        {
            "from": MAIL_FROM,
            "to": [to],
            "subject": subject,
            "text": text,
        }
    ).encode("utf8")
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
            "User-Agent": "sector-rift-mp/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            resp.read()
    except urllib.error.HTTPError as err:
        detail = err.read().decode("utf8", errors="ignore")
        print(f"[recover] resend failed {err.code}: {detail[:300]}")
        raise RuntimeError(f"resend {err.code}: {detail[:180]}")


def _http_json(status: int, obj: dict, extra_headers: str = "") -> bytes:
    body = json.dumps(obj)
    return (
        f"HTTP/1.1 {status} {'OK' if status < 400 else 'ERROR'}\r\n"
        "Content-Type: application/json\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Access-Control-Allow-Methods: POST, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Content-Type\r\n"
        f"{extra_headers}"
        f"Content-Length: {len(body.encode('utf8'))}\r\n\r\n{body}"
    ).encode("utf8")


def read_http_body(sock: socket.socket, req: bytes) -> Tuple[str, str, dict]:
    header_end = req.find(b"\r\n\r\n")
    raw_headers = req[: header_end if header_end >= 0 else len(req)]
    body = req[header_end + 4 :] if header_end >= 0 else b""
    lines = raw_headers.decode("utf8", errors="ignore").split("\r\n")
    first = lines[0].split(" ") if lines else []
    method = first[0].upper() if first else "GET"
    path = first[1] if len(first) > 1 else "/"
    content_len = 0
    for line in lines[1:]:
        if line.lower().startswith("content-length:"):
            try:
                content_len = int(line.split(":", 1)[1].strip() or 0)
            except ValueError:
                content_len = 0
    content_len = max(0, min(content_len, 8192))
    while len(body) < content_len:
        chunk = sock.recv(min(4096, content_len - len(body)))
        if not chunk:
            break
        body += chunk
    payload = {}
    if body:
        try:
            parsed = json.loads(body.decode("utf8"))
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            payload = {}
    return method, path.split("?", 1)[0], payload


def handle_recover(method: str, path: str, payload: dict) -> bytes:
    if method == "OPTIONS":
        return _http_json(204, {})
    if method != "POST":
        return _http_json(405, {"ok": False, "error": "method"})
    if not RESEND_API_KEY:
        return _http_json(503, {"ok": False, "error": "not_configured"})
    try:
        if path == "/recover/request":
            email = str(payload.get("email") or "").strip().lower()
            if not re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email):
                return _http_json(200, {"ok": True})
            key = recover_email_key(email)
            now = time.time()
            with _recover_lock:
                last = _recover_last.get(key) or 0
                if now - last < 60:
                    return _http_json(200, {"ok": True})
            rec = firebase_get("emails/" + urllib.parse.quote(key, safe=""))
            if not rec or not rec.get("callsign"):
                return _http_json(200, {"ok": True})
            callsign = str(rec.get("callsign") or "").strip().upper()
            code = f"{random.randint(100000, 999999)}"
            firebase_set(
                "passwordResets/" + urllib.parse.quote(key, safe=""),
                {
                    "hash": recover_hash(email, code),
                    "callsign": callsign,
                    "expiresAt": int(now * 1000) + 15 * 60 * 1000,
                    "attempts": 0,
                },
            )
            send_recover_email(email, code, callsign)
            with _recover_lock:
                _recover_last[key] = now
            print(f"[recover] sent code to {email} for {callsign}")
            return _http_json(200, {"ok": True})
        if path == "/recover/confirm":
            email = str(payload.get("email") or "").strip().lower()
            code = str(payload.get("code") or "").strip()
            password = str(payload.get("password") or "").strip()
            if not re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email) or not re.match(r"^\d{6}$", code) or len(password) < 4:
                return _http_json(400, {"ok": False, "error": "invalid"})
            key = recover_email_key(email)
            rec = firebase_get("passwordResets/" + urllib.parse.quote(key, safe=""))
            if not rec or not rec.get("hash") or int(rec.get("expiresAt") or 0) < int(time.time() * 1000):
                return _http_json(400, {"ok": False, "error": "expired"})
            attempts = int(rec.get("attempts") or 0)
            reset_path = "passwordResets/" + urllib.parse.quote(key, safe="")
            if attempts >= 5:
                firebase_set(reset_path, None)
                return _http_json(400, {"ok": False, "error": "expired"})
            if rec.get("hash") != recover_hash(email, code):
                rec["attempts"] = attempts + 1
                firebase_set(reset_path, rec)
                return _http_json(400, {"ok": False, "error": "invalid"})
            callsign = str(rec.get("callsign") or "").strip().upper()
            if not callsign:
                return _http_json(400, {"ok": False, "error": "invalid"})
            firebase_set("users/" + urllib.parse.quote(callsign, safe="") + "/warpKey", password)
            firebase_set(reset_path, None)
            return _http_json(200, {"ok": True})
        return _http_json(404, {"ok": False, "error": "not_found"})
    except Exception as err:
        print(f"[recover] {type(err).__name__}: {err}")
        return _http_json(500, {"ok": False, "error": "server", "detail": str(err)[:220]})


def handle_http(sock: socket.socket, req: bytes) -> None:
    path = "/"
    method = "GET"
    payload: dict = {}
    try:
        method, path, payload = read_http_body(sock, req)
    except Exception:
        try:
            line = req.decode("utf8", errors="ignore").split("\r\n", 1)[0]
            parts = line.split(" ")
            if len(parts) >= 2:
                method = parts[0].upper()
                path = parts[1].split("?", 1)[0]
        except Exception:
            pass
    if path.startswith("/recover"):
        resp = handle_recover(method, path, payload)
        try:
            sock.sendall(resp)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass
        return
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
        body = "Sector Rift MP — server-owned world — connect via WebSocket\n"
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
        f"[Sector Rift MP] ws://0.0.0.0:{PORT}  enemyHz={ENEMY_HZ}  debrisHz={DEBRIS_HZ}  "
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
