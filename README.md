# Star Raiders Multiplayer Server

Realtime WebSocket sync for **ship movement, shots, hits, and sector enemies**.

Uses **Python 3** (already on your PC) — no npm/Node required.

## Start the server (required)

Double-click:

`mp-server/start-mp.bat`

Or in a terminal:

```bat
cd "C:\Users\Tbroo\Desktop\Star Raiders Update\mp-server"
py -3 server.py
```

Leave that window open. Check http://127.0.0.1:8787/health → `{ ok: true }`.

## Play

1. Soft-refresh the game on **both** clients (`1.10.b25071156`).
2. Log in and meet in the **same sector**.
3. Movement, lasers, and raider HP/deaths sync over the WebSocket room (~20 Hz).

### Same PC (two browsers)
Auto-connects to `ws://127.0.0.1:8787`.

### Two PCs on Wi‑Fi
1. Run `start-mp.bat` on one PC.
2. Get that PC’s IPv4 (`ipconfig`).
3. On both game clients set before load (or edit `dist/index.html`):

```js
window.__SR_MP_WS__ = "ws://192.168.x.x:8787";
```

Allow Python through Windows Firewall for port **8787** if needed.

### Friends on the internet
Deploy `server.py` (or `server.js`) to Render / Railway / Fly and set:

```js
window.__SR_MP_WS__ = "wss://your-host";
```

## Firebase still handles
Login, saves, inventory, missions.  
**Live combat** uses this server when connected (`mpWsLive`).
