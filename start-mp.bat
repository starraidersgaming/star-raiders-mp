@echo off
title Star Raiders Multiplayer Server
cd /d "%~dp0"
echo.
echo  Starting Star Raiders MP on ws://0.0.0.0:8787
echo  Keep this window open while playing with 2+ clients.
echo.
echo  Same PC: game auto-connects to ws://127.0.0.1:8787
echo  Other PCs: set window.__SR_MP_WS__ = "ws://YOUR_LAN_IP:8787"
echo.
py -3 server.py
if errorlevel 1 python server.py
pause
