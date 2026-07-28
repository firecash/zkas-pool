#!/bin/sh
set -eu

tcp_ok=0
http_ok=0
kaspa_ok=0

timeout 2 bash -c '</dev/tcp/127.0.0.1/5577' 2>/dev/null && tcp_ok=1 || true
curl -fsS --max-time 3 http://127.0.0.1:3035/api/stats >/dev/null && http_ok=1 || true
timeout 2 bash -c '</dev/tcp/127.0.0.1/16215' 2>/dev/null && kaspa_ok=1 || true

if [ "$tcp_ok" -ne 1 ] || [ "$http_ok" -ne 1 ] || [ "$kaspa_ok" -ne 1 ]; then
    systemctl restart zkas-old5577.service
fi
