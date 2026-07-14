#!/usr/bin/env bash
# ZKas stratum-bridge watchdog (systemd edition).
# The bridge's stats HTTP server (:3033) hangs after long uptime from memory
# bloat while stratum/mining keeps working. After 2 consecutive failed
# /api/stats health checks this restarts the bridge THROUGH SYSTEMD.
# Never start the bridge via tmux — a tmux bridge steals the stratum ports
# from the systemd one on restart (see 2026-07-14 incident).
set -u

DIR=/root/work/zkas-pool
LOG="$DIR/bridge-watchdog.log"
STATE=/tmp/zkas-bridge-wd-fails
stamp() { date -u +'%Y-%m-%dT%H:%M:%SZ'; }

# If the operator stopped the pool on purpose, do nothing.
systemctl is-enabled --quiet zkas-pool || exit 0

code=$(curl -s -m 8 -o /dev/null -w '%{http_code}' http://127.0.0.1:3033/api/stats 2>/dev/null)
if [ "$code" = "200" ]; then
  echo 0 > "$STATE"
  # Bridge healthy — also make sure the address-masking redactor (:3034) is up,
  # since the public landing reads through it.
  rcode=$(curl -s -m 8 -o /dev/null -w '%{http_code}' http://127.0.0.1:3034/api/stats 2>/dev/null)
  if [ "$rcode" != "200" ]; then
    echo "$(stamp) redactor unhealthy code=$rcode — restarting via systemd" >> "$LOG"
    systemctl restart zkas-pool-redactor
  fi
  exit 0
fi

fails=$(( $(cat "$STATE" 2>/dev/null || echo 0) + 1 ))
echo "$fails" > "$STATE"
echo "$(stamp) unhealthy code=$code fails=$fails" >> "$LOG"

# Require 2 consecutive failures (~2 checks) before acting, to avoid flapping.
[ "$fails" -ge 2 ] || exit 0

echo "$(stamp) restarting zkas-pool via systemd" >> "$LOG"
systemctl restart zkas-pool
echo 0 > "$STATE"
echo "$(stamp) restart issued" >> "$LOG"
