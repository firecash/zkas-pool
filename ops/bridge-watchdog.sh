#!/usr/bin/env bash
# FireCash stratum-bridge watchdog.
# The bridge's stats HTTP server (:3033) hangs after long uptime from memory
# bloat while stratum/mining keeps working. This restarts the bridge (in the
# `bridge` tmux session) after 2 consecutive failed /api/stats health checks.
set -u

DIR=/root/work/firecash-pool
LOG="$DIR/bridge-watchdog.log"
STATE=/tmp/bridge-wd-fails
stamp() { date -u +'%Y-%m-%dT%H:%M:%SZ'; }

code=$(curl -s -m 8 -o /dev/null -w '%{http_code}' http://127.0.0.1:3033/api/stats 2>/dev/null)
if [ "$code" = "200" ]; then
  echo 0 > "$STATE"
  # Bridge healthy — also make sure the address-masking redactor (:3034) is up,
  # since the public landing reads through it. Restart it standalone if down.
  rcode=$(curl -s -m 8 -o /dev/null -w '%{http_code}' http://127.0.0.1:3034/api/stats 2>/dev/null)
  if [ "$rcode" != "200" ]; then
    echo "$(stamp) redactor unhealthy code=$rcode — restarting" >> "$LOG"
    pkill -f pool-redactor.py 2>/dev/null
    sleep 2
    tmux has-session -t redactor 2>/dev/null || tmux new-session -d -s redactor -c "$DIR"
    tmux send-keys -t redactor "cd $DIR && python3 pool-redactor.py 2>&1 | tee -a redactor.log" Enter
  fi
  exit 0
fi

fails=$(( $(cat "$STATE" 2>/dev/null || echo 0) + 1 ))
echo "$fails" > "$STATE"
echo "$(stamp) unhealthy code=$code fails=$fails" >> "$LOG"

# Require 2 consecutive failures (~2 checks) before acting, to avoid flapping.
[ "$fails" -ge 2 ] || exit 0

echo "$(stamp) restarting bridge" >> "$LOG"
pkill -TERM -f 'bin/stratum-bridge' 2>/dev/null
sleep 8
pkill -KILL -f 'bin/stratum-bridge' 2>/dev/null
sleep 2

tmux has-session -t bridge 2>/dev/null || tmux new-session -d -s bridge -c "$DIR"
tmux send-keys -t bridge "cd $DIR && ./bin/stratum-bridge --node-mode external --config ./firecash-bridge.yaml 2>&1 | tee -a bridge-run-\$(date +%Y%m%d).log" Enter

echo 0 > "$STATE"
echo "$(stamp) restart issued" >> "$LOG"
