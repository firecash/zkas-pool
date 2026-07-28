#!/bin/sh
set -eu

if ! curl -fsS --max-time 3 http://127.0.0.1:3034/api/stats >/dev/null; then
    systemctl restart firecash-pool-redactor.service
fi
