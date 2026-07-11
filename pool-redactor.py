#!/usr/bin/env python3
"""Privacy redactor + miner lookup for the FireCash pool's public stats.

The stratum bridge's own /api/stats and /metrics expose full miner wallet
addresses. On a shielded-by-default chain that is a privacy leak, so this tiny
proxy is the ONLY pool endpoint exposed publicly:

  GET /api/stats          -> pool-wide stats, every address MASKED.
  GET /api/miner?address= -> stats for ONE address (you must supply the full
                             address, so it only ever reveals your own data).

Bind loopback; nginx proxies it.
"""
import json
import re
import urllib.request
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BRIDGE = "http://127.0.0.1:3033"
LISTEN = ("127.0.0.1", 3034)
REWARD_FC = 44  # coinbase reward per block, paid straight to the miner's address
_METRICS = {
    "found": "ks_blocks_accepted_by_node",
    "confirmed": "ks_blocks_mined",
    "pending": "ks_blocks_not_confirmed_blue",
}
_RES = {k: re.compile(m + r'\{[^}]*wallet="([^"]+)"[^}]*\}\s+([0-9.eE+-]+)') for k, m in _METRICS.items()}


def mask_addr(a):
    if not isinstance(a, str) or ":" not in a:
        return "—"
    hrp, _, body = a.partition(":")
    if len(body) <= 12:
        return f"{hrp}:{body}"
    return f"{hrp}:{body[:4]}…{body[-4:]}"


def blocks_by_wallet():
    """{wallet: {found, confirmed, pending}} from /metrics. ks_blocks_accepted_by_node
    is 'blocks found'; ks_blocks_mined is confirmed/paid; ks_blocks_not_confirmed_blue
    is still maturing. Duplicate per-session series are 0, so summing is safe."""
    out = {}
    try:
        text = urllib.request.urlopen(BRIDGE + "/metrics", timeout=8).read().decode()
    except Exception:
        return out
    for key, rx in _RES.items():
        for wallet, val in rx.findall(text):
            try:
                out.setdefault(wallet, {"found": 0, "confirmed": 0, "pending": 0})[key] += int(float(val))
            except Exception:
                pass
    return out


def redact(stats, bbw):
    """Public pool feed. Per-worker hashrate + shares stay visible (operators want
    the live worker view), but every wallet ADDRESS is masked to a censored form —
    so you can see the distribution of hashrate without learning who owns it. A miner
    sees their OWN full stats via /api/miner (self-lookup by exact address)."""
    workers = stats.get("workers") or []
    pool_hashrate_hs = sum((w.get("hashrate") or 0) for w in workers) * 1e9  # GH/s -> H/s
    return {
        "networkHashrate": stats.get("networkHashrate"),
        "networkBlockCount": stats.get("networkBlockCount"),
        "networkDifficulty": stats.get("networkDifficulty"),
        "activeWorkers": stats.get("activeWorkers") or len(workers),
        "poolHashrate": pool_hashrate_hs,
        "totalBlocks": stats.get("totalBlocks"),
        "blocksAccepted": sum(v["found"] for v in bbw.values()),
        "totalShares": stats.get("totalShares"),
        "bridgeUptime": stats.get("bridgeUptime"),
        # Per-worker rows: hashrate + shares kept; ADDRESS masked.
        "workers": [{
            "worker": w.get("worker") or "—",
            "wallet": mask_addr(w.get("wallet")),
            "hashrate": w.get("hashrate"),
            "shares": w.get("shares"),
        } for w in workers],
        # Recent blocks: hash + time + worker label; ADDRESS masked.
        "blocks": [{
            "worker": b.get("worker") or "—",
            "wallet": mask_addr(b.get("wallet")),
            "hash": b.get("hash"),
            "timestamp": b.get("timestamp"),
        } for b in (stats.get("blocks") or [])[:20]],
    }


def miner(address, stats, bbw):
    """Stats for one exact address. Caller supplied the full address, so this only
    ever exposes that address's own data — no enumeration of others."""
    address = (address or "").strip()
    workers = [w for w in (stats.get("workers") or []) if w.get("wallet") == address]
    blk = bbw.get(address, {"found": 0, "confirmed": 0, "pending": 0})
    confirmed = blk["confirmed"]
    pending = blk.get("pending") or max(0, blk["found"] - confirmed)
    return {
        "address": address,
        "found": bool(workers) or address in bbw,
        "workers": [{
            "worker": w.get("worker") or "—",
            "hashrate": w.get("hashrate"),   # GH/s
            "shares": w.get("shares") or 0,
        } for w in workers],
        "totalHashrate": sum((w.get("hashrate") or 0) for w in workers),  # GH/s
        "totalShares": sum((w.get("shares") or 0) for w in workers),
        "blocksFound": blk["found"],
        "blocksConfirmed": confirmed,
        "blocksPending": pending,
        "paidFc": confirmed * REWARD_FC,      # already yours on-chain
        "pendingFc": pending * REWARD_FC,     # maturing / confirming
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        path = u.path.rstrip("/")
        try:
            if path in ("/api/stats", "/pubstats"):
                stats = json.loads(urllib.request.urlopen(BRIDGE + "/api/stats", timeout=8).read())
                self._send(200, json.dumps(redact(stats, blocks_by_wallet())))
            elif path == "/api/miner":
                addr = (parse_qs(u.query).get("address") or [""])[0]
                if not addr:
                    self._send(400, json.dumps({"error": "address required"}))
                    return
                stats = json.loads(urllib.request.urlopen(BRIDGE + "/api/stats", timeout=8).read())
                self._send(200, json.dumps(miner(addr, stats, blocks_by_wallet())))
            else:
                self._send(404, json.dumps({"error": "not found"}))
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}))


if __name__ == "__main__":
    ThreadingHTTPServer(LISTEN, Handler).serve_forever()
