#!/usr/bin/env python3
"""Privacy redactor + miner lookup for the ZKas pool's public stats.

Computes ALL pool stats (pool + per-worker hashrate, shares, blocks) directly
from the stratum bridge's per-instance Prometheus metrics, so the dashboard works
independent of the bridge's /api/stats aggregator. Every wallet address is masked
on the public feed; a miner sees their own full stats via /api/miner.

  GET /api/stats          -> pool-wide stats, every address MASKED.
  GET /api/miner?address= -> stats for ONE address (supply the full address).

Bind loopback; nginx proxies it.
"""
import collections
import json
import re
import threading
import time
import urllib.request
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

INSTANCE_PORTS = [2114, 2115, 2116, 2117, 2118]
LISTEN = ("127.0.0.1", 3034)
REWARD_FC = 60          # coinbase reward per block at the live 1 BPS rate (60 ZKAS/block;
                        # see consensus/src/processes/coinbase.rs — 60 FC/s ÷ 1 BPS = 60/block)
TWO32 = 2 ** 32
SAMPLE_SECS = 15        # scrape cadence
WINDOW_SECS = 600       # hashrate = Δ(share-diff) · 2^32 / Δt over a 10-min rolling window
START = time.time()


def _rx(metric):
    return re.compile(r'^' + metric + r'\{([^}]*)\}\s+([0-9.eE+-]+)', re.M)


RX_DIFF = _rx("ks_valid_share_diff_counter")   # cumulative Σ share difficulty per worker
RX_SHARES = _rx("ks_valid_share_counter")       # cumulative valid shares per worker
RX_FOUND = _rx("ks_blocks_accepted_by_node")    # blocks found per worker
RX_MINED = _rx("ks_blocks_mined")               # confirmed/paid per worker
RX_PENDING = _rx("ks_blocks_not_confirmed_blue")  # maturing per worker
RX_NETHR = re.compile(r'^ks_estimated_network_hashrate_gauge\s+([0-9.eE+-]+)', re.M)
RX_NETBLK = re.compile(r'^ks_network_block_count\s+([0-9.eE+-]+)', re.M)
RX_NETDIFF = re.compile(r'^ks_network_difficulty_gauge\s+([0-9.eE+-]+)', re.M)


def _labels(s):
    return dict(re.findall(r'(\w+)="([^"]*)"', s))


def _agg_max(rx, text, out):
    """Aggregate a per-worker counter keyed by (wallet, worker), taking the max across
    duplicate label series (katpool emits a series with and without the miner label)."""
    for labels, val in rx.findall(text):
        lb = _labels(labels)
        w, wk = lb.get("wallet"), lb.get("worker")
        if not w:
            continue
        try:
            v = float(val)
        except ValueError:
            continue
        k = (w, wk)
        if v > out.get(k, 0.0):
            out[k] = v


# ---- shared state, refreshed by a background sampler --------------------------
_lock = threading.Lock()
_state = {
    "networkHashrate": 0.0, "networkBlockCount": 0, "networkDifficulty": 0.0,
    "activeWorkers": 0, "totalShares": 0, "totalBlocks": 0,
    "bridgeUptime": 0, "workers": [], "blocks": [],
}
_hist = {}   # (wallet, worker) -> deque[(ts, cumulative_diff)] over WINDOW_SECS


def sample():
    text = ""
    for p in INSTANCE_PORTS:
        try:
            text += urllib.request.urlopen(f"http://127.0.0.1:{p}/metrics", timeout=6).read().decode() + "\n"
        except Exception:
            continue
    if not text:
        return

    diff, shares, found, mined, pending = {}, {}, {}, {}, {}
    _agg_max(RX_DIFF, text, diff)
    _agg_max(RX_SHARES, text, shares)
    _agg_max(RX_FOUND, text, found)
    _agg_max(RX_MINED, text, mined)
    _agg_max(RX_PENDING, text, pending)

    def _gmax(rx):
        vals = [float(v) for v in rx.findall(text)]
        return max(vals) if vals else 0.0

    now = time.time()
    workers = []
    for k, cur in diff.items():
        wallet, worker = k
        dq = _hist.setdefault(k, collections.deque())
        # counter reset (bridge restart) → the cumulative counter dropped: start fresh.
        if dq and cur < dq[-1][1]:
            dq.clear()
        dq.append((now, cur))
        while len(dq) > 1 and now - dq[0][0] > WINDOW_SECS:
            dq.popleft()
        # Hashrate = Δ(share-difficulty) · 2^32 / Δt over the rolling window (smooth,
        # and non-zero for any worker that shared within the window → correct active count).
        hr_ghs = 0.0
        if len(dq) >= 2:
            ot, od = dq[0]
            dt = now - ot
            if dt > 0 and cur >= od:
                hr_ghs = (cur - od) * TWO32 / dt / 1e9
        workers.append({
            "worker": worker or "—",
            "wallet": wallet,
            "hashrate": hr_ghs,                 # GH/s
            "shares": int(shares.get(k, 0)),
        })
    # Also surface workers that are CONNECTED at the bridge but have not landed a
    # valid share yet (e.g. a small rig stuck on too-high difficulty, or one that
    # just connected). Prometheus only emits a series once a worker shares, so
    # without this they connect but never appear in the dashboard / miner lookup.
    seen = {(w["wallet"], w["worker"]) for w in workers}
    try:
        braw = urllib.request.urlopen("http://127.0.0.1:3033/api/stats", timeout=5).read().decode()
        for bw in (json.loads(braw).get("workers") or []):
            wallet = bw.get("wallet")
            worker = bw.get("worker") or "—"
            if not wallet or (wallet, worker) in seen:
                continue
            seen.add((wallet, worker))
            workers.append({
                "worker": worker,
                "wallet": wallet,
                "hashrate": 0.0,                       # no valid share yet → warming up
                "shares": int(bw.get("shares") or 0),
                "difficulty": bw.get("currentDifficulty"),
                "warmingUp": True,
            })
    except Exception:
        pass

    # drop workers gone since last scrape
    live = set(diff.keys())
    for k in list(_hist.keys()):
        if k not in live:
            _hist.pop(k, None)

    blocks_found = int(sum(found.values()))
    with _lock:
        _state.update({
            "networkHashrate": _gmax(RX_NETHR),
            "networkBlockCount": int(_gmax(RX_NETBLK)),
            "networkDifficulty": _gmax(RX_NETDIFF),
            # A worker is "active" if it is present in the current scrape (connected and
            # tracked), not only if we've computed a hashrate yet — computing a rate needs
            # two samples, so requiring hashrate>0 undercounts right after a (re)connect.
            "activeWorkers": len(workers),
            "totalShares": int(sum(shares.values())),
            "totalBlocks": blocks_found,
            "bridgeUptime": int(now - START),
            "workers": workers,
            "blocks": [],
        })
        bbw = {}
        for (w, wk) in set(list(found) + list(mined) + list(pending)):
            d = bbw.setdefault(w, {"found": 0, "confirmed": 0, "pending": 0})
            d["found"] += int(found.get((w, wk), 0))
            d["confirmed"] += int(mined.get((w, wk), 0))
            d["pending"] += int(pending.get((w, wk), 0))
        _state["_bbw"] = bbw


def sampler_loop():
    while True:
        try:
            sample()
        except Exception:
            pass
        time.sleep(SAMPLE_SECS)


def mask_addr(a):
    if not isinstance(a, str) or ":" not in a:
        return "—"
    hrp, _, body = a.partition(":")
    if len(body) <= 12:
        return f"{hrp}:{body}"
    return f"{hrp}:{body[:4]}…{body[-4:]}"


def blocks_by_wallet():
    """{wallet: {found, confirmed, pending}} aggregated across that wallet's workers."""
    with _lock:
        return {w: dict(v) for w, v in _state.get("_bbw", {}).items()}


def snapshot():
    with _lock:
        s = dict(_state)
    s["workers"] = list(_state.get("workers", []))
    return s


def redact(stats, bbw):
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
        "workers": [{
            "worker": w.get("worker") or "—",
            "wallet": mask_addr(w.get("wallet")),
            "hashrate": w.get("hashrate"),
            "shares": w.get("shares"),
            "difficulty": w.get("difficulty"),
            "warmingUp": bool(w.get("warmingUp")),
        } for w in sorted(workers, key=lambda x: -(x.get("hashrate") or 0))],
        "blocks": [],
    }


def miner(address, stats, bbw):
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
            "difficulty": w.get("difficulty"),
            "warmingUp": bool(w.get("warmingUp")),
        } for w in workers],
        "totalHashrate": sum((w.get("hashrate") or 0) for w in workers),  # GH/s
        "totalShares": sum((w.get("shares") or 0) for w in workers),
        "blocksFound": blk["found"],
        "blocksConfirmed": confirmed,
        "blocksPending": pending,
        "paidFc": confirmed * REWARD_FC,
        "pendingFc": pending * REWARD_FC,
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
                self._send(200, json.dumps(redact(snapshot(), blocks_by_wallet())))
            elif path == "/api/miner":
                addr = (parse_qs(u.query).get("address") or [""])[0]
                if not addr:
                    self._send(400, json.dumps({"error": "address required"}))
                    return
                self._send(200, json.dumps(miner(addr, snapshot(), blocks_by_wallet())))
            else:
                self._send(404, json.dumps({"error": "not found"}))
        except Exception as e:
            self._send(502, json.dumps({"error": str(e)}))


if __name__ == "__main__":
    sample()  # prime one sample so the first request isn't empty
    threading.Thread(target=sampler_loop, daemon=True).start()
    ThreadingHTTPServer(LISTEN, Handler).serve_forever()
