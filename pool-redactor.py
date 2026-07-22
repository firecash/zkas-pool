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
import os
import re
import subprocess
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
SAMPLE_SECS = 5         # scrape cadence (server-side refresh; client polls ~5s too)
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

# Authoritative network hashrate + difficulty straight from the node. The bridge's
# ks_estimated_network_hashrate_gauge was observed WRONG (256 GH/s while the node
# measured ~68 TH/s), producing the impossible "pool hashrate > network hashrate".
# The node's EstimateNetworkHashesPerSecond is the real measured total-network rate
# (Σ blueWork / Δt over the window) — the same figure Kaspa explorers report.
NODE_RPC = "127.0.0.1:16110"
PROTO_DIR = "/root/work/rusty-kaspa/rpc/grpc/core/proto"
HR_WINDOW = 1000       # blocks; node's blueWork/time averaging window for the estimate
_NODE = {"hr": 0.0, "diff": 0.0, "ts": 0.0}
NODE_TTL = 20.0        # seconds; refresh at roughly the scrape cadence

def _node_rpc(payload):
    return subprocess.run(
        ["grpcurl", "-plaintext", "-import-path", PROTO_DIR, "-proto", "messages.proto",
         "-d", payload, NODE_RPC, "protowire.RPC/MessageStream"],
        capture_output=True, text=True, timeout=8).stdout

def node_stats():
    """(network_hashrate_H/s, network_difficulty) measured by the node, cached for
    NODE_TTL. Both come straight from the node so the dashboard matches reality
    regardless of the bridge's gauges. Returns last-good (or zeros) on failure."""
    now = time.time()
    if now - _NODE["ts"] < NODE_TTL and _NODE["hr"] > 0:
        return _NODE["hr"], _NODE["diff"]
    try:
        out = _node_rpc('{"estimateNetworkHashesPerSecondRequest":{"windowSize":%d}}' % HR_WINDOW)
        m = re.search(r'"networkHashesPerSecond":\s*"?([0-9.eE+]+)"?', out)
        dag = _node_rpc('{"getBlockDagInfoRequest":{}}')
        md = re.search(r'"difficulty":\s*([0-9.eE+]+)', dag)
        if m:
            _NODE["hr"] = float(m.group(1))
            if md:
                _NODE["diff"] = float(md.group(1))
            _NODE["ts"] = now
    except Exception:
        pass  # keep last good values
    return _NODE["hr"], _NODE["diff"]


def _labels(s):
    return dict(re.findall(r'(\w+)="([^"]*)"', s))


def _agg_sessions(rx, text, out):
    """Aggregate a per-worker counter keyed by (wallet, worker).

    katpool emits duplicate series for one connection (with and without the
    `miner` label) AND a brand-new series per (re)connect — the `ip` label
    carries the source ip:port, which changes every session. So: take the MAX
    within one (wallet, worker, ip) session (dedupes the label variants) and
    SUM across sessions. The old max-only aggregation froze a miner's shares/
    blocks at the previous session's value after a stop/resume (new series
    restarts at 0 and never exceeds the old max) — the live "numbers never
    change again" bug."""
    per_session = {}
    for labels, val in rx.findall(text):
        lb = _labels(labels)
        w, wk = lb.get("wallet"), lb.get("worker")
        if not w:
            continue
        try:
            v = float(val)
        except ValueError:
            continue
        sess = (w, wk, lb.get("ip") or "")
        if v > per_session.get(sess, 0.0):
            per_session[sess] = v
    for (w, wk, _ip), v in per_session.items():
        k = (w, wk)
        out[k] = out.get(k, 0.0) + v


# ---- shared state, refreshed by a background sampler --------------------------
_lock = threading.Lock()
_state = {
    "networkHashrate": 0.0, "networkBlockCount": 0, "networkDifficulty": 0.0,
    "activeWorkers": 0, "totalShares": 0, "totalBlocks": 0,
    "bridgeUptime": 0, "workers": [], "blocks": [],
}
_hist = {}   # (wallet, worker) -> deque[(ts, cumulative_diff)] over WINDOW_SECS
# Rolling (ts, pool_blocks_cumulative, network_blocks_cumulative) for the pool's
# block-find share. Pool hashrate = network_hashrate · (Δpool_blocks / Δnet_blocks):
# the pool's real fraction of the network, so it is always ≤ network and the rest
# is the other miners. This replaces summing the bridge's (inflated) per-worker rates.
_blockshare = collections.deque()
BLOCKSHARE_WINDOW = 900   # 10-15 min of blocks for a stable share estimate
BLOCKSHARE_MIN_NET = 30   # need at least this many network blocks before trusting it

# ---- per-wallet payout history (solo model: 1 confirmed block = 60 ZKAS paid
# by the chain to that wallet). Fed from the bridge's recent-blocks list and
# persisted to disk so it survives redactor AND bridge restarts. ------------
PAYOUT_HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "redactor-payout-history.json")
PAYOUT_HISTORY_CAP = 200        # per wallet
_payouts_lock = threading.Lock()
_payouts = {}                   # wallet -> [{"ts","worker","hash"}] newest LAST
_payouts_seen = set()           # block hashes already recorded
_payouts_dirty = False


def _payouts_load():
    global _payouts, _payouts_seen
    try:
        with open(PAYOUT_HISTORY_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _payouts = {w: list(v)[-PAYOUT_HISTORY_CAP:] for w, v in data.items()
                        if isinstance(v, list)}
            _payouts_seen = {e.get("hash") for v in _payouts.values() for e in v
                             if isinstance(e, dict) and e.get("hash")}
    except Exception:
        _payouts, _payouts_seen = {}, set()


def _payouts_record(blocks):
    """Append new bridge blocks (wallet, worker, hash, timestamp) to history."""
    global _payouts_dirty
    with _payouts_lock:
        for b in blocks or []:
            h, w = b.get("hash"), b.get("wallet")
            if not h or not w or h in _payouts_seen:
                continue
            ts = b.get("timestamp")
            try:
                ts = int(float(ts))
            except (TypeError, ValueError):
                ts = int(time.time())
            lst = _payouts.setdefault(w, [])
            lst.append({"ts": ts, "worker": b.get("worker") or "—", "hash": h})
            del lst[:-PAYOUT_HISTORY_CAP]
            _payouts_seen.add(h)
            _payouts_dirty = True


def _payouts_save():
    global _payouts_dirty
    with _payouts_lock:
        if not _payouts_dirty:
            return
        snap = {w: list(v) for w, v in _payouts.items()}
        _payouts_dirty = False
    tmp = PAYOUT_HISTORY_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snap, f)
        os.replace(tmp, PAYOUT_HISTORY_FILE)
    except Exception:
        pass


def payout_history(wallet, limit=50):
    with _payouts_lock:
        lst = list(_payouts.get(wallet, []))
    return [{"ts": e["ts"], "worker": e["worker"], "hash": e["hash"],
             "amountFc": REWARD_FC} for e in reversed(lst[-limit:])]


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
    _agg_sessions(RX_DIFF, text, diff)
    _agg_sessions(RX_SHARES, text, shares)
    _agg_sessions(RX_FOUND, text, found)
    _agg_sessions(RX_MINED, text, mined)
    _agg_sessions(RX_PENDING, text, pending)

    def _gmax(rx):
        vals = [float(v) for v in rx.findall(text)]
        return max(vals) if vals else 0.0

    # The stratum bridge already computes a session-aware per-worker hashrate,
    # share count and current difficulty. Our own Δ(share-diff)/Δt is unreliable
    # for miners that reconnect often (each reconnect adds a new ip-labelled
    # Prometheus series, so the max-aggregated counter is non-monotonic and the
    # rate flaps to 0). So we PREFER the bridge's value and only fall back to our
    # computed rate when the bridge has none.
    bridge_by_key = {}
    bridge_total_blocks = None   # bridge's central cumulative pool-block count
    try:
        braw = urllib.request.urlopen("http://127.0.0.1:3033/api/stats", timeout=5).read().decode()
        bjson = json.loads(braw)
        # Central pool-block total from the bridge — monotonic within a bridge
        # session (unlike the sum of per-worker Prometheus series, which drops when
        # a worker disconnects). Used for the block-find share so worker churn
        # doesn't reset the window.
        bt = bjson.get("totalBlocks")
        if bt is not None:
            bridge_total_blocks = int(bt)
        # Record newly found blocks into the persistent per-wallet payout
        # history (solo model: each confirmed block = one 60-ZKAS payout).
        _payouts_record(bjson.get("blocks") or [])
        _payouts_save()
        for bw in (bjson.get("workers") or []):
            w = bw.get("wallet")
            wk = bw.get("worker") or "—"
            if not w:
                continue
            bridge_by_key[(w, wk)] = {
                "hr": float(bw.get("hashrate") or 0.0),          # GH/s
                "shares": int(bw.get("shares") or 0),
                "diff": bw.get("currentDifficulty"),
            }
    except Exception:
        pass

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
        # Fallback hashrate = Δ(share-difficulty) · 2^32 / Δt over the rolling window.
        hr_ghs = 0.0
        if len(dq) >= 2:
            ot, od = dq[0]
            dt = now - ot
            if dt > 0 and cur >= od:
                hr_ghs = (cur - od) * TWO32 / dt / 1e9
        b = bridge_by_key.get(k)
        hr_final = b["hr"] if (b and b["hr"] > 0) else hr_ghs   # prefer bridge, session-aware
        workers.append({
            "worker": worker or "—",
            "wallet": wallet,
            "hashrate": hr_final if b else 0.0,  # a dead session has no live rate
            "shares": int(shares.get(k, 0)) or (b["shares"] if b else 0),
            "difficulty": b["diff"] if b else None,
            # Prometheus counters persist for every session since bridge start;
            # only workers the bridge currently tracks are actually connected.
            "online": b is not None,
        })
    # Also surface workers that are CONNECTED at the bridge but have not landed a
    # valid share yet (e.g. a small rig stuck on too-high difficulty, or one that
    # just connected). Prometheus only emits a series once a worker shares, so
    # without this they connect but never appear in the dashboard / miner lookup.
    seen = {(w["wallet"], w["worker"]) for w in workers}
    for (wallet, worker), b in bridge_by_key.items():
        if (wallet, worker) in seen:
            continue
        workers.append({
            "worker": worker,
            "wallet": wallet,
            "hashrate": b["hr"],                       # bridge rate (may be 0 = warming up)
            "shares": b["shares"],
            "difficulty": b["diff"],
            "warmingUp": b["hr"] <= 0,
            "online": True,
        })

    # drop workers gone since last scrape
    live = set(diff.keys())
    for k in list(_hist.keys()):
        if k not in live:
            _hist.pop(k, None)

    blocks_found = int(sum(found.values()))

    # ---- Network + pool hashrate, both grounded in the node ----------------
    # Network = the node's measured EstimateNetworkHashesPerSecond (authoritative;
    # includes every miner, not just ours). Difficulty likewise from the node.
    net_hs, net_diff = node_stats()
    net_blocks = int(_gmax(RX_NETBLK))          # cumulative network blocks (from node)
    if net_diff <= 0:                           # node unreachable → fall back to bridge gauge
        net_diff = _gmax(RX_NETDIFF)

    # Pool hashrate = network × the pool's share of blocks found over a rolling
    # window. This is the pool's TRUE fraction of the network, so it is always ≤
    # network and the remainder is the other miners. (The bridge's per-worker rates
    # are optimistic and summed to MORE than the whole network — hence pool>network.)
    raw_sum_hs = sum((w.get("hashrate") or 0) for w in workers) * 1e9  # GH/s -> H/s
    # Use the bridge's central monotonic block total; only fall back to the
    # (churn-sensitive) per-worker sum if the bridge total is unavailable.
    pool_blocks = bridge_total_blocks if bridge_total_blocks is not None else blocks_found
    bs = _blockshare
    if bs and (pool_blocks < bs[-1][1] or net_blocks < bs[-1][2]):
        bs.clear()                              # counter reset (bridge/node restart)
    bs.append((now, pool_blocks, net_blocks))
    while len(bs) > 1 and now - bs[0][0] > BLOCKSHARE_WINDOW:
        bs.popleft()
    pool_hs = None
    dpool = dnet = -1
    if net_hs > 0 and len(bs) >= 2:
        dpool = pool_blocks - bs[0][1]
        dnet = net_blocks - bs[0][2]
        if dnet >= BLOCKSHARE_MIN_NET and dpool >= 0:
            pool_hs = net_hs * min(1.0, dpool / dnet)
    if pool_hs is None:                         # not enough block history yet
        pool_hs = min(raw_sum_hs, net_hs) if net_hs > 0 else raw_sum_hs
    if net_hs <= 0:                             # node fully unreachable: degrade gracefully
        net_hs = max(raw_sum_hs, pool_hs)

    # Rescale the per-worker rates so they sum to the true pool hashrate — keeps
    # relative rig sizes but makes the workers add up to the real pool total.
    scale = (pool_hs / raw_sum_hs) if raw_sum_hs > 0 else 0.0
    for w in workers:
        if w.get("hashrate"):
            w["hashrate"] = w["hashrate"] * scale

    with _lock:
        _state.update({
            "networkHashrate": net_hs,
            "poolHashrate": pool_hs,
            "networkBlockCount": net_blocks,
            "networkDifficulty": net_diff,
            # A worker is "active" if the bridge currently tracks its connection.
            # Prom counter series persist for every session since bridge start, so
            # counting raw series keys inflates this with long-disconnected rigs.
            "activeWorkers": sum(1 for w in workers if w.get("online")),
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
    # poolHashrate is computed at scrape time as the block-find share of the node's
    # network hashrate (workers are already rescaled to sum to it); fall back to the
    # worker sum only if an older snapshot lacks the field.
    pool_hashrate_hs = stats.get("poolHashrate")
    if pool_hashrate_hs is None:
        pool_hashrate_hs = sum((w.get("hashrate") or 0) for w in workers) * 1e9
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
        } for w in sorted(workers, key=lambda x: -(x.get("hashrate") or 0))
          if w.get("online")],
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
            "hashrate": w.get("hashrate"),   # GH/s (0 for offline sessions)
            "shares": w.get("shares") or 0,
            "difficulty": w.get("difficulty"),
            "warmingUp": bool(w.get("warmingUp")),
            "online": bool(w.get("online")),
        } for w in workers],
        "totalHashrate": sum((w.get("hashrate") or 0) for w in workers if w.get("online")),  # GH/s
        "totalShares": sum((w.get("shares") or 0) for w in workers),
        "blocksFound": blk["found"],
        "blocksConfirmed": confirmed,
        "blocksPending": pending,
        "paidFc": confirmed * REWARD_FC,
        "pendingFc": pending * REWARD_FC,
        # Per-block payout history (solo model: each confirmed block paid
        # 60 ZKAS straight to this wallet by the chain). Newest first.
        "payouts": payout_history(address),
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
    _payouts_load()  # payout history survives redactor + bridge restarts
    sample()  # prime one sample so the first request isn't empty
    threading.Thread(target=sampler_loop, daemon=True).start()
    ThreadingHTTPServer(LISTEN, Handler).serve_forever()
