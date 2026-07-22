"use client";

import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import { bffUrl, fetchBff } from "./client";
import type {
  ActiveMinersHistory,
  ActiveSessions,
  BalanceResponse,
  BlocksPage,
  BucketToken,
  CyclesPage,
  CycleDetailPage,
  FirmwareBreakdown,
  GeoBreakdown,
  PoolRejectsResponse,
  FullRebateResponse,
  HashrateHistory,
  MinerPayoutsPage,
  MinerProfile,
  NetworkContext,
  PoolStats,
  RejectsResponse,
  WorkersResponse,
} from "./types";
import { LIVE_HASHRATE_POLL_MS, LIVE_HASHRATE_WINDOW_SECS } from "../hashrate-live";

/** Default live-refresh cadence for pool-wide widgets (ms). */
const LIVE_MS = 10_000;
const NETWORK_MS = 60_000;

interface RangeArgs {
  from?: string;
  to?: string;
  bucket?: BucketToken;
}

/** Flatten a range into a plain query record for {@link bffUrl}. */
function rangeQuery(args: RangeArgs): Record<string, string | undefined> {
  return { from: args.from, to: args.to, bucket: args.bucket };
}

function useBff<T>(
  key: readonly unknown[],
  url: string,
  refetchInterval: number | false = false,
  enabled = true,
): UseQueryResult<T, Error> {
  return useQuery<T, Error>({
    queryKey: key,
    queryFn: ({ signal }) => fetchBff<T>(url, signal),
    refetchInterval,
    refetchIntervalInBackground: true,
    refetchOnMount: "always",
    enabled,
  });
}

export function usePoolStats(windowSecs?: number) {
  const interval =
    windowSecs === LIVE_HASHRATE_WINDOW_SECS ? LIVE_HASHRATE_POLL_MS : LIVE_MS;
  return useBff<PoolStats>(
    ["pool", "stats", windowSecs ?? null],
    bffUrl("/api/v1/pool/stats", { window: windowSecs }),
    interval,
  );
}

/** Live headline hashrate: 5-minute sliding window, 5-second poll. */
export function usePoolLiveStats() {
  return usePoolStats(LIVE_HASHRATE_WINDOW_SECS);
}

export function usePoolHashrateHistory(args: RangeArgs) {
  return useBff<HashrateHistory>(
    ["pool", "hashrate/history", args],
    bffUrl("/api/v1/pool/hashrate/history", rangeQuery(args)),
    LIVE_MS,
  );
}

export function useActiveMinersHistory(args: RangeArgs) {
  return useBff<ActiveMinersHistory>(
    ["pool", "miners/history", args],
    bffUrl("/api/v1/pool/miners/history", rangeQuery(args)),
    LIVE_MS,
  );
}

// useLeaderboard removed — ZKas does not expose per-miner/top-miner rankings.

export function useFirmware(windowSecs?: number) {
  return useBff<FirmwareBreakdown>(
    ["pool", "firmware", windowSecs ?? null],
    bffUrl("/api/v1/pool/firmware", { window: windowSecs }),
    LIVE_MS,
  );
}

export function usePoolRejects(windowSecs?: number) {
  return useBff<PoolRejectsResponse>(
    ["pool", "rejects", windowSecs ?? null],
    bffUrl("/api/v1/pool/rejects", { window: windowSecs }),
    LIVE_MS,
  );
}

export function usePoolGeo(windowSecs?: number) {
  return useBff<GeoBreakdown>(
    ["pool", "geo", windowSecs ?? null],
    bffUrl("/api/v1/pool/geo", { window: windowSecs }),
    LIVE_MS,
  );
}

export function useActiveSessions() {
  return useBff<ActiveSessions>(
    ["pool", "active-sessions"],
    bffUrl("/api/v1/pool/active-sessions"),
    LIVE_MS,
  );
}

export function useBlocks(limit?: number, before?: number) {
  return useBff<BlocksPage>(
    ["pool", "blocks", limit ?? null, before ?? null],
    bffUrl("/api/v1/pool/blocks", { limit, before }),
    LIVE_MS,
  );
}

export function usePayoutCycles(limit?: number, before?: number) {
  return useBff<CyclesPage>(
    ["pool", "payouts", limit ?? null, before ?? null],
    bffUrl("/api/v1/pool/payouts", { limit, before }),
    LIVE_MS,
  );
}

export function usePayoutCycle(cycleId: number | null) {
  return useBff<CycleDetailPage>(
    ["pool", "payouts", "detail", cycleId],
    bffUrl(`/api/v1/pool/payouts/${cycleId}`),
    LIVE_MS,
    cycleId != null,
  );
}

export function useNetworkContext() {
  return useBff<NetworkContext>(["network"], "/api/network", NETWORK_MS);
}

// ---- per-miner -------------------------------------------------------

export function useMinerProfile(address: string, enabled = true) {
  return useBff<MinerProfile>(
    ["miner", address, "profile"],
    bffUrl(`/api/v1/miners/${encodeURIComponent(address)}`),
    LIVE_MS,
    enabled,
  );
}

export function useMinerWorkers(address: string, enabled = true) {
  return useBff<WorkersResponse>(
    ["miner", address, "workers"],
    bffUrl(`/api/v1/miners/${encodeURIComponent(address)}/workers`),
    LIVE_MS,
    enabled,
  );
}

export function useMinerHashrateHistory(address: string, args: RangeArgs, enabled = true) {
  return useBff<HashrateHistory>(
    ["miner", address, "hashrate/history", args],
    bffUrl(`/api/v1/miners/${encodeURIComponent(address)}/hashrate/history`, rangeQuery(args)),
    LIVE_MS,
    enabled,
  );
}

export function useMinerPayouts(address: string, limit?: number, before?: number, enabled = true) {
  return useBff<MinerPayoutsPage>(
    ["miner", address, "payouts", limit ?? null, before ?? null],
    bffUrl(`/api/v1/miners/${encodeURIComponent(address)}/payouts`, { limit, before }),
    LIVE_MS,
    enabled,
  );
}

export function useMinerRejects(address: string, enabled = true) {
  return useBff<RejectsResponse>(
    ["miner", address, "rejects"],
    bffUrl(`/api/v1/miners/${encodeURIComponent(address)}/rejects`),
    LIVE_MS,
    enabled,
  );
}

export function useMinerBalance(address: string, enabled = true) {
  return useBff<BalanceResponse>(
    ["miner", address, "balance"],
    bffUrl(`/api/v1/balance/${encodeURIComponent(address)}`),
    LIVE_MS,
    enabled,
  );
}

export function useFullRebate(address: string, enabled = true) {
  return useBff<FullRebateResponse>(
    ["miner", address, "full_rebate"],
    bffUrl(`/api/v1/full_rebate/${encodeURIComponent(address)}`),
    LIVE_MS,
    enabled,
  );
}
