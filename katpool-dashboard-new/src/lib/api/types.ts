/**
 * TypeScript mirror of the katpool `/api/v1` wire contract.
 *
 * Authoritative source: `api/src/models.rs` + `api/tests/wire_contract.rs`.
 * All on-chain amounts are {@link KasAmount} (decimal strings, never floats);
 * hashrate fields are JSON numbers (a rate). Keep these in lockstep with the
 * Rust DTOs.
 */

/** A signed on-chain amount: raw sompi + human KAS, both decimal strings. */
export interface KasAmount {
  sompi: string;
  kas: string;
}

/** The stable error envelope returned by the API for any non-2xx. */
export interface ApiErrorBody {
  error: { code: string; message: string };
}

export interface BlockCounts {
  found: number;
  submitted_to_node: number;
  confirmed_blue: number;
  matured: number;
  orphaned: number;
}

/**
 * Total blocks the pool has ever found, across every lifecycle status.
 * `BlockCounts.found` alone is only the transient just-detected state (≈0 in
 * steady state, since blocks immediately progress to confirmed/matured), so the
 * "Blocks found" headline must sum all statuses.
 */
export function totalBlocksFound(b: BlockCounts): number {
  return (
    b.found + b.submitted_to_node + b.confirmed_blue + b.matured + b.orphaned
  );
}

export interface PayoutTotals {
  kas_confirmed: KasAmount;
  nacho_confirmed: KasAmount;
  confirmed_payouts: number;
}

export interface TreasuryView {
  captured_at: string;
  kas_balance: KasAmount;
  nacho_balance: string;
  daa_score: number;
  blue_score: number;
}

export interface PoolStats {
  as_of: string;
  window_secs: number;
  miners_active: number;
  workers_active: number;
  hashrate_hs: number;
  accepted_shares: number;
  blocks: BlockCounts;
  payouts: PayoutTotals;
  treasury: TreasuryView | null;
}

export interface HashrateSnapshot {
  hashrate_hs: number;
  window_secs: number;
}

export interface HashratePointView {
  bucket_start: string;
  hashrate_hs: number;
  /** Present when the bucket was still open at the series `to` bound. */
  partial?: boolean;
}

export type BucketToken = "1m" | "5m" | "1h" | "1d";

export interface HashrateHistory {
  from: string;
  to: string;
  bucket: BucketToken;
  points: HashratePointView[];
}

export type BlockStatus =
  | "found"
  | "submitted_to_node"
  | "confirmed_blue"
  | "matured"
  | "orphaned";

export interface BlockView {
  id: number;
  hash: string;
  status: BlockStatus;
  daa_score: number;
  blue_score: number | null;
  found_at: string;
  confirmed_at: string | null;
  matured_at: string | null;
  reward: KasAmount | null;
}

export interface BlocksPage {
  blocks: BlockView[];
  next_before: number | null;
}

export type PayoutKind = "kas" | "nacho";
export type CycleStatus =
  | "planned"
  | "broadcasting"
  | "partially_settled"
  | "settled"
  | "failed";

export interface CycleView {
  id: number;
  kind: PayoutKind;
  status: CycleStatus;
  daa_start: number;
  daa_end: number;
  planned_at: string;
  settled_at: string | null;
  total: KasAmount;
  total_recipients: number;
}

export interface CyclesPage {
  cycles: CycleView[];
  next_before: number | null;
}

export interface CycleRecipientView {
  payout_id: number;
  address: string;
  amount: KasAmount;
  status: PayoutStatus;
  tx_hash: string | null;
  krc20_commit_hash: string | null;
  krc20_reveal_hash: string | null;
  nacho_amount: string | null;
}

export interface CycleDetailPage {
  cycle: CycleView;
  recipients: CycleRecipientView[];
}

// LeaderboardEntryView / LeaderboardResponse removed — ZKas exposes no
// per-miner or top-miner rankings (miner privacy).

export interface ActiveMinersPointView {
  bucket_start: string;
  miners: number;
}

export interface ActiveMinersHistory {
  from: string;
  to: string;
  bucket: BucketToken;
  points: ActiveMinersPointView[];
}

export interface FirmwareEntryView {
  app: string | null;
  workers: number;
  sessions: number;
}

export interface FirmwareBreakdown {
  window_secs: number;
  entries: FirmwareEntryView[];
}

export interface GeoEntryView {
  country: string;
  workers: number;
  sessions: number;
}

export interface GeoBreakdown {
  window_secs: number;
  entries: GeoEntryView[];
}

export interface ActiveSessions {
  active_sessions: number;
  active_workers: number;
}

export interface KasBalanceView {
  allocated: KasAmount;
  paid: KasAmount;
  payable: KasAmount;
}

export interface NachoRebateView {
  accrued: KasAmount;
  paid: KasAmount;
  pending: KasAmount;
}

export interface BalanceResponse {
  address: string;
  network: string;
  kas: KasBalanceView;
  nacho_rebate: NachoRebateView;
}

export interface MinerProfile {
  address: string;
  network: string;
  first_seen_at: string;
  last_seen_at: string;
  window_secs: number;
  accepted_shares: number;
  rejected_shares: number;
  hashrate_hs: number;
  workers_count: number;
  kas: KasBalanceView;
  nacho_rebate: NachoRebateView;
}

export interface WorkerView {
  name: string;
  first_seen_at: string;
  last_seen_at: string;
  accepted_shares: number;
  hashrate_hs: number;
}

export interface WorkersResponse {
  address: string;
  window_secs: number;
  workers: WorkerView[];
}

export type PayoutStatus =
  | "planned"
  | "submitted"
  | "accepted"
  | "confirmed"
  | "failed";

export interface MinerPayoutView {
  id: number;
  cycle_id: number;
  kind: PayoutKind;
  amount: KasAmount;
  status: PayoutStatus;
  tx_hash: string | null;
  krc20_commit_hash: string | null;
  krc20_reveal_hash: string | null;
  planned_at: string;
  submitted_at: string | null;
  confirmed_at: string | null;
  failure_reason: string | null;
  nacho_amount: string | null;
}

export interface MinerPayoutsPage {
  address: string;
  payouts: MinerPayoutView[];
  next_before: number | null;
}

export interface RejectReasonCount {
  reason: string;
  count: number;
}

export interface RejectsResponse {
  address: string;
  window_secs: number;
  total: number;
  by_reason: RejectReasonCount[];
}

export interface PoolRejectsResponse {
  window_secs: number;
  total: number;
  by_reason: RejectReasonCount[];
}

export interface FullRebateResponse {
  address: string;
  tier: "standard" | "elite" | null;
  full_rebate: boolean;
}

/** Aggregated network + market context produced by the BFF `/api/network`. */
export interface NetworkContext {
  /** Network hashrate in H/s (normalized from the Kaspa API's TH/s). */
  network_hashrate_hs: number;
  difficulty: number;
  block_reward_kas: number;
  circulating_supply_kas: number;
  max_supply_kas: number;
  blue_score: number | null;
  next_halving: {
    timestamp: number;
    date: string;
    reward_kas: number;
  } | null;
  prices: {
    kas_usd: number | null;
    kas_market_cap_usd: number | null;
    kas_change_24h: number | null;
    nacho_usd: number | null;
    nacho_change_24h: number | null;
  };
  /** Sources that failed to resolve (degraded mode), for UI affordances. */
  degraded: string[];
}
