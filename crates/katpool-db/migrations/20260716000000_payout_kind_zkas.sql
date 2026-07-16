-- Add the ZKas shielded payout cycle kind.
--
-- ZKas has no transparent UTXOs: the pool treasury holds Orchard shielded
-- notes and pays each miner with one shielded transaction per recipient
-- (payout-zkas crate). Cycle/payout rows reuse the existing tables; only the
-- kind discriminator is new. `ADD VALUE` is safe inside the migration
-- transaction because nothing in this migration uses the new value.

ALTER TYPE payout_kind ADD VALUE IF NOT EXISTS 'zkas';

INSERT INTO pool_meta (key, value) VALUES
    ('schema_payout_kind_zkas_migration', '20260716000000_payout_kind_zkas')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
