import type { Metadata } from "next";
import { PageHeader } from "@/components/dashboard/page-header";
import { OverviewHero } from "@/features/overview/hero";
import { HashratePanel } from "@/features/overview/hashrate-panel";
import { MinersPanel } from "@/features/overview/miners-panel";
import { NetworkPanel } from "@/features/overview/network-panel";
import { BlocksSummary } from "@/features/overview/blocks-summary";
import { FirmwarePanel } from "@/features/firmware/firmware-panel";
import { GeoPanel } from "@/features/geo/geo-panel";
import { PayoutsSummary } from "@/features/overview/payouts-summary";
import { LiveBlockFeed } from "@/features/blocks/live-block-feed";
import { HalvingModule } from "@/features/network/halving-module";

export const metadata: Metadata = {
  description:
    "Live ZKas mining pool overview: real-time pool and network hashrate, active miners, blocks found, and payouts. Miner privacy: no per-miner or top-miner rankings.",
  alternates: { canonical: "/" },
};

export default function OverviewPage() {
  return (
    <div className="space-y-6">
      <PageHeader
        title="Pool Overview"
        description="Live hashrate, miners, blocks and rewards across the katpool network."
      />

      <OverviewHero />

      <div className="grid grid-cols-1 items-stretch gap-6 xl:grid-cols-3">
        <div className="xl:col-span-2">
          <HashratePanel />
        </div>
        <NetworkPanel />
      </div>

      <div className="grid grid-cols-1 items-stretch gap-6 xl:grid-cols-3">
        <div className="xl:col-span-2">
          <LiveBlockFeed />
        </div>
        <HalvingModule />
      </div>

      <div className="grid grid-cols-1 items-stretch gap-6 lg:grid-cols-2">
        <MinersPanel />
        <BlocksSummary />
      </div>

      <div className="grid grid-cols-1 items-stretch gap-6 lg:grid-cols-2">
        <GeoPanel />
        <FirmwarePanel />
      </div>

      <PayoutsSummary />
    </div>
  );
}
