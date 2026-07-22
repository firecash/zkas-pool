"use client";

import { useState, type ReactNode } from "react";
import { QueryClient, QueryClientProvider, keepPreviousData } from "@tanstack/react-query";
import { ThemeProvider } from "next-themes";
import { SearchFocusProvider } from "@/components/shell/search-focus";
import { LiveQuerySync } from "@/components/live-query-sync";
import { DashboardApiError } from "@/lib/api/client";

/** App-wide client providers: theming + a single React Query client. */
export function Providers({ children }: { children: ReactNode }) {
  const [client] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 0,
            gcTime: 5 * 60_000,
            // Tolerate transient *server* blips (Railway/BFF cold start, a
            // dropped poll, a 5xx) so a single miss never flashes a hard error
            // in place of a live panel — but never retry a 4xx. Retrying a 429
            // (rate limited) or other client error just amplifies load into a
            // retry storm that keeps the upstream's rate budget exhausted; we
            // let `keepPreviousData` hold the panel and wait for the next poll.
            retry: (failureCount, error) => {
              const status = error instanceof DashboardApiError ? error.status : 0;
              if (status >= 400 && status < 500) return false;
              return failureCount < 2;
            },
            retryDelay: (attempt) => Math.min(1_000 * 2 ** attempt, 15_000),
            // Refetch on focus. Browsers pause `refetchInterval` while a tab is
            // backgrounded, so without this a tab left open shows frozen data
            // (e.g. "last block 2 days ago") until the next poll fires *after*
            // you return. Re-enabled now that `placeholderData: keepPreviousData`
            // holds the last good panel during the refetch — so focus refresh is
            // instant and flicker-free. `staleTime` still gates it: a refetch
            // only fires when the cached data is older than 10s.
            refetchOnWindowFocus: true,
            refetchOnReconnect: true,
            // Live updates without a page reload: poll every 5s so hashrate,
            // workers, blocks, shares, height etc. tick in real time. Browsers
            // pause this while the tab is backgrounded; `refetchOnWindowFocus`
            // above catches the app back up the moment you return. `staleTime: 0`
            // means each interval actually refetches, and `keepPreviousData`
            // holds the panel so the refresh is flicker-free.
            refetchInterval: 5_000,
            // Keep the last good data on screen across refetches and range
            // changes instead of collapsing to skeletons/errors.
            placeholderData: keepPreviousData,
          },
        },
      }),
  );

  return (
    <ThemeProvider attribute="class" defaultTheme="dark" enableSystem disableTransitionOnChange>
      <QueryClientProvider client={client}>
        <LiveQuerySync />
        <SearchFocusProvider>{children}</SearchFocusProvider>
      </QueryClientProvider>
    </ThemeProvider>
  );
}
