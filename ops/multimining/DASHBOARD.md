Dashboard tables render 25 rows by default, with 50 and 100 also available. Pages
remain selected during polling, and filters reset the corresponding page. CSV
exports contain the complete filtered dataset, including rows on other pages.

Worker hashrate is credited share work in gigahashes divided by a shared rolling
300-second window. A newly started bridge uses its runtime (at least one second)
until the window fills. Worker names and reconnects do not reset this denominator.
`poolHashrate` is H/s, worker `hashrate` remains GH/s, and
`hashrateWindowSeconds` describes the nominal window. Network rate is separate.
This estimate naturally fluctuates with share arrival and assigned difficulty.

A single bridge's global dashboard covers every configured instance. To include
another listener, put it in the same configuration rather than combining lifetime
hashrates from unrelated processes.

Optional restart/migration history: set `RKSTRATUM_DASHBOARD_HISTORY` to a readable
JSON file containing `blocks` (the same objects as `/api/stats`) and
`shares_by_instance` (an object mapping instance labels to cumulative counts).
The file is read once per process. Deduplicated historical blocks and share totals
appear in the dashboard; they do not contribute to active workers or hashrate.
Keep deployment files outside the repository. Before a restart, an operator can
export the combined stats and per-instance share totals into a new snapshot.
The bridge does not overwrite the input file or persist wallet credentials.

Validation: cargo test --locked -p kaspa-stratum-bridge --all-targets;
with Playwright installed, node ops/multimining/check-pagination.cjs.
The isolated browser test uses synthetic data: 5000 blocks and 120 workers.
