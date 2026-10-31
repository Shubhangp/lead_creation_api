# Dashboard stats (pre-computed daily counters)

`/admin/stats_dashboard` no longer reads raw rows. Every write to a tracked table
updates small per-day counter items in `stats_daily`; the dashboard reads only
those, so it stays fast however big the tables get.

```
 any writer (API, CSV uploads, scripts, dashboard)
        │ put / update
        ▼
 tracked tables ──DynamoDB Stream──▶ Lambda ratecut-stats-aggregator
 (leads, *_response_logs, rcs_queue)      │ old→new difference, batched, exactly-once
                                          ▼
                                   stats_daily  (1 item per table per day)
                                          │ few small reads
                                          ▼
                     GET /api/v1/unified-stats/v2/*  ──▶  dashboard
```

## First-time setup (once)

```bash
npm i -D @aws-sdk/client-iam @aws-sdk/client-lambda   # only the setup script needs these
node scripts/stats/setup.js --dry-run                 # see what will change
node scripts/stats/setup.js                           # table, role, Lambda, streams
node scripts/stats/backfill.js                        # history (rows before the cutover)
node scripts/stats/verify.js --day 2026-10-29         # spot-check vs the old engine
```

Then deploy the API (new `/v2` routes) and the dashboard.

While `backfill.js` runs (minutes per table), avoid old-data imports and status CSV
uploads for those tables — those rows could be counted twice. Normal lead traffic is fine.

## Day to day

Nothing to do. Numbers appear on the dashboard ~10–20 s after a write.

| Task | Command |
|---|---|
| Changed `stats/specs.js` (new dimension, new category rule) | `node scripts/stats/setup.js --code-only` then `node scripts/stats/backfill.js --rebuild --only <key> --from <first day> --to <yesterday>` |
| Numbers look wrong for some days | `node scripts/stats/backfill.js --rebuild --only <key> --from … --to …` |
| Add a new table | add a spec in `stats/specs.js`, then `setup.js --only <key>` and `backfill.js --only <key>` |
| Lambda health | CloudWatch log group `/aws/lambda/ratecut-stats-aggregator` (one JSON line per batch: counted / skipped / errors) |

## What is counted

- Day = UTC date of `createdAt` (RCS: `scheduledTime`), same as the old dashboard.
- Every row counts, whatever its source (the old code only counted sources listed in
  `RESPONSELOG_SOURCES` / `LEAD_SOURCES`, so totals can be a little higher now).
- Updates move a row between categories (e.g. a CSV status upload that marks a loan
  disbursed) on the row's original day.
- Deletes are ignored: purging old logs keeps their history in the stats.
- "Unique" figures (phones, PANs, leads) are HyperLogLog estimates, about ±3%.

## Storage layout (`stats_daily`, PK `tbl`, SK `sk`)

| sk | written by | content |
|---|---|---|
| `YYYY-MM-DD` | Lambda (ADD) | live counters |
| `YYYY-MM-DD#bf` | backfill (PUT) | history counters |
| `YYYY-MM-DD#u#<name>` | both (max-merge) | unique-count sketch |
| `_meta` / `<table>` | setup | cutover time, stream ARN |
| `_idem#<table>` / … | Lambda | retry markers, auto-deleted by TTL after 3 days |

Counter names: `t` total · `c|cat` · `s|source` · `sc|source|cat` · `d|dimension|value`
· `n|metric` sum · `k|metric` count · `h|HH` hour (IST).

## Old endpoints

`/api/v1/unified-stats`, `/by-date`, `/quick` still exist (other tools may call
them) but they read raw rows and can still exhaust memory on big ranges. The
dashboard no longer uses them; remove them once nothing else does.
