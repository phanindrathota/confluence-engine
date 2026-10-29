# Confluence Engine

Phase 0 of the Confluence Engine: collect QuantData market-structure data
point-in-time, and backfill history, into a bronze/silver lakehouse
(gzip JSON + Parquet, queried with DuckDB, Delta-ready).

Later phases (features, contract simulator, judge, learner) build on this
data. This repo never places orders; see `AGENTS.md`.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
cp .env.example .env                                    # then add QUANTDATA_API_KEY
```

## First run, in this order

```bash
# 1. Check the key, measure how far back your plan goes, save sample payloads.
confluence probe

# 2. Pull the last 30 days (newest first, resumable; Ctrl-C and rerun any time).
confluence backfill --days 30

# 3. Start live collection on a trading day (08:25–15:20 CT).
#    After the close it pulls the day in backtest format and posts a health report.
confluence collect --forever
```

Read `data/probe/report.json` after step 1. `lookback` shows which past sessions
returned data, and `samples.*.always_null` lists columns that came back empty,
which means a field name differs from the docs and a normalizer needs a fix.

## Commands

| Command | What it does |
| --- | --- |
| `confluence probe [--ticker SPY]` | Auth check, history depth, one sample per endpoint |
| `confluence collect [--forever] [--once all\|order_flow,...]` | Live collector; `--once` runs jobs immediately for testing |
| `confluence backfill [--days N \| --start D --end D] [--tickers A,B] [--datasets ...] [--redo] [--force]` | Resumable history pull; pauses during market hours unless `--force` |
| `confluence health [--date D] [--post]` | Gap and lag report for a session; `--post` sends it to Discord |
| `confluence gate [--sessions 10]` | Counts clean live sessions; Phase 0 passes at 5 |
| `confluence rebuild-silver --yes` | Rebuilds silver from bronze after a normalizer change |
| `confluence sql "SELECT ..."` | DuckDB over the silver views |

Examples:

```bash
confluence sql "SELECT ticker, count(*), sum(premium) FROM flow_print_first
                WHERE session_date = DATE '2026-10-27' GROUP BY 1 ORDER BY 3 DESC"
confluence sql "SELECT quantile_cont(lag_ms, [0.5, 0.95]) FROM flow_print_first WHERE source='live'"
```

## Rate budget

QuantData allows 240 requests/min and 20/s **per user**, shared by every key on
the account (your other agent included). The collector:

- caps itself at `QD_BUDGET_PER_MIN` (default 150) and 15/s,
- reads `X-RateLimit-Remaining` and slows down when the shared bucket drops to
  `QD_RESERVE_REQUESTS`, so it yields to the other agent,
- honors `Retry-After` on 429s and stops after 3 in a row.

Live load with the default 23-ticker universe is roughly 90–100 requests/min.
Backfill is roughly 300 requests per session, so a year takes several hours;
it pauses during market hours so it never competes with live collection.

## What is collected

| Dataset | Live cadence | Backfill |
| --- | --- | --- |
| Order flow (sweeps, blocks; premium ≥ $50K) | 20 s | per ticker per session |
| Net drift (1m) | 60 s | 1m, full session |
| Stock bars (1m) | 60 s | 1m, full session |
| GEX + DEX by strike, full chain | 180 s | interval-map, 5m |
| Charm + vanna by strike | 600 s | interval-map, 5m |
| Dark flow (5m) | 300 s | 5m |
| Volatility drift (realized vs implied) | 300 s | full session |
| News | 120 s | per session |

## Running it every day

The simplest setup is one long-lived process: `confluence collect --forever`.
On macOS or Linux, run it under `tmux`, `launchd` or `systemd`; on Windows, a
Task Scheduler task at 08:15 CT running `confluence collect` (without
`--forever`) exits by itself after the close.

## Known limits

- QuantData streaming is "coming soon"; everything is REST polling.
- Historical rows carry trade time, not publication time. The live collector
  measures the publication lag (`lag_ms`) so backtests can apply it.
- Option bars are trade prices, not quotes. Fill realism comes in Phase 2
  (Schwab quotes).
- NYSE holidays in `config/universe.yaml` cover late 2025 through 2027; verify yearly.

## Layout

```
config/universe.yaml     tickers, cadences, thresholds, holidays
src/confluence/
  qd_client.py           HTTP, retries, bronze capture
  ratelimit.py           shared-quota limiter
  queries.py             request bodies (documented fields only)
  datasets.py            endpoint -> silver table routing
  storage/bronze.py      raw capture
  storage/silver.py      schemas and normalizers
  collector/live.py      async workers
  collector/backfill.py  resumable history pull
  probe.py, health.py, query.py, cli.py
docs/SCHEMA.md           tables, grains, point-in-time rules
tests/                   fake QuantData server + pipeline tests
```
