# Data layout and schema

```
data/
  bronze/source=quantdata/endpoint=<path>/dt=<session>/part-<pid>-<hour>.jsonl.gz
  silver/<table>/session_date=<session>/part-*.parquet
  state/backfill.sqlite      resumable backfill checkpoint
  state/heartbeat.json       live collector status, rewritten every minute
  probe/                     sample payloads and the probe report
```

## Bronze

One JSON line per HTTP exchange: `request_id, endpoint, tag (live|backfill|probe),
request, status, received_ts, latency_ms, ratelimit{limit,remaining,reset}, payload`.
Append-only. Silver is rebuilt from it with `confluence rebuild-silver --yes`.

## Silver

Every row has `session_date`, `received_ts` (UTC, when we got it) and `source`
(`live` or `backfill`). Time-based rows also have `event_ts` (UTC, trade time or
bucket start). **Point-in-time rule: at decision time T, use only rows with
`received_ts <= T`.**

| Table | Grain | Source endpoints | Notes |
| --- | --- | --- | --- |
| `flow_print` | option print × observation | order-flow/consolidated | `lag_ms` = received_ts − event_ts. Premium floor $50K at collection |
| `net_drift` | ticker × bucket × observation | net-drift | Per-bucket, not cumulative. `bucket_closed=false` means the bucket was still filling |
| `bar` | ticker × bucket × observation | stock-price-over-time | No volume on this endpoint |
| `dark_flow` | ticker × bucket × observation | dark-flow | Not collected for SPX |
| `exposure_strike` | ticker × snapshot × greek × expiry × strike | exposure-by-strike (live), interval-map (backfill and end of day) | `endpoint` column tells them apart. Interval-map has no `representation`; scales may differ, so compare like with like |
| `metric_series` | ticker × bucket × metric | volatility-drift | Long format because the leaf names are not documented |
| `news_item` | article × observation | news-articles | Raw article JSON kept in `raw` |

## Convenience views (DuckDB, `confluence sql`)

- `flow_print_first`: each print at its earliest `received_ts`
- `net_drift_latest`, `bar_latest`: final value per bucket (after-the-fact analysis only)
- `news_first`: each article at its earliest `received_ts`
- `bronze`: all raw records

## Change log

- 0.1.0: initial schema.
