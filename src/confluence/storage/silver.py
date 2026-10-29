"""Silver layer: typed, point-in-time tables built from bronze responses.

Every row carries:
  event_ts     when the thing happened (trade time or bucket start), UTC
  received_ts  when we received the response that told us, UTC
  session_date the trading session it belongs to
  source       'live' or 'backfill'

Normalizers are pure functions from a QuantData payload to rows, so the same
code serves live collection, backfill and `confluence rebuild-silver`.
Unknown or undocumented leaf fields are tolerated rather than guessed.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from ..clock import UTC, from_epoch_ms


# ---------- schemas ----------
TS = pa.timestamp("ms", tz="UTC")
COMMON = [("session_date", pa.date32()), ("received_ts", TS), ("source", pa.string())]

SCHEMAS: dict[str, pa.Schema] = {
    "flow_print": pa.schema([
        ("id", pa.string()), ("ticker", pa.string()), ("event_ts", TS),
        ("contract_type", pa.string()), ("expiration_date", pa.date32()), ("dte", pa.float64()),
        ("strike", pa.float64()), ("premium", pa.float64()), ("size", pa.int64()),
        ("volume", pa.int64()), ("open_interest", pa.int64()), ("option_price", pa.float64()),
        ("bid", pa.float64()), ("ask", pa.float64()), ("stock_price", pa.float64()),
        ("iv", pa.float64()), ("trade_side", pa.string()), ("trade_type", pa.string()),
        ("consolidation_type", pa.string()), ("sentiment", pa.string()),
        ("is_golden_sweep", pa.bool_()), ("is_unusual", pa.bool_()), ("is_opening", pa.bool_()),
        ("is_vol_gt_oi", pa.bool_()), ("money_type", pa.string()), ("moneyness_pct", pa.float64()),
        ("delta", pa.float64()), ("gamma", pa.float64()), ("lag_ms", pa.int64()), *COMMON,
    ]),
    "net_drift": pa.schema([
        ("ticker", pa.string()), ("event_ts", TS), ("bucket", pa.string()),
        ("call_premium", pa.float64()), ("put_premium", pa.float64()),
        ("call_volume", pa.int64()), ("put_volume", pa.int64()),
        ("mid_call_premium", pa.float64()), ("mid_put_premium", pa.float64()),
        ("stock_price", pa.float64()), ("bucket_closed", pa.bool_()), *COMMON,
    ]),
    "bar": pa.schema([
        ("ticker", pa.string()), ("event_ts", TS), ("bucket", pa.string()),
        ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
        ("bucket_closed", pa.bool_()), *COMMON,
    ]),
    "dark_flow": pa.schema([
        ("ticker", pa.string()), ("event_ts", TS), ("bucket", pa.string()),
        ("notional", pa.float64()), ("shares", pa.int64()), ("trade_count", pa.int64()),
        ("stock_price", pa.float64()), ("bucket_closed", pa.bool_()), *COMMON,
    ]),
    "exposure_strike": pa.schema([
        ("ticker", pa.string()), ("event_ts", TS), ("greek", pa.string()),
        ("representation", pa.string()), ("expiration_date", pa.date32()), ("strike", pa.float64()),
        ("call_exposure", pa.float64()), ("put_exposure", pa.float64()),
        ("stock_price", pa.float64()), ("endpoint", pa.string()), *COMMON,
    ]),
    "metric_series": pa.schema([   # long format for series with undocumented leaf names
        ("ticker", pa.string()), ("event_ts", TS), ("dataset", pa.string()),
        ("metric", pa.string()), ("value", pa.float64()), *COMMON,
    ]),
    "news_item": pa.schema([
        ("id", pa.string()), ("event_ts", TS), ("title", pa.string()),
        ("tickers", pa.list_(pa.string())), ("topics", pa.list_(pa.string())),
        ("raw", pa.string()), *COMMON,
    ]),
}


# ---------- tolerant field access ----------
def _norm(key: str) -> str:
    return "".join(ch for ch in key.lower() if ch.isalnum())


def pick(obj: dict | None, *names: str, default: Any = None) -> Any:
    """Get a field by any spelling. QuantData field names ignore case and separators."""
    if not isinstance(obj, dict):
        return default
    index = {_norm(k): v for k, v in obj.items()}
    for n in names:
        v = index.get(_norm(n))
        if v is not None:
            return v
    return default


def _f(v) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _i(v) -> int | None:
    f = _f(v)
    return None if f is None else int(f)


def _b(v) -> bool | None:
    if v is None:
        return None
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)


def _d(v) -> date | None:
    if v is None:
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def _ts(v) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
        return from_epoch_ms(float(v))
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _bucket_ms(aggregation: str | None) -> int | None:
    if not aggregation:
        return None
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
    try:
        return int(aggregation[:-1]) * units[aggregation[-1]]
    except (ValueError, KeyError):
        return None


# ---------- normalizers ----------
def normalize_order_flow(payload: dict, ctx: dict) -> list[dict]:
    rows = []
    received: datetime = ctx["received_ts"]
    for r in (payload or {}).get("data") or []:
        event = _ts(pick(r, "tradeTime"))
        greeks = pick(r, "greeks") or {}
        money = pick(r, "moneyness") or {}
        rows.append({
            "id": str(pick(r, "id", default="")),
            "ticker": pick(r, "ticker"),
            "event_ts": event,
            "contract_type": pick(r, "contractType"),
            "expiration_date": _d(pick(r, "expirationDate")),
            "dte": _f(pick(r, "dte")),
            "strike": _f(pick(r, "strikePrice")),
            "premium": _f(pick(r, "premium")),
            "size": _i(pick(r, "size")),
            "volume": _i(pick(r, "volume")),
            "open_interest": _i(pick(r, "openInterest")),
            "option_price": _f(pick(r, "optionPrice")),
            "bid": _f(pick(r, "bidPrice")),
            "ask": _f(pick(r, "askPrice")),
            "stock_price": _f(pick(r, "stockPrice")),
            "iv": _f(pick(r, "impliedVolatility")),
            "trade_side": pick(r, "tradeSideCode", "tradeSide"),
            "trade_type": pick(r, "tradeType"),
            "consolidation_type": pick(r, "tradeConsolidationType"),
            "sentiment": pick(r, "sentimentType", "sentiment"),
            "is_golden_sweep": _b(pick(r, "isGoldenSweep")),
            "is_unusual": _b(pick(r, "isUnusual")),
            "is_opening": _b(pick(r, "isOpeningPosition")),
            "is_vol_gt_oi": _b(pick(r, "isVolumeGreaterThanOpenInterest")),
            "money_type": pick(money, "moneyType"),
            "moneyness_pct": _f(pick(money, "degreeInPercent")),
            "delta": _f(pick(greeks, "delta")),
            "gamma": _f(pick(greeks, "gamma")),
            "lag_ms": int((received - event).total_seconds() * 1000) if event else None,
            **_common(ctx),
        })
    return rows


def _time_series(payload: dict, ctx: dict, build: Callable[[dict], dict]) -> list[dict]:
    """Rows for maps keyed by epoch-ms bucket start. Marks buckets still open at receipt."""
    received: datetime = ctx["received_ts"]
    width = _bucket_ms(ctx.get("aggregation"))
    rows = []
    data = (payload or {}).get("data") or {}
    if not isinstance(data, dict):
        return rows
    for key, leaf in data.items():
        event = _ts(key)
        if event is None or not isinstance(leaf, dict):
            continue
        closed = None if width is None else (event.timestamp() * 1000 + width) <= received.timestamp() * 1000
        rows.append({"ticker": ctx["ticker"], "event_ts": event, "bucket": ctx.get("aggregation"),
                     "bucket_closed": closed, **build(leaf), **_common(ctx)})
    return rows


def normalize_net_drift(payload: dict, ctx: dict) -> list[dict]:
    return _time_series(payload, ctx, lambda b: {
        "call_premium": _f(pick(b, "netCallPremium")),
        "put_premium": _f(pick(b, "netPutPremium")),
        "call_volume": _i(pick(b, "netCallVolume")),
        "put_volume": _i(pick(b, "netPutVolume")),
        "mid_call_premium": _f(pick(b, "midMarketCallPremium")),
        "mid_put_premium": _f(pick(b, "midMarketPutPremium")),
        "stock_price": _f(pick(b, "stockPrice")),
    })


def normalize_bars(payload: dict, ctx: dict) -> list[dict]:
    return _time_series(payload, ctx, lambda b: {
        "open": _f(pick(b, "openPrice", "open")), "high": _f(pick(b, "highPrice", "high")),
        "low": _f(pick(b, "lowPrice", "low")), "close": _f(pick(b, "closePrice", "close")),
    })


def normalize_dark_flow(payload: dict, ctx: dict) -> list[dict]:
    return _time_series(payload, ctx, lambda b: {
        "notional": _f(pick(b, "notionalValue", "notional")), "shares": _i(pick(b, "size")),
        "trade_count": _i(pick(b, "tradeCount")), "stock_price": _f(pick(b, "stockPrice")),
    })


def normalize_metric_series(payload: dict, ctx: dict) -> list[dict]:
    """Long format: one row per numeric leaf. Used where leaf names are not documented."""
    rows = []
    data = (payload or {}).get("data") or {}
    if not isinstance(data, dict):
        return rows
    for key, leaf in data.items():
        event = _ts(key)
        if event is None or not isinstance(leaf, dict):
            continue
        for metric, value in _flatten(leaf).items():
            v = _f(value)
            if v is not None:
                rows.append({"ticker": ctx["ticker"], "event_ts": event, "dataset": ctx["dataset"],
                             "metric": metric, "value": v, **_common(ctx)})
    return rows


def normalize_exposure_by_strike(payload: dict, ctx: dict) -> list[dict]:
    rows = []
    data = (payload or {}).get("data") or {}
    event = ctx.get("snapshot_ts") or ctx["received_ts"]
    for ticker, block in (data.items() if isinstance(data, dict) else []):
        exposure = pick(block, "exposureMap") or {}
        spot = _f(pick(block, "stockPrice"))
        for exp, strikes in exposure.items():
            for strike, cell in (strikes or {}).items():
                rows.append({
                    "ticker": ticker, "event_ts": event, "greek": ctx["greek"],
                    "representation": ctx.get("representation"), "expiration_date": _d(exp),
                    "strike": _f(strike), "call_exposure": _f(pick(cell, "callExposure")),
                    "put_exposure": _f(pick(cell, "putExposure")), "stock_price": spot,
                    "endpoint": "exposure-by-strike", **_common(ctx),
                })
    return rows


def normalize_interval_map(payload: dict, ctx: dict) -> list[dict]:
    rows = []
    data = (payload or {}).get("data") or {}
    for bucket, grid in (data.items() if isinstance(data, dict) else []):
        event = _ts(bucket)
        for exp, strikes in (grid or {}).items():
            for strike, legs in (strikes or {}).items():
                rows.append({
                    "ticker": ctx["ticker"], "event_ts": event, "greek": ctx["greek"],
                    "representation": None, "expiration_date": _d(exp), "strike": _f(strike),
                    "call_exposure": _f(pick(legs, "CALL")), "put_exposure": _f(pick(legs, "PUT")),
                    "stock_price": None, "endpoint": "interval-map", **_common(ctx),
                })
    return rows


def normalize_news(payload: dict, ctx: dict) -> list[dict]:
    rows = []
    for r in (payload or {}).get("data") or []:
        tickers = pick(r, "tickers", "ticker") or []
        if isinstance(tickers, dict):
            tickers = list(tickers.keys())
        elif isinstance(tickers, list):
            tickers = [t if isinstance(t, str) else str(pick(t, "ticker", "symbol", default=t)) for t in tickers]
        else:
            tickers = [str(tickers)]
        topics = pick(r, "topics") or []
        rows.append({
            "id": str(pick(r, "id", default="")), "event_ts": _ts(pick(r, "publishedTime")),
            "title": pick(r, "title"), "tickers": tickers,
            "topics": [str(t) for t in topics] if isinstance(topics, list) else [str(topics)],
            "raw": json.dumps(r, default=str)[:20000], **_common(ctx),
        })
    return rows


def _flatten(obj: dict, prefix: str = "") -> dict:
    out = {}
    for k, v in obj.items():
        name = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(_flatten(v, name + "."))
        else:
            out[name] = v
    return out


def _common(ctx: dict) -> dict:
    return {"session_date": ctx["session_date"], "received_ts": ctx["received_ts"], "source": ctx["source"]}


# ---------- writer ----------
class SilverWriter:
    """Buffers rows per table and flushes them to parquet part files.

    data/silver/<table>/session_date=<d>/part-<ts>-<uuid>.parquet
    """

    def __init__(self, root: Path, flush_rows: int = 5000, flush_seconds: float = 60.0):
        self.root = root / "silver"
        self.flush_rows = flush_rows
        self.flush_seconds = flush_seconds
        self._buf: dict[tuple[str, date], list[dict]] = {}
        self._last_flush = time.monotonic()
        self._lock = threading.Lock()
        self.rows_written: dict[str, int] = {}

    def add(self, table: str, rows: list[dict]) -> None:
        if not rows:
            return
        with self._lock:
            for row in rows:
                self._buf.setdefault((table, row["session_date"]), []).append(row)
            due = (sum(len(v) for v in self._buf.values()) >= self.flush_rows
                   or time.monotonic() - self._last_flush >= self.flush_seconds)
        if due:
            self.flush()

    def flush(self) -> list[Path]:
        with self._lock:
            buf, self._buf = self._buf, {}
            self._last_flush = time.monotonic()
        written = []
        for (table, session), rows in buf.items():
            schema = SCHEMAS[table]
            cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
            tbl = pa.table(cols, schema=schema)
            out = self.root / table / f"session_date={session.isoformat()}"
            out.mkdir(parents=True, exist_ok=True)
            path = out / f"part-{int(time.time() * 1000)}-{os.getpid()}-{uuid.uuid4().hex[:6]}.parquet"
            pq.write_table(tbl, path, compression="zstd")
            written.append(path)
            self.rows_written[table] = self.rows_written.get(table, 0) + len(rows)
        return written
