"""First-run probe: confirms the key works, measures history depth on your plan,
and saves one sample response per Phase 0 endpoint so the normalizers can be
checked against real payloads before collection starts.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta

from . import queries as Q
from .datasets import to_silver
from .qd_client import EP, QuantDataError
from .runtime import Runtime

log = logging.getLogger(__name__)

LOOKBACK_DAYS = [1, 30, 90, 180, 270, 365, 450]


async def run_probe(rt: Runtime, ticker: str = "SPY") -> dict:
    report: dict = {"ticker": ticker, "auth": None, "lookback": {}, "samples": {}}
    cal = rt.cal
    today = cal.session_date()
    last = today if cal.is_trading_day(today) and not cal.in_collect_window() else cal.previous_trading_day(today)

    # 1. Auth and headers.
    resp = await rt.client.post(EP["gainers_losers"], {}, last, tag="probe")
    report["auth"] = {"status": resp.status, "limiter": rt.limiter.stats()}

    # 2. How far back does this plan go? One small call per offset.
    for days in LOOKBACK_DAYS:
        d = _nearest_trading_day(rt, last - timedelta(days=days))
        try:
            r = await rt.client.post(EP["net_drift"], Q.net_drift(ticker, Q.window(session=d), "1h"), d, tag="probe")
            buckets = len(r.data or {}) if isinstance(r.data, dict) else 0
            report["lookback"][d.isoformat()] = "unavailable" if r.unavailable else f"ok ({buckets} buckets)"
        except QuantDataError as exc:
            report["lookback"][d.isoformat()] = f"error: {exc}"

    # 3. One sample per dataset for the last full session, with normalized row counts.
    when = Q.window(session=last)
    samples = {
        "order_flow": (EP["order_flow"], {**Q.order_flow([ticker], 100000, when), "size": 5}),
        "net_drift": (EP["net_drift"], Q.net_drift(ticker, when, "5m")),
        "stock_price": (EP["stock_price"], Q.stock_price(ticker, when, "5m")),
        "dark_flow": (EP["dark_flow"], Q.dark_flow(ticker, when, "15m")),
        "volatility_drift": (EP["volatility_drift"], Q.volatility_drift(ticker, when)),
        "exposure_by_strike": (EP["exposure_by_strike"],
                               Q.exposure_by_strike(ticker, "GAMMA", "PER_ONE_PERCENT_MOVE", session=last)),
        "interval_map": (EP["interval_map"], Q.interval_map(ticker, "GAMMA", when, "30m")),
    }
    out_dir = rt.s.data_dir / "probe"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, (endpoint, body) in samples.items():
        try:
            r = await rt.client.post(endpoint, body, last, tag="probe")
            (out_dir / f"{name}.json").write_text(json.dumps({"request": body, "response": r.payload}, indent=2)[:2_000_000])
            converted = to_silver(endpoint, body, r.payload, _dt(r.received_ts), last, "probe")
            rows = converted[1] if converted else []
            empty_cols = sorted(k for k in (rows[0] if rows else {}) if all(row.get(k) is None for row in rows))
            report["samples"][name] = {"status": r.status, "rows": len(rows), "always_null": empty_cols}
        except QuantDataError as exc:
            report["samples"][name] = {"error": str(exc)}

    report["limiter"] = rt.limiter.stats()
    (out_dir / "report.json").write_text(json.dumps(report, indent=2))
    return report


def _nearest_trading_day(rt: Runtime, d: date) -> date:
    while not rt.cal.is_trading_day(d):
        d -= timedelta(days=1)
    return d


def _dt(value: str):
    from datetime import datetime
    return datetime.fromisoformat(value)
