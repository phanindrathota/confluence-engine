"""Endpoint -> silver table routing, shared by live, backfill and rebuild.

The context for a normalizer is derived only from the request body and the
receipt time, so any bronze record can be replayed into silver later.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Callable

from .qd_client import EP
from .storage import silver as S


@dataclass(frozen=True)
class Route:
    table: str
    normalize: Callable[[dict, dict], list[dict]]
    dataset: str


ROUTES: dict[str, Route] = {
    EP["order_flow"]: Route("flow_print", S.normalize_order_flow, "order_flow"),
    EP["net_drift"]: Route("net_drift", S.normalize_net_drift, "net_drift"),
    EP["stock_price"]: Route("bar", S.normalize_bars, "stock_price"),
    EP["dark_flow"]: Route("dark_flow", S.normalize_dark_flow, "dark_flow"),
    EP["exposure_by_strike"]: Route("exposure_strike", S.normalize_exposure_by_strike, "exposure_by_strike"),
    EP["interval_map"]: Route("exposure_strike", S.normalize_interval_map, "interval_map"),
    EP["volatility_drift"]: Route("metric_series", S.normalize_metric_series, "volatility_drift"),
    EP["news"]: Route("news_item", S.normalize_news, "news"),
}


def context_from_request(endpoint: str, body: dict, received_ts: datetime, session: date, source: str) -> dict:
    flt = body.get("filter") or {}
    ticker = flt.get("ticker") or flt.get("tickers")
    if isinstance(ticker, list):
        ticker = ticker[0] if len(ticker) == 1 else None
    snapshot = body.get("snapshotTime")
    return {
        "ticker": ticker,
        "greek": body.get("greekMode"),
        "representation": body.get("representationMode"),
        "aggregation": body.get("aggregationPeriod"),
        "snapshot_ts": S._ts(snapshot) if snapshot else None,
        "dataset": ROUTES[endpoint].dataset if endpoint in ROUTES else endpoint,
        "received_ts": received_ts,
        "session_date": session,
        "source": source,
    }


def to_silver(endpoint: str, body: dict, payload: dict | None, received_ts: datetime,
              session: date, source: str) -> tuple[str, list[dict]] | None:
    route = ROUTES.get(endpoint)
    if route is None or not payload:
        return None
    ctx = context_from_request(endpoint, body, received_ts, session, source)
    return route.table, route.normalize(payload, ctx)
