"""Request bodies for each dataset, shared by the live collector and backfill.

Every field used here is documented in https://quantdata.us/llms-full.txt.
Time windows are either a sessionDate (whole session) or a timeRange
(start inclusive, end exclusive, ISO instants).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from .clock import iso_z


def window(session: date | None = None, start: datetime | None = None, end: datetime | None = None) -> dict:
    if start is not None and end is not None:
        return {"timeRange": {"startTime": iso_z(start), "endTime": iso_z(end)}}
    if session is not None:
        return {"sessionDate": session.isoformat()}
    return {}


def order_flow(tickers: list[str], min_premium: float, when: dict, size: int = 100) -> dict:
    return {
        **when,
        "filterExpression": {
            "conjunction": "AND",
            "filters": [
                {"field": "TICKER", "operation": "=", "values": list(tickers)},
                {"field": "PREMIUM", "operation": ">=", "value": min_premium},
            ],
        },
        "size": size,
        "sort": {"field": "TRADE_TIME", "direction": "ASCENDING"},
    }


def net_drift(ticker: str, when: dict, aggregation: str = "1m") -> dict:
    # One ticker per call so the response carries stockPrice per bucket.
    return {**when, "aggregationPeriod": aggregation, "filter": {"ticker": ticker}}


def stock_price(ticker: str, when: dict, aggregation: str = "1m") -> dict:
    return {**when, "aggregationPeriod": aggregation, "filter": {"ticker": ticker}}


def dark_flow(ticker: str, when: dict, aggregation: str = "5m") -> dict:
    return {**when, "aggregationPeriod": aggregation, "filter": {"ticker": ticker}}


def volatility_drift(ticker: str, when: dict) -> dict:
    return {**when, "filter": {"ticker": ticker}}


def exposure_by_strike(ticker: str, greek: str, representation: str, snapshot: datetime | None = None,
                       session: date | None = None) -> dict:
    # The whole chain is collected; horizon selection (0DTE for indices,
    # next 7 days for stocks) happens downstream so nothing is lost.
    body: dict[str, Any] = {"greekMode": greek, "representationMode": representation,
                            "filter": {"ticker": ticker}}
    if snapshot is not None:
        body["snapshotTime"] = iso_z(snapshot)
    elif session is not None:
        body["sessionDate"] = session.isoformat()
    return body


def interval_map(ticker: str, greek: str, when: dict, aggregation: str = "5m") -> dict:
    return {**when, "greekMode": greek, "aggregationPeriod": aggregation, "filter": {"ticker": ticker}}


def news(tickers: list[str], start: datetime, end: datetime, size: int = 100) -> dict:
    # News accepts timeRange only, and is always sorted publishedTime DESCENDING.
    return {**window(start=start, end=end), "filter": {"tickers": list(tickers)}, "size": size}
