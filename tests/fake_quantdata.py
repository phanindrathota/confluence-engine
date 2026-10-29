"""A fake QuantData server for tests, returning payloads shaped like the docs."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx

UTC = timezone.utc


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _buckets(body: dict, minutes: int = 1, n_default: int = 10):
    tr = body.get("timeRange")
    if tr:
        start = datetime.fromisoformat(tr["startTime"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(tr["endTime"].replace("Z", "+00:00"))
    else:
        d = datetime.fromisoformat(body.get("sessionDate", "2026-10-27")).replace(tzinfo=UTC)
        start = d.replace(hour=13, minute=30)
        end = start + timedelta(minutes=minutes * n_default)
    t = start.replace(second=0, microsecond=0)
    while t < end:
        yield t
        t += timedelta(minutes=minutes)


class FakeQuantData:
    def __init__(self, unavailable_before: str | None = None, rate_limit_first: int = 0):
        self.calls: list[tuple[str, dict]] = []
        self.unavailable_before = unavailable_before
        self.rate_limit_first = rate_limit_first
        self.flow_rows = 7  # total prints for pagination tests

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def _headers(self):
        return {"X-RateLimit-Limit": "240", "X-RateLimit-Remaining": "200", "X-RateLimit-Reset": "30"}

    def handle(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        path = request.url.path
        self.calls.append((path, body))
        if self.rate_limit_first > 0:
            self.rate_limit_first -= 1
            return httpx.Response(429, headers={**self._headers(), "Retry-After": "0",
                                                "X-RateLimit-Remaining": "0"},
                                  json={"status": 429, "retryAfterSeconds": 0})
        if self.unavailable_before and body.get("sessionDate", "9999") < self.unavailable_before:
            return httpx.Response(422, headers=self._headers(),
                                  json={"type": "data-unavailable", "status": 422, "title": "Data Unavailable"})
        fn = {
            "/v1/options/tool/net-drift": self.net_drift,
            "/v1/equities/tool/stock-price-over-time": self.bars,
            "/v1/equities/tool/dark-flow": self.dark_flow,
            "/v1/options/tool/volatility-drift": self.vol_drift,
            "/v1/options/tool/exposure-by-strike": self.exposure,
            "/v1/options/tool/interval-map": self.interval_map,
            "/v1/options/tool/order-flow/consolidated": self.order_flow,
            "/v1/news/tool/news-articles": self.news,
            "/v1/options/tool/gainers-losers": lambda b: {"data": {"SPY": {"premium": 1.0}}},
        }[path]
        return httpx.Response(200, headers=self._headers(), json=fn(body))

    def net_drift(self, b):
        return {"data": {str(_ms(t)): {"netCallPremium": 150000.0, "netPutPremium": -80000.0,
                                        "netCallVolume": 1250, "netPutVolume": -620,
                                        "midMarketCallPremium": 2.85, "midMarketPutPremium": 1.92,
                                        "stockPrice": 585.5} for t in _buckets(b)}}

    def bars(self, b):
        return {"data": {str(_ms(t)): {"openPrice": 1, "highPrice": 2, "lowPrice": 0.5, "closePrice": 1.5}
                         for t in _buckets(b)}}

    def dark_flow(self, b):
        return {"data": {str(_ms(t)): {"notionalValue": 1e6, "size": 1000, "tradeCount": 12, "stockPrice": 10}
                         for t in _buckets(b, 5, 3)}}

    def vol_drift(self, b):
        return {"data": {str(_ms(t)): {"arv": 0.18, "iv": 0.21, "stockPrice": 585.5} for t in _buckets(b, 1, 5)}}

    def exposure(self, b):
        t = b["filter"]["ticker"]
        return {"data": {t: {"exposureMap": {"2026-10-28": {"585.0": {"callExposure": 184201, "putExposure": -92410},
                                                             "590.0": {"callExposure": 221340}}},
                             "stockPrice": 585.5}}}

    def interval_map(self, b):
        return {"data": {str(_ms(t)): {"2026-10-28": {"585.0": {"CALL": 184201, "PUT": -92410}}}
                         for t in _buckets(b, 5, 3)}}

    def order_flow(self, b):
        start = 0
        if b.get("searchAfter"):
            start = int(b["searchAfter"][1])
        size = min(b.get("size", 50), 3)
        base = datetime(2026, 10, 28, 14, 0, tzinfo=UTC)
        rows = []
        for i in range(start, min(start + size, self.flow_rows)):
            rows.append({"id": f"p{i}", "ticker": "SPY", "contractType": "CALL", "expirationDate": "2026-10-28",
                         "dte": 0.0, "strikePrice": 590.0, "premium": 260000.0 + i, "size": 500,
                         "volume": 500, "optionPrice": 2.85, "askPrice": 2.86, "bidPrice": 2.80,
                         "stockPrice": 585.5, "impliedVolatility": 0.22, "tradeSideCode": "ABOVE_ASK",
                         "tradeConsolidationType": "SWEEP", "isGoldenSweep": i == 0, "isUnusual": True,
                         "greeks": {"delta": 0.31, "gamma": 0.05},
                         "moneyness": {"moneyType": "OTM", "degreeInPercent": 0.8},
                         "tradeTime": _ms(base + timedelta(seconds=i))})
        out = {"data": rows}
        end = start + len(rows)
        if end < self.flow_rows:
            out["nextSearchAfter"] = [str(_ms(base)), str(end)]
        return out

    def news(self, b):
        return {"data": [{"id": "n1", "publishedTime": _ms(datetime.now(UTC)), "title": "SPY rallies",
                          "tickers": ["SPY"], "topics": ["MARKETS"]}]}
