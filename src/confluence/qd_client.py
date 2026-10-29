"""QuantData REST client. Every request is rate-limited and captured to bronze.

Only endpoints and fields documented at https://quantdata.us/llms-full.txt are used.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from .clock import now_utc
from .ratelimit import RateLimiter
from .storage.bronze import BronzeWriter

log = logging.getLogger(__name__)

# Paths from the QuantData reference.
EP = {
    "order_flow": "/v1/options/tool/order-flow/consolidated",
    "net_drift": "/v1/options/tool/net-drift",
    "exposure_by_strike": "/v1/options/tool/exposure-by-strike",
    "interval_map": "/v1/options/tool/interval-map",
    "volatility_drift": "/v1/options/tool/volatility-drift",
    "iv_rank": "/v1/options/tool/iv-rank",
    "gainers_losers": "/v1/options/tool/gainers-losers",
    "stock_price": "/v1/equities/tool/stock-price-over-time",
    "dark_flow": "/v1/equities/tool/dark-flow",
    "news": "/v1/news/tool/news-articles",
}


class QuantDataError(Exception):
    def __init__(self, status: int, problem: dict | None, endpoint: str):
        self.status = status
        self.problem = problem or {}
        self.endpoint = endpoint
        detail = self.problem.get("detail") or self.problem.get("title") or ""
        super().__init__(f"{endpoint} -> HTTP {status} {detail}".strip())


class FatalAuthError(QuantDataError):
    """401/403: retrying will not help. Stop and fix the key or subscription."""


@dataclass
class Response:
    endpoint: str
    status: int
    payload: dict | None
    received_ts: str          # ISO UTC instant the response was received
    request_id: str
    latency_ms: float

    @property
    def data(self) -> Any:
        return (self.payload or {}).get("data")

    @property
    def unavailable(self) -> bool:
        return self.status == 422


class QuantDataClient:
    def __init__(
        self,
        api_key: str,
        base_url: str,
        limiter: RateLimiter,
        bronze: BronzeWriter | None,
        transport: httpx.AsyncBaseTransport | None = None,
        max_retries: int = 3,
        timeout_s: float = 30.0,
    ):
        self.limiter = limiter
        self.bronze = bronze
        self.max_retries = max_retries
        self._http = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=timeout_s,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()

    async def post(self, endpoint: str, body: dict, session: date, tag: str = "live") -> Response:
        """POST with rate limiting, retries and bronze capture.

        Returns a Response for 2xx and for 422 (data unavailable).
        Raises FatalAuthError on 401/403 and QuantDataError on other failures.
        """
        attempt, consecutive_429 = 0, 0
        while True:
            await self.limiter.acquire()
            request_id = uuid.uuid4().hex[:12]
            sent = time.perf_counter()
            try:
                r = await self._http.post(endpoint, json=body)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                attempt += 1
                if attempt > self.max_retries:
                    raise QuantDataError(0, {"detail": repr(exc)}, endpoint) from exc
                await asyncio.sleep(min(2 ** attempt, 20))
                continue

            latency = (time.perf_counter() - sent) * 1000
            received = now_utc()
            if r.status_code != 429:
                # A 429 always reports Remaining=0, even for a 1 s burst rejection,
                # so it says nothing about the minute bucket. Retry-After governs it.
                self.limiter.update_from_headers(r.headers)
            payload = _json_or_none(r)
            self._capture(endpoint, body, r, payload, received, session, request_id, latency, tag)

            if r.status_code == 429:
                consecutive_429 += 1
                retry_after = float(r.headers.get("Retry-After") or (payload or {}).get("retryAfterSeconds") or 1)
                wait = self.limiter.on_429(retry_after)
                log.warning("429 on %s, waiting %.1fs (%d in a row)", endpoint, wait, consecutive_429)
                if consecutive_429 >= 3:
                    raise QuantDataError(429, payload, endpoint)
                continue
            if r.status_code in (401, 403):
                raise FatalAuthError(r.status_code, payload, endpoint)
            if r.status_code >= 500:
                attempt += 1
                if attempt > self.max_retries:
                    raise QuantDataError(r.status_code, payload, endpoint)
                await asyncio.sleep(min(2 ** attempt, 20))
                continue
            if r.status_code >= 400 and r.status_code != 422:
                raise QuantDataError(r.status_code, payload, endpoint)

            return Response(endpoint, r.status_code, payload, received.isoformat(), request_id, latency)

    def _capture(self, endpoint, body, r, payload, received, session, request_id, latency, tag):
        if self.bronze is None:
            return
        self.bronze.write(endpoint, session, {
            "request_id": request_id,
            "endpoint": endpoint,
            "tag": tag,
            "request": body,
            "status": r.status_code,
            "received_ts": received.isoformat(),
            "latency_ms": round(latency, 1),
            "ratelimit": {
                "limit": r.headers.get("X-RateLimit-Limit"),
                "remaining": r.headers.get("X-RateLimit-Remaining"),
                "reset": r.headers.get("X-RateLimit-Reset"),
            },
            "payload": payload if payload is not None else r.text[:2000],
        })

    async def paginate(self, endpoint: str, body: dict, session: date, max_pages: int, tag: str = "live"):
        """Walk a table-shaped endpoint via nextSearchAfter. Yields one Response per page."""
        page_body = dict(body)
        for _ in range(max_pages):
            resp = await self.post(endpoint, page_body, session, tag)
            yield resp
            cursor = (resp.payload or {}).get("nextSearchAfter")
            if resp.unavailable or not cursor or not resp.data:
                return
            page_body = {**body, "searchAfter": cursor}


def _json_or_none(r: httpx.Response) -> dict | None:
    try:
        value = r.json()
        return value if isinstance(value, dict) else {"data": value}
    except ValueError:
        return None
