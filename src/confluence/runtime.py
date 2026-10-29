"""Wires settings, limiter, client, bronze and silver into one object."""
from __future__ import annotations

import logging
from datetime import date, datetime

import httpx

from .clock import MarketCalendar
from .datasets import to_silver
from .qd_client import QuantDataClient, Response
from .ratelimit import RateLimiter
from .settings import Settings
from .storage.bronze import BronzeWriter
from .storage.silver import SilverWriter

log = logging.getLogger(__name__)


class Runtime:
    def __init__(self, settings: Settings, transport: httpx.AsyncBaseTransport | None = None):
        self.s = settings
        self.cal = MarketCalendar(settings)
        self.limiter = RateLimiter(budget_per_min=settings.budget_per_min, reserve=settings.reserve_requests)
        self.bronze = BronzeWriter(settings.data_dir)
        self.silver = SilverWriter(settings.data_dir)
        self.client = QuantDataClient(settings.api_key, settings.base_url, self.limiter, self.bronze,
                                      transport=transport)

    async def fetch(self, endpoint: str, body: dict, session: date, source: str) -> tuple[Response, list[dict]]:
        """POST, then normalize the response into silver. Returns the response and its rows."""
        resp = await self.client.post(endpoint, body, session, tag=source)
        rows = self.ingest(endpoint, body, resp, session, source)
        return resp, rows

    def ingest(self, endpoint: str, body: dict, resp: Response, session: date, source: str) -> list[dict]:
        if resp.unavailable:
            return []
        out = to_silver(endpoint, body, resp.payload, datetime.fromisoformat(resp.received_ts), session, source)
        if out is None:
            return []
        table, rows = out
        self.silver.add(table, rows)
        return rows

    async def aclose(self) -> None:
        self.silver.flush()
        await self.client.aclose()
