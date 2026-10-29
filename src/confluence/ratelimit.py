"""Client-side limiter for QuantData's shared per-user quota.

QuantData enforces 240 requests per 60 s (sliding) plus 20 per 1 s, per user,
across every key on the account. This limiter enforces three things before
each request:

1. Our own budget (QD_BUDGET_PER_MIN) over a 60 s sliding window.
2. A burst cap below the server's 20/s.
3. The server's live view of the shared bucket (X-RateLimit-Remaining /
   X-RateLimit-Reset), so we yield when another agent on the account is busy.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque

log = logging.getLogger(__name__)


class RateLimiter:
    def __init__(
        self,
        budget_per_min: int = 150,
        burst_per_sec: int = 15,
        reserve: int = 25,
        clock=time.monotonic,
        sleep=asyncio.sleep,
    ):
        self.budget = budget_per_min
        self.burst = burst_per_sec
        self.reserve = reserve
        self._clock = clock
        self._sleep = sleep
        self._minute: deque[float] = deque()
        self._second: deque[float] = deque()
        self._lock = asyncio.Lock()
        # Server-reported state of the shared bucket.
        self._server_remaining: int | None = None
        self._server_reset_at: float | None = None
        self._blocked_until: float = 0.0
        self.total_requests = 0
        self.total_429 = 0

    def _prune(self, now: float) -> None:
        while self._minute and now - self._minute[0] >= 60:
            self._minute.popleft()
        while self._second and now - self._second[0] >= 1:
            self._second.popleft()

    def _wait_needed(self, now: float) -> float:
        self._prune(now)
        waits = [0.0]
        if now < self._blocked_until:
            waits.append(self._blocked_until - now)
        if len(self._minute) >= self.budget:
            waits.append(60 - (now - self._minute[0]) + 0.01)
        if len(self._second) >= self.burst:
            waits.append(1 - (now - self._second[0]) + 0.01)
        if (
            self._server_remaining is not None
            and self._server_reset_at is not None
            and self._server_remaining <= self.reserve
            and now < self._server_reset_at
        ):
            # Spread what is left of the shared bucket across the reset window.
            spare = max(self._server_remaining - self.reserve, 0)
            gap = (self._server_reset_at - now) / max(spare, 1)
            waits.append(min(gap, self._server_reset_at - now))
        return max(waits)

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = self._clock()
                wait = self._wait_needed(now)
                if wait <= 0:
                    break
                await self._sleep(wait)
            now = self._clock()
            self._minute.append(now)
            self._second.append(now)
            self.total_requests += 1
            if self._server_remaining is not None:
                # Assume our request consumed one; the response will correct it.
                self._server_remaining -= 1

    def update_from_headers(self, headers) -> None:
        try:
            remaining = headers.get("X-RateLimit-Remaining")
            reset = headers.get("X-RateLimit-Reset")
            if remaining is not None:
                self._server_remaining = int(remaining)
            if reset is not None:
                self._server_reset_at = self._clock() + float(reset)
        except (TypeError, ValueError):
            log.debug("unparseable rate-limit headers: %s", dict(headers))

    def on_429(self, retry_after: float) -> float:
        """Block all callers until Retry-After passes. Returns the wait applied."""
        self.total_429 += 1
        wait = max(float(retry_after), 1.0) + random.uniform(0.05, 0.5)
        self._blocked_until = max(self._blocked_until, self._clock() + wait)
        return wait

    def stats(self) -> dict:
        now = self._clock()
        self._prune(now)
        return {
            "sent_last_60s": len(self._minute),
            "budget_per_min": self.budget,
            "server_remaining": self._server_remaining,
            "total_requests": self.total_requests,
            "total_429": self.total_429,
        }
