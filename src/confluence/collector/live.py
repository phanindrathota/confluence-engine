"""Live collector: one async worker per dataset, each on its own cadence.

Workers only run inside the collect window (08:25-15:20 CT on trading days).
After the window closes, the same day is pulled again through the backfill
path, so every live day also has the exact history format the backtests use.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Awaitable, Callable

from .. import queries as Q
from ..clock import now_utc
from ..datasets import to_silver
from ..qd_client import EP, FatalAuthError, QuantDataError
from ..runtime import Runtime

log = logging.getLogger(__name__)


@dataclass
class WorkerStats:
    runs: int = 0
    errors: int = 0
    last_ok: str | None = None
    last_error: str | None = None
    rows: int = 0


@dataclass
class LiveState:
    session: date | None = None
    flow_watermark: datetime | None = None
    seen_flow_ids: set[str] = field(default_factory=set)
    seen_news_ids: set[str] = field(default_factory=set)
    stats: dict[str, WorkerStats] = field(default_factory=dict)

    def reset_for(self, session: date) -> None:
        if self.session != session:
            self.session = session
            self.flow_watermark = None
            self.seen_flow_ids.clear()
            self.seen_news_ids.clear()


class LiveCollector:
    def __init__(self, rt: Runtime, now: Callable[[], datetime] = now_utc):
        self.rt = rt
        self.s = rt.s
        self.now = now
        self.state = LiveState()
        self._stop = asyncio.Event()

    # ----- jobs -----
    async def job_order_flow(self) -> int:
        cfg = self.s.live["order_flow"]
        session = self.state.session
        end = self.now()
        start_of_day, _ = self.rt.cal.collect_window(session)
        if self.state.flow_watermark is None:
            start = start_of_day
        else:
            # Overlap by 2 minutes: prints can be published late. Dedupe by id.
            start = max(start_of_day, self.state.flow_watermark - timedelta(minutes=2))
        body = Q.order_flow(self.s.universe, cfg["min_premium"], Q.window(start=start, end=end))
        new = 0
        async for resp in self.rt.client.paginate(EP["order_flow"], body, session, cfg["max_pages_per_cycle"]):
            rows = self._new_rows(resp, body)
            new += len(rows)
            for r in rows:
                if r["event_ts"] and (self.state.flow_watermark is None or r["event_ts"] > self.state.flow_watermark):
                    self.state.flow_watermark = r["event_ts"]
        return new

    def _new_rows(self, resp, body) -> list[dict]:
        if resp.unavailable:
            return []
        out = to_silver(EP["order_flow"], body, resp.payload, datetime.fromisoformat(resp.received_ts),
                        self.state.session, "live")
        if out is None:
            return []
        table, rows = out
        fresh = [r for r in rows if r["id"] and r["id"] not in self.state.seen_flow_ids]
        self.state.seen_flow_ids.update(r["id"] for r in fresh)
        self.rt.silver.add(table, fresh)
        return fresh

    async def _per_ticker(self, build: Callable[[str], dict], endpoint: str,
                          tickers: list[str] | None = None) -> int:
        session = self.state.session
        tickers = tickers or self.s.universe

        async def one(t: str) -> int:
            try:
                _, rows = await self.rt.fetch(endpoint, build(t), session, "live")
                return len(rows)
            except FatalAuthError:
                raise
            except QuantDataError as exc:
                log.warning("%s %s failed: %s", endpoint, t, exc)
                return 0

        return sum(await asyncio.gather(*(one(t) for t in tickers)))

    async def job_net_drift(self) -> int:
        cfg = self.s.live["net_drift"]
        end = self.now()
        when = Q.window(start=end - timedelta(minutes=10), end=end)
        return await self._per_ticker(lambda t: Q.net_drift(t, when, cfg["aggregation"]), EP["net_drift"])

    async def job_stock_bars(self) -> int:
        cfg = self.s.live["stock_bars"]
        end = self.now()
        when = Q.window(start=end - timedelta(minutes=10), end=end)
        return await self._per_ticker(lambda t: Q.stock_price(t, when, cfg["aggregation"]), EP["stock_price"])

    async def _exposure(self, cfg: dict) -> int:
        total = 0
        for greek in cfg["greeks"]:
            total += await self._per_ticker(
                lambda t, g=greek: Q.exposure_by_strike(t, g, cfg["representation"]),
                EP["exposure_by_strike"])
        return total

    async def job_exposure_fast(self) -> int:
        return await self._exposure(self.s.live["exposure_fast"])

    async def job_exposure_slow(self) -> int:
        return await self._exposure(self.s.live["exposure_slow"])

    async def job_dark_flow(self) -> int:
        cfg = self.s.live["dark_flow"]
        end = self.now()
        when = Q.window(start=end - timedelta(minutes=30), end=end)
        # Dark flow is an equities dataset; SPX has no stock prints.
        tickers = [t for t in self.s.universe if t != "SPX"]
        return await self._per_ticker(lambda t: Q.dark_flow(t, when, cfg["aggregation"]), EP["dark_flow"], tickers)

    async def job_vol_drift(self) -> int:
        when = Q.window(session=self.state.session)
        return await self._per_ticker(lambda t: Q.volatility_drift(t, when), EP["volatility_drift"])

    async def job_news(self) -> int:
        end = self.now()
        body = Q.news(self.s.universe, end - timedelta(minutes=30), end)
        fresh_total = 0
        async for resp in self.rt.client.paginate(EP["news"], body, self.state.session, max_pages=3):
            out = to_silver(EP["news"], body, resp.payload, datetime.fromisoformat(resp.received_ts),
                            self.state.session, "live")
            if not out:
                continue
            table, rows = out
            fresh = [r for r in rows if r["id"] and r["id"] not in self.state.seen_news_ids]
            self.state.seen_news_ids.update(r["id"] for r in fresh)
            self.rt.silver.add(table, fresh)
            fresh_total += len(fresh)
        return fresh_total

    # ----- scheduling -----
    def jobs(self) -> dict[str, tuple[int, Callable[[], Awaitable[int]]]]:
        L = self.s.live
        return {
            "order_flow": (L["order_flow"]["cadence_s"], self.job_order_flow),
            "net_drift": (L["net_drift"]["cadence_s"], self.job_net_drift),
            "stock_bars": (L["stock_bars"]["cadence_s"], self.job_stock_bars),
            "exposure_fast": (L["exposure_fast"]["cadence_s"], self.job_exposure_fast),
            "exposure_slow": (L["exposure_slow"]["cadence_s"], self.job_exposure_slow),
            "dark_flow": (L["dark_flow"]["cadence_s"], self.job_dark_flow),
            "vol_drift": (L["vol_drift"]["cadence_s"], self.job_vol_drift),
            "news": (L["news"]["cadence_s"], self.job_news),
        }

    async def run_once(self, name: str) -> int:
        """Run a single job immediately (used by tests and `confluence collect --once`)."""
        self.state.reset_for(self.rt.cal.session_date(self.now()))
        cadence, fn = self.jobs()[name]
        return await self._run_job(name, fn)

    async def _run_job(self, name: str, fn) -> int:
        st = self.state.stats.setdefault(name, WorkerStats())
        st.runs += 1
        try:
            n = await fn()
            st.rows += n
            st.last_ok = self.now().isoformat()
            return n
        except FatalAuthError:
            self._stop.set()
            raise
        except Exception as exc:  # keep the worker alive; one bad cycle must not end collection
            st.errors += 1
            st.last_error = f"{self.now().isoformat()} {exc!r}"[:300]
            log.exception("job %s failed", name)
            return 0

    async def _worker(self, name: str, cadence: int, fn) -> None:
        # Minute-aligned jobs fire a few seconds after the boundary so the
        # previous bucket has closed.
        offset = 5 if cadence % 60 == 0 else 0
        while not self._stop.is_set():
            now = self.now()
            if self.rt.cal.in_collect_window(now):
                self.state.reset_for(self.rt.cal.session_date(now))
                await self._run_job(name, fn)
            epoch = time.time()
            delay = cadence - (epoch % cadence) + offset
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def _heartbeat(self) -> None:
        path = self.s.data_dir / "state" / "heartbeat.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        while not self._stop.is_set():
            self.rt.silver.flush()
            payload = {
                "at": self.now().isoformat(),
                "session": str(self.state.session),
                "in_window": self.rt.cal.in_collect_window(self.now()),
                "limiter": self.rt.limiter.stats(),
                "workers": {k: vars(v) for k, v in self.state.stats.items()},
                "silver_rows": self.rt.silver.rows_written,
            }
            path.write_text(json.dumps(payload, indent=2, default=str))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    async def _end_of_day(self, after_day: Callable[[date], Awaitable[None]] | None) -> None:
        """When the collect window closes, run the end-of-day pass once per session."""
        done: set[date] = set()
        while not self._stop.is_set():
            now = self.now()
            session = self.rt.cal.session_date(now)
            _, end = self.rt.cal.collect_window(session)
            if (self.rt.cal.is_trading_day(session) and now > end and session not in done
                    and self.state.session == session):
                done.add(session)
                self.rt.silver.flush()
                if after_day:
                    try:
                        await after_day(session)
                    except Exception:
                        log.exception("end-of-day pass failed for %s", session)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    def stop(self) -> None:
        self._stop.set()

    async def run(self, after_day: Callable[[date], Awaitable[None]] | None = None) -> None:
        tasks = [asyncio.create_task(self._worker(n, c, f), name=n) for n, (c, f) in self.jobs().items()]
        tasks.append(asyncio.create_task(self._heartbeat(), name="heartbeat"))
        tasks.append(asyncio.create_task(self._end_of_day(after_day), name="end_of_day"))
        try:
            await self._stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.rt.silver.flush()
