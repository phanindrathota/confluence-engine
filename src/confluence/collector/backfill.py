"""Backfill: pull whole past sessions, resumably, through the same normalizers.

Per session and ticker it fetches net drift and stock bars (1m), dark flow
(5m), volatility drift, interval-map exposure for each greek, and order flow
above the premium floor. News is pulled once per session for the universe.

Progress is checkpointed in data/state/backfill.sqlite, so a stopped run
resumes where it left off. Newest sessions go first: recent data matters most.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from .. import queries as Q
from ..clock import now_utc
from ..qd_client import EP, FatalAuthError, QuantDataError
from ..runtime import Runtime

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Job:
    session: date
    dataset: str
    ticker: str          # "*" for universe-wide jobs (news)


class Checkpoint:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS done (
                session TEXT, dataset TEXT, ticker TEXT, status TEXT,
                rows INTEGER, requests INTEGER, finished_at TEXT, detail TEXT,
                PRIMARY KEY (session, dataset, ticker))""")
        self.db.commit()

    def is_done(self, job: Job) -> bool:
        row = self.db.execute(
            "SELECT status FROM done WHERE session=? AND dataset=? AND ticker=?",
            (job.session.isoformat(), job.dataset, job.ticker)).fetchone()
        return row is not None and row[0] in ("ok", "unavailable")

    def mark(self, job: Job, status: str, rows: int, requests: int, detail: str = "") -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO done VALUES (?,?,?,?,?,?,?,?)",
            (job.session.isoformat(), job.dataset, job.ticker, status, rows, requests,
             now_utc().isoformat(), detail[:500]))
        self.db.commit()

    def summary(self) -> list[tuple]:
        return self.db.execute(
            "SELECT status, COUNT(*), SUM(rows), SUM(requests) FROM done GROUP BY status").fetchall()


class Backfiller:
    def __init__(self, rt: Runtime, redo: bool = False, force_during_session: bool = False):
        self.rt = rt
        self.s = rt.s
        self.cfg = rt.s.backfill
        self.redo = redo
        self.force = force_during_session
        self.ckpt = Checkpoint(rt.s.data_dir / "state" / "backfill.sqlite")

    def jobs_for(self, session: date, tickers: list[str], datasets: list[str] | None = None) -> list[Job]:
        per_ticker = ["net_drift", "stock_price", "dark_flow", "volatility_drift",
                      *[f"interval_map:{g}" for g in self.cfg["interval_map_greeks"]], "order_flow"]
        jobs = [Job(session, d, t) for t in tickers for d in per_ticker
                if not (d == "dark_flow" and t == "SPX")]
        jobs.append(Job(session, "news", "*"))
        if datasets:
            jobs = [j for j in jobs if j.dataset.split(":")[0] in datasets]
        return jobs

    async def _wait_if_session_open(self) -> None:
        if self.force or not self.cfg.get("pause_during_session", True):
            return
        while self.rt.cal.in_collect_window():
            log.info("collect window is open; backfill paused (use --force to override)")
            await asyncio.sleep(300)

    async def run_job(self, job: Job) -> tuple[str, int, int]:
        """Returns (status, rows, requests)."""
        s, t = job.session, job.ticker
        when = Q.window(session=s)
        name, _, greek = job.dataset.partition(":")
        if name == "order_flow":
            body = Q.order_flow([t], self.cfg["min_premium"], when)
            rows = reqs = 0
            unavailable = False
            async for resp in self.rt.client.paginate(EP["order_flow"], body, s,
                                                      self.cfg["max_flow_pages_per_ticker_day"], tag="backfill"):
                reqs += 1
                unavailable = unavailable or resp.unavailable
                rows += len(self.rt.ingest(EP["order_flow"], body, resp, s, "backfill"))
            return ("unavailable" if unavailable and rows == 0 else "ok"), rows, reqs
        if name == "news":
            start, _ = self.rt.cal.collect_window(s)
            start = start.replace(hour=0, minute=0)
            body = Q.news(self.s.universe, start, start + timedelta(days=1))
            rows = reqs = 0
            async for resp in self.rt.client.paginate(EP["news"], body, s, 20, tag="backfill"):
                reqs += 1
                rows += len(self.rt.ingest(EP["news"], body, resp, s, "backfill"))
            return "ok", rows, reqs

        builders = {
            "net_drift": (EP["net_drift"], lambda: Q.net_drift(t, when, "1m")),
            "stock_price": (EP["stock_price"], lambda: Q.stock_price(t, when, "1m")),
            "dark_flow": (EP["dark_flow"], lambda: Q.dark_flow(t, when, "5m")),
            "volatility_drift": (EP["volatility_drift"], lambda: Q.volatility_drift(t, when)),
            "interval_map": (EP["interval_map"],
                             lambda: Q.interval_map(t, greek, when, self.cfg["interval_map_aggregation"])),
        }
        endpoint, build = builders[name]
        resp, rows = await self.rt.fetch(endpoint, build(), s, "backfill")
        return ("unavailable" if resp.unavailable else "ok"), len(rows), 1

    async def run(self, sessions: list[date], tickers: list[str], datasets: list[str] | None = None,
                  concurrency: int = 4) -> dict:
        totals = {"ok": 0, "unavailable": 0, "error": 0, "skipped": 0, "rows": 0}
        sem = asyncio.Semaphore(concurrency)
        for session in sorted(sessions, reverse=True):
            await self._wait_if_session_open()
            jobs = [j for j in self.jobs_for(session, tickers, datasets) if self.redo or not self.ckpt.is_done(j)]
            totals["skipped"] += len(self.jobs_for(session, tickers, datasets)) - len(jobs)

            async def one(job: Job):
                async with sem:
                    try:
                        status, rows, reqs = await self.run_job(job)
                        self.ckpt.mark(job, status, rows, reqs)
                        totals[status] += 1
                        totals["rows"] += rows
                    except FatalAuthError:
                        raise
                    except QuantDataError as exc:
                        self.ckpt.mark(job, "error", 0, 0, str(exc))
                        totals["error"] += 1
                        log.warning("backfill %s failed: %s", job, exc)

            await asyncio.gather(*(one(j) for j in jobs))
            self.rt.silver.flush()
            log.info("session %s done: %s | limiter %s", session, totals, self.rt.limiter.stats())
        return totals


def default_range(rt: Runtime, days: int) -> list[date]:
    today = rt.cal.session_date()
    start = today - timedelta(days=days)
    end = rt.cal.previous_trading_day(today) if rt.cal.in_collect_window() else today
    return rt.cal.trading_days(start, end)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()
