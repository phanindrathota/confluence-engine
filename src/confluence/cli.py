"""Command line: confluence <command> [options]

  probe            Check the key, measure history depth, save sample payloads.
  collect          Run the live collector (add --forever to run day after day).
  backfill         Pull past sessions into bronze and silver, resumably.
  health           Data-quality report for a session; --post sends it to Discord.
  gate             Count clean live sessions (Phase 0 passes at 5).
  rebuild-silver   Replay bronze through the normalizers into a fresh silver.
  sql              Run a DuckDB query over the silver views.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import shutil
import signal
import sys
from datetime import date, datetime

from . import health as H
from .clock import MarketCalendar
from .collector.backfill import Backfiller, default_range, parse_date
from .collector.live import LiveCollector
from .datasets import to_silver
from .probe import run_probe
from .runtime import Runtime
from .settings import load_settings
from .storage.bronze import iter_bronze
from .storage.silver import SilverWriter


def _setup_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def _probe(args) -> int:
    rt = Runtime(load_settings())
    try:
        report = await run_probe(rt, args.ticker)
    finally:
        await rt.aclose()
    print(json.dumps(report, indent=2))
    print(f"\nSamples saved to {rt.s.data_dir / 'probe'}")
    return 0


async def _collect(args) -> int:
    rt = Runtime(load_settings())
    collector = LiveCollector(rt)
    if args.once:
        try:
            for name in (args.once.split(",") if args.once != "all" else collector.jobs()):
                n = await collector.run_once(name)
                print(f"{name}: {n} rows")
        finally:
            await rt.aclose()
        return 0

    async def after_day(session: date) -> None:
        # Pull today's history in the backtest format, then report health.
        await Backfiller(rt, force_during_session=True).run([session], rt.s.universe)
        report = H.session_report(rt.s, session)
        text = H.format_report(report)
        logging.info("\n%s", text)
        H.post_discord(rt.s.discord_health_webhook, text)
        if not args.forever:
            collector.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, collector.stop)
        except NotImplementedError:  # Windows
            pass
    logging.info("collector started; window %s-%s %s", rt.s.collect_start, rt.s.collect_end, rt.s.timezone)
    try:
        await collector.run(after_day=after_day)
    finally:
        await rt.aclose()
    return 0


async def _backfill(args) -> int:
    rt = Runtime(load_settings())
    cal = rt.cal
    if args.start:
        end = parse_date(args.end) if args.end else cal.session_date()
        sessions = cal.trading_days(parse_date(args.start), end)
    else:
        sessions = default_range(rt, args.days)
    tickers = args.tickers.split(",") if args.tickers else rt.s.universe
    datasets = args.datasets.split(",") if args.datasets else None
    bf = Backfiller(rt, redo=args.redo, force_during_session=args.force)
    logging.info("backfilling %d sessions (%s to %s), %d tickers",
                 len(sessions), sessions[0] if sessions else "-", sessions[-1] if sessions else "-", len(tickers))
    try:
        totals = await bf.run(sessions, tickers, datasets, concurrency=args.concurrency)
    finally:
        await rt.aclose()
    print(json.dumps(totals, indent=2))
    print("checkpoint:", bf.ckpt.summary())
    return 0


def _health(args) -> int:
    s = load_settings(require_key=False)
    cal = MarketCalendar(s)
    session = parse_date(args.date) if args.date else cal.session_date()
    report = H.session_report(s, session)
    text = H.format_report(report)
    print(text)
    if args.post:
        print("posted to Discord" if H.post_discord(s.discord_health_webhook, text) else "Discord not configured")
    return 0 if report["clean"] else 1


def _gate(args) -> int:
    s = load_settings(require_key=False)
    cal = MarketCalendar(s)
    end = cal.session_date()
    sessions = cal.trading_days(cal.previous_trading_day(end, args.sessions), end)
    clean, notes = H.gate_status(s, sessions)
    print("\n".join(notes))
    print(f"\n{clean} clean sessions of {len(sessions)}; Phase 0 gate {'PASSED' if clean >= 5 else 'not yet'}")
    return 0 if clean >= 5 else 1


def _rebuild(args) -> int:
    s = load_settings(require_key=False)
    silver_dir = s.data_dir / "silver"
    if silver_dir.exists():
        if not args.yes:
            print(f"This deletes {silver_dir} and rebuilds it from bronze. Re-run with --yes.")
            return 1
        shutil.rmtree(silver_dir)
    writer = SilverWriter(s.data_dir, flush_rows=50_000)
    n = 0
    for rec in iter_bronze(s.data_dir):
        if rec.get("status") != 200 or not isinstance(rec.get("payload"), dict) or rec.get("tag") == "probe":
            continue
        received = datetime.fromisoformat(rec["received_ts"])
        session = _session_of(rec, s)
        tag = rec.get("tag", "live")
        out = to_silver(rec["endpoint"], rec["request"], rec["payload"], received, session,
                        "backfill" if tag == "backfill" else tag)
        if out:
            writer.add(*out)
            n += len(out[1])
    writer.flush()
    print(f"rebuilt {n} silver rows: {writer.rows_written}")
    return 0


def _session_of(rec: dict, s) -> date:
    req = rec.get("request") or {}
    if req.get("sessionDate"):
        return date.fromisoformat(req["sessionDate"])
    return MarketCalendar(s).session_date(datetime.fromisoformat(rec["received_ts"]))


def _sql(args) -> int:
    from .query import connect
    s = load_settings(require_key=False)
    con = connect(s.data_dir)
    print(con.execute(args.query).fetchdf().to_string() if _has_pandas() else con.sql(args.query))
    return 0


def _has_pandas() -> bool:
    try:
        import pandas  # noqa: F401
        return True
    except ImportError:
        return False


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="confluence", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("probe", help="check key, history depth and payload shapes")
    sp.add_argument("--ticker", default="SPY")

    sc = sub.add_parser("collect", help="run the live collector")
    sc.add_argument("--forever", action="store_true", help="keep running across sessions")
    sc.add_argument("--once", help="run job(s) once now: all, or a comma list (order_flow,net_drift,...)")

    sb = sub.add_parser("backfill", help="pull past sessions")
    sb.add_argument("--days", type=int, default=30, help="calendar days back from today (default 30)")
    sb.add_argument("--start", help="YYYY-MM-DD (overrides --days)")
    sb.add_argument("--end", help="YYYY-MM-DD (default today)")
    sb.add_argument("--tickers", help="comma list (default: whole universe)")
    sb.add_argument("--datasets", help="comma list: net_drift,stock_price,dark_flow,volatility_drift,"
                                       "interval_map,order_flow,news")
    sb.add_argument("--redo", action="store_true", help="re-fetch jobs already done")
    sb.add_argument("--force", action="store_true", help="run during market hours")
    sb.add_argument("--concurrency", type=int, default=4)

    sh = sub.add_parser("health", help="data-quality report for a session")
    sh.add_argument("--date")
    sh.add_argument("--post", action="store_true", help="also post to Discord")

    sg = sub.add_parser("gate", help="Phase 0 gate: clean live sessions")
    sg.add_argument("--sessions", type=int, default=10, help="look back this many sessions")

    sr = sub.add_parser("rebuild-silver", help="rebuild silver from bronze")
    sr.add_argument("--yes", action="store_true")

    sq = sub.add_parser("sql", help="query silver with DuckDB")
    sq.add_argument("query")

    args = p.parse_args(argv)
    try:
        level = load_settings(require_key=False).log_level
    except Exception:
        level = "INFO"
    _setup_logging(level)

    handlers = {
        "probe": lambda: asyncio.run(_probe(args)),
        "collect": lambda: asyncio.run(_collect(args)),
        "backfill": lambda: asyncio.run(_backfill(args)),
        "health": lambda: _health(args),
        "gate": lambda: _gate(args),
        "rebuild-silver": lambda: _rebuild(args),
        "sql": lambda: _sql(args),
    }
    return handlers[args.cmd]()


if __name__ == "__main__":
    sys.exit(main())
