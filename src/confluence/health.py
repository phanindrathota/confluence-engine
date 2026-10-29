"""Daily data-quality report and the Phase 0 gate check.

Gate: 5 clean sessions, where clean means no gap over 2 minutes in the 1m
net drift and stock bars for any ticker during the regular session.
"""
from __future__ import annotations

import logging
from datetime import date

import httpx

from .clock import MarketCalendar, from_epoch_ms
from .query import connect
from .settings import Settings

log = logging.getLogger(__name__)
MAX_GAP_MIN = 2


def session_report(settings: Settings, session: date) -> dict:
    cal = MarketCalendar(settings)
    open_, close = cal.regular_session(session)
    con = connect(settings.data_dir)
    params = [session, open_, close]

    def gaps(table: str) -> list[tuple]:
        # Distinct closed 1m buckets per ticker, the largest gap between them,
        # and the gap from the open to the first bucket and from the last to the close.
        return con.execute(f"""
            WITH b AS (
              SELECT DISTINCT ticker, event_ts FROM {table}
              WHERE session_date = ? AND bucket = '1m' AND source = 'live' AND event_ts >= ? AND event_ts < ?),
            g AS (
              SELECT ticker, event_ts,
                     date_diff('minute', lag(event_ts) OVER (PARTITION BY ticker ORDER BY event_ts), event_ts) AS gap
              FROM b)
            SELECT ticker, count(*) AS buckets, coalesce(max(gap), 0) AS max_gap,
                   epoch_ms(min(event_ts)) AS first_ms, epoch_ms(max(event_ts)) AS last_ms
            FROM g GROUP BY ticker ORDER BY ticker""", params).fetchall()

    expected = int((close - open_).total_seconds() // 60)
    report = {"session": session.isoformat(), "expected_buckets": expected, "tables": {}, "flow": {}, "exposure": {}}
    clean = True
    for table in ("net_drift", "bar"):
        rows = gaps(table)
        seen = {r[0] for r in rows}
        # A ticker is expected if the dataset has it at all (SPX has no stock bars, for example).
        available = {t for (t,) in con.execute(
            f"SELECT DISTINCT ticker FROM {table} WHERE session_date = ?", [session]).fetchall()}
        missing = [t for t in settings.universe if t in available and t not in seen]
        if not rows:
            missing = missing or ["(no live data)"]
        bad = []
        for ticker, n, max_gap, first_ms, last_ms in rows:
            first_ts, last_ts = from_epoch_ms(first_ms), from_epoch_ms(last_ms)
            edge_gap = max(int((first_ts - open_).total_seconds() // 60),
                           int((close - last_ts).total_seconds() // 60) - 1)
            worst = max(max_gap - 1, edge_gap)   # consecutive 1m buckets differ by 1
            if worst > MAX_GAP_MIN or n < expected * 0.95:
                bad.append({"ticker": ticker, "buckets": n, "worst_gap_min": worst})
        report["tables"][table] = {"tickers": len(rows), "missing": missing, "problems": bad}
        clean = clean and not missing and not bad

    flow = con.execute("""
        SELECT count(*), count(DISTINCT ticker),
               quantile_cont(lag_ms, 0.5), quantile_cont(lag_ms, 0.95)
        FROM flow_print_first WHERE session_date = ? AND source = 'live'""", [session]).fetchone()
    report["flow"] = {"prints": flow[0], "tickers": flow[1],
                      "lag_ms_p50": flow[2], "lag_ms_p95": flow[3]}

    expo = con.execute("""
        SELECT greek, count(DISTINCT ticker), count(DISTINCT (ticker, event_ts))
        FROM exposure_strike WHERE session_date = ? AND endpoint = 'exposure-by-strike'
        GROUP BY greek ORDER BY greek""", [session]).fetchall()
    report["exposure"] = {g: {"tickers": t, "snapshots": n} for g, t, n in expo}
    report["clean"] = clean
    return report


def format_report(r: dict) -> str:
    lines = [f"**Confluence Phase 0 health, {r['session']}**: {'CLEAN' if r['clean'] else 'GAPS FOUND'}"]
    for table, t in r["tables"].items():
        status = "ok" if not t["missing"] and not t["problems"] else "check"
        lines.append(f"- {table}: {t['tickers']} tickers, {status}")
        if t["missing"]:
            lines.append(f"  missing: {', '.join(t['missing'])}")
        for p in t["problems"][:8]:
            lines.append(f"  {p['ticker']}: {p['buckets']} buckets, worst gap {p['worst_gap_min']} min")
    f = r["flow"]
    if f["prints"]:
        lines.append(f"- flow: {f['prints']} prints, {f['tickers']} tickers, "
                     f"publication lag p50 {f['lag_ms_p50'] / 1000:.1f}s, p95 {f['lag_ms_p95'] / 1000:.1f}s")
    else:
        lines.append("- flow: no live prints")
    for g, e in r["exposure"].items():
        lines.append(f"- {g}: {e['snapshots']} snapshots across {e['tickers']} tickers")
    return "\n".join(lines)


def post_discord(webhook: str | None, text: str) -> bool:
    if not webhook:
        return False
    try:
        r = httpx.post(webhook, json={"content": text[:1900]}, timeout=10)
        return r.status_code < 300
    except httpx.HTTPError:
        log.exception("discord post failed")
        return False


def gate_status(settings: Settings, sessions: list[date]) -> tuple[int, list[str]]:
    """Counts clean sessions among those given. Phase 0 passes at 5."""
    clean, notes = 0, []
    for s in sessions:
        r = session_report(settings, s)
        clean += r["clean"]
        notes.append(f"{s}: {'clean' if r['clean'] else 'gaps'}")
    return clean, notes
