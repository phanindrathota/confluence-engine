from datetime import date, datetime, timezone

from fake_quantdata import FakeQuantData

from confluence.cli import main
from confluence.collector.backfill import Backfiller
from confluence.collector.live import LiveCollector
from confluence.query import connect
from confluence.qd_client import EP
from confluence.runtime import Runtime
from confluence.storage.bronze import iter_bronze

UTC = timezone.utc
SESSION = date(2026, 10, 28)
MIDDAY = datetime(2026, 10, 28, 17, 0, tzinfo=UTC)   # 12:00 CT


def rt_with(settings, fake):
    return Runtime(settings, transport=fake.transport())


async def test_429_is_retried(settings):
    fake = FakeQuantData(rate_limit_first=1)
    rt = rt_with(settings, fake)
    resp = await rt.client.post(EP["net_drift"], {"sessionDate": "2026-10-28", "filter": {"ticker": "SPY"}}, SESSION)
    assert resp.status == 200 and rt.limiter.total_429 == 1
    await rt.aclose()


async def test_live_jobs_write_bronze_and_point_in_time_silver(settings):
    fake = FakeQuantData()
    rt = rt_with(settings, fake)
    lc = LiveCollector(rt, now=lambda: MIDDAY)
    for job in ["order_flow", "net_drift", "stock_bars", "exposure_fast", "dark_flow", "vol_drift", "news"]:
        await lc.run_once(job)
    # A second order-flow cycle must not duplicate prints.
    await lc.run_once("order_flow")
    await rt.aclose()

    con = connect(settings.data_dir)
    n_flow, n_ids = con.execute("SELECT count(*), count(DISTINCT id) FROM flow_print").fetchone()
    assert n_flow == n_ids == 7                       # paginated across 3 pages, deduped
    lag = con.execute("SELECT min(lag_ms) FROM flow_print").fetchone()[0]
    assert lag > 0                                    # received after the trade happened

    drift = con.execute("SELECT count(DISTINCT ticker), bool_and(received_ts >= event_ts) FROM net_drift").fetchone()
    assert drift == (len(settings.universe), True)
    # The bucket still open at receipt is flagged, the rest are closed.
    assert con.execute("SELECT count(*) FROM net_drift WHERE NOT bucket_closed").fetchone()[0] == 0

    expo = con.execute("SELECT count(*), count(DISTINCT ticker) FROM exposure_strike WHERE greek='GAMMA'").fetchone()
    assert expo == (2 * len(settings.universe), len(settings.universe))
    assert con.execute("SELECT count(*) FROM dark_flow WHERE ticker='SPX'").fetchone()[0] == 0
    assert con.execute("SELECT count(DISTINCT metric) FROM metric_series").fetchone()[0] == 3
    assert con.execute("SELECT count(*) FROM news_item").fetchone()[0] == 1
    assert sum(1 for _ in iter_bronze(settings.data_dir)) == len(fake.calls)


async def test_backfill_is_resumable_and_marks_unavailable(settings):
    fake = FakeQuantData(unavailable_before="2026-10-27")
    rt = rt_with(settings, fake)
    bf = Backfiller(rt, force_during_session=True)
    sessions = [date(2026, 10, 26), date(2026, 10, 27)]
    totals = await bf.run(sessions, ["SPY", "NVDA"])
    first_calls = len(fake.calls)
    assert totals["unavailable"] > 0 and totals["ok"] > 0 and totals["error"] == 0

    again = await bf.run(sessions, ["SPY", "NVDA"])
    assert len(fake.calls) == first_calls             # nothing re-fetched
    assert again["skipped"] == totals["ok"] + totals["unavailable"]
    await rt.aclose()

    con = connect(settings.data_dir)
    im = con.execute("SELECT count(DISTINCT greek) FROM exposure_strike WHERE endpoint='interval-map'").fetchone()[0]
    assert im == 4
    assert con.execute("SELECT count(*) FROM flow_print WHERE source='backfill'").fetchone()[0] == 14


async def test_rebuild_silver_reproduces_rows(settings, monkeypatch):
    fake = FakeQuantData()
    rt = rt_with(settings, fake)
    lc = LiveCollector(rt, now=lambda: MIDDAY)
    await lc.run_once("net_drift")
    await rt.aclose()
    before = connect(settings.data_dir).execute("SELECT count(*) FROM net_drift").fetchone()[0]

    monkeypatch.setenv("DATA_DIR", str(settings.data_dir))
    assert main(["rebuild-silver", "--yes"]) == 0
    after = connect(settings.data_dir).execute("SELECT count(*) FROM net_drift").fetchone()[0]
    assert after == before > 0


def test_calendar(settings):
    from confluence.clock import MarketCalendar
    cal = MarketCalendar(settings)
    assert not cal.is_trading_day(date(2026, 11, 26))           # Thanksgiving
    assert cal.is_trading_day(date(2026, 11, 27))
    _, end = cal.collect_window(date(2026, 11, 27))             # early close
    assert end.hour == 12 and end.minute == 20
    assert cal.in_collect_window(MIDDAY)
    assert not cal.in_collect_window(datetime(2026, 10, 31, 17, 0, tzinfo=UTC))  # Saturday
