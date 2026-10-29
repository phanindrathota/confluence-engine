"""Market calendar and timestamp helpers. All market times are US/Central."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .settings import Settings

UTC = timezone.utc


def now_utc() -> datetime:
    return datetime.now(UTC)


def to_epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def from_epoch_ms(ms: int | float) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def iso_z(dt: datetime) -> str:
    """ISO 8601 instant in the form QuantData expects: 2026-05-13T14:30:00Z."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class MarketCalendar:
    def __init__(self, settings: Settings):
        self.s = settings
        self.tz = ZoneInfo(settings.timezone)

    def is_trading_day(self, d: date) -> bool:
        return d.weekday() < 5 and d not in self.s.holidays

    def local(self, dt: datetime) -> datetime:
        return dt.astimezone(self.tz)

    def session_date(self, dt: datetime | None = None) -> date:
        return self.local(dt or now_utc()).date()

    def _at(self, d: date, t: time) -> datetime:
        return datetime.combine(d, t, self.tz)

    def regular_close(self, d: date) -> datetime:
        close = time(12, 0) if d in self.s.early_closes else self.s.regular_close
        return self._at(d, close)

    def collect_window(self, d: date) -> tuple[datetime, datetime]:
        start = self._at(d, self.s.collect_start)
        end = self._at(d, self.s.collect_end)
        if d in self.s.early_closes:
            end = self.regular_close(d) + timedelta(minutes=20)
        return start, end

    def in_collect_window(self, dt: datetime | None = None) -> bool:
        dt = dt or now_utc()
        d = self.session_date(dt)
        if not self.is_trading_day(d):
            return False
        start, end = self.collect_window(d)
        return start <= dt <= end

    def regular_session(self, d: date) -> tuple[datetime, datetime]:
        return self._at(d, self.s.regular_open), self.regular_close(d)

    def trading_days(self, start: date, end: date) -> list[date]:
        """Inclusive range of trading days, oldest first."""
        out, d = [], start
        while d <= end:
            if self.is_trading_day(d):
                out.append(d)
            d += timedelta(days=1)
        return out

    def previous_trading_day(self, d: date, n: int = 1) -> date:
        while n > 0:
            d -= timedelta(days=1)
            if self.is_trading_day(d):
                n -= 1
        return d
