"""Bronze layer: every raw API response, append-only, as gzip JSON lines.

Layout:
  data/bronze/source=quantdata/endpoint=<name>/dt=<session date>/part-<pid>-<hour>.jsonl.gz

Each line is one HTTP exchange: request body, status, timing, rate-limit
headers and the untouched payload. DuckDB reads these files directly, and
`confluence rebuild-silver` replays them through the normalizers.
"""
from __future__ import annotations

import gzip
import json
import os
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator


class BronzeWriter:
    def __init__(self, root: Path):
        self.root = root / "bronze" / "source=quantdata"
        self._lock = threading.Lock()

    def path_for(self, endpoint: str, session: date, received: datetime) -> Path:
        name = endpoint.strip("/").replace("/", "_")
        part = f"part-{os.getpid()}-{received.strftime('%H')}.jsonl.gz"
        return self.root / f"endpoint={name}" / f"dt={session.isoformat()}" / part

    def write(self, endpoint: str, session: date, record: dict[str, Any]) -> Path:
        received = datetime.fromisoformat(record["received_ts"])
        path = self.path_for(endpoint, session, received)
        line = (json.dumps(record, separators=(",", ":"), default=str) + "\n").encode()
        with self._lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Appending gzip members is valid gzip; readers see one stream.
            with gzip.open(path, "ab") as fh:
                fh.write(line)
        return path


def iter_bronze(root: Path, endpoint: str | None = None, session: date | None = None) -> Iterator[dict]:
    base = root / "bronze" / "source=quantdata"
    ep = f"endpoint={endpoint.strip('/').replace('/', '_')}" if endpoint else "endpoint=*"
    dt = f"dt={session.isoformat()}" if session else "dt=*"
    for path in sorted(base.glob(f"{ep}/{dt}/*.jsonl.gz")):
        with gzip.open(path, "rt") as fh:
            for line in fh:
                if line.strip():
                    yield json.loads(line)
