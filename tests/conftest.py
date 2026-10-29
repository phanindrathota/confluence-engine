import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from confluence.settings import load_settings  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("QUANTDATA_API_KEY", "qd_" + "a" * 32)
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("QD_BUDGET_PER_MIN", "1000")
    return load_settings(env_file=tmp_path / "none.env", config_file=ROOT / "config" / "universe.yaml")
