from datetime import datetime, timezone

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def test_bronze_key_is_stable_for_same_day():
    bronze = importlib.import_module("Bronze")

    first = datetime(2026, 8, 5, 10, 30, 15, tzinfo=timezone.utc)
    second = datetime(2026, 8, 5, 23, 59, 59, tzinfo=timezone.utc)

    assert bronze.construir_key("AAPL", first) == bronze.construir_key("AAPL", second)


def test_silver_key_is_stable_for_same_day():
    prata = importlib.import_module("Prata")

    first = datetime(2026, 8, 5, 10, 30, 15, tzinfo=timezone.utc)
    second = datetime(2026, 8, 5, 23, 59, 59, tzinfo=timezone.utc)

    assert prata.construir_silver_key("AAPL", first) == prata.construir_silver_key("AAPL", second)


def test_gold_key_is_stable_for_same_day():
    ouro = importlib.import_module("ouro")

    first = datetime(2026, 8, 5, 10, 30, 15, tzinfo=timezone.utc)
    second = datetime(2026, 8, 5, 23, 59, 59, tzinfo=timezone.utc)

    assert ouro.construir_ouro_key("AAPL", first) == ouro.construir_ouro_key("AAPL", second)
