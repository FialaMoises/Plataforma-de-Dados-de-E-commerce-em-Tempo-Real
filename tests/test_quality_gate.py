"""Testes da lógica do Quality Gate (Silver) com SparkSession local.

Usa ``transforms.run_quality_checks`` — a mesma função usada pelo job
``quality_gate.py`` — sem duplicação de lógica.

Pulam automaticamente se pyspark não estiver instalado no host.
"""

import datetime as dt

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from transforms import run_quality_checks  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("tests-quality-gate")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


# ── Construtor de linhas Silver ─────────────────────────────────────────────

_SILVER_COLS = [
    "event_id",
    "user_id",
    "product_id",
    "quantity",
    "unit_price",
    "gross_amount",
    "currency",
    "event_ts",
    "channel",
    "event_date",
]


def _silver_row(
    event_id="e1",
    user_id=1,
    product_id=10,
    qty=2,
    price=5.0,
    currency="BRL",
    event_date=None,
):
    ts = dt.datetime(2026, 6, 28, 12, 0, 0)
    if event_date is None:
        event_date = ts.date()
    return (
        event_id, user_id, product_id, qty, price,
        qty * price, currency, ts, "web", event_date,
    )


def _make_df(spark, rows):
    return spark.createDataFrame(rows, _SILVER_COLS)


# ── Testes ──────────────────────────────────────────────────────────────────


def test_all_checks_pass(spark):
    rows = [
        _silver_row("a", user_id=1, product_id=10, qty=2, price=5.0),
        _silver_row("b", user_id=2, product_id=20, qty=1, price=10.0),
    ]
    checks, _ = run_quality_checks(_make_df(spark, rows))
    for name, (ok, _detail, _blocking) in checks.items():
        assert ok, f"check {name} deveria ter passado"


def test_null_keys_detected(spark):
    ts = dt.datetime(2026, 6, 28, 12, 0)
    rows = [
        _silver_row("a"),
        (None, 1, 10, 2, 5.0, 10.0, "BRL", ts, "web", ts.date()),
    ]
    checks, _ = run_quality_checks(_make_df(spark, rows))
    assert not checks["no_null_keys"][0]


def test_duplicate_event_ids_detected(spark):
    rows = [_silver_row("dup"), _silver_row("dup", user_id=2)]
    checks, _ = run_quality_checks(_make_df(spark, rows))
    assert not checks["unique_event_id"][0]


def test_negative_price_detected(spark):
    rows = [_silver_row("a", price=-1.0)]
    checks, _ = run_quality_checks(_make_df(spark, rows))
    assert not checks["price_positive"][0]


def test_zero_quantity_detected(spark):
    rows = [_silver_row("a", qty=0)]
    checks, _ = run_quality_checks(_make_df(spark, rows))
    assert not checks["quantity_positive"][0]


def test_inconsistent_gross_amount_detected(spark):
    # gross_amount deveria ser qty*price=10, mas forçamos 999
    ts = dt.datetime(2026, 6, 28, 12, 0)
    rows = [("a", 1, 10, 2, 5.0, 999.0, "BRL", ts, "web", ts.date())]
    checks, _ = run_quality_checks(_make_df(spark, rows))
    assert not checks["gross_amount_consistent"][0]


def test_low_currency_completeness_detected(spark):
    # 1 de 2 com currency preenchido = 50% < 99%
    ts = dt.datetime(2026, 6, 28, 12, 0)
    rows = [
        _silver_row("a"),
        ("b", 1, 10, 2, 5.0, 10.0, None, ts, "web", ts.date()),
    ]
    checks, _ = run_quality_checks(_make_df(spark, rows))
    assert not checks["currency_completeness"][0]
