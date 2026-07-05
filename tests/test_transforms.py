"""Testes da transformação Bronze->Silver com SparkSession local.

Pulam automaticamente se pyspark não estiver instalado no host
(no CI o pyspark está em requirements-dev.txt).
"""

import datetime as dt

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from transforms import clean_purchases, revenue_windows  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _bronze_row(
    event_id, user_id=1, product_id=10, qty=2, price=5.0, currency="BRL", ingest_offset=0
):
    ts = dt.datetime(2026, 6, 25, 12, 0, 0)
    return (
        event_id,
        "purchase",
        user_id,
        product_id,
        qty,
        price,
        currency,
        ts,
        "web",
        "1.0.0",
        ts + dt.timedelta(seconds=ingest_offset),
        ts.date(),
    )


_COLS = [
    "event_id",
    "event_type",
    "user_id",
    "product_id",
    "quantity",
    "price",
    "currency",
    "event_ts",
    "channel",
    "schema_version",
    "ingest_ts",
    "event_date",
]


def test_dedup_mantem_ingestao_mais_recente(spark):
    rows = [
        _bronze_row("a", price=5.0, ingest_offset=0),
        _bronze_row("a", price=9.0, ingest_offset=10),  # mais recente vence
        _bronze_row("b", price=3.0),
    ]
    out = clean_purchases(spark.createDataFrame(rows, _COLS))
    result = {r["event_id"]: r["unit_price"] for r in out.collect()}
    assert result == {"a": 9.0, "b": 3.0}


def test_descarta_preco_e_quantidade_invalidos(spark):
    rows = [
        _bronze_row("ok", qty=2, price=5.0),
        _bronze_row("neg", qty=2, price=-1.0),
        _bronze_row("zero", qty=0, price=5.0),
    ]
    out = clean_purchases(spark.createDataFrame(rows, _COLS))
    assert {r["event_id"] for r in out.collect()} == {"ok"}


def test_gross_amount_calculado(spark):
    rows = [_bronze_row("x", qty=3, price=7.0)]
    out = clean_purchases(spark.createDataFrame(rows, _COLS)).collect()[0]
    assert out["gross_amount"] == pytest.approx(21.0)


def test_descarta_moeda_fora_do_dominio(spark):
    rows = [_bronze_row("ok", currency="BRL"), _bronze_row("bad", currency="XYZ")]
    out = clean_purchases(spark.createDataFrame(rows, _COLS))
    assert {r["event_id"] for r in out.collect()} == {"ok"}


def test_revenue_windows_agrega_por_janela_de_1min(spark):
    base = dt.datetime(2026, 6, 25, 12, 0, 0)
    rows = [
        # janela 12:00 — 2 pedidos BRL: 2*10 + 3*10 = 50, items=5
        ("e1", base, "BRL", 2, 20.0),
        ("e2", base + dt.timedelta(seconds=30), "BRL", 3, 30.0),
        # janela 12:01 — 1 pedido BRL
        ("e3", base + dt.timedelta(minutes=1, seconds=10), "BRL", 1, 7.0),
        # janela 12:00 — moeda diferente, grupo separado
        ("e4", base + dt.timedelta(seconds=5), "USD", 1, 9.0),
    ]
    df = spark.createDataFrame(
        rows, ["event_id", "event_ts", "currency", "quantity", "gross_amount"]
    )
    out = {(str(r["window_start"]), r["currency"]): r for r in revenue_windows(df).collect()}

    w0_brl = out[("2026-06-25 12:00:00", "BRL")]
    assert w0_brl["orders"] == 2
    assert w0_brl["items_sold"] == 5
    assert w0_brl["revenue"] == pytest.approx(50.0)
    assert w0_brl["avg_ticket"] == pytest.approx(25.0)

    assert out[("2026-06-25 12:01:00", "BRL")]["orders"] == 1
    assert out[("2026-06-25 12:00:00", "USD")]["revenue"] == pytest.approx(9.0)
