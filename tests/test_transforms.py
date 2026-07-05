"""Testes da transformação Bronze->Silver com SparkSession local.

Pulam automaticamente se pyspark não estiver instalado no host
(no CI o pyspark está em requirements-dev.txt).
"""

import datetime as dt

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from transforms import clean_purchases  # noqa: E402


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
