"""Testes das agregações Gold (daily_revenue e top_products) com SparkSession local.

Usa ``transforms.compute_daily_revenue`` e ``transforms.compute_top_products``
— as mesmas funções usadas por ``gold_aggregations.py`` — sem duplicação.

Pulam automaticamente se pyspark não estiver instalado no host.
"""

import datetime as dt

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from transforms import compute_daily_revenue, compute_top_products  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("tests-gold-aggregations")
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
    event_id, product_id=10, qty=1, price=10.0, currency="BRL", date_offset=0,
):
    base = dt.date(2026, 6, 25)
    d = base + dt.timedelta(days=date_offset)
    ts = dt.datetime.combine(d, dt.time(12, 0, 0))
    return (event_id, 1, product_id, qty, price, qty * price, currency, ts, "web", d)


# ── Testes daily_revenue ────────────────────────────────────────────────────


def test_daily_revenue_agrupa_por_dia_e_moeda(spark):
    rows = [
        _silver_row("e1", qty=2, price=10.0, currency="BRL"),
        _silver_row("e2", qty=3, price=5.0, currency="BRL"),
        _silver_row("e3", qty=1, price=20.0, currency="USD"),
        _silver_row("e4", qty=1, price=8.0, currency="BRL", date_offset=1),
    ]
    df = spark.createDataFrame(rows, _SILVER_COLS)
    result = {
        (r["event_date"], r["currency"]): r
        for r in compute_daily_revenue(df).collect()
    }

    brl_d0 = result[(dt.date(2026, 6, 25), "BRL")]
    assert brl_d0["orders"] == 2
    assert brl_d0["items_sold"] == 5
    assert brl_d0["revenue"] == pytest.approx(35.0)
    assert brl_d0["avg_ticket"] == pytest.approx(17.5)

    usd_d0 = result[(dt.date(2026, 6, 25), "USD")]
    assert usd_d0["orders"] == 1
    assert usd_d0["revenue"] == pytest.approx(20.0)

    brl_d1 = result[(dt.date(2026, 6, 26), "BRL")]
    assert brl_d1["orders"] == 1
    assert brl_d1["revenue"] == pytest.approx(8.0)


def test_daily_revenue_avg_ticket(spark):
    rows = [
        _silver_row("e1", qty=1, price=100.0),
        _silver_row("e2", qty=1, price=200.0),
        _silver_row("e3", qty=1, price=300.0),
    ]
    df = spark.createDataFrame(rows, _SILVER_COLS)
    r = compute_daily_revenue(df).collect()[0]
    assert r["orders"] == 3
    assert r["revenue"] == pytest.approx(600.0)
    assert r["avg_ticket"] == pytest.approx(200.0)


# ── Testes top_products ─────────────────────────────────────────────────────


def test_top_products_ranking_correto(spark):
    rows = [
        _silver_row("e1", product_id=1, qty=1, price=50.0),
        _silver_row("e2", product_id=2, qty=1, price=100.0),
        _silver_row("e3", product_id=3, qty=1, price=30.0),
    ]
    df = spark.createDataFrame(rows, _SILVER_COLS)
    result = {
        r["product_id"]: r["rank"]
        for r in compute_top_products(df).collect()
    }
    assert result[2] == 1  # maior receita
    assert result[1] == 2
    assert result[3] == 3


def test_top_products_limita_a_20(spark):
    # 25 produtos distintos; apenas top 20 devem aparecer
    rows = [
        _silver_row(f"e{i}", product_id=i, qty=1, price=float(i))
        for i in range(1, 26)
    ]
    df = spark.createDataFrame(rows, _SILVER_COLS)
    result = compute_top_products(df).collect()
    assert len(result) == 20
    # O produto com menor receita (product_id=1, revenue=1.0) NÃO deve estar
    ranks = {r["product_id"] for r in result}
    for excluded in range(1, 6):
        assert excluded not in ranks


def test_top_products_ranking_por_dia_independente(spark):
    rows = [
        _silver_row("e1", product_id=1, qty=1, price=100.0, date_offset=0),
        _silver_row("e2", product_id=2, qty=1, price=50.0, date_offset=0),
        _silver_row("e3", product_id=2, qty=1, price=200.0, date_offset=1),
        _silver_row("e4", product_id=1, qty=1, price=10.0, date_offset=1),
    ]
    df = spark.createDataFrame(rows, _SILVER_COLS)
    collected = compute_top_products(df).collect()
    result = {
        (r["event_date"], r["product_id"]): r["rank"]
        for r in collected
    }
    # dia 0: product 1 (100) > product 2 (50)
    assert result[(dt.date(2026, 6, 25), 1)] == 1
    assert result[(dt.date(2026, 6, 25), 2)] == 2
    # dia 1: product 2 (200) > product 1 (10)
    assert result[(dt.date(2026, 6, 26), 2)] == 1
    assert result[(dt.date(2026, 6, 26), 1)] == 2
