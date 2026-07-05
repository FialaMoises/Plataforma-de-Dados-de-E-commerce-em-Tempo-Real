"""Dashboard da Plataforma Lakehouse (Camada 8 — Analytics).

Lê as tabelas Gold/Bronze/Silver (Iceberg) via Trino e a saúde do streaming via
Kafka. Três visões — Executivo, Produto, Operacional — todas sobre o dado REAL
produzido pelo pipeline. Atualiza ao vivo.
"""

from __future__ import annotations

import logging
import os

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

import trino

logger = logging.getLogger(__name__)

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TOPIC_PURCHASES", "purchases")
TOP_PRODUCTS_CHART_LIMIT = 15

st.set_page_config(
    page_title="E-commerce Lakehouse", page_icon="🛒", layout="wide",
)


# ── Infraestrutura de acesso a dados ────────────────────────────────────────


@st.cache_resource
def _conn():
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="dashboard", catalog="iceberg",
    )


def query_trino(sql: str) -> pd.DataFrame:
    """Executa SQL no Trino e devolve um DataFrame (vazio em caso de erro)."""
    try:
        cur = _conn().cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        logger.exception("Erro ao executar query Trino")
        return pd.DataFrame()


def topic_depth() -> int | None:
    """Total de eventos no tópico (soma dos high-watermarks das partições).

    O Spark Structured Streaming guarda offsets no checkpoint (não como consumer
    group no Kafka), então o lag do pipeline é medido como tópico - Bronze, e não
    via ``kafka-consumer-groups``.
    """
    try:
        from confluent_kafka import Consumer, TopicPartition

        c = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": "dash-depth-probe",
                "enable.auto.commit": False,
            }
        )
        meta = c.list_topics(topic=TOPIC, timeout=10)
        parts = meta.topics[TOPIC].partitions
        total = 0
        for p in parts:
            _, hi = c.get_watermark_offsets(
                TopicPartition(TOPIC, p), timeout=5,
            )
            total += hi
        c.close()
        return total
    except Exception:
        logger.exception("Kafka indisponível para medir profundidade")
        return None


def _fmt_money(v: float) -> str:
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ── Sidebar ─────────────────────────────────────────────────────────────────


def _render_sidebar() -> None:
    st.sidebar.title("🛒 Lakehouse")
    st.sidebar.caption("E-commerce em tempo real")
    auto = st.sidebar.toggle("Atualizar ao vivo", value=True)
    interval = st.sidebar.slider("Intervalo (s)", 5, 60, 15, disabled=not auto)
    if auto:
        st_autorefresh(interval=interval * 1000, key="tick")
    if st.sidebar.button("Atualizar agora"):
        st.rerun()
    st.sidebar.divider()
    st.sidebar.caption(
        f"Trino: {TRINO_HOST}:{TRINO_PORT}\n\nKafka: {KAFKA_BOOTSTRAP}",
    )


# ── Header KPIs ─────────────────────────────────────────────────────────────


def _render_kpi_header(daily: pd.DataFrame) -> None:
    c1, c2, c3, c4 = st.columns(4)
    if not daily.empty:
        total_orders = int(daily["orders"].sum())
        brl = daily[daily["currency"] == "BRL"]
        rev_brl = float(brl["revenue"].iloc[0]) if not brl.empty else 0.0
        ticket_brl = (
            rev_brl / int(brl["orders"].iloc[0]) if not brl.empty else 0.0
        )
        c1.metric(
            "Pedidos (todas moedas)",
            f"{total_orders:,}".replace(",", "."),
        )
        c2.metric("Receita BRL", f"R$ {_fmt_money(rev_brl)}")
        c3.metric("Ticket médio BRL", f"R$ {_fmt_money(ticket_brl)}")
        c4.metric(
            "Itens vendidos",
            f"{int(daily['items'].sum()):,}".replace(",", "."),
        )
    else:
        st.info(
            "Sem dados no Gold ainda. Rode o pipeline: bootstrap → stream → batch."
        )


# ── Tab Executivo ───────────────────────────────────────────────────────────


def _render_executive_tab(daily: pd.DataFrame) -> None:
    st.subheader("Receita por moeda")
    if not daily.empty:
        st.bar_chart(daily.set_index("currency")["revenue"], color="#4C9BE8")
        st.dataframe(
            daily.assign(
                avg_ticket=(daily["revenue"] / daily["orders"]).round(2),
            ).sort_values("revenue", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
    trend = query_trino(
        "SELECT event_date, sum(revenue) revenue "
        "FROM iceberg.gold.daily_revenue "
        "GROUP BY event_date ORDER BY event_date"
    )
    if not trend.empty and len(trend) > 1:
        st.subheader("Tendência diária de receita")
        st.line_chart(trend.set_index("event_date")["revenue"])


# ── Tab Produto ─────────────────────────────────────────────────────────────


def _render_product_tab() -> None:
    st.subheader("Produtos mais vendidos (por receita)")
    top = query_trino(
        "SELECT product_id, items_sold, revenue, rank "
        "FROM iceberg.gold.top_products "
        "WHERE event_date = (SELECT max(event_date) FROM iceberg.gold.top_products) "
        "ORDER BY rank LIMIT 20"
    )
    if not top.empty:
        st.bar_chart(
            top.head(TOP_PRODUCTS_CHART_LIMIT)
            .set_index("product_id")["revenue"],
            color="#7BC86C",
        )
        st.dataframe(top, use_container_width=True, hide_index=True)
    else:
        st.info("gold.top_products ainda vazia — rode o batch (gold).")

    st.divider()
    _render_abandoned_carts()


def _render_abandoned_carts() -> None:
    st.subheader("🛒 Carrinhos abandonados (streaming stateful)")
    ab = query_trino(
        "SELECT count(*) abandonados, round(sum(cart_value), 2) valor_perdido, "
        "round(avg(items), 2) itens_medios FROM iceberg.gold.abandoned_carts"
    )
    carts_total = query_trino(
        "SELECT count(distinct cart_id) n FROM iceberg.bronze.carts",
    )
    if not ab.empty and int(ab["abandonados"].iloc[0] or 0) > 0:
        n_ab = int(ab["abandonados"].iloc[0])
        n_carts = int(carts_total["n"].iloc[0]) if not carts_total.empty else 0
        taxa = (n_ab / n_carts * 100) if n_carts else 0
        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Carrinhos abandonados", f"{n_ab:,}".replace(",", "."))
        a2.metric("Taxa de abandono", f"{taxa:.1f}%")
        a3.metric(
            "Valor perdido",
            f"R$ {_fmt_money(float(ab['valor_perdido'].iloc[0] or 0))}",
        )
        a4.metric(
            "Itens médios/carrinho",
            f"{float(ab['itens_medios'].iloc[0] or 0):.1f}",
        )
        recent = query_trino(
            "SELECT cart_id, items, cart_value, last_activity_ts, abandoned_at "
            "FROM iceberg.gold.abandoned_carts ORDER BY abandoned_at DESC LIMIT 15"
        )
        if not recent.empty:
            st.caption("Abandonos mais recentes")
            st.dataframe(recent, use_container_width=True, hide_index=True)
    else:
        st.info(
            "gold.abandoned_carts vazia — rode `slice.sh stream-abandoned`.",
        )


# ── Tab Operacional ─────────────────────────────────────────────────────────


def _render_operational_tab() -> None:
    _render_revenue_per_minute()
    _render_pipeline_health()
    _render_data_quality()
    _render_pipeline_lag()


def _render_revenue_per_minute() -> None:
    st.subheader("Receita por minuto (event-time, Fase 2)")
    rpm = query_trino(
        "SELECT window_start, currency, revenue "
        "FROM iceberg.gold.revenue_per_minute ORDER BY window_start"
    )
    if not rpm.empty:
        pivot = rpm.pivot_table(
            index="window_start",
            columns="currency",
            values="revenue",
            aggfunc="sum",
        ).fillna(0)
        st.line_chart(pivot)
    else:
        st.info(
            "gold.revenue_per_minute vazia — rode `slice.sh stream-rpm`.",
        )


def _render_pipeline_health() -> None:
    st.subheader("Saúde do pipeline")
    h1, h2, h3, h4 = st.columns(4)
    bronze = query_trino(
        "SELECT count(*) n, count(distinct event_id) d "
        "FROM iceberg.bronze.purchases"
    )
    if not bronze.empty:
        n, d = int(bronze["n"].iloc[0]), int(bronze["d"].iloc[0])
        h1.metric("Bronze (linhas)", f"{n:,}".replace(",", "."))
        h2.metric(
            "Idempotência",
            "OK" if n == d else "FALHA",
            delta=None if n == d else f"{n - d} dup",
            delta_color="off" if n == d else "inverse",
        )
    dlq = query_trino("SELECT count(*) n FROM iceberg.bronze.purchases_dlq")
    if not dlq.empty:
        h3.metric(
            "DLQ (inválidos)",
            f"{int(dlq['n'].iloc[0]):,}".replace(",", "."),
        )
    silver = query_trino("SELECT count(*) n FROM iceberg.silver.purchases")
    if not silver.empty:
        h4.metric(
            "Silver (linhas)",
            f"{int(silver['n'].iloc[0]):,}".replace(",", "."),
        )


def _render_data_quality() -> None:
    st.markdown("**Data Quality (checagens ao vivo sobre a Silver)**")
    dq = query_trino(
        "SELECT "
        "count_if(event_id IS NULL OR user_id IS NULL OR product_id IS NULL) null_keys, "
        "count(*) - count(distinct event_id) dup_ids, "
        "count_if(unit_price <= 0) bad_price, "
        "count_if(quantity <= 0) bad_qty, "
        "count_if(currency NOT IN ('BRL','USD','EUR')) bad_ccy "
        "FROM iceberg.silver.purchases"
    )
    if not dq.empty:
        checks = {
            "chaves não nulas": int(dq["null_keys"].iloc[0]) == 0,
            "event_id único": int(dq["dup_ids"].iloc[0]) == 0,
            "preço > 0": int(dq["bad_price"].iloc[0]) == 0,
            "quantidade > 0": int(dq["bad_qty"].iloc[0]) == 0,
            "moeda no domínio": int(dq["bad_ccy"].iloc[0]) == 0,
        }
        cols = st.columns(len(checks))
        for col, (name, ok) in zip(cols, checks.items(), strict=False):
            col.metric(name, "PASS" if ok else "FAIL")
        if all(checks.values()):
            st.success("Quality Gate: APROVADO")
        else:
            st.error("Quality Gate: REPROVADO")


def _render_pipeline_lag() -> None:
    st.markdown("**Lag do pipeline (Kafka → Bronze)**")
    depth = topic_depth()
    bronze = query_trino(
        "SELECT count(*) n FROM iceberg.bronze.purchases",
    )
    bronze_n = int(bronze["n"].iloc[0]) if not bronze.empty else 0
    if depth is not None:
        k1, k2, k3 = st.columns(3)
        k1.metric("Eventos no tópico", f"{depth:,}".replace(",", "."))
        k2.metric(
            "Ingeridos no Bronze", f"{bronze_n:,}".replace(",", "."),
        )
        lag = max(0, depth - bronze_n)
        k3.metric(
            "Lag do pipeline",
            f"{lag:,}".replace(",", "."),
            help=(
                "Eventos no tópico ainda não ingeridos. "
                "Cresce se o stream Bronze estiver parado."
            ),
            delta_color="inverse",
        )
        if lag > 0:
            st.caption(
                "⚠️ Lag > 0: o stream Bronze não está rodando ou está atrás. "
                "Rode `slice.sh stream` para zerar."
            )
    else:
        st.caption("Kafka indisponível para medir profundidade do tópico.")


# ── Composição principal ────────────────────────────────────────────────────

_render_sidebar()
st.title("Plataforma de Dados de E-commerce — Painel")

daily = query_trino(
    "SELECT currency, sum(orders) orders, sum(items_sold) items, "
    "sum(revenue) revenue FROM iceberg.gold.daily_revenue GROUP BY currency"
)
_render_kpi_header(daily)

tab_exec, tab_prod, tab_ops = st.tabs(
    ["📈 Executivo", "📦 Produto", "⚙️ Operacional"],
)
with tab_exec:
    _render_executive_tab(daily)
with tab_prod:
    _render_product_tab()
with tab_ops:
    _render_operational_tab()
