"""Gold stateful: detecção de CARRINHOS ABANDONADOS.

Sessionização com estado por CARRINHO (cart_id) usando `applyInPandasWithState`
(arbitrary stateful processing) e timeout de EVENT-TIME.

Lógica (lê apenas o tópico `carts`, keyed por cart_id):
  * add_to_cart / remove_from_cart  -> acumula/retira itens do carrinho;
  * checkout                        -> carrinho CONVERTIDO, limpa o estado;
  * sem checkout por ABANDON_MINUTES de event-time -> TIMEOUT -> emite o carrinho
    como ABANDONADO em `gold.abandoned_carts`.

Keying por cart_id (e não user_id) evita contaminação entre múltiplas sessões do
mesmo usuário — cada carrinho é uma sessão isolada. Ver ADR-0006.

Rodar (streaming contínuo):
    spark-submit /opt/spark/jobs/gold_abandoned_carts.py
"""

import logging

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from schemas import ABANDONED_CART_EVENT_SCHEMA, KAFKA_BOOTSTRAP

logger = logging.getLogger(__name__)

CHECKPOINT = "/opt/spark/checkpoints/gold_abandoned_carts"
WATERMARK = "1 minute"
ABANDON_MINUTES = 2
ABANDON_MS = ABANDON_MINUTES * 60 * 1000
SECONDS_TO_MS = 1000
MAX_OFFSETS_PER_TRIGGER = 20000

OUTPUT_SCHEMA = StructType(
    [
        StructField("cart_id", StringType()),
        StructField("user_id", LongType()),
        StructField("items", IntegerType()),
        StructField("cart_value", DoubleType()),
        StructField("last_activity_ms", LongType()),
    ]
)

STATE_SCHEMA = StructType(
    [
        StructField("items", IntegerType()),
        StructField("cart_value", DoubleType()),
        StructField("last_ts", LongType()),
        StructField("user_id", LongType()),
    ]
)

spark = SparkSession.builder.appName("gold-abandoned-carts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "carts")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    .load()
    .selectExpr("CAST(value AS STRING) AS raw_value")
    .select(F.from_json("raw_value", ABANDONED_CART_EVENT_SCHEMA).alias("e"))
    .filter(F.col("e.event_id").isNotNull() & F.col("e.cart_id").isNotNull())
    .select(
        F.col("e.cart_id").alias("cart_id"),
        F.col("e.user_id").alias("user_id"),
        F.col("e.event_type").alias("kind"),
        F.coalesce(F.col("e.quantity"), F.lit(0)).alias("quantity"),
        (
            F.coalesce(F.col("e.quantity"), F.lit(0))
            * F.coalesce(F.col("e.price"), F.lit(0.0))
        ).alias("gross_amount"),
        F.to_timestamp("e.timestamp").alias("event_ts"),
        (F.to_timestamp("e.timestamp").cast("double") * SECONDS_TO_MS)
        .cast("long")
        .alias("event_ts_ms"),
    )
    .withWatermark("event_ts", WATERMARK)
)


# ── Funções auxiliares da state machine ─────────────────────────────────────


def _abandoned_cart_row(cart_id, user_id, items, value, last_ts):
    """Constrói a linha de saída para um carrinho abandonado."""
    return pd.DataFrame(
        [
            {
                "cart_id": cart_id,
                "user_id": int(user_id) if user_id is not None else None,
                "items": int(items),
                "cart_value": float(value),
                "last_activity_ms": int(last_ts),
            }
        ]
    )


def _accumulate_cart_events(pdf_iter, items, value, last_ts, user_id):
    """Processa eventos de carrinho acumulando itens e valor.

    Retorna (items, value, last_ts, user_id, converted).
    """
    converted = False
    for pdf in pdf_iter:
        for r in pdf.sort_values("event_ts_ms").itertuples(index=False):
            ts = int(r.event_ts_ms)
            if r.kind == "checkout":
                converted = True
            elif r.kind == "add_to_cart":
                items = (items or 0) + int(r.quantity)
                value = (value or 0.0) + float(r.gross_amount)
                last_ts = max(last_ts or 0, ts)
                user_id = int(r.user_id)
            else:  # remove_from_cart
                items = max(0, (items or 0) - int(r.quantity))
                last_ts = max(last_ts or 0, ts)
    return items, value, last_ts, user_id, converted


def update_cart(key, pdf_iter, state):
    """State machine por carrinho. Gerador: emite linha só no abandono."""
    cart_id = key[0]

    # ── Timeout: o watermark ultrapassou o prazo de abandono ───────────────
    if state.hasTimedOut:
        items, value, last_ts, user_id = state.get
        state.remove()
        if items and items > 0:
            yield _abandoned_cart_row(cart_id, user_id, items, value, last_ts)
        return

    # ── Processa eventos do micro-batch ────────────────────────────────────
    prev = state.get if state.exists else (0, 0.0, 0, None)
    items, value, last_ts, user_id, converted = _accumulate_cart_events(
        pdf_iter, *prev
    )

    if converted or not items or items <= 0:
        state.remove()  # checkout ou carrinho vazio -> sem abandono
        return

    # ── Prazo de abandono já passou (evento chegou tarde) -> emite agora ───
    timeout_at = int(last_ts) + ABANDON_MS
    if timeout_at <= state.getCurrentWatermarkMs():
        state.remove()
        yield _abandoned_cart_row(cart_id, user_id, items, value, last_ts)
        return

    state.update((int(items), float(value), int(last_ts), user_id))
    state.setTimeoutTimestamp(timeout_at)


abandoned = events.groupBy("cart_id").applyInPandasWithState(
    update_cart,
    OUTPUT_SCHEMA,
    STATE_SCHEMA,
    "append",
    GroupStateTimeout.EventTimeTimeout,
)


def write_abandoned(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        logger.info("[abandoned] batch %d: 0 carrinhos abandonados.", batch_id)
        return
    session = batch_df.sparkSession
    out = (
        batch_df.withColumn(
            "last_activity_ts",
            (F.col("last_activity_ms") / SECONDS_TO_MS).cast("timestamp"),
        )
        .withColumn("abandoned_at", F.current_timestamp())
        .withColumn("abandon_date", F.current_date())
        .select(
            "user_id",
            "cart_id",
            "items",
            "cart_value",
            "last_activity_ts",
            "abandoned_at",
            "abandon_date",
        )
    )
    out.createOrReplaceTempView("ab_updates")
    # MERGE por cart_id -> idempotente (reprocesso do checkpoint não duplica).
    session.sql("""
        MERGE INTO lakehouse.gold.abandoned_carts t
        USING ab_updates s
        ON t.cart_id = s.cart_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info(
        "[abandoned] batch %d: %d carrinhos abandonados.",
        batch_id,
        batch_df.count(),
    )


query = (
    abandoned.writeStream.foreachBatch(write_abandoned)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="20 seconds")
    .start()
)
query.awaitTermination()
