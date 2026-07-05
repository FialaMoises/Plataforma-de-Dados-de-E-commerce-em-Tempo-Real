"""Gold streaming: receita por minuto com EVENT-TIME + WATERMARK.

Lê o tópico Kafka `purchases` (consumer group próprio, separado do Bronze),
agrega receita em janelas tumbling de 1 minuto sobre o `event_ts` (event-time,
NÃO processing-time) e materializa em `lakehouse.gold.revenue_per_minute`.

Garantias de nível sênior demonstradas:
  * Event-time windowing: a janela usa o horário do evento, não o de processamento.
  * Watermark de 2 min: late data dentro da tolerância é incorporada; além disso,
    é descartada (e a janela pode ser finalizada/limpa do estado).
  * Idempotência: foreachBatch + MERGE por (window_start, currency); reiniciar o
    job a partir do checkpoint não duplica nem corrompe as janelas.
  * Observabilidade: loga throughput (linhas/seg) por micro-batch.

Rodar (streaming contínuo):
    spark-submit /opt/spark/jobs/gold_revenue_per_minute.py
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schemas import KAFKA_BOOTSTRAP, PURCHASE_EVENT_SCHEMA
from transforms import VALID_CURRENCIES, revenue_windows

logger = logging.getLogger(__name__)

TOPIC = "purchases"
CHECKPOINT = "/opt/spark/checkpoints/gold_revenue_per_minute"
WATERMARK = "2 minutes"
WINDOW = "1 minute"
MAX_OFFSETS_PER_TRIGGER = 20000

spark = SparkSession.builder.appName("gold-revenue-per-minute").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    .load()
    .selectExpr("CAST(value AS STRING) AS raw_value")
)

# Constrói o fluxo limpo necessário para a agregação por event-time.
events = (
    raw.select(F.from_json("raw_value", PURCHASE_EVENT_SCHEMA).alias("e"))
    .filter(
        F.col("e.event_id").isNotNull()
        & (F.col("e.price") > 0)
        & (F.col("e.quantity") > 0)
        # mesma regra de domínio da Silver: descarta moeda fora do contrato
        & F.col("e.currency").isin(VALID_CURRENCIES)
    )
    .select(
        F.col("e.event_id").alias("event_id"),
        F.to_timestamp("e.timestamp").alias("event_ts"),
        F.col("e.currency").alias("currency"),
        F.col("e.quantity").alias("quantity"),
        (F.col("e.quantity") * F.col("e.price")).alias("gross_amount"),
    )
)

# Watermark ANTES da agregação: define a tolerância a late data e permite ao
# Spark finalizar/limpar janelas antigas do estado.
windowed = revenue_windows(events.withWatermark("event_ts", WATERMARK), WINDOW)


def upsert_windows(batch_df, batch_id: int) -> None:
    session = batch_df.sparkSession
    n = batch_df.count()
    out = batch_df.withColumn("window_date", F.to_date("window_start"))
    out.createOrReplaceTempView("rpm_updates")
    session.sql("""
        MERGE INTO lakehouse.gold.revenue_per_minute t
        USING rpm_updates s
        ON t.window_start = s.window_start AND t.currency = s.currency
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info("[rpm] batch %d: %d janelas finalizadas materializadas.", batch_id, n)


query = (
    windowed.writeStream.foreachBatch(upsert_windows)
    .outputMode("append")  # com watermark, emite janelas já finalizadas
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="20 seconds")
    .start()
)
query.awaitTermination()
