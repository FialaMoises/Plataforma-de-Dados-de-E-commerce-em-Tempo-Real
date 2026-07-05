"""Bronze: Kafka (`purchases`, JSON) -> Iceberg `lakehouse.bronze.purchases`.

Garantias de nível sênior demonstradas aqui:
  * Exactly-once efetivo no destino: checkpoint do Structured Streaming + MERGE
    idempotente por `event_id` (reprocessar o mesmo offset NÃO duplica receita).
  * Dead Letter Queue: payloads que falham no parse/contrato vão para
    `bronze.purchases_dlq` em vez de derrubar o stream.
  * Schema-on-read explícito (não inferimos schema de JSON em streaming).

Rodar (streaming contínuo):
    spark-submit /opt/spark/jobs/bronze_ingest.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "purchases"
CHECKPOINT = "/opt/spark/checkpoints/bronze_purchases"

# Schema-on-read: o contrato v1 traduzido para tipos Spark.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("user_id", LongType()),
        StructField("product_id", LongType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType()),
        StructField("currency", StringType()),
        StructField("timestamp", StringType()),
        StructField("channel", StringType()),
        StructField("schema_version", StringType()),
    ]
)

spark = SparkSession.builder.appName("bronze-ingest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 5000)
    .load()
    .selectExpr("CAST(value AS STRING) AS raw_value")
)

# Parse tolerante: linhas inválidas viram parsed=null e seguem para a DLQ.
parsed = raw.withColumn("parsed", F.from_json("raw_value", EVENT_SCHEMA))


def write_batch(batch_df, batch_id: int) -> None:
    batch_df = batch_df.persist()
    now = F.current_timestamp()

    # ── DLQ: não parseou OU faltam campos obrigatórios mínimos ──────────────
    bad = batch_df.filter(
        F.col("parsed").isNull()
        | F.col("parsed.event_id").isNull()
        | F.col("parsed.product_id").isNull()
        | F.col("parsed.price").isNull()
    ).select(
        F.col("raw_value"),
        F.lit("parse_or_required_field_violation").alias("error_reason"),
        now.alias("ingest_ts"),
        F.current_date().alias("ingest_date"),
    )
    if not bad.isEmpty():
        bad.writeTo("lakehouse.bronze.purchases_dlq").append()

    # ── Bronze válido: normaliza colunas + dedup intra-batch por event_id ───
    good = (
        batch_df.filter(F.col("parsed.event_id").isNotNull())
        .select(
            F.col("parsed.event_id").alias("event_id"),
            F.col("parsed.event_type").alias("event_type"),
            F.col("parsed.user_id").alias("user_id"),
            F.col("parsed.product_id").alias("product_id"),
            F.col("parsed.quantity").alias("quantity"),
            F.col("parsed.price").alias("price"),
            F.col("parsed.currency").alias("currency"),
            F.to_timestamp("parsed.timestamp").alias("event_ts"),
            F.col("parsed.channel").alias("channel"),
            F.col("parsed.schema_version").alias("schema_version"),
            now.alias("ingest_ts"),
        )
        .withColumn("event_date", F.to_date("event_ts"))
        .dropDuplicates(["event_id"])
    )

    good.createOrReplaceTempView("updates")
    # MERGE idempotente: se o event_id já existe no Bronze, não reinsere.
    spark.sql("""
        MERGE INTO lakehouse.bronze.purchases t
        USING updates s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    batch_df.unpersist()
    print(f"[bronze] batch {batch_id} processado.")


query = (
    parsed.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="15 seconds")
    .start()
)
query.awaitTermination()
