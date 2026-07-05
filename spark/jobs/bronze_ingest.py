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

import logging
from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schemas import KAFKA_BOOTSTRAP, PURCHASE_EVENT_SCHEMA
from transforms import write_bronze_batch

logger = logging.getLogger(__name__)

TOPIC = "purchases"
CHECKPOINT = "/opt/spark/checkpoints/bronze_purchases"
MAX_OFFSETS_PER_TRIGGER = 5000

spark = SparkSession.builder.appName("bronze-ingest").getOrCreate()
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

# Parse tolerante: linhas inválidas viram parsed=null e seguem para a DLQ.
parsed = raw.withColumn("parsed", F.from_json("raw_value", PURCHASE_EVENT_SCHEMA))

write_batch = partial(
    write_bronze_batch,
    table="lakehouse.bronze.purchases",
    dlq_table="lakehouse.bronze.purchases_dlq",
    required_parsed_fields=["event_id", "product_id", "price"],
    schema=PURCHASE_EVENT_SCHEMA,
)

query = (
    parsed.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="15 seconds")
    .start()
)
query.awaitTermination()
