"""Bronze: Kafka (`carts`, JSON) -> Iceberg `lakehouse.bronze.carts`.

Mesmo padrão do bronze_ingest de purchases: checkpoint + MERGE idempotente por
event_id, com Dead Letter Queue para payloads inválidos.

Rodar (streaming contínuo):
    spark-submit /opt/spark/jobs/bronze_carts.py
"""

import logging
from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schemas import CART_EVENT_SCHEMA, KAFKA_BOOTSTRAP
from transforms import write_bronze_batch

logger = logging.getLogger(__name__)

TOPIC = "carts"
CHECKPOINT = "/opt/spark/checkpoints/bronze_carts"
MAX_OFFSETS_PER_TRIGGER = 5000

spark = SparkSession.builder.appName("bronze-carts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

parsed = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    .load()
    .selectExpr("CAST(value AS STRING) AS raw_value")
    .withColumn("parsed", F.from_json("raw_value", CART_EVENT_SCHEMA))
)

write_batch = partial(
    write_bronze_batch,
    table="lakehouse.bronze.carts",
    dlq_table="lakehouse.bronze.carts_dlq",
    required_parsed_fields=["event_id", "cart_id", "product_id"],
    schema=CART_EVENT_SCHEMA,
)

query = (
    parsed.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="15 seconds")
    .start()
)
query.awaitTermination()
