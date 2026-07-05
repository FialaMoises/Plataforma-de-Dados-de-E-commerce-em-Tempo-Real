"""Silver: Bronze -> `lakehouse.silver.purchases` (batch idempotente).

Idempotente via MERGE por event_id: rodar o job N vezes produz o mesmo Silver.
Processa apenas a janela recente por padrão (incremental-friendly); para backfill,
passe --full.
"""

import sys

from pyspark.sql import SparkSession
from transforms import clean_purchases

spark = SparkSession.builder.appName("silver-purchases").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

full = "--full" in sys.argv

bronze = spark.read.table("lakehouse.bronze.purchases")
if not full:
    # incremental: últimos 2 dias (cobre atrasos de event-time).
    bronze = bronze.where("event_date >= date_sub(current_date(), 2)")

silver = clean_purchases(bronze)
silver.createOrReplaceTempView("silver_updates")

spark.sql("""
    MERGE INTO lakehouse.silver.purchases t
    USING silver_updates s
    ON t.event_id = s.event_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

n = spark.read.table("lakehouse.silver.purchases").count()
print(f"[silver] OK — total de linhas na Silver: {n}")
spark.stop()
