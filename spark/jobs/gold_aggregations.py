"""Gold: Silver -> agregações prontas para consumo (batch idempotente).

Produz:
  gold.daily_revenue  -> orders, items_sold, revenue, avg_ticket por (dia, moeda)
  gold.top_products   -> ranking de produtos por receita por dia

Idempotente por overwrite particionado: reexecutar recalcula as partições da janela.
"""

import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gold-aggregations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# overwrite dinâmico: substitui só as partições tocadas, não a tabela inteira.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

full = "--full" in sys.argv
silver = spark.read.table("lakehouse.silver.purchases")
if not full:
    silver = silver.where("event_date >= date_sub(current_date(), 2)")

# ── daily_revenue ───────────────────────────────────────────────────────────
daily = (
    silver.groupBy("event_date", "currency")
    .agg(
        F.countDistinct("event_id").alias("orders"),
        F.sum("quantity").alias("items_sold"),
        F.round(F.sum("gross_amount"), 2).alias("revenue"),
    )
    .withColumn("avg_ticket", F.round(F.col("revenue") / F.col("orders"), 2))
)
daily.writeTo("lakehouse.gold.daily_revenue").overwritePartitions()

# ── top_products ──────────────────────────────────────────────────────────────
by_product = silver.groupBy("event_date", "product_id").agg(
    F.sum("quantity").alias("items_sold"),
    F.round(F.sum("gross_amount"), 2).alias("revenue"),
)
w = Window.partitionBy("event_date").orderBy(F.col("revenue").desc())
top = by_product.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= 20)
top.writeTo("lakehouse.gold.top_products").overwritePartitions()

print("[gold] agregações atualizadas:")
spark.read.table("lakehouse.gold.daily_revenue").orderBy(F.col("event_date").desc()).show(10, False)
spark.stop()
