"""Mostra o estado do lakehouse: contagens por camada + KPIs do Gold.

Também valida a idempotência do Bronze: nº de linhas == nº de event_id distintos
(se o MERGE estiver correto, não há duplicatas apesar dos eventos repetidos).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("show-gold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.table("lakehouse.bronze.purchases")
b_rows = bronze.count()
b_ids = bronze.select("event_id").distinct().count()
dlq = spark.read.table("lakehouse.bronze.purchases_dlq").count()
silver = spark.read.table("lakehouse.silver.purchases").count()

idem = "OK (sem duplicatas)" if b_rows == b_ids else "FALHA: há duplicatas!"
print("\n──────── ESTADO DO LAKEHOUSE ────────")
print(f"  bronze.purchases       : {b_rows} linhas / {b_ids} event_id distintos")
print(f"  -> idempotência Bronze  : {idem}")
print(f"  bronze.purchases_dlq   : {dlq} payloads inválidos (DLQ)")
print(f"  silver.purchases       : {silver} linhas")
print("─────────────────────────────────────\n")

print("KPIs — gold.daily_revenue (top 10 dias):")
(
    spark.read.table("lakehouse.gold.daily_revenue")
    .orderBy(F.col("event_date").desc(), F.col("revenue").desc())
    .show(10, False)
)

print("KPIs — gold.top_products (top 10 do dia mais recente):")
g = spark.read.table("lakehouse.gold.top_products")
latest = g.agg(F.max("event_date")).collect()[0][0]
if latest:
    g.filter(F.col("event_date") == latest).orderBy("rank").show(10, False)

spark.stop()
