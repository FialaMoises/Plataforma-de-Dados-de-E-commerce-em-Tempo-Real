"""Gold: Silver -> agregações prontas para consumo (batch idempotente).

Produz:
  gold.daily_revenue  -> orders, items_sold, revenue, avg_ticket por (dia, moeda)
  gold.top_products   -> ranking de produtos por receita por dia

Idempotente por overwrite particionado: reexecutar recalcula as partições da janela.

A lógica de agregação está em ``transforms.compute_daily_revenue`` e
``transforms.compute_top_products`` para ser reutilizável pelos testes.
"""

import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transforms import compute_daily_revenue, compute_top_products

logger = logging.getLogger(__name__)


def main() -> None:
    spark = SparkSession.builder.appName("gold-aggregations").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # overwrite dinâmico: substitui só as partições tocadas, não a tabela inteira.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    full = "--full" in sys.argv
    silver = spark.read.table("lakehouse.silver.purchases")
    if not full:
        silver = silver.where("event_date >= date_sub(current_date(), 2)")

    # ── daily_revenue ──────────────────────────────────────────────────────
    daily = compute_daily_revenue(silver)
    daily.writeTo("lakehouse.gold.daily_revenue").overwritePartitions()

    # ── top_products ───────────────────────────────────────────────────────
    top = compute_top_products(silver)
    top.writeTo("lakehouse.gold.top_products").overwritePartitions()

    logger.info("[gold] agregações atualizadas:")
    spark.read.table("lakehouse.gold.daily_revenue").orderBy(
        F.col("event_date").desc()
    ).show(10, False)
    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
