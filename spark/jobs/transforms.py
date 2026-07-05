"""Transformações puras Bronze -> Silver (reutilizadas pelo job e pelos testes).

Mantidas como funções de DataFrame->DataFrame para serem testáveis com uma
SparkSession local, sem Kafka/Iceberg.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Moedas aceitas pelo contrato v1.
VALID_CURRENCIES = ["BRL", "USD", "EUR"]


def clean_purchases(bronze: DataFrame) -> DataFrame:
    """Aplica tipagem, deduplicação e regras de negócio para a camada Silver.

    Regras:
      * dedup por event_id (mantém a ingestão mais recente)
      * quantity > 0 e unit_price > 0   (regra de negócio do contrato)
      * currency dentro do domínio permitido
      * descarta linhas com chaves nulas
      * deriva gross_amount = quantity * unit_price
    """
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").desc())

    deduped = bronze.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    return deduped.filter(
        F.col("event_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("price") > 0)
        & F.col("currency").isin(VALID_CURRENCIES)
    ).select(
        "event_id",
        "user_id",
        "product_id",
        "quantity",
        F.col("price").alias("unit_price"),
        (F.col("quantity") * F.col("price")).alias("gross_amount"),
        "currency",
        "event_ts",
        "channel",
        F.to_date("event_ts").alias("event_date"),
    )
