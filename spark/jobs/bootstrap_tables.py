"""Cria namespaces e tabelas Iceberg do slice (idempotente).

Camadas Medallion:
  bronze.purchases       -> dados brutos, append-only com dedup por event_id
  bronze.purchases_dlq   -> payloads que falharam no parse/contrato (Dead Letter)
  silver.purchases       -> limpo, tipado, deduplicado, regras de negócio aplicadas
  gold.daily_revenue     -> receita/ticket médio por dia (e moeda)
  gold.top_products      -> produtos mais vendidos por dia
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bootstrap-tables").getOrCreate()

for ns in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{ns}")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.bronze.purchases (
    event_id        STRING,
    event_type      STRING,
    user_id         BIGINT,
    product_id      BIGINT,
    quantity        INT,
    price           DOUBLE,
    currency        STRING,
    event_ts        TIMESTAMP,
    channel         STRING,
    schema_version  STRING,
    ingest_ts       TIMESTAMP,
    event_date      DATE
) USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.bronze.purchases_dlq (
    raw_value       STRING,
    error_reason    STRING,
    ingest_ts       TIMESTAMP,
    ingest_date     DATE
) USING iceberg
PARTITIONED BY (ingest_date)
TBLPROPERTIES ('format-version'='2')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.purchases (
    event_id        STRING,
    user_id         BIGINT,
    product_id      BIGINT,
    quantity        INT,
    unit_price      DOUBLE,
    gross_amount    DOUBLE,
    currency        STRING,
    event_ts        TIMESTAMP,
    channel         STRING,
    event_date      DATE
) USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.daily_revenue (
    event_date      DATE,
    currency        STRING,
    orders          BIGINT,
    items_sold      BIGINT,
    revenue         DOUBLE,
    avg_ticket      DOUBLE
) USING iceberg
PARTITIONED BY (event_date)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.top_products (
    event_date      DATE,
    product_id      BIGINT,
    items_sold      BIGINT,
    revenue         DOUBLE,
    rank            INT
) USING iceberg
PARTITIONED BY (event_date)
""")

print("[bootstrap] namespaces e tabelas criadas.")
for row in spark.sql("SHOW TABLES IN lakehouse.bronze").collect():
    print("  bronze:", row)
spark.stop()
