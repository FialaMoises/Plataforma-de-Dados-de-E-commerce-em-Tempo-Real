"""Cria namespaces e tabelas Iceberg do slice (idempotente).

Camadas Medallion:
  bronze.purchases       -> dados brutos, append-only com dedup por event_id
  bronze.purchases_dlq   -> payloads de purchase que falharam no parse/contrato
  bronze.carts           -> eventos de carrinho (add/remove/checkout)
  bronze.carts_dlq       -> payloads de cart que falharam no parse/contrato
  silver.purchases       -> limpo, tipado, deduplicado, regras de negócio aplicadas
  gold.daily_revenue     -> receita/ticket médio por dia (e moeda)
  gold.top_products      -> produtos mais vendidos por dia
  gold.revenue_per_minute -> receita por minuto (event-time + watermark)
  gold.abandoned_carts   -> carrinhos abandonados (stateful streaming)
  gold.dim_date          -> dimensão de data (calendário)
  gold.dim_products      -> dimensão de produtos (SCD Type 2)
  gold.dim_users         -> dimensão de usuários (SCD Type 2)
  gold.fact_sales        -> fato de vendas (star schema)
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
CREATE TABLE IF NOT EXISTS lakehouse.bronze.carts_dlq (
    raw_value       STRING,
    error_reason    STRING,
    ingest_ts       TIMESTAMP,
    ingest_date     DATE
) USING iceberg
PARTITIONED BY (ingest_date)
TBLPROPERTIES ('format-version'='2')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.bronze.carts (
    event_id        STRING,
    event_type      STRING,
    cart_id         STRING,
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
CREATE TABLE IF NOT EXISTS lakehouse.gold.revenue_per_minute (
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    currency        STRING,
    orders          BIGINT,
    items_sold      BIGINT,
    revenue         DOUBLE,
    avg_ticket      DOUBLE,
    window_date     DATE
) USING iceberg
PARTITIONED BY (window_date)
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
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

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.abandoned_carts (
    user_id          BIGINT,
    cart_id          STRING,
    items            INT,
    cart_value       DOUBLE,
    last_activity_ts TIMESTAMP,
    abandoned_at     TIMESTAMP,
    abandon_date     DATE
) USING iceberg
PARTITIONED BY (abandon_date)
TBLPROPERTIES ('format-version'='2')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date (
    date_key        INT     COMMENT 'Surrogate key no formato YYYYMMDD',
    full_date       DATE,
    day             INT,
    month           INT,
    year            INT,
    quarter         INT,
    day_of_week     INT     COMMENT '1=domingo, 7=sabado',
    day_name        STRING,
    month_name      STRING,
    is_weekend      BOOLEAN
) USING iceberg
TBLPROPERTIES ('format-version'='2')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_products (
    product_key     BIGINT  COMMENT 'Surrogate key auto-gerada',
    product_id      BIGINT  COMMENT 'Chave natural do produto',
    valid_from      DATE    COMMENT 'Data da primeira aparicao (SCD2)',
    valid_to        DATE    COMMENT 'NULL = registro corrente (SCD2)',
    is_current      BOOLEAN
) USING iceberg
TBLPROPERTIES ('format-version'='2')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_users (
    user_key        BIGINT  COMMENT 'Surrogate key auto-gerada',
    user_id         BIGINT  COMMENT 'Chave natural do usuario',
    valid_from      DATE    COMMENT 'Data da primeira aparicao (SCD2)',
    valid_to        DATE    COMMENT 'NULL = registro corrente (SCD2)',
    is_current      BOOLEAN
) USING iceberg
TBLPROPERTIES ('format-version'='2')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_sales (
    event_id        STRING,
    date_key        INT,
    product_key     BIGINT,
    user_key        BIGINT,
    quantity        INT,
    unit_price      DOUBLE,
    gross_amount    DOUBLE,
    currency        STRING,
    channel         STRING,
    event_ts        TIMESTAMP,
    event_date      DATE
) USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
""")

print("[bootstrap] namespaces e tabelas criadas.")
for ns in ["bronze", "silver", "gold"]:
    print(f"  {ns}:")
    for row in spark.sql(f"SHOW TABLES IN lakehouse.{ns}").collect():
        print(f"    {row}")
spark.stop()
