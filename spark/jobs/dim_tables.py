"""Dimensional Modeling: cria/atualiza dimensoes e tabela fato no Gold.

Modelagem dimensional (star schema) sobre os dados da Silver para viabilizar
consultas analiticas eficientes e semanticamente claras. A escolha pelo star
schema se justifica porque:
  * Facilita joins previsíveis e performáticos (fato -> dimensao via surrogate key)
  * Permite slicing/dicing por qualquer dimensao sem reprocessar agregações
  * Suporta SCD Type 2 (dimensoes que mudam ao longo do tempo)
  * É o padrão mais compatível com ferramentas de BI (Superset, Metabase, etc.)

Tabelas produzidas:
  gold.dim_date      -> dimensao de data (dia, mes, ano, trimestre, dia da semana)
  gold.dim_products  -> dimensao de produtos (SCD Type 2, por enquanto só product_id)
  gold.dim_users     -> dimensao de usuarios (SCD Type 2, por enquanto só user_id)
  gold.fact_sales    -> tabela fato com FK para as dimensoes

Idempotente via MERGE por chave natural. Pode ser reexecutado sem duplicar dados.

Rodar:
    spark-submit /opt/spark/jobs/dim_tables.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("dim-tables").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Bootstrap: cria tabelas dimensionais se nao existem ────────────────────
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

silver = spark.read.table("lakehouse.silver.purchases")

# ══════════════════════════════════════════════════════════════════════════════
# 1. dim_date: gera todas as datas entre min e max event_date da Silver
# ══════════════════════════════════════════════════════════════════════════════
date_range = silver.agg(
    F.min("event_date").alias("min_date"),
    F.max("event_date").alias("max_date"),
).collect()[0]

if date_range["min_date"] is not None:
    dates = (
        spark.sql(f"""
            SELECT explode(sequence(
                DATE '{date_range["min_date"]}',
                DATE '{date_range["max_date"]}',
                INTERVAL 1 DAY
            )) AS full_date
        """)
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn(
            "is_weekend",
            F.dayofweek("full_date").isin(1, 7),  # 1=domingo, 7=sabado
        )
    )

    dates.createOrReplaceTempView("dim_date_updates")
    spark.sql("""
        MERGE INTO lakehouse.gold.dim_date t
        USING dim_date_updates s
        ON t.date_key = s.date_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    dim_date_count = spark.read.table("lakehouse.gold.dim_date").count()
    print(f"[dim] dim_date atualizada: {dim_date_count} datas.")
else:
    print("[dim] Silver vazia, dim_date nao atualizada.")

# ══════════════════════════════════════════════════════════════════════════════
# 2. dim_products: SCD Type 2 (por enquanto apenas product_id)
# ══════════════════════════════════════════════════════════════════════════════
new_products = (
    silver.select("product_id")
    .distinct()
    .join(
        spark.read.table("lakehouse.gold.dim_products")
        .filter(F.col("is_current"))
        .select("product_id"),
        on="product_id",
        how="left_anti",
    )
    .withColumn(
        "product_key",
        F.monotonically_increasing_id()
        + F.lit(spark.read.table("lakehouse.gold.dim_products").count()),
    )
    .withColumn("valid_from", F.current_date())
    .withColumn("valid_to", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit(True))
)

new_products_count = new_products.count()
if new_products_count > 0:
    new_products.writeTo("lakehouse.gold.dim_products").append()
    print(f"[dim] dim_products: {new_products_count} novos produtos adicionados.")
else:
    print("[dim] dim_products: nenhum produto novo encontrado.")

total_products = spark.read.table("lakehouse.gold.dim_products").count()
print(f"[dim] dim_products total: {total_products} registros.")

# ══════════════════════════════════════════════════════════════════════════════
# 3. dim_users: SCD Type 2 (por enquanto apenas user_id)
# ══════════════════════════════════════════════════════════════════════════════
new_users = (
    silver.select("user_id")
    .distinct()
    .join(
        spark.read.table("lakehouse.gold.dim_users").filter(F.col("is_current")).select("user_id"),
        on="user_id",
        how="left_anti",
    )
    .withColumn(
        "user_key",
        F.monotonically_increasing_id()
        + F.lit(spark.read.table("lakehouse.gold.dim_users").count()),
    )
    .withColumn("valid_from", F.current_date())
    .withColumn("valid_to", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit(True))
)

new_users_count = new_users.count()
if new_users_count > 0:
    new_users.writeTo("lakehouse.gold.dim_users").append()
    print(f"[dim] dim_users: {new_users_count} novos usuarios adicionados.")
else:
    print("[dim] dim_users: nenhum usuario novo encontrado.")

total_users = spark.read.table("lakehouse.gold.dim_users").count()
print(f"[dim] dim_users total: {total_users} registros.")

# ══════════════════════════════════════════════════════════════════════════════
# 4. fact_sales: tabela fato com FK para dimensoes
# ══════════════════════════════════════════════════════════════════════════════
dim_products = (
    spark.read.table("lakehouse.gold.dim_products")
    .filter(F.col("is_current"))
    .select(
        F.col("product_key"),
        F.col("product_id").alias("dim_product_id"),
    )
)

dim_users = (
    spark.read.table("lakehouse.gold.dim_users")
    .filter(F.col("is_current"))
    .select(
        F.col("user_key"),
        F.col("user_id").alias("dim_user_id"),
    )
)

fact = (
    silver.join(dim_products, silver["product_id"] == dim_products["dim_product_id"], "left")
    .join(dim_users, silver["user_id"] == dim_users["dim_user_id"], "left")
    .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int"))
    .select(
        "event_id",
        "date_key",
        "product_key",
        "user_key",
        "quantity",
        "unit_price",
        "gross_amount",
        "currency",
        "channel",
        "event_ts",
        "event_date",
    )
)

fact.createOrReplaceTempView("fact_sales_updates")
spark.sql("""
    MERGE INTO lakehouse.gold.fact_sales t
    USING fact_sales_updates s
    ON t.event_id = s.event_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

fact_count = spark.read.table("lakehouse.gold.fact_sales").count()
print(f"[dim] fact_sales atualizada: {fact_count} linhas.")

# ── Resumo final ───────────────────────────────────────────────────────────
print("\n──────── RESUMO DIMENSIONAL MODELING ────────")
print(f"  gold.dim_date     : {spark.read.table('lakehouse.gold.dim_date').count()} datas")
print(f"  gold.dim_products : {total_products} produtos")
print(f"  gold.dim_users    : {total_users} usuarios")
print(f"  gold.fact_sales   : {fact_count} fatos")
print("─────────────────────────────────────────────")

spark.stop()
