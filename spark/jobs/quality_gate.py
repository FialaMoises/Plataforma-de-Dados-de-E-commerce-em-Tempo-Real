"""Data Quality Gate sobre a camada Silver.

Funciona como CIRCUIT BREAKER: se qualquer expectativa BLOQUEANTE falhar, o job
sai com código != 0 e a DAG do Airflow NÃO publica o Gold. Isso impede que dado
ruim chegue ao dashboard executivo.

Dimensões cobertas (os "pilares" de observabilidade de dados):
  schema/integridade  -> chaves não nulas, event_id único
  negócio             -> unit_price > 0, quantity > 0, gross_amount coerente
  completeness        -> taxa de preenchimento mínima de currency
  freshness           -> existe dado recente (event_date >= hoje-1)
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("quality-gate").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.table("lakehouse.silver.purchases")
total = df.count()

results = []  # (nome, ok, detalhe, bloqueante)


def check(name: str, ok: bool, detail: str, blocking: bool = True) -> None:
    results.append((name, bool(ok), detail, blocking))


if total == 0:
    check("non_empty", False, "Silver vazia", blocking=True)
else:
    null_keys = df.filter(
        F.col("event_id").isNull() | F.col("user_id").isNull() | F.col("product_id").isNull()
    ).count()
    check("no_null_keys", null_keys == 0, f"{null_keys} linhas com chave nula")

    distinct_ids = df.select("event_id").distinct().count()
    check(
        "unique_event_id", distinct_ids == total, f"{total - distinct_ids} duplicatas de event_id"
    )

    bad_price = df.filter(F.col("unit_price") <= 0).count()
    check("price_positive", bad_price == 0, f"{bad_price} linhas com unit_price <= 0")

    bad_qty = df.filter(F.col("quantity") <= 0).count()
    check("quantity_positive", bad_qty == 0, f"{bad_qty} linhas com quantity <= 0")

    bad_amount = df.filter(
        F.abs(F.col("gross_amount") - F.col("quantity") * F.col("unit_price")) > 0.01
    ).count()
    check(
        "gross_amount_consistent",
        bad_amount == 0,
        f"{bad_amount} linhas com gross_amount incoerente",
    )

    filled = df.filter(F.col("currency").isNotNull()).count()
    rate = filled / total
    check("currency_completeness", rate >= 0.99, f"completeness={rate:.4f} (min 0.99)")

    fresh = df.filter(F.col("event_date") >= F.date_sub(F.current_date(), 1)).count()
    check("freshness", fresh > 0, "sem dados nas últimas 24h", blocking=False)

print("\n──────── DATA QUALITY GATE (Silver) ────────")
print(f"Total de linhas avaliadas: {total}\n")
failed_blocking = 0
for name, ok, detail, blocking in results:
    status = "PASS" if ok else ("FAIL" if blocking else "WARN")
    print(f"  [{status:4}] {name:26} {detail}")
    if not ok and blocking:
        failed_blocking += 1

print("────────────────────────────────────────────")
if failed_blocking:
    print(f"GATE REPROVADO: {failed_blocking} expectativa(s) bloqueante(s) falharam.")
    sys.exit(1)
print("GATE APROVADO.")
spark.stop()
sys.exit(0)
