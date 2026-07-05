"""Data Quality Gate sobre a camada Silver.

Funciona como CIRCUIT BREAKER: se qualquer expectativa BLOQUEANTE falhar, o job
sai com código != 0 e a DAG do Airflow NÃO publica o Gold. Isso impede que dado
ruim chegue ao dashboard executivo.

Dimensões cobertas (os "pilares" de observabilidade de dados):
  schema/integridade  -> chaves não nulas, event_id único
  negócio             -> unit_price > 0, quantity > 0, gross_amount coerente
  completeness        -> taxa de preenchimento mínima de currency
  freshness           -> existe dado recente (event_date >= hoje-1)

A lógica dos checks está em ``transforms.run_quality_checks`` para ser
reutilizável pelos testes sem replicação de código.
"""

import logging
import sys

from pyspark.sql import SparkSession
from transforms import run_quality_checks

logger = logging.getLogger(__name__)


def main() -> None:
    spark = SparkSession.builder.appName("quality-gate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.table("lakehouse.silver.purchases")
    checks, total = run_quality_checks(df)

    logger.info("──────── DATA QUALITY GATE (Silver) ────────")
    logger.info("Total de linhas avaliadas: %d\n", total)

    failed_blocking = 0
    for name, (ok, detail, blocking) in checks.items():
        status = "PASS" if ok else ("FAIL" if blocking else "WARN")
        logger.info("  [%4s] %-26s %s", status, name, detail)
        if not ok and blocking:
            failed_blocking += 1

    logger.info("────────────────────────────────────────────")
    if failed_blocking:
        logger.error(
            "GATE REPROVADO: %d expectativa(s) bloqueante(s) falharam.",
            failed_blocking,
        )
        spark.stop()
        sys.exit(1)

    logger.info("GATE APROVADO.")
    spark.stop()
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
