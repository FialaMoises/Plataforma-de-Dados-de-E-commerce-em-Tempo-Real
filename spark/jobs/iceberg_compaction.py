"""Iceberg Compaction: reescreve data files, expira snapshots e remove orphan files.

Streaming com micro-batches (15-20s de trigger) produz centenas de arquivos Parquet
pequenos por partição. Isso degrada a performance de leitura porque cada scan precisa
abrir muitos file handles e o overhead de metadata domina o I/O real.

Este job resolve o problema de small files executando, para cada tabela Iceberg:
  1. rewrite_data_files  -> consolida arquivos pequenos em poucos maiores (target ~256 MB)
  2. expire_snapshots    -> remove snapshots mais antigos que 7 dias, liberando metadata
  3. remove_orphan_files -> apaga data files que não estão referenciados por nenhum snapshot

Deve rodar periodicamente (ex.: diariamente via Airflow) fora do horário de pico.

Rodar:
    spark-submit /opt/spark/jobs/iceberg_compaction.py
"""

import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

TABLES = [
    "lakehouse.bronze.purchases",
    "lakehouse.bronze.carts",
    "lakehouse.silver.purchases",
    "lakehouse.gold.daily_revenue",
    "lakehouse.gold.top_products",
    "lakehouse.gold.revenue_per_minute",
    "lakehouse.gold.abandoned_carts",
]

SNAPSHOT_RETENTION_DAYS = 7

spark = SparkSession.builder.appName("iceberg-compaction").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

cutoff = (
    datetime.utcnow() - timedelta(days=SNAPSHOT_RETENTION_DAYS)
).strftime("%Y-%m-%d %H:%M:%S")

summary: list[tuple[str, str, str, str]] = []

for table in TABLES:
    logger.info("\n[compaction] ── %s ──", table)

    # ── 1. Rewrite data files (compaction) ────────────────────────────────
    try:
        result = spark.sql(
            f"CALL lakehouse.system.rewrite_data_files(table => '{table}')"
        )
        rows = result.collect()
        info = rows[0] if rows else None
        detail = (
            f"rewritten_data_files_count={info['rewritten_data_files_count']}, "
            f"added_data_files_count={info['added_data_files_count']}"
            if info
            else "OK"
        )
        logger.info("  [rewrite_data_files] %s", detail)
        summary.append((table, "rewrite_data_files", "OK", detail))
    except Exception:
        logger.exception("  [rewrite_data_files] ERRO em %s", table)
        summary.append((table, "rewrite_data_files", "ERRO", "ver log"))

    # ── 2. Expire snapshots (libera metadata antigo) ──────────────────────
    try:
        result = spark.sql(
            f"CALL lakehouse.system.expire_snapshots("
            f"table => '{table}', "
            f"older_than => TIMESTAMP '{cutoff}')"
        )
        rows = result.collect()
        info = rows[0] if rows else None
        detail = (
            f"deleted_data_files_count={info['deleted_data_files_count']}, "
            f"deleted_manifest_files_count={info['deleted_manifest_files_count']}, "
            f"deleted_manifest_lists_count={info['deleted_manifest_lists_count']}"
            if info
            else "OK"
        )
        logger.info("  [expire_snapshots] %s", detail)
        summary.append((table, "expire_snapshots", "OK", detail))
    except Exception:
        logger.exception("  [expire_snapshots] ERRO em %s", table)
        summary.append((table, "expire_snapshots", "ERRO", "ver log"))

    # ── 3. Remove orphan files (data files não referenciados) ─────────────
    try:
        result = spark.sql(
            f"CALL lakehouse.system.remove_orphan_files(table => '{table}')"
        )
        rows = result.collect()
        n_orphans = len(rows)
        detail = f"orphan_files_removed={n_orphans}"
        logger.info("  [remove_orphan_files] %s", detail)
        summary.append((table, "remove_orphan_files", "OK", detail))
    except Exception:
        logger.exception("  [remove_orphan_files] ERRO em %s", table)
        summary.append((table, "remove_orphan_files", "ERRO", "ver log"))

# ── Resumo final ─────────────────────────────────────────────────────────
logger.info("\n══════════ RESUMO DA COMPACTACAO ══════════")
logger.info("  Tabelas processadas: %d", len(TABLES))
logger.info(
    "  Snapshot retention : %d dias (cutoff: %s)", SNAPSHOT_RETENTION_DAYS, cutoff
)
ok_count = sum(1 for _, _, status, _ in summary if status == "OK")
err_count = sum(1 for _, _, status, _ in summary if status == "ERRO")
logger.info("  Operacoes OK       : %d", ok_count)
logger.info("  Operacoes com erro : %d", err_count)
for table, op, status, detail in summary:
    flag = "OK" if status == "OK" else "!!"
    logger.info("  [%s] %s.%s: %s", flag, table, op, detail)
logger.info("═══════════════════════════════════════════")

spark.stop()
