"""DLQ Replay: tenta reprocessar registros da Dead Letter Queue.

Padrão Dead Letter Queue (DLQ) Replay:
  Registros que falharam no parse durante a ingestão Bronze (schema inválido,
  campos obrigatórios ausentes, JSON malformado) são armazenados em
  ``bronze.purchases_dlq`` pelo job bronze_ingest.py em vez de serem descartados.

  Este job lê a DLQ e tenta re-parsear o ``raw_value`` usando o mesmo schema.
  Cenários em que o replay funciona:
    * O schema do produtor foi corrigido upstream e o JSON agora é válido
    * Uma nova versão do schema foi deployada que aceita o formato antigo
    * Erros transientes de encoding foram resolvidos

  Registros que continuam falhando são mantidos na DLQ (não deletados).
  Use ``--purge`` para remover da DLQ os registros replayados com sucesso.

Rodar:
    spark-submit /opt/spark/jobs/dlq_replay.py
    spark-submit /opt/spark/jobs/dlq_replay.py --purge
"""

import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schemas import PURCHASE_EVENT_SCHEMA
from transforms import parsed_columns

logger = logging.getLogger(__name__)

purge = "--purge" in sys.argv

spark = SparkSession.builder.appName("dlq-replay").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

dlq = spark.read.table("lakehouse.bronze.purchases_dlq")
total_dlq = dlq.count()

if total_dlq == 0:
    logger.info("[dlq-replay] DLQ vazia, nada a reprocessar.")
    spark.stop()
    sys.exit(0)

logger.info("[dlq-replay] %d registros na DLQ. Tentando re-parse...", total_dlq)

# ── Re-parse: aplica o mesmo schema do bronze_ingest ─────────────────────
reparsed = dlq.withColumn("parsed", F.from_json("raw_value", PURCHASE_EVENT_SCHEMA))

# Registros que agora parseiam com sucesso (mesmos critérios do bronze_ingest).
now = F.current_timestamp()
select_cols = parsed_columns(PURCHASE_EVENT_SCHEMA)
select_cols.append(now.alias("ingest_ts"))

good = (
    reparsed.filter(
        F.col("parsed").isNotNull()
        & F.col("parsed.event_id").isNotNull()
        & F.col("parsed.product_id").isNotNull()
        & F.col("parsed.price").isNotNull()
    )
    .select(*select_cols)
    .withColumn("event_date", F.to_date("event_ts"))
    .dropDuplicates(["event_id"])
)

good.persist()
replayed_count = good.count()

# Registros que continuam inválidos.
still_bad_count = total_dlq - replayed_count

if replayed_count > 0:
    # MERGE idempotente: se o event_id já existe no Bronze, não reinsere.
    good.createOrReplaceTempView("dlq_replayed")
    spark.sql("""
        MERGE INTO lakehouse.bronze.purchases t
        USING dlq_replayed s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info(
        "[dlq-replay] %d registros replayados para bronze.purchases.", replayed_count
    )

    # ── Purge: remove da DLQ os registros replayados com sucesso ─────────
    if purge:
        # Recria a DLQ apenas com os registros que continuam inválidos.
        still_bad = reparsed.filter(
            F.col("parsed").isNull()
            | F.col("parsed.event_id").isNull()
            | F.col("parsed.product_id").isNull()
            | F.col("parsed.price").isNull()
        ).select("raw_value", "error_reason", "ingest_ts", "ingest_date")

        still_bad.writeTo("lakehouse.bronze.purchases_dlq").overwritePartitions()
        logger.info(
            "[dlq-replay] --purge: %d registros removidos da DLQ.", replayed_count
        )
    else:
        logger.info(
            "[dlq-replay] registros replayados permanecem na DLQ. Use --purge para remover."
        )
else:
    logger.info("[dlq-replay] nenhum registro conseguiu ser re-parseado.")

good.unpersist()

# ── Resumo ───────────────────────────────────────────────────────────────
logger.info("──────── RESUMO DLQ REPLAY ────────")
logger.info("  Total na DLQ         : %d", total_dlq)
logger.info("  Replayados (sucesso) : %d", replayed_count)
logger.info("  Ainda invalidos      : %d", still_bad_count)
if purge and replayed_count > 0:
    logger.info("  Purgados da DLQ      : %d", replayed_count)
logger.info("───────────────────────────────────")

spark.stop()
