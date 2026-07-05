"""DAG de manutenção: compaction Iceberg + DLQ replay + tabelas dimensionais.

Roda diariamente fora do horário de pico. Garante que o lakehouse se mantenha
saudável (small files compactados, snapshots expirados, DLQ drenada, dimensões
atualizadas).
"""

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

SUBMIT = "docker exec spark spark-submit /opt/spark/jobs/{job}"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="maintenance_pipeline",
    description="Compaction Iceberg + DLQ replay + tabelas dimensionais (diário)",
    schedule="0 4 * * *",  # 04h UTC, fora do pico
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["maintenance", "iceberg", "dlq", "dimensions"],
) as dag:
    compaction = BashOperator(
        task_id="iceberg_compaction",
        bash_command=SUBMIT.format(job="iceberg_compaction.py"),
    )

    dlq_replay = BashOperator(
        task_id="dlq_replay",
        bash_command=SUBMIT.format(job="dlq_replay.py"),
    )

    dim_tables = BashOperator(
        task_id="build_dimensions",
        bash_command=SUBMIT.format(job="dim_tables.py"),
    )

    # Compaction primeiro (otimiza leituras), depois DLQ e dims em paralelo.
    compaction >> [dlq_replay, dim_tables]
