"""DAG batch do pipeline `purchase`: Silver -> Quality Gate -> Gold.

O Bronze é alimentado de forma contínua pelo job de streaming (fora desta DAG).
Esta DAG roda a parte batch e materializa o Gold APENAS se o Data Quality Gate
aprovar — o gate é um circuit breaker (task que falha bloqueia o downstream).

Cada task dispara um `spark-submit` no container `spark` via socket do Docker.
"""

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

SUBMIT = "docker exec spark spark-submit /opt/spark/jobs/{job}"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="purchase_batch_pipeline",
    description="Silver -> Data Quality Gate -> Gold (idempotente, backfill-friendly)",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["medallion", "purchase", "gold"],
) as dag:
    silver = BashOperator(
        task_id="build_silver",
        bash_command=SUBMIT.format(job="silver_purchases.py"),
    )

    # Circuit breaker: se sair != 0, o Gold NÃO roda.
    quality_gate = BashOperator(
        task_id="data_quality_gate",
        bash_command=SUBMIT.format(job="quality_gate.py"),
    )

    gold = BashOperator(
        task_id="build_gold",
        bash_command=SUBMIT.format(job="gold_aggregations.py"),
    )

    silver >> quality_gate >> gold
