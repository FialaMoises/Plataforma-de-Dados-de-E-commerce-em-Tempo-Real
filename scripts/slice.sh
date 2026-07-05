#!/usr/bin/env bash
# Helper portável (funciona no Git Bash do Windows, sem `make`).
# Uso: bash scripts/slice.sh <up|bootstrap|stream|batch|show|down|clean>
set -euo pipefail
cd "$(dirname "$0")/.."

# No Git Bash (Windows), o MSYS converte caminhos /opt/... para C:\...\Git\opt\...
# e quebra os argumentos do spark-submit. Desabilita essa conversão.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

COMPOSE="docker compose"
SUBMIT="$COMPOSE exec -T spark /opt/spark/bin/spark-submit /opt/spark/jobs"

case "${1:-help}" in
  up)
    [ -f .env ] || cp .env.example .env
    $COMPOSE --profile core up -d --build
    echo "Aguardando serviços ficarem saudáveis (~25s)..."
    sleep 25
    echo "Pronto. Rode: bash scripts/slice.sh bootstrap"
    ;;
  bootstrap)  $SUBMIT/bootstrap_tables.py ;;
  stream)           $COMPOSE exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze_ingest.py ;;
  stream-carts)     $COMPOSE exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze_carts.py ;;
  stream-rpm)       $COMPOSE exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/gold_revenue_per_minute.py ;;
  stream-abandoned) $COMPOSE exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/gold_abandoned_carts.py ;;
  lag)        bash scripts/kafka_lag.sh ;;
  batch)
    $SUBMIT/silver_purchases.py
    $SUBMIT/quality_gate.py
    $SUBMIT/gold_aggregations.py
    $SUBMIT/show_gold.py
    ;;
  show)  $SUBMIT/show_gold.py ;;
  dashboard)
    $COMPOSE --profile bi up -d --build
    echo "Dashboard: http://localhost:8501  |  Trino: http://localhost:8083"
    ;;
  monitoring)
    $COMPOSE --profile monitoring up -d
    echo "Prometheus: http://localhost:9090  Grafana: http://localhost:3001 (admin/admin)"
    ;;
  compaction) $SUBMIT/iceberg_compaction.py ;;
  dlq-replay) $SUBMIT/dlq_replay.py ;;
  dims) $SUBMIT/dim_tables.py ;;
  down)  $COMPOSE --profile core --profile orchestration --profile bi --profile monitoring down ;;
  clean) $COMPOSE --profile core --profile orchestration --profile bi --profile monitoring down -v ;;
  *) echo "alvos: up | bootstrap | stream | stream-carts | stream-rpm | stream-abandoned | batch | show | lag | dashboard | monitoring | compaction | dlq-replay | dims | down | clean" ;;
esac
