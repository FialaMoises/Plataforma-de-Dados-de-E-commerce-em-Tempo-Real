#!/usr/bin/env bash
# Helper portável (funciona no Git Bash do Windows, sem `make`).
# Uso: bash scripts/slice.sh <up|bootstrap|stream|batch|show|down|clean>
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose"
SUBMIT="$COMPOSE exec -T spark spark-submit /opt/spark/jobs"

case "${1:-help}" in
  up)
    [ -f .env ] || cp .env.example .env
    $COMPOSE --profile core up -d --build
    echo "Aguardando serviços ficarem saudáveis..."
    sleep 25
    echo "Pronto. Rode: bash scripts/slice.sh bootstrap"
    ;;
  bootstrap) $SUBMIT/bootstrap_tables.py ;;
  stream)    $COMPOSE exec spark spark-submit /opt/spark/jobs/bronze_ingest.py ;;
  batch)
    $SUBMIT/silver_purchases.py
    $SUBMIT/quality_gate.py
    $SUBMIT/gold_aggregations.py
    $SUBMIT/show_gold.py
    ;;
  show)  $SUBMIT/show_gold.py ;;
  down)  $COMPOSE --profile core --profile orchestration --profile bi down ;;
  clean) $COMPOSE --profile core --profile orchestration --profile bi down -v ;;
  *) echo "alvos: up | bootstrap | stream | batch | show | down | clean" ;;
esac
