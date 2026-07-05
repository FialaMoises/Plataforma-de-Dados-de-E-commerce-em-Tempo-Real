# Atalhos da plataforma. No Windows sem `make`, use os comandos `docker compose`
# equivalentes (ver README) ou rode `bash scripts/slice.sh <alvo>`.

COMPOSE := docker compose
EXEC    := $(COMPOSE) exec -T spark /opt/spark/bin/spark-submit /opt/spark/jobs

# No Git Bash (Windows) rode via `bash scripts/slice.sh <alvo>` — ele já trata a
# conversão de paths do MSYS, que quebra os argumentos /opt/... do spark-submit.

.PHONY: help up down logs ps bootstrap stream stream-carts stream-rpm stream-abandoned silver dq gold show slice batch lag dashboard clean test lint monitoring compaction dlq-replay dims

help:           ## Lista os alvos
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

up:             ## Sobe a stack core (kafka, minio, iceberg, spark, generator)
	cp -n .env.example .env || true
	$(COMPOSE) --profile core up -d --build

down:           ## Derruba tudo (mantém volumes)
	$(COMPOSE) --profile core --profile orchestration --profile bi --profile monitoring down

clean:          ## Derruba tudo e apaga volumes (zera o lakehouse)
	$(COMPOSE) --profile core --profile orchestration --profile bi --profile monitoring down -v

ps:             ## Status dos serviços
	$(COMPOSE) ps

logs:           ## Logs do generator
	$(COMPOSE) logs -f generator

bootstrap:      ## Cria namespaces e tabelas Iceberg
	$(EXEC)/bootstrap_tables.py

stream:         ## Inicia a ingestão streaming Bronze (foreground; Ctrl+C para parar)
	$(COMPOSE) exec spark spark-submit /opt/spark/jobs/bronze_ingest.py

stream-carts:   ## Inicia a ingestão streaming de carrinhos (foreground)
	$(COMPOSE) exec spark spark-submit /opt/spark/jobs/bronze_carts.py

stream-rpm:     ## Inicia o streaming de receita por minuto (foreground)
	$(COMPOSE) exec spark spark-submit /opt/spark/jobs/gold_revenue_per_minute.py

stream-abandoned: ## Inicia detecção de carrinhos abandonados (foreground)
	$(COMPOSE) exec spark spark-submit /opt/spark/jobs/gold_abandoned_carts.py

silver:         ## Bronze -> Silver
	$(EXEC)/silver_purchases.py

dq:             ## Roda o Data Quality Gate sobre a Silver
	$(EXEC)/quality_gate.py

gold:           ## Silver -> Gold
	$(EXEC)/gold_aggregations.py

show:           ## Mostra contagens por camada + KPIs do Gold
	$(EXEC)/show_gold.py

slice: silver dq gold show  ## Pipeline batch completo (Silver -> DQ -> Gold -> show)

batch: slice    ## Alias para slice (pipeline batch completo)

lag:            ## Mostra o lag dos consumer groups Kafka
	bash scripts/kafka_lag.sh

dashboard:      ## Sobe Trino + dashboard Streamlit (perfil bi)
	$(COMPOSE) --profile bi up -d --build
	@echo "Dashboard: http://localhost:8501  |  Trino: http://localhost:8083"

monitoring:     ## Sobe Prometheus + Grafana + Kafka Exporter
	$(COMPOSE) --profile monitoring up -d
	@echo "Prometheus: http://localhost:9090  Grafana: http://localhost:3001 (admin/admin)"

compaction:     ## Roda compaction + snapshot expiration do Iceberg
	$(EXEC)/iceberg_compaction.py

dlq-replay:     ## Tenta reprocessar eventos da DLQ
	$(EXEC)/dlq_replay.py

dims:           ## Gera tabelas dimensionais + fact_sales
	$(EXEC)/dim_tables.py

test:           ## Testes unitários (host)
	pytest -q

lint:           ## Lint + format check
	ruff check . && ruff format --check .
