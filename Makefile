# Atalhos da plataforma. No Windows sem `make`, use os comandos `docker compose`
# equivalentes (ver README) ou rode `bash scripts/slice.sh <alvo>`.

COMPOSE := docker compose
EXEC    := $(COMPOSE) exec -T spark spark-submit /opt/spark/jobs

.PHONY: help up down logs ps bootstrap stream silver dq gold show slice clean test lint

help:           ## Lista os alvos
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

up:             ## Sobe a stack core (kafka, minio, iceberg, spark, generator)
	cp -n .env.example .env || true
	$(COMPOSE) --profile core up -d --build

down:           ## Derruba tudo (mantém volumes)
	$(COMPOSE) --profile core --profile orchestration --profile bi down

clean:          ## Derruba tudo e apaga volumes (zera o lakehouse)
	$(COMPOSE) --profile core --profile orchestration --profile bi down -v

ps:             ## Status dos serviços
	$(COMPOSE) ps

logs:           ## Logs do generator
	$(COMPOSE) logs -f generator

bootstrap:      ## Cria namespaces e tabelas Iceberg
	$(EXEC)/bootstrap_tables.py

stream:         ## Inicia a ingestão streaming Bronze (foreground; Ctrl+C para parar)
	$(COMPOSE) exec spark spark-submit /opt/spark/jobs/bronze_ingest.py

silver:         ## Bronze -> Silver
	$(EXEC)/silver_purchases.py

dq:             ## Roda o Data Quality Gate sobre a Silver
	$(EXEC)/quality_gate.py

gold:           ## Silver -> Gold
	$(EXEC)/gold_aggregations.py

show:           ## Mostra contagens por camada + KPIs do Gold
	$(EXEC)/show_gold.py

slice: silver dq gold show  ## Pipeline batch completo (Silver -> DQ -> Gold -> show)

test:           ## Testes unitários (host)
	pytest -q

lint:           ## Lint + format check
	ruff check . && ruff format --check .
