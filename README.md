# Plataforma Lakehouse de E-commerce em Tempo Real

Plataforma de dados que ingere eventos de e-commerce em tempo real, processa em
**streaming + batch** sobre uma arquitetura **Lakehouse (Medallion / Apache
Iceberg)**, aplica **data quality como circuit breaker** e materializa KPIs de
negócio — tudo executável localmente com Docker Compose.

> **Estado atual:** Fase 0 (fundação) + Fase 1 (**vertical slice do evento
> `purchase`**) implementadas e rodando ponta a ponta. As demais fases estão no
> [roadmap](docs/roadmap.md). Esta é uma fatia *fina e completa* de propósito —
> ver decisão em [docs/roadmap.md](docs/roadmap.md).

## Por que este projeto é diferente

Não é "mais um pipeline". O foco está nas **garantias** que separam um Data
Engineer sênior:

- ✅ **Exactly-once efetivo no sink** — checkpoint + `MERGE` idempotente por
  `event_id`. O simulador injeta duplicatas e provamos que a receita não infla.
- ✅ **Dead Letter Queue** — eventos inválidos são desviados, nunca derrubam o
  stream nem contaminam o Bronze.
- ✅ **Data Quality Gate (circuit breaker)** — o Gold só é publicado se as
  expectativas passarem; senão a DAG falha.
- ✅ **Data contract versionado** — schema do evento validado no producer e em CI.
- ✅ **Decisões documentadas** — 4 [ADRs](docs/adr/) com trade-offs explícitos.
- ✅ **Caminho local → cloud** sem reescrever jobs (Iceberg REST→Glue, MinIO→S3).

## Arquitetura

Ver **[docs/architecture.md](docs/architecture.md)** (diagrama + garantias).

```
Simulador → Kafka → [Spark Structured Streaming] → Bronze (Iceberg)
                                              ↘ DLQ
Bronze → [Spark batch] → Silver → [Data Quality Gate] → Gold → Dashboards
                                        ↑ orquestrado por Airflow
```

## Stack

| Camada | Tecnologia |
|--------|-----------|
| Geração | Python + Faker + JSON Schema |
| Streaming | Apache Kafka (KRaft) |
| Object store | MinIO (S3-compatible) |
| Table format | **Apache Iceberg** (REST catalog) |
| Processamento | Apache Spark 3.5 (Structured Streaming + batch) |
| Orquestração | Apache Airflow |
| Qualidade | Quality gate em Spark (→ Great Expectations no roadmap) |
| BI | Metabase / Grafana |
| IaC / CI | Terraform · GitHub Actions |

## Como rodar (vertical slice)

Pré-requisitos: Docker + Docker Compose. (`make` é opcional — no Windows use os
comandos `docker compose` ou `bash scripts/slice.sh`.)

```bash
# 1. Subir a stack core (Kafka, MinIO, Iceberg REST, Spark, simulador)
cp .env.example .env
docker compose --profile core up -d --build
#    (com make:  make up)

# 2. Criar namespaces e tabelas Iceberg
docker compose exec -T spark spark-submit /opt/spark/jobs/bootstrap_tables.py
#    (make bootstrap)

# 3. Iniciar a ingestão streaming Bronze (deixe rodando em um terminal)
docker compose exec spark spark-submit /opt/spark/jobs/bronze_ingest.py
#    (make stream)   — Ctrl+C após ~1 min para acumular dados

# 4. Rodar o pipeline batch: Silver → Quality Gate → Gold → relatório
docker compose exec -T spark spark-submit /opt/spark/jobs/silver_purchases.py
docker compose exec -T spark spark-submit /opt/spark/jobs/quality_gate.py
docker compose exec -T spark spark-submit /opt/spark/jobs/gold_aggregations.py
docker compose exec -T spark spark-submit /opt/spark/jobs/show_gold.py
#    (make slice)
```

Ou tudo com o helper portável:

```bash
bash scripts/slice.sh up
bash scripts/slice.sh bootstrap
bash scripts/slice.sh stream      # Ctrl+C após ~1 min
bash scripts/slice.sh batch
```

Consoles úteis: MinIO → http://localhost:9001 · Spark UI → http://localhost:4040

### Perfis opcionais
```bash
docker compose --profile orchestration up -d   # Airflow em http://localhost:8081 (admin/admin)
docker compose --profile bi up -d              # Metabase em http://localhost:3000
```

## Testes e qualidade de código

```bash
pip install -r requirements-dev.txt
pytest -q          # testes do simulador + transformação Silver
ruff check .       # lint
```
CI (GitHub Actions): lint + validação de contratos + testes + build de imagem +
`docker compose config`.

## Documentação

- [Arquitetura e garantias](docs/architecture.md)
- [ADRs (decisões)](docs/adr/) — [Iceberg](docs/adr/0001-table-format-iceberg.md) ·
  [Spark Streaming](docs/adr/0002-streaming-engine-spark.md) ·
  [Data contract](docs/adr/0003-data-contract-json-schema.md) ·
  [Medallion + Quality gate](docs/adr/0004-medallion-and-quality-gate.md)
- [Roadmap](docs/roadmap.md) · [Backlog técnico](docs/backlog.md) ·
  [Observabilidade & governança](docs/observability.md)

## Limitações conhecidas (consciente, não acidental)
- Slice cobre **um** evento (`purchase`); replicação dos demais está no roadmap.
- Data contract em JSON Schema (não Avro+Registry ainda) — ver
  [ADR-0003](docs/adr/0003-data-contract-json-schema.md).
- Orquestração local dispara `docker exec` (não usar em produção; lá → EMR/MWAA).
- Terraform é stub validável (`plan`), não aplicado.
