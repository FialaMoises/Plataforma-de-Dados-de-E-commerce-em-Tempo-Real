# Plataforma Lakehouse de E-commerce em Tempo Real

Plataforma de dados que ingere eventos de e-commerce em tempo real, processa em
**streaming + batch** sobre uma arquitetura **Lakehouse (Medallion / Apache
Iceberg)**, aplica **data quality como circuit breaker** e materializa KPIs de
negócio — tudo executável localmente com Docker Compose.

> **Estado atual:** Todas as 8 fases do roadmap estão implementadas e rodando
> ponta a ponta. Ver detalhes em [docs/roadmap.md](docs/roadmap.md).

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
- ✅ **Decisões documentadas** — 8 [ADRs](docs/adr/) com trade-offs explícitos.
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
| Schema Registry | Confluent Schema Registry (Avro, BACKWARD compat) |
| Qualidade | Quality gate em Spark (→ Great Expectations no roadmap) |
| Modelagem | dbt (Trino adapter) — staging, marts, dimensions, facts |
| Observabilidade | Prometheus + Grafana + kafka-exporter |
| BI / Dashboard | Streamlit + Trino |
| IaC / CI | Terraform · GitHub Actions |

## Como rodar (vertical slice)

Pré-requisitos: Docker + Docker Compose. Forma recomendada (e à prova de Windows)
é o helper `scripts/slice.sh` — no Git Bash o MSYS converte caminhos `/opt/...`
e quebra os argumentos do `spark-submit`; o script já trata isso.

```bash
bash scripts/slice.sh up          # sobe a stack core (Kafka, MinIO, Iceberg, Spark, simulador)
bash scripts/slice.sh bootstrap   # cria namespaces e tabelas Iceberg
bash scripts/slice.sh stream      # ingestão streaming Bronze — Ctrl+C após ~1 min
bash scripts/slice.sh batch       # Silver → Quality Gate → Gold → relatório
```

Equivalente com `make` (Linux/Mac): `make up && make bootstrap && make stream` e `make slice`.

Forma manual (em Linux/Mac, ou no Windows com `MSYS_NO_PATHCONV=1` exportado):

```bash
docker compose --profile core up -d --build
docker compose exec -T spark /opt/spark/bin/spark-submit /opt/spark/jobs/bootstrap_tables.py
docker compose exec    spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze_ingest.py   # Ctrl+C após ~1 min
docker compose exec -T spark /opt/spark/bin/spark-submit /opt/spark/jobs/silver_purchases.py
docker compose exec -T spark /opt/spark/bin/spark-submit /opt/spark/jobs/quality_gate.py
docker compose exec -T spark /opt/spark/bin/spark-submit /opt/spark/jobs/gold_aggregations.py
docker compose exec -T spark /opt/spark/bin/spark-submit /opt/spark/jobs/show_gold.py
```

Consoles úteis: MinIO → http://localhost:9001 · Spark master UI → http://localhost:8090
· Spark application UI (durante um job) → http://localhost:4040
· Schema Registry → http://localhost:8085 · Prometheus → http://localhost:9090
· Grafana → http://localhost:3001

> **Validado E2E** (2026-06-25): Bronze 123.706 linhas / 123.706 event_id distintos
> (idempotência OK), 759 inválidos na DLQ, Quality Gate 7/7 PASS, Gold com receita
> e ticket médio por moeda + top 20 produtos.

### Dashboard de visualização (Camada 8 — Analytics)

Front-end em Streamlit lendo o Gold real via **Trino** (engine SQL sobre Iceberg),
com 3 visões + saúde do pipeline ao vivo:

```bash
bash scripts/slice.sh dashboard     # sobe Trino + dashboard (perfil bi)
# requer o perfil `core` já rodando
```
- **Dashboard** → http://localhost:8501
  - 📈 **Executivo**: receita por moeda, ticket médio, tendência diária
  - 📦 **Produto**: produtos mais vendidos (por receita)
  - ⚙️ **Operacional**: receita por minuto (event-time), Bronze/Silver/DLQ,
    idempotência, Data Quality ao vivo e **lag do pipeline** (tópico Kafka → Bronze)
- **Trino** (SQL ad-hoc) → http://localhost:8083

### Perfis opcionais
```bash
docker compose --profile orchestration up -d   # Airflow em http://localhost:8081 (admin/admin)
docker compose --profile metabase up -d        # Metabase (BI clássico) em http://localhost:3000
docker compose --profile monitoring up -d      # Prometheus http://localhost:9090 + Grafana http://localhost:3001 (admin/admin)
```

### Manutenção do Lakehouse

```bash
bash scripts/slice.sh compaction      # compaction Iceberg + snapshot expiration + orphan cleanup
bash scripts/slice.sh dlq-replay      # tenta reprocessar eventos da DLQ
bash scripts/slice.sh dims            # gera tabelas dimensionais (dim_date, dim_products, dim_users, fact_sales)
bash scripts/slice.sh monitoring      # sobe Prometheus + Grafana
```

### Modelagem dimensional (dbt)

O diretório `dbt/` contém o projeto dbt com adapter Trino para modelagem
dimensional sobre o lakehouse Iceberg:

- **Staging**: `stg_purchases` — limpeza e tipagem dos dados Bronze/Silver.
- **Marts**: `daily_revenue`, `top_products` — KPIs agregados para o dashboard.
- **Dimensions**: `dim_date`, `dim_products` (SCD2), `dim_users` (SCD2).
- **Facts**: `fact_sales` — tabela fato central do star schema.
- **Testes**: `unique`, `not_null`, `accepted_values`, `relationships`.

## Testes e qualidade de código

```bash
pip install -r requirements-dev.txt
pytest -q          # 5 suítes de teste (simulador, transforms, quality gate, agregações Gold, contratos) — 45 test cases
ruff check .       # lint
```
CI (GitHub Actions): lint + validação de contratos + testes + build de imagem +
`docker compose config`.

## Documentação

- [Arquitetura e garantias](docs/architecture.md)
- [ADRs (decisões)](docs/adr/) — [Iceberg](docs/adr/0001-table-format-iceberg.md) ·
  [Spark Streaming](docs/adr/0002-streaming-engine-spark.md) ·
  [Data contract](docs/adr/0003-data-contract-json-schema.md) ·
  [Medallion + Quality gate](docs/adr/0004-medallion-and-quality-gate.md) ·
  [Event-time windowing](docs/adr/0005-event-time-windowing-watermark.md) ·
  [Stateful abandoned carts](docs/adr/0006-stateful-abandoned-carts.md) ·
  [Avro + Schema Registry](docs/adr/0007-avro-schema-registry.md) ·
  [Modelagem dimensional](docs/adr/0008-dimensional-modeling.md)
- [Roadmap](docs/roadmap.md) · [Backlog técnico](docs/backlog.md) ·
  [Observabilidade & governança](docs/observability.md)

## Limitações conhecidas (consciente, não acidental)
- Slice cobre **um** evento (`purchase`); replicação dos demais está no roadmap.
- Orquestração local dispara `docker exec` (não usar em produção; lá → EMR/MWAA).
- Schema Registry e Avro schemas estão configurados; a serialização no producer será migrada de JSON para Avro na próxima iteração.
