# Backlog técnico

Priorização: **P0** (bloqueia maturidade) · **P1** (alto impacto) · **P2** (melhoria).

## Confiabilidade & correção
- [x] **P0** Migrar contrato para Avro + Schema Registry (compat BACKWARD) — schemas `.avsc` + Registry no Compose + script de registro.
- [x] **P0** Replay automatizado da DLQ (`dlq_replay.py` + DAG `maintenance_pipeline`).
- [x] **P1** Watermarking + tratamento de *late data* no streaming (event-time).
- [x] **P1** Compaction agendada do Iceberg (`iceberg_compaction.py` — rewrite + expire + orphans).
- [x] **P2** Snapshot expiration / retenção de metadados Iceberg (incluído no job de compaction).

## Modelagem & analytics
- [x] **P0** dbt para Silver→Gold + testes (`unique`, `not_null`, `accepted_values`) — projeto `dbt/`.
- [x] **P1** `dim_products` e `dim_users` como **SCD Type 2** (`valid_from/valid_to`) — `dim_tables.py`.
- [x] **P1** `dim_date` e modelo estrela `fact_sales` — `dim_tables.py` + `dbt/models/facts/`.
- [x] **P2** Trino/DuckDB como engine de consulta para o BI sobre o Gold. *(Trino integrado — dashboard Streamlit lê via Trino)*

## DataOps & observabilidade
- [x] **P0** Prometheus + Grafana: lag do consumer, throughput, freshness, taxa de DQ — perfil `monitoring`.
- [ ] **P1** OpenLineage (Airflow + Spark) → Marquez para lineage ponta a ponta.
- [x] **P1** Alertas (lag crescente — Prometheus alerting rules + Grafana).
- [x] **P2** Terraform real (S3 + Glue + IAM) com `validate` no CI.

## Plataforma
- [x] **P1** Perfis de recurso no Compose (não subir tudo) + limites de memória.
- [ ] **P2** Secrets via SOPS/Vault (hoje `.env` no `.gitignore`).
- [ ] **P2** Replicar pipeline para os demais 7 tipos de evento.

## Dívidas conhecidas (honestidade > polimento)
- O DQ atual é um job Spark próprio; migrar para Great Expectations/Soda dá
  profiling e relatórios mais ricos.
- A orquestração dispara `docker exec` via socket — ok para local, **não** para
  produção (lá: SparkSubmitOperator/EMR).
- OpenLineage (lineage ponta a ponta) é o único item P1 que permanece aberto.
- Dimensões SCD2 são stubs (sem atributos ricos); em produção, enriquecer com catálogo real.
