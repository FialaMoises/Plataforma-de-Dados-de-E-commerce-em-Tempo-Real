# Backlog técnico

Priorização: **P0** (bloqueia maturidade) · **P1** (alto impacto) · **P2** (melhoria).

## Confiabilidade & correção
- [ ] **P0** Migrar contrato para Avro + Schema Registry (compat BACKWARD) — fecha o gap do ADR-0003.
- [ ] **P0** Replay automatizado da DLQ (consumidor dedicado + curadoria + alerta).
- [ ] **P1** Watermarking + tratamento de *late data* no streaming (event-time).
- [ ] **P1** Compaction agendada do Iceberg (`rewrite_data_files`) — resolve *small files*.
- [ ] **P2** Snapshot expiration / retenção de metadados Iceberg.

## Modelagem & analytics
- [ ] **P0** dbt para Silver→Gold + testes (`unique`, `not_null`, `accepted_values`).
- [ ] **P1** `dim_products` e `dim_users` como **SCD Type 2** (`valid_from/valid_to`).
- [ ] **P1** `dim_date` e modelo estrela `fact_sales`.
- [ ] **P2** Trino/DuckDB como engine de consulta para o BI sobre o Gold.

## DataOps & observabilidade
- [ ] **P0** Prometheus + Grafana: lag do consumer, throughput, freshness, taxa de DQ.
- [ ] **P1** OpenLineage (Airflow + Spark) → Marquez para lineage ponta a ponta.
- [ ] **P1** Alertas (freshness estourada, gate reprovado, lag crescente).
- [ ] **P2** Terraform real (S3 + Glue + IAM) com `plan` no CI.

## Plataforma
- [ ] **P1** Perfis de recurso no Compose (não subir tudo) + limites de memória.
- [ ] **P2** Secrets via SOPS/Vault (hoje `.env` no `.gitignore`).
- [ ] **P2** Replicar pipeline para os demais 7 tipos de evento.

## Dívidas conhecidas (honestidade > polimento)
- O DQ atual é um job Spark próprio; migrar para Great Expectations/Soda dá
  profiling e relatórios mais ricos.
- A orquestração dispara `docker exec` via socket — ok para local, **não** para
  produção (lá: SparkSubmitOperator/EMR).
