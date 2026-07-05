# ADR-0001 — Table format: Apache Iceberg

- **Status:** Aceito
- **Data:** 2026-06-25
- **Decisores:** Engenharia de Dados

## Contexto

"Lakehouse" exige um *table format* transacional sobre o object store (ACID,
schema evolution, time travel). As opções maduras são Apache Iceberg, Delta Lake
e Apache Hudi. O ambiente é local (MinIO/S3) e o objetivo é demonstrar uma
arquitetura cloud-agnóstica e portável.

## Opções consideradas

| Critério | **Iceberg** | Delta Lake | Hudi |
|----------|-------------|-----------|------|
| Multi-engine (Spark/Trino/Flink/DuckDB) | ✅ forte | parcial (melhor no Databricks) | parcial |
| Hidden partitioning | ✅ | ❌ | ❌ |
| Padrão aberto / governança neutra | ✅ (ASF) | ✅ (Linux Foundation) | ✅ (ASF) |
| Ergonomia de `MERGE` no Spark | ✅ | ✅ (melhor) | média |
| Ecossistema de catálogo (REST/JDBC/Glue/Nessie) | ✅ rico | crescente | médio |

## Decisão

Adotar **Apache Iceberg** com **REST catalog** e warehouse no MinIO (S3FileIO).

## Justificativa

- *Engine-agnostic*: o mesmo dado é lido por Spark (escrita), Trino/DuckDB
  (consulta) sem cópia — encaixa na estratégia de desacoplar storage de compute.
- *Hidden partitioning* + evolução de partição sem reescrever queries.
- Caminho de produção claro: trocar o REST catalog local por AWS Glue/Nessie e o
  MinIO por S3 — **sem mudar o código dos jobs**.

## Consequências

- (+) Portabilidade e narrativa cloud-native forte.
- (−) `MERGE`/upsert é um pouco menos ergonômico que no Delta; mitigado com views.
- (−) Exige um serviço de catálogo (REST) a mais na stack local.
