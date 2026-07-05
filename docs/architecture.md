# Arquitetura

## Visão geral

```mermaid
flowchart LR
    subgraph gen[Geração]
        SIM[Simulador de eventos\nJSON + Avro contracts]
    end
    subgraph stream[Streaming]
        K[(Kafka\ntopics: purchases, carts\n6 partições, key=user_id)]
        SR[Schema Registry\nAvro BACKWARD compat]
    end
    subgraph lake[Lakehouse - Iceberg sobre MinIO]
        B[(bronze.purchases\nappend + MERGE idempotente)]
        BC[(bronze.carts)]
        DLQ[(bronze.*_dlq\nDead Letter Queues)]
        S[(silver.purchases\nlimpo, tipado, dedup, regras)]
        G1[(gold.daily_revenue)]
        G2[(gold.top_products)]
        G3[(gold.revenue_per_minute\nevent-time + watermark)]
        G4[(gold.abandoned_carts\nstateful streaming)]
        D1[(gold.dim_date / dim_products / dim_users)]
        F1[(gold.fact_sales\nstar schema)]
    end
    subgraph proc[Processamento - Spark]
        SS[Structured Streaming\nbronze_ingest + bronze_carts]
        RPM[Streaming: revenue/min\nevent-time windowed]
        AB[Streaming: abandoned carts\napplyInPandasWithState]
        SB1[Batch: silver]
        DQ{{Data Quality Gate\ncircuit breaker}}
        SB2[Batch: gold + dims]
    end
    subgraph orch[Orquestração]
        AF[Airflow DAGs\nbatch + maintenance]
    end
    subgraph obs[Observabilidade]
        PROM[Prometheus + Grafana\nKafka lag, throughput, alertas]
    end

    SIM -->|produce| K
    K --> SS
    K --> RPM & AB
    SS -->|válido| B & BC
    SS -->|inválido| DLQ
    B --> SB1 --> S
    S --> DQ
    DQ -->|aprovado| SB2
    SB2 --> G1 & G2 & D1 & F1
    RPM --> G3
    AB --> G4
    AF -. orquestra .-> SB1 & DQ & SB2
    G1 & G2 & G3 & G4 --> BI[Dashboards\nStreamlit / Metabase]
    K --> PROM
```

## Camadas e garantias

| Camada | Tabela Iceberg | Responsabilidade | Garantia-chave |
|--------|----------------|------------------|----------------|
| Bronze | `bronze.purchases`, `bronze.carts` | Dados brutos normalizados | **Idempotência** (MERGE por `event_id` + checkpoint) |
| DLQ | `bronze.purchases_dlq`, `bronze.carts_dlq` | Payloads inválidos | Stream não quebra; incidentes mensuráveis |
| Silver | `silver.purchases` | Limpeza, tipagem, dedup, regras de negócio | Determinística e reprocessável |
| Gold (agregações) | `gold.daily_revenue`, `gold.top_products` | KPIs de negócio | Só publicado se passar no **DQ Gate** |
| Gold (streaming) | `gold.revenue_per_minute`, `gold.abandoned_carts` | KPIs tempo-real (event-time) | Watermark + idempotência |
| Gold (dimensional) | `gold.dim_date`, `gold.dim_products`, `gold.dim_users`, `gold.fact_sales` | Star schema para BI | SCD Type 2 + MERGE |

## Por que estas decisões (índice de ADRs)

- [ADR-0001](adr/0001-table-format-iceberg.md) — Iceberg como table format.
- [ADR-0002](adr/0002-streaming-engine-spark.md) — Spark Structured Streaming.
- [ADR-0003](adr/0003-data-contract-json-schema.md) — Data contract (JSON Schema → Avro).
- [ADR-0004](adr/0004-medallion-and-quality-gate.md) — Medallion + Quality Gate.
- [ADR-0005](adr/0005-event-time-windowing-watermark.md) — Event-time windowing com watermark.
- [ADR-0006](adr/0006-stateful-abandoned-carts.md) — Carrinhos abandonados (stateful).
- [ADR-0007](adr/0007-avro-schema-registry.md) — Avro + Schema Registry.
- [ADR-0008](adr/0008-dimensional-modeling.md) — Modelagem dimensional (star schema).

## Garantias de correção demonstráveis

1. **Exactly-once efetivo no sink** — o simulador injeta duplicatas
   (`DUPLICATE_RATE`); `show_gold.py` prova que `linhas == event_id distintos` no
   Bronze. Reprocessar offsets não infla a receita.
2. **DLQ** — o simulador injeta inválidos (`BAD_EVENT_RATE`); eles aparecem em
   `bronze.purchases_dlq`, nunca no Bronze válido.
3. **Quality gate** — `quality_gate.py` retorna != 0 e bloqueia o Gold quando uma
   regra bloqueante falha.

## Caminho para produção (sem reescrever os jobs)

| Local (slice) | Produção (cloud) |
|---------------|------------------|
| MinIO | Amazon S3 |
| Iceberg REST local | AWS Glue Catalog / Nessie |
| Spark no container | EMR / EMR Serverless / Glue |
| JSON + JSON Schema | Avro + Schema Registry (compat BACKWARD) |
| Schema Registry local | Confluent Cloud / MSK Schema Registry |
| Airflow local | MWAA / Astronomer |
| Prometheus + Grafana local | CloudWatch + Grafana Cloud |
| Trino local | Athena / Starburst / Trino on EKS |
| dbt local | dbt Cloud / dbt Core em CI |
