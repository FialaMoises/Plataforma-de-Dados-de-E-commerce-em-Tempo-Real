# Roadmap por fases (MVP → Produção)

Princípio: **fatia vertical primeiro**. Um pipeline fino e completo (`purchase`
ponta-a-ponta) antes de replicar para os outros eventos.

| Fase | Entrega | Status |
|------|---------|--------|
| **0 — Fundação** | Repo, Docker Compose (perfis), Makefile, `.env`, CI, ADRs, diagrama | ✅ feito |
| **1 — Vertical slice `purchase`** | Simulador → Kafka → Bronze (Iceberg) → Silver → Gold → DQ gate → `show_gold` | ✅ feito |
| **2 — Streaming real** | Watermarking, receita por minuto, carrinhos abandonados, métricas de lag | ⏳ |
| **3 — Qualidade & contratos** | Migração Avro + Schema Registry (compat BACKWARD); replay da DLQ | ⏳ |
| **4 — Modelagem dimensional** | dbt; `dim_users`/`dim_products` (SCD2), `dim_date`; `fact_sales`/`fact_views` | ⏳ |
| **5 — Orquestração completa** | DAGs ingest/silver/gold/dq; backfill parametrizado por data | 🟡 DAG batch pronta |
| **6 — Observabilidade** | Prometheus + Grafana; lag Kafka, throughput, freshness, taxa de DQ; alertas | ⏳ |
| **7 — DataOps/Produção** | Terraform (S3/Glue), CI/CD com build+push de imagens, DQ no PR | 🟡 Terraform stub |
| **8 — Polimento** | Metabase com dashboards de negócio, GIF/vídeo demo, blog post | ⏳ |

## Próximos eventos a replicar (depois do `purchase`)
`product_views` → `searches` → `carts` (add/remove) → `logins` → `signups` →
`reviews`. Cada um: contrato versionado, tópico, Bronze/Silver, e fatos/dimensões.
