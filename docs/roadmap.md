# Roadmap por fases (MVP → Produção)

Princípio: **fatia vertical primeiro**. Um pipeline fino e completo (`purchase`
ponta-a-ponta) antes de replicar para os outros eventos.

| Fase | Entrega | Status |
|------|---------|--------|
| **0 — Fundação** | Repo, Docker Compose (perfis), Makefile, `.env`, CI, ADRs, diagrama | ✅ feito |
| **1 — Vertical slice `purchase`** | Simulador → Kafka → Bronze (Iceberg) → Silver → Gold → DQ gate → `show_gold` | ✅ feito |
| **2 — Streaming real** | Watermarking + receita por minuto (event-time) + late data + lag do Kafka + carrinhos abandonados (stateful) | ✅ feito |
| **3 — Qualidade & contratos** | Avro schemas (`.avsc`) + Schema Registry no Compose + script de registro; DLQ replay (`dlq_replay.py`) | ✅ feito |
| **4 — Modelagem dimensional** | dbt (`dbt/`); `dim_users`/`dim_products` (SCD2), `dim_date`, `fact_sales` (Spark + dbt) | ✅ feito |
| **5 — Orquestração completa** | DAGs `purchase_batch_pipeline` (batch + dims) + `maintenance_pipeline` (compaction + DLQ + dims) | ✅ feito |
| **6 — Observabilidade** | Prometheus + Grafana + kafka-exporter (perfil `monitoring`); alertas (lag, throughput) | ✅ feito |
| **7 — DataOps/Produção** | Terraform (S3/Glue/IAM/DLQ archive) + `terraform validate` no CI | ✅ feito |
| **8 — Analytics / BI** | Trino + dashboard Streamlit (Executivo/Produto/Operacional) sobre o Gold real | ✅ feito |

## O que resta (P2 / nice-to-have)
- [ ] OpenLineage (Airflow + Spark) → Marquez para lineage ponta a ponta.
- [ ] Secrets via SOPS/Vault (hoje `.env` no `.gitignore`).
- [ ] Replicar pipeline para os demais eventos: `product_views` → `searches` → `logins` → `signups` → `reviews`.
- [ ] GIF/vídeo demo para portfólio.
- [ ] Migrar DQ para Great Expectations/Soda (profiling mais rico).
