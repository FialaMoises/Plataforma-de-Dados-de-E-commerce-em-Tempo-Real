# ADR-0008 — Modelagem dimensional (star schema + SCD Type 2)

- **Status:** Aceito
- **Data:** 2026-06-29 (Fase 4)

## Contexto

O Gold atual tem agregações diretas (daily_revenue, top_products) úteis para
dashboards, mas não há um modelo dimensional formal que permita consultas ad-hoc
com performance (joins eficientes) e rastreabilidade temporal de dimensões.

## Decisão

Implementar um **star schema** no Gold com:

- `gold.dim_date` — dimensão de calendário (dia, mês, trimestre, dia da semana).
- `gold.dim_products` — dimensão de produtos, **SCD Type 2** com `valid_from`,
  `valid_to`, `is_current`. Permite rastrear mudanças no catálogo ao longo do tempo.
- `gold.dim_users` — dimensão de usuários, mesmo padrão SCD Type 2.
- `gold.fact_sales` — fato de vendas, referenciando dimensões por chave natural
  (`date_key`, `product_id`, `user_id`).

O job `dim_tables.py` materializa todas as dimensões e o fato em um único batch
idempotente (MERGE por chave natural).

## Justificativa

- **Star schema** é o padrão para data warehouses analíticos — familiar para
  analistas de BI e otimizado para ferramentas como Metabase, Looker, Power BI.
- **SCD Type 2** preserva o histórico de mudanças nas dimensões, essencial para
  análises temporais corretas ("qual era o preço médio do produto X no Q1?").
- **dbt** (`dbt/`) complementa com testes de integridade e documentação gerada.

## Consequências

- (+) Consultas ad-hoc eficientes sobre o Gold (Trino/DuckDB).
- (+) Historicidade de dimensões para análises temporais.
- (+) dbt gera documentação e testes automatizados.
- (−) Dimensões SCD2 iniciais são stubs (sem atributos ricos de produto/usuário,
  pois o slice gera IDs sintéticos). Em produção, enriquecer com catálogo real.
