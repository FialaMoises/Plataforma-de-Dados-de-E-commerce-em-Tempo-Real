# ADR-0004 — Medallion + Data Quality como circuit breaker

- **Status:** Aceito
- **Data:** 2026-06-25

## Contexto

Precisamos impedir que dado ruim chegue ao dashboard executivo, e organizar as
transformações de forma auditável.

## Decisão

1. **Arquitetura Medallion** (Bronze imutável / Silver limpo / Gold agregado),
   cada camada como namespace Iceberg.
2. **Data Quality Gate** entre Silver e Gold: um job que executa expectativas
   (integridade, regras de negócio, completeness, freshness) e **sai com código
   != 0** se uma expectativa *bloqueante* falhar. Na DAG do Airflow, isso impede a
   task `build_gold` de rodar — é um **circuit breaker**.
3. **Dead Letter Queue** (`bronze.purchases_dlq`): eventos que falham no
   parse/contrato são desviados, nunca derrubam o stream nem contaminam o Bronze.

## Justificativa

- Falhar cedo e visível é melhor que publicar KPI errado silenciosamente.
- Bronze imutável + `event_id` permite reprocessar Silver/Gold a qualquer momento
  (idempotência) sem perder o histórico bruto.

## Consequências

- (+) Garantia de que o Gold só existe se passou no DQ.
- (+) Incidentes de qualidade ficam isolados (DLQ) e mensuráveis.
- (−) Um gate reprovado bloqueia o pipeline → exige runbook de resposta
  (ver `docs/observability.md`).
