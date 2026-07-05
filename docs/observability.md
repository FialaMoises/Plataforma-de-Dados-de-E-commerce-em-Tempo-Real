# Estratégia de observabilidade e governança

## Os 5 pilares de qualidade de dados (e onde cobrimos)

| Pilar | Métrica | Onde |
|-------|---------|------|
| **Freshness** | event_date mais recente vs agora | `quality_gate.py` (check `freshness`) |
| **Volume** | linhas/dia vs baseline | `show_gold.py` / futuro: alerta |
| **Schema** | conformidade com contrato v1 | producer (jsonschema) + schema-on-read |
| **Distribution** | % nulos, domínio de `currency`, `price>0` | `quality_gate.py` |
| **Lineage** | origem→destino de cada tabela | planejado: OpenLineage + Marquez |

## Métricas de pipeline (Fase 6 — Prometheus/Grafana)
- **Kafka consumer lag** por partição (saúde do streaming).
- **Throughput** (eventos/s ingeridos; linhas/batch).
- **Tempo de processamento** por job (silver/gold).
- **Taxa de DLQ** (= inválidos / total) — proxy de saúde do upstream.
- **Resultado do DQ gate** (pass/fail por execução).

## Runbook — gate reprovado
1. Ler a saída do `quality_gate` (qual expectativa falhou e o detalhe).
2. Inspecionar `bronze.purchases_dlq` e a partição `event_date` afetada na Silver.
3. Decidir: corrigir upstream (contrato/producer) **ou** quarentenar a partição.
4. Reprocessar Silver/Gold da janela (idempotente) e reabrir o gate.

## Governança
- **Data contracts versionados** em `contracts/` (fonte da verdade do schema).
- **Imutabilidade do Bronze** = trilha de auditoria / reprocessamento.
- **Catálogo & lineage** (planejado: DataHub/OpenMetadata ou Marquez).
- **Classificação de dados / PII**: `user_id` é pseudônimo; sem PII real no slice
  (decisão de design). Em produção: mascaramento na Silver + políticas de acesso.
- **Retenção**: Kafka 24h (Bronze é a fonte da verdade, não o broker).
