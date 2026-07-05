# Estratégia de observabilidade e governança

## Os 5 pilares de qualidade de dados (e onde cobrimos)

| Pilar | Métrica | Onde |
|-------|---------|------|
| **Freshness** | event_date mais recente vs agora | `quality_gate.py` (check `freshness`) |
| **Volume** | linhas/dia vs baseline | `show_gold.py` / Grafana dashboard |
| **Schema** | conformidade com contrato v1 | producer (jsonschema) + Schema Registry (Avro) + schema-on-read |
| **Distribution** | % nulos, domínio de `currency`, `price>0` | `quality_gate.py` |
| **Lineage** | origem→destino de cada tabela | planejado: OpenLineage + Marquez |

## Métricas de pipeline (Fase 6 — Prometheus/Grafana)

Stack: **Prometheus** + **Grafana** + **kafka-exporter** (perfil `monitoring`).

```bash
bash scripts/slice.sh monitoring   # sobe Prometheus + Grafana + kafka-exporter
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3001 (admin/admin)
```

Métricas coletadas:
- **Kafka consumer lag** por partição e grupo (kafka-exporter → Prometheus).
- **Throughput** (mensagens/s por tópico).
- **Partições por tópico** (kafka_topic_partitions).
- **Tempo de processamento** por job (via logs do Spark — futuro: JMX exporter).
- **Taxa de DLQ** (= inválidos / total) — proxy de saúde do upstream.
- **Resultado do DQ gate** (pass/fail por execução).

### Alertas (Prometheus rules)
- `KafkaLagHigh`: lag > 10.000 por 5 minutos → warning.
- `KafkaLagCritical`: lag > 50.000 por 2 minutos → critical.
- `KafkaTopicEmpty`: sem mensagens por 10 minutos → warning.

Regras definidas em `monitoring/alerting/rules.yml`.

## Dashboard ao vivo (Streamlit — Fase 8)

O dashboard Streamlit (`dashboard/app.py`) lê o Gold real via Trino e exibe:
- **Aba Executivo**: receita por moeda, ticket médio, tendência diária.
- **Aba Produto**: top produtos por receita + carrinhos abandonados.
- **Aba Operacional**: receita por minuto (event-time), Bronze/Silver/DLQ,
  idempotência, Data Quality ao vivo e lag do pipeline.

## Runbook — gate reprovado
1. Ler a saída do `quality_gate` (qual expectativa falhou e o detalhe).
2. Inspecionar `bronze.purchases_dlq` e a partição `event_date` afetada na Silver.
3. Decidir: corrigir upstream (contrato/producer) **ou** quarentenar a partição.
4. Reprocessar Silver/Gold da janela (idempotente) e reabrir o gate.

## Runbook — lag crescente
1. Verificar se o stream Bronze está rodando (`slice.sh stream`).
2. Checar Grafana → painel "Pipeline Health" → kafka consumer lag.
3. Se o lag é de um consumer group específico, reiniciar o job correspondente.
4. Se persistir, verificar se o Spark está com OOM (limites de memória no Compose).

## Manutenção do Iceberg
- **Compaction**: `slice.sh compaction` ou DAG `maintenance_pipeline` (diária 04h).
  Reescreve data files fragmentados em arquivos maiores (resolve small files).
- **Snapshot expiration**: mesmo job, retém snapshots dos últimos 7 dias.
- **Orphan files**: remove arquivos de dados órfãos (sem referência em metadados).

## Governança
- **Data contracts versionados** em `contracts/` (JSON Schema + Avro).
- **Schema Registry** (Confluent, compat BACKWARD) para evolução segura.
- **Imutabilidade do Bronze** = trilha de auditoria / reprocessamento.
- **Catálogo & lineage** (planejado: DataHub/OpenMetadata ou Marquez).
- **Classificação de dados / PII**: `user_id` é pseudônimo; sem PII real no slice
  (decisão de design). Em produção: mascaramento na Silver + políticas de acesso.
- **Retenção**: Kafka 24h (Bronze é a fonte da verdade, não o broker).
- **DLQ replay**: `slice.sh dlq-replay` ou DAG `maintenance_pipeline`.
  Tenta reprocessar eventos da DLQ que podem ter sido corrigidos upstream.
