# ADR-0002 — Engine de streaming: Spark Structured Streaming

- **Status:** Aceito
- **Data:** 2026-06-25

## Contexto

A ingestão Bronze precisa consumir Kafka continuamente com garantias de
entrega. Candidatos: Spark Structured Streaming, Apache Flink, Kafka Streams.

## Decisão

Usar **Spark Structured Streaming** para Bronze (e Spark batch para Silver/Gold),
com **micro-batch + checkpointing** e **MERGE idempotente** no destino Iceberg.

## Justificativa

- Um único engine (Spark) cobre streaming **e** batch → menos superfície
  operacional e curva de aprendizado no portfólio.
- `foreachBatch` + `MERGE INTO ... ON event_id` dá **exactly-once efetivo no
  sink** (o checkpoint garante reprocesso seguro; o MERGE deduplica).
- Flink seria superior em latência sub-segundo e estado complexo, mas é
  *overkill* para o SLA deste caso (KPIs por minuto, não por milissegundo).

## Consequências

- (+) Stack mais simples; mesmo modelo mental para stream e batch.
- (−) Latência de micro-batch (segundos), não event-at-a-time.
- **Trigger de revisão:** se surgir requisito de latência < 1s ou *stateful*
  pesado (sessionization complexa), reavaliar Flink — ver roadmap.
