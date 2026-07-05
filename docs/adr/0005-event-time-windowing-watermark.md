# ADR-0005 — Event-time windowing com watermark (receita por minuto)

- **Status:** Aceito
- **Data:** 2026-06-25 (Fase 2)

## Contexto

A métrica "receita por minuto" precisa refletir QUANDO a compra aconteceu
(event-time), não quando o pipeline a processou (processing-time). Eventos chegam
fora de ordem e com atraso (mobile offline, retries, rebalance de partição).

## Decisão

Agregar em janelas **tumbling de 1 minuto sobre `event_ts`** com
**watermark de 2 minutos**, materializando em `gold.revenue_per_minute` via
Structured Streaming + `foreachBatch` + `MERGE` por `(window_start, currency)`.

## Justificativa

- **Event-time + watermark** é o padrão correto para métricas temporais: janelas
  ficam estáveis e reproduzíveis independentemente de quando o dado chega.
- **Watermark de 2 min** é o trade-off completude × latência: late data dentro de
  2 min é incorporada; além disso é descartada e o Spark libera o estado da janela
  (evita estado infinito).
- **MERGE por chave de janela** dá idempotência: reprocessar do checkpoint ou
  reprocessar late data atualiza a janela em vez de duplicar.
- O simulador injeta `LATE_EVENT_RATE` (event-time 1–5 min no passado) para que o
  comportamento de watermark seja demonstrável, não apenas afirmado.

## Consequências

- (+) KPI temporal correto e idempotente; estado limitado.
- (−) Janelas só finalizam após o watermark passar → latência de ~2 min para o
  valor final daquele minuto (aceitável para o caso; documentado).
- **Trigger de revisão:** se for exigida latência menor, reduzir o watermark
  (mais janelas reabertas) ou migrar para Flink (ver ADR-0002).
