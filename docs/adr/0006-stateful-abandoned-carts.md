# ADR-0006 — Carrinhos abandonados: streaming stateful (applyInPandasWithState)

- **Status:** Aceito
- **Data:** 2026-06-25 (fecha a Fase 2)

## Contexto

"Carrinho abandonado" não é um evento — é a **ausência** de uma compra após
atividade de carrinho dentro de uma janela. Detectar isso exige estado por
sessão e um disparo por timeout, não uma agregação simples.

## Decisão

Usar **`applyInPandasWithState`** (arbitrary stateful processing do Spark
Structured Streaming) com **timeout de EVENT-TIME**, keyed por **`cart_id`**:

- lê apenas o tópico `carts` (`add_to_cart` / `remove_from_cart` / `checkout`);
- estado por carrinho = (itens, valor, última atividade, user_id);
- `checkout` → carrinho **convertido**, limpa o estado;
- sem checkout por `ABANDON_MINUTES` de event-time → **timeout** → emite o
  carrinho em `gold.abandoned_carts`.

> **Por que `cart_id` e não `user_id`:** a primeira versão keava por user_id e
> unia carts+purchases. Como o espaço de usuários é pequeno, um mesmo usuário
> tinha várias sessões e a compra de uma sessão "convertia" o estado de outra —
> inflando a taxa de abandono (~75% medido vs ~40% real). Keying por `cart_id`
> isola cada sessão e a conversão vira um evento explícito (`checkout`) do próprio
> carrinho. Bug pego no teste E2E.

## Justificativa

- É o padrão correto para "A sem B dentro de T" com tolerância a desordem.
- Timeout por **event-time** (watermark), não wall-clock → reprodutível.
- Estado limpo no convert/timeout → **sem vazamento** de estado.
- `MERGE` por `cart_id` na escrita → idempotente sob reprocessamento.

## Consequências e armadilhas resolvidas

- (+) Demonstra estado distribuído tolerante a falha (checkpoint) — sinal forte.
- **Bug real corrigido no E2E:** evento de carrinho **atrasado** podia gerar
  `setTimeoutTimestamp` anterior ao watermark → `INVALID_TIMEOUT_TIMESTAMP`.
  Solução: se `last_ts + timeout <= watermark`, o carrinho **já** está
  abandonado e é emitido na hora (via `getCurrentWatermarkMs()`), sem agendar
  timeout no passado.
- (−) `applyInPandasWithState` exige pandas/pyarrow no executor (ver spark/Dockerfile).
- **Trigger de revisão:** o `checkout` é o sinal de conversão; em produção, ligar
  o `checkout` ao pedido real (order_id) para reconciliar receita × abandono.
