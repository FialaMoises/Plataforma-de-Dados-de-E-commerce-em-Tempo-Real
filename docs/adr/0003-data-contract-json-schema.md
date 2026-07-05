# ADR-0003 — Data Contract: JSON Schema versionado (com caminho p/ Avro)

- **Status:** Aceito (para o slice) — evolução planejada
- **Data:** 2026-06-25

## Contexto

Os eventos precisam de um contrato versionado e validável. Opções: JSON Schema
validado no producer; Avro/Protobuf + Confluent Schema Registry (compatibilidade
forçada no broker).

## Decisão

No **vertical slice**: payload **JSON** validado contra
`contracts/purchase.v1.schema.json` (Draft 2020-12) **no producer**; schema-on-read
explícito no Spark (sem inferência). O `event_id` é a chave natural de
idempotência.

Em **produção** (planejado): migrar para **Avro + Confluent Schema Registry** com
regra de compatibilidade `BACKWARD`, eliminando a desserialização manual.

## Justificativa

- Reduz peças móveis no slice (sem Schema Registry + sem lidar com o *wire format*
  Confluent de 5 bytes no Spark), mantendo o **sinal de data contract**:
  versão explícita, validação no produtor, schema-on-read no consumidor.
- A migração para Avro é incremental e não muda a modelagem das tabelas.

## Consequências

- (+) Slice roda com menos serviços; contrato continua versionado e testado (CI).
- (−) A compatibilidade não é *forçada* pelo broker ainda (é responsabilidade do
  producer + CI). Esse gap é exatamente o que a migração para Schema Registry fecha.
- **Definition of Done da migração:** quebra de compat reprovada automaticamente
  no CI e no registro.
