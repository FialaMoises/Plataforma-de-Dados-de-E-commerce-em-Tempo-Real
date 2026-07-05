# ADR-0007 — Migração para Avro + Schema Registry (compatibilidade BACKWARD)

- **Status:** Aceito
- **Data:** 2026-06-29 (Fase 3)

## Contexto

O contrato v1 usa JSON Schema validado no producer (Python `jsonschema`). Isso
funciona para o slice local, mas tem limitações para produção:

- JSON é verboso (texto), Avro é compacto (binário) → menor uso de rede e disco.
- JSON Schema não tem registro centralizado de compatibilidade; evolução do schema
  depende de disciplina manual.
- Consumidores precisam saber o schema a priori (hardcoded no código).

## Decisão

Adicionar **Confluent Schema Registry** à stack e disponibilizar schemas **Avro**
(`.avsc`) em `contracts/` como fonte da verdade. A migração é incremental:

1. **Fase atual**: JSON Schema permanece como contrato legível; Avro coexiste.
2. **Próximo passo**: producer serializa em Avro com `AvroSerializer`; consumers
   deserializam via `AvroDeserializer` (schema fetched do Registry).
3. **Compatibilidade BACKWARD** configurada no Registry = campos novos devem ter
   default; campos existentes não podem ser removidos sem deprecação.

## Justificativa

- Schema Registry como **single source of truth** para compatibilidade.
- Avro reduz payload ~60% vs JSON para os mesmos eventos.
- Compatibilidade BACKWARD garante que consumers antigos leiam schemas novos.
- `scripts/register_schemas.sh` registra os `.avsc` no Registry (idempotente).

## Consequências

- (+) Evolução de schema segura e auditável.
- (+) Payload menor, serialização mais rápida.
- (+) Consumers não precisam hardcodar o schema (fetch do Registry).
- (−) Dependência adicional (Schema Registry) na stack local.
- (−) Período de coexistência JSON+Avro até migração completa dos consumers.
