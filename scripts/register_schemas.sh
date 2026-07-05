#!/usr/bin/env bash
# Registra os schemas Avro no Schema Registry (compatibilidade BACKWARD).
# Uso: bash scripts/register_schemas.sh
#
# Pré-requisito: Schema Registry rodando (docker compose --profile core up -d).
# Os schemas .avsc ficam em contracts/ e são a fonte da verdade do contrato.
set -euo pipefail

REGISTRY=${SCHEMA_REGISTRY_URL:-http://localhost:8085}

register() {
  local subject=$1
  local avsc_file=$2
  echo "Registrando $subject a partir de $avsc_file ..."
  # Escapa o JSON do schema para embutir como string dentro do payload.
  local schema
  schema=$(python3 -c "import json,sys;print(json.dumps(json.dumps(json.load(open('$avsc_file')))))")
  curl -s -X POST "$REGISTRY/subjects/$subject/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\": \"AVRO\", \"schema\": $schema}" | python3 -m json.tool
  echo
}

register "purchases-value" "contracts/purchase.v1.avsc"
register "carts-value"     "contracts/cart.v1.avsc"

echo "Schemas registrados. Verificando:"
curl -s "$REGISTRY/subjects" | python3 -m json.tool
