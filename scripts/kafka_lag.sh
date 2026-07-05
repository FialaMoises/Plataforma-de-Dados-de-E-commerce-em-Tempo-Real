#!/usr/bin/env bash
# Observabilidade (Fase 2): mostra o lag dos consumer groups no tópico `purchases`.
# O lag = quantos eventos o consumidor está atrás do fim do log; é a métrica-chave
# de saúde de um pipeline de streaming. Uso: bash scripts/kafka_lag.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1

echo "=== Consumer groups ==="
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list 2>/dev/null

echo
echo "=== Lag por grupo (TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG) ==="
for g in $(docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 --list 2>/dev/null); do
  echo "--- group: $g ---"
  docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$g" 2>/dev/null \
    | awk 'NR==1 || $1!="" {print $1, $3, $4, $5, $6, $7}' | column -t
done
