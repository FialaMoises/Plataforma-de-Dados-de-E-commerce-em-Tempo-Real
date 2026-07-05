"""Simulador de eventos `purchase` para o vertical slice.

Produz eventos realistas em JSON para o tópico Kafka `purchases`, validando cada
evento contra o data contract versionado (contracts/purchase.v1.schema.json).

Injeta propositalmente:
  - eventos inválidos (BAD_EVENT_RATE)  -> exercita DLQ + Data Quality gate
  - eventos duplicados (DUPLICATE_RATE) -> exercita a idempotência do Bronze (MERGE)

As funções de geração são puras e testáveis sem Kafka (ver tests/).
"""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from jsonschema import Draft202012Validator


def _resolve_contract() -> Path:
    """Resolve o contrato tanto no container (generator/contracts/) quanto no
    host durante os testes (contracts/ na raiz do repo)."""
    here = Path(__file__).resolve().parent
    for candidate in (here / "contracts", here.parent / "contracts"):
        path = candidate / "purchase.v1.schema.json"
        if path.exists():
            return path
    raise FileNotFoundError("purchase.v1.schema.json não encontrado")


CONTRACT_PATH = _resolve_contract()

# Catálogo pequeno e estável de produtos/usuários para gerar joins coerentes depois.
N_USERS = 5_000
N_PRODUCTS = 800
CURRENCIES = ["BRL", "BRL", "BRL", "USD", "EUR"]  # BRL dominante
CHANNELS = ["web", "ios", "android"]


def _price_for(product_id: int) -> float:
    """Preço determinístico-ish por produto (faixa realista)."""
    base = 9.90 + (product_id % 50) * 7.5
    return round(base * random.uniform(0.8, 1.2), 2)


def build_event(now: datetime | None = None) -> dict:
    """Gera um evento `purchase` válido segundo o contrato v1."""
    now = now or datetime.now(UTC)
    product_id = random.randint(1, N_PRODUCTS)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "purchase",
        "user_id": random.randint(1, N_USERS),
        "product_id": product_id,
        "quantity": random.randint(1, 5),
        "price": _price_for(product_id),
        "currency": random.choice(CURRENCIES),
        "timestamp": now.isoformat().replace("+00:00", "Z"),
        "channel": random.choice(CHANNELS),
        "schema_version": "1.0.0",
    }


def corrupt_event(event: dict) -> dict:
    """Quebra um evento de uma das formas que vemos em produção."""
    broken = dict(event)
    mode = random.choice(["neg_price", "zero_qty", "missing_field", "bad_type", "bad_currency"])
    if mode == "neg_price":
        broken["price"] = -abs(broken["price"])
    elif mode == "zero_qty":
        broken["quantity"] = 0
    elif mode == "missing_field":
        broken.pop("product_id", None)
    elif mode == "bad_type":
        broken["user_id"] = "not-an-int"
    elif mode == "bad_currency":
        broken["currency"] = "XYZ"
    broken["_corruption"] = mode  # marca só para inspeção/demonstração
    return broken


def _build_validator() -> Draft202012Validator:
    schema = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def is_valid(event: dict, validator: Draft202012Validator) -> bool:
    return validator.is_valid(event)


def _run() -> None:
    from confluent_kafka import Producer  # import tardio: testes não precisam de Kafka

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.environ.get("TOPIC_PURCHASES", "purchases")
    eps = float(os.environ.get("EVENTS_PER_SECOND", "20"))
    bad_rate = float(os.environ.get("BAD_EVENT_RATE", "0.03"))
    dup_rate = float(os.environ.get("DUPLICATE_RATE", "0.02"))

    validator = _build_validator()
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 50})
    interval = 1.0 / eps if eps > 0 else 0.05

    print(
        f"[generator] -> {bootstrap} topic={topic} eps={eps} bad={bad_rate} dup={dup_rate}",
        flush=True,
    )
    sent = invalid = dups = 0
    last_event: dict | None = None

    while True:
        # particiona por user_id para preservar ordem por usuário (key = user_id).
        if last_event and random.random() < dup_rate:
            event = last_event  # reenvia idêntico -> mesma event_id
            dups += 1
        else:
            event = build_event()
            if random.random() < bad_rate:
                event = corrupt_event(event)
            last_event = event

        # conta inválidos pela validação real contra o contrato (não pela flag).
        if not is_valid(event, validator):
            invalid += 1

        key = str(event.get("user_id", "unknown")).encode()
        producer.produce(topic, key=key, value=json.dumps(event).encode())
        producer.poll(0)
        sent += 1
        if sent % 200 == 0:
            print(f"[generator] sent={sent} invalid={invalid} dups={dups}", flush=True)
            producer.flush()
        time.sleep(interval)


if __name__ == "__main__":
    _run()
