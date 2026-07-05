"""Simulador de eventos de e-commerce (`purchase` e `cart`).

Produz, em JSON validado contra os data contracts versionados:
  - tópico `purchases`: compras (slice + Fase 2)
  - tópico `carts`: sessões de carrinho (add/remove) para detecção de abandono

Injeta propositalmente:
  - inválidos (BAD_EVENT_RATE)  -> exercita DLQ + Data Quality gate
  - duplicados (DUPLICATE_RATE) -> exercita a idempotência do Bronze (MERGE)
  - atrasados (LATE_EVENT_RATE)  -> exercita o watermark (Fase 2)

Sessões de carrinho usam uma faixa de user_id dedicada (>= CART_USER_BASE) para
que o abandono seja correlacionável de forma limpa: um usuário de carrinho que
NÃO gera purchase dentro do timeout é considerado abandono pelo job stateful.

As funções de geração são puras e testáveis sem Kafka (ver tests/).
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from jsonschema import Draft202012Validator

logger = logging.getLogger(__name__)

N_USERS = 5_000
N_PRODUCTS = 800
CURRENCIES = ["BRL", "BRL", "BRL", "USD", "EUR"]  # BRL dominante
CHANNELS = ["web", "ios", "android"]
CART_USER_BASE = 90_000  # faixa dedicada para usuários de sessões de carrinho
MAX_CART_ITEMS = 3
MAX_QUANTITY = 5
MAX_LATE_MINUTES = 5
LOG_INTERVAL = 200


def _resolve_contract(filename: str) -> Path:
    """Resolve o contrato no container e no host."""
    here = Path(__file__).resolve().parent
    for candidate in (here / "contracts", here.parent / "contracts"):
        path = candidate / filename
        if path.exists():
            return path
    raise FileNotFoundError(f"{filename} não encontrado")


def _price_for(product_id: int) -> float:
    """Preço determinístico-ish por produto (faixa realista)."""
    base = 9.90 + (product_id % 50) * 7.5
    return round(base * random.uniform(0.8, 1.2), 2)


def build_event(now: datetime | None = None, user_id: int | None = None) -> dict:
    """Gera um evento `purchase` válido segundo o contrato v1."""
    now = now or datetime.now(UTC)
    product_id = random.randint(1, N_PRODUCTS)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "purchase",
        "user_id": user_id if user_id is not None else random.randint(1, N_USERS),
        "product_id": product_id,
        "quantity": random.randint(1, MAX_QUANTITY),
        "price": _price_for(product_id),
        "currency": random.choice(CURRENCIES),
        "timestamp": now.isoformat().replace("+00:00", "Z"),
        "channel": random.choice(CHANNELS),
        "schema_version": "1.0.0",
    }


def build_cart_event(
    user_id: int,
    cart_id: str,
    action: str = "add_to_cart",
    now: datetime | None = None,
) -> dict:
    """Gera um evento de carrinho válido segundo o contrato cart.v1."""
    now = now or datetime.now(UTC)
    product_id = random.randint(1, N_PRODUCTS)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": action,
        "cart_id": cart_id,
        "user_id": user_id,
        "product_id": product_id,
        "quantity": random.randint(1, MAX_QUANTITY - 1),
        "price": _price_for(product_id),
        "currency": random.choice(CURRENCIES),
        "timestamp": now.isoformat().replace("+00:00", "Z"),
        "channel": random.choice(CHANNELS),
        "schema_version": "1.0.0",
    }


def corrupt_event(event: dict) -> dict:
    """Quebra um evento de uma das formas que vemos em produção."""
    broken = dict(event)
    mode = random.choice([
        "neg_price", "zero_qty", "missing_field", "bad_type", "bad_currency",
    ])
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


def _build_validator(filename: str) -> Draft202012Validator:
    schema = json.loads(_resolve_contract(filename).read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def is_valid(event: dict, validator: Draft202012Validator) -> bool:
    return validator.is_valid(event)


# ── Helpers internos do loop principal ──────────────────────────────────────


def _emit(producer, topic: str, event: dict) -> None:
    """Envia um evento para o Kafka."""
    producer.produce(
        topic,
        key=str(event.get("user_id", "?")).encode(),
        value=json.dumps(event).encode(),
    )
    producer.poll(0)


def _emit_cart_session(
    producer, topic_carts, topic_purchases, config, validators, stats
) -> None:
    """Gera uma sessão de carrinho completa (add_to_cart + possível checkout)."""
    cart_user = CART_USER_BASE + random.randint(0, 9_999)
    cart_id = str(uuid.uuid4())
    now = datetime.now(UTC)

    for _ in range(random.randint(1, MAX_CART_ITEMS)):
        ce = build_cart_event(cart_user, cart_id, "add_to_cart", now)
        if not is_valid(ce, validators["cart"]):
            stats["invalid"] += 1
        _emit(producer, topic_carts, ce)
        stats["carts"] += 1

    if random.random() < config["cart_conv"]:
        # converteu: checkout fecha o carrinho + compra associada
        _emit(
            producer, topic_carts,
            build_cart_event(cart_user, cart_id, "checkout", now),
        )
        stats["carts"] += 1
        _emit(producer, topic_purchases, build_event(user_id=cart_user))
        stats["sent"] += 1
    else:
        stats["abandoned"] += 1


def _generate_purchase(config, validators, stats, last_event):
    """Gera um evento de compra avulsa (com possível duplicata/late/bad).

    Retorna o evento gerado (para possível duplicação no próximo ciclo).
    """
    if last_event and random.random() < config["dup_rate"]:
        event = last_event
        stats["dups"] += 1
    else:
        if random.random() < config["late_rate"]:
            backdated = datetime.now(UTC) - timedelta(
                minutes=random.randint(1, MAX_LATE_MINUTES)
            )
            event = build_event(now=backdated)
            stats["late"] += 1
        else:
            event = build_event()
        if random.random() < config["bad_rate"]:
            event = corrupt_event(event)
        last_event = event

    if not is_valid(event, validators["purchase"]):
        stats["invalid"] += 1

    return event, last_event


def _run() -> None:
    from confluent_kafka import Producer  # import tardio: testes não precisam de Kafka

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.environ.get("TOPIC_PURCHASES", "purchases")
    topic_carts = os.environ.get("TOPIC_CARTS", "carts")
    eps = float(os.environ.get("EVENTS_PER_SECOND", "20"))

    config = {
        "bad_rate": float(os.environ.get("BAD_EVENT_RATE", "0.03")),
        "dup_rate": float(os.environ.get("DUPLICATE_RATE", "0.02")),
        "late_rate": float(os.environ.get("LATE_EVENT_RATE", "0.05")),
        "cart_rate": float(os.environ.get("CART_SESSION_RATE", "0.15")),
        "cart_conv": float(os.environ.get("CART_CONVERSION", "0.6")),
    }

    validators = {
        "purchase": _build_validator("purchase.v1.schema.json"),
        "cart": _build_validator("cart.v1.schema.json"),
    }
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 50})
    interval = 1.0 / eps if eps > 0 else 0.05

    logger.info(
        "[generator] -> %s purchases=%s carts=%s eps=%s bad=%s dup=%s "
        "late=%s cart=%s conv=%s",
        bootstrap, topic, topic_carts, eps,
        config["bad_rate"], config["dup_rate"], config["late_rate"],
        config["cart_rate"], config["cart_conv"],
    )

    stats = {"sent": 0, "invalid": 0, "dups": 0, "late": 0, "carts": 0, "abandoned": 0}
    last_event: dict | None = None

    while True:
        if random.random() < config["cart_rate"]:
            _emit_cart_session(
                producer, topic_carts, topic, config, validators, stats,
            )
        else:
            event, last_event = _generate_purchase(
                config, validators, stats, last_event,
            )
            _emit(producer, topic, event)
            stats["sent"] += 1

        if stats["sent"] % LOG_INTERVAL == 0 and stats["sent"] > 0:
            logger.info(
                "[generator] purchases=%d carts=%d invalid=%d "
                "dups=%d late=%d abandoned~%d",
                stats["sent"], stats["carts"], stats["invalid"],
                stats["dups"], stats["late"], stats["abandoned"],
            )
            producer.flush()

        time.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )
    _run()
