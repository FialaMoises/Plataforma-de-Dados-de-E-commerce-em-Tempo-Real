"""Testes de validação dos contratos JSON Schema (purchase.v1 e cart.v1).

Verifica que eventos válidos são aceitos e que diversas violações são
corretamente rejeitadas. Usa jsonschema (já em requirements-dev.txt).
"""

import json
import uuid
from pathlib import Path

import jsonschema
import pytest

_CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"


@pytest.fixture(scope="module")
def purchase_schema():
    with open(_CONTRACTS_DIR / "purchase.v1.schema.json") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def cart_schema():
    with open(_CONTRACTS_DIR / "cart.v1.schema.json") as f:
        return json.load(f)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _valid_purchase(**overrides):
    ev = {
        "event_id": str(uuid.uuid4()),
        "event_type": "purchase",
        "user_id": 1,
        "product_id": 10,
        "quantity": 2,
        "price": 9.99,
        "currency": "BRL",
        "timestamp": "2026-06-25T12:00:00Z",
        "channel": "web",
        "schema_version": "1.0.0",
    }
    ev.update(overrides)
    return ev


def _valid_cart(event_type="add_to_cart", **overrides):
    ev = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "cart_id": str(uuid.uuid4()),
        "user_id": 1,
        "product_id": 10,
        "quantity": 1,
        "price": 5.50,
        "currency": "USD",
        "timestamp": "2026-06-25T14:30:00Z",
        "channel": "ios",
        "schema_version": "1.0.0",
    }
    ev.update(overrides)
    return ev


def _is_valid(event, schema):
    try:
        jsonschema.validate(event, schema)
        return True
    except jsonschema.ValidationError:
        return False


# ── Purchase: eventos válidos ────────────────────────────────────────────────


def test_purchase_valido_aceito(purchase_schema):
    assert _is_valid(_valid_purchase(), purchase_schema)


def test_purchase_valido_sem_opcionais(purchase_schema):
    ev = _valid_purchase()
    del ev["channel"]
    del ev["schema_version"]
    assert _is_valid(ev, purchase_schema)


def test_purchase_todas_as_moedas(purchase_schema):
    for cur in ("BRL", "USD", "EUR"):
        assert _is_valid(_valid_purchase(currency=cur), purchase_schema)


# ── Purchase: campos obrigatórios ausentes ───────────────────────────────────


@pytest.mark.parametrize(
    "field",
    [
        "event_id",
        "event_type",
        "user_id",
        "product_id",
        "quantity",
        "price",
        "currency",
        "timestamp",
    ],
)
def test_purchase_campo_obrigatorio_ausente(purchase_schema, field):
    ev = _valid_purchase()
    del ev[field]
    assert not _is_valid(ev, purchase_schema)


# ── Purchase: tipos errados ──────────────────────────────────────────────────


def test_purchase_user_id_string_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(user_id="abc"), purchase_schema)


def test_purchase_quantity_float_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(quantity=2.5), purchase_schema)


def test_purchase_price_string_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(price="dez"), purchase_schema)


# ── Purchase: valores fora do range ─────────────────────────────────────────


def test_purchase_price_zero_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(price=0), purchase_schema)


def test_purchase_price_negativo_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(price=-1.0), purchase_schema)


def test_purchase_quantity_zero_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(quantity=0), purchase_schema)


def test_purchase_quantity_acima_maximo_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(quantity=101), purchase_schema)


def test_purchase_user_id_zero_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(user_id=0), purchase_schema)


# ── Purchase: enums inválidos ────────────────────────────────────────────────


def test_purchase_moeda_invalida_rejeitada(purchase_schema):
    assert not _is_valid(_valid_purchase(currency="XYZ"), purchase_schema)


def test_purchase_channel_invalido_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(channel="desktop"), purchase_schema)


def test_purchase_event_type_errado_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(event_type="refund"), purchase_schema)


# ── Purchase: propriedades adicionais ────────────────────────────────────────


def test_purchase_campo_extra_rejeitado(purchase_schema):
    assert not _is_valid(_valid_purchase(extra_field="oops"), purchase_schema)


# ── Cart: eventos válidos ────────────────────────────────────────────────────


def test_cart_add_valido(cart_schema):
    assert _is_valid(_valid_cart("add_to_cart"), cart_schema)


def test_cart_remove_valido(cart_schema):
    assert _is_valid(_valid_cart("remove_from_cart"), cart_schema)


def test_cart_checkout_valido(cart_schema):
    assert _is_valid(_valid_cart("checkout"), cart_schema)


# ── Cart: campos obrigatórios ausentes ───────────────────────────────────────


@pytest.mark.parametrize(
    "field",
    [
        "event_id",
        "event_type",
        "cart_id",
        "user_id",
        "product_id",
        "quantity",
        "price",
        "currency",
        "timestamp",
    ],
)
def test_cart_campo_obrigatorio_ausente(cart_schema, field):
    ev = _valid_cart()
    del ev[field]
    assert not _is_valid(ev, cart_schema)


# ── Cart: validações de domínio ──────────────────────────────────────────────


def test_cart_event_type_invalido_rejeitado(cart_schema):
    assert not _is_valid(_valid_cart("wishlist"), cart_schema)


def test_cart_price_negativo_rejeitado(cart_schema):
    assert not _is_valid(_valid_cart(price=-5.0), cart_schema)


def test_cart_quantity_zero_rejeitado(cart_schema):
    assert not _is_valid(_valid_cart(quantity=0), cart_schema)


def test_cart_campo_extra_rejeitado(cart_schema):
    assert not _is_valid(_valid_cart(bonus=True), cart_schema)
