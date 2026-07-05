"""Testes das funções puras do simulador (não precisam de Kafka)."""

import simulator
from simulator import build_cart_event, build_event, corrupt_event, is_valid

_PURCHASE_V = simulator._build_validator("purchase.v1.schema.json")
_CART_V = simulator._build_validator("cart.v1.schema.json")


def test_build_event_respeita_o_contrato():
    for _ in range(200):
        assert is_valid(build_event(), _PURCHASE_V), "evento gerado deveria ser válido"


def test_build_event_aceita_user_id_explicito():
    ev = build_event(user_id=90123)
    assert ev["user_id"] == 90123
    assert is_valid(ev, _PURCHASE_V)


def test_event_id_e_unico_entre_eventos():
    ids = {build_event()["event_id"] for _ in range(500)}
    assert len(ids) == 500


def test_corrupt_event_sempre_viola_o_contrato():
    for _ in range(200):
        bad = corrupt_event(build_event())
        assert not is_valid(bad, _PURCHASE_V), f"deveria ser inválido: {bad['_corruption']}"


def test_corrupt_event_marca_o_modo_de_falha():
    bad = corrupt_event(build_event())
    assert bad["_corruption"] in {
        "neg_price",
        "zero_qty",
        "missing_field",
        "bad_type",
        "bad_currency",
    }


def test_build_cart_event_respeita_o_contrato():
    cart_id = "00000000-0000-0000-0000-000000000001"
    for action in ("add_to_cart", "remove_from_cart"):
        for _ in range(50):
            ev = build_cart_event(91000, cart_id, action)
            assert is_valid(ev, _CART_V), f"cart {action} deveria ser válido"
            assert ev["cart_id"] == cart_id
            assert ev["event_type"] == action
