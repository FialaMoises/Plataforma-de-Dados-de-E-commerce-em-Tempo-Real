"""Testes das funções puras do simulador (não precisam de Kafka)."""

import simulator
from simulator import build_event, corrupt_event, is_valid


def test_build_event_respeita_o_contrato():
    v = simulator._build_validator()
    for _ in range(200):
        assert is_valid(build_event(), v), "evento gerado deveria ser válido"


def test_event_id_e_unico_entre_eventos():
    ids = {build_event()["event_id"] for _ in range(500)}
    assert len(ids) == 500


def test_corrupt_event_sempre_viola_o_contrato():
    v = simulator._build_validator()
    for _ in range(200):
        bad = corrupt_event(build_event())
        assert not is_valid(bad, v), f"evento corrompido deveria ser inválido: {bad['_corruption']}"


def test_corrupt_event_marca_o_modo_de_falha():
    bad = corrupt_event(build_event())
    assert bad["_corruption"] in {
        "neg_price",
        "zero_qty",
        "missing_field",
        "bad_type",
        "bad_currency",
    }
