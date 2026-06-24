"""
Integration test against a REAL Confluent Schema Registry.

Requires the schema registered (scripts/register_schemas.py) and the env var:
    docker-compose up -d
    SCHEMA_REGISTRY_URL=http://localhost:8081 python scripts/register_schemas.py
    SCHEMA_REGISTRY_URL=http://localhost:8081 python -m pytest sdk/python/tests/test_serde_integration.py
"""
import os
import struct

import pytest

from protobuf_contracts import Transaction, PredictiveAnalyzer
from protobuf_contracts.serde import KafkaSerde, SchemaForeignError, _encode_indexes

SR_URL = os.environ.get("SCHEMA_REGISTRY_URL")

pytestmark = pytest.mark.skipif(not SR_URL, reason="SCHEMA_REGISTRY_URL not set")


def test_roundtrip_against_real_sr():
    serde = KafkaSerde(sr_url=SR_URL)
    serde.bind("transactions", Transaction)

    original = Transaction(
        transactionAmount="9.99",
        final_decision="APPROVED",
        predictiveAnalyzer=PredictiveAnalyzer(isAllowed=True, reason="ok"),
    )
    framed = serde.produce("transactions", original)
    assert serde.consume("transactions", framed) == original


def test_bogus_id_rejected_by_real_sr():
    serde = KafkaSerde(sr_url=SR_URL)
    serde.bind("transactions", Transaction)
    bad = struct.pack(">BI", 0x00, 987654) + _encode_indexes([1]) + Transaction().SerializeToString()
    with pytest.raises(SchemaForeignError):
        serde.consume("transactions", bad)
