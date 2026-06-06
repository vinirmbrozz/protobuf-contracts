"""
Unit tests for truther_contracts.serde — no network, no Docker required.

Tests:
- Framing helpers (_frame / _parse_frame) — byte-level assertions
- produce() — valid message, wrong type, unregistered topic
- consume() — valid roundtrip, invalid magic byte, frame too short, unknown schema_id
- KafkaSerde constructor — missing SR_URL raises ValueError
- _validate_known_type — non-Message subclass raises TypeError
"""

import os
import struct
from unittest.mock import MagicMock, patch

import pytest

from truther_contracts import PredictiveAnalyzer, Transaction
from truther_contracts.serde import (
    InvalidMagicByteError,
    KafkaSerde,
    SchemaRegistryClient,
    SerdeError,
    UnknownSchemaError,
    _get_proto_schema,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_SR_URL = "http://sr.test:8081"
SCHEMA_ID = 42


def _make_serde(schema_id: int = SCHEMA_ID) -> KafkaSerde:
    """Return a KafkaSerde with a mocked SR client, startup already called."""
    serde = KafkaSerde(sr_url=MOCK_SR_URL)
    # Patch the internal SR client so no HTTP is made
    serde._sr._subject_to_id["transactions-value"] = schema_id
    serde._sr._id_to_subject[schema_id] = "transactions-value"
    serde._topic_to_class["transactions"] = Transaction
    serde._topic_to_schema_id["transactions"] = schema_id
    return serde


def _make_transaction(amount: str = "199.99", decision: str = "APPROVED") -> Transaction:
    pa = PredictiveAnalyzer()
    pa.isAllowed = True
    pa.reason = "ok"
    pa.cardId = "card-001"
    pa.userId = "user-001"
    pa.walletAddress = "0xdeadbeef"
    pa.allowance = "5000.00"
    pa.transactionId = "txn-001"

    tx = Transaction()
    tx.transactionAmount = amount
    tx.predictiveAnalyzer.CopyFrom(pa)
    tx.final_decision = decision
    return tx


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

class TestKafkaSerdeConstructor:
    def test_raises_when_no_sr_url(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SCHEMA_REGISTRY_URL", None)
            with pytest.raises(ValueError, match="SCHEMA_REGISTRY_URL"):
                KafkaSerde()

    def test_accepts_sr_url_from_env(self):
        with patch.dict(os.environ, {"SCHEMA_REGISTRY_URL": MOCK_SR_URL}):
            serde = KafkaSerde()
            assert serde._sr._base_url == MOCK_SR_URL

    def test_accepts_sr_url_as_kwarg(self):
        serde = KafkaSerde(sr_url=MOCK_SR_URL)
        assert serde._sr._base_url == MOCK_SR_URL


# ---------------------------------------------------------------------------
# _frame / _parse_frame — internal byte-level helpers
# ---------------------------------------------------------------------------

class TestFrameHelpers:
    def test_frame_magic_byte(self):
        framed = KafkaSerde._frame(1, b"\x01")
        assert framed[0] == 0x00

    def test_frame_schema_id_big_endian(self):
        framed = KafkaSerde._frame(SCHEMA_ID, b"\x00")
        schema_id_bytes = framed[1:5]
        assert struct.unpack(">I", schema_id_bytes)[0] == SCHEMA_ID

    def test_frame_msg_index_byte(self):
        framed = KafkaSerde._frame(1, b"\x00")
        assert framed[5] == 0x00

    def test_frame_payload_appended(self):
        payload = b"\x0a\x03abc"
        framed = KafkaSerde._frame(1, payload)
        assert framed[6:] == payload

    def test_frame_total_length(self):
        payload = b"\x01\x02\x03"
        framed = KafkaSerde._frame(1, payload)
        assert len(framed) == 6 + len(payload)

    def test_parse_frame_roundtrip(self):
        payload = b"\x0a\x05hello"
        framed = KafkaSerde._frame(SCHEMA_ID, payload)
        schema_id, decoded = KafkaSerde._parse_frame(framed)
        assert schema_id == SCHEMA_ID
        assert decoded == payload

    def test_parse_frame_rejects_bad_magic_byte(self):
        bad = bytes([0x01]) + struct.pack(">I", 1) + b"\x00" + b"\x0a"
        with pytest.raises(InvalidMagicByteError, match="magic byte"):
            KafkaSerde._parse_frame(bad)

    def test_parse_frame_rejects_too_short(self):
        with pytest.raises(SerdeError, match="too short"):
            KafkaSerde._parse_frame(b"\x00\x00\x00")

    def test_parse_frame_rejects_empty(self):
        with pytest.raises(SerdeError, match="too short"):
            KafkaSerde._parse_frame(b"")


# ---------------------------------------------------------------------------
# produce()
# ---------------------------------------------------------------------------

class TestProduce:
    def test_produce_returns_bytes(self):
        serde = _make_serde()
        tx = _make_transaction()
        result = serde.produce("transactions", tx)
        assert isinstance(result, bytes)

    def test_produce_magic_byte(self):
        serde = _make_serde()
        result = serde.produce("transactions", _make_transaction())
        assert result[0] == 0x00

    def test_produce_schema_id_in_frame(self):
        serde = _make_serde(schema_id=99)
        result = serde.produce("transactions", _make_transaction())
        assert struct.unpack(">I", result[1:5])[0] == 99

    def test_produce_msg_index_byte(self):
        serde = _make_serde()
        result = serde.produce("transactions", _make_transaction())
        assert result[5] == 0x00

    def test_produce_payload_deserializable(self):
        serde = _make_serde()
        tx = _make_transaction("299.99", "DENIED")
        framed = serde.produce("transactions", tx)
        tx2 = Transaction()
        tx2.ParseFromString(framed[6:])
        assert tx2.transactionAmount == "299.99"
        assert tx2.final_decision == "DENIED"

    def test_produce_raises_for_unregistered_topic(self):
        serde = _make_serde()
        tx = _make_transaction()
        with pytest.raises(RuntimeError, match="not registered"):
            serde.produce("unknown-topic", tx)

    def test_produce_raises_for_wrong_message_type(self):
        serde = _make_serde()
        pa = PredictiveAnalyzer()  # not Transaction
        with pytest.raises(TypeError, match="Expected Transaction"):
            serde.produce("transactions", pa)


# ---------------------------------------------------------------------------
# consume()
# ---------------------------------------------------------------------------

class TestConsume:
    def test_consume_roundtrip(self):
        serde = _make_serde()
        tx = _make_transaction("499.00", "APPROVED")
        framed = serde.produce("transactions", tx)
        result = serde.consume("transactions", framed, Transaction)
        assert isinstance(result, Transaction)
        assert result.transactionAmount == "499.00"
        assert result.final_decision == "APPROVED"

    def test_consume_preserves_nested_message(self):
        serde = _make_serde()
        tx = _make_transaction()
        framed = serde.produce("transactions", tx)
        result = serde.consume("transactions", framed, Transaction)
        assert result.predictiveAnalyzer.isAllowed is True
        assert result.predictiveAnalyzer.cardId == "card-001"
        assert result.predictiveAnalyzer.userId == "user-001"

    def test_consume_raises_invalid_magic_byte(self):
        serde = _make_serde()
        bad = bytes([0x01]) + struct.pack(">I", SCHEMA_ID) + b"\x00" + b"\x0a"
        with pytest.raises(InvalidMagicByteError, match="magic byte"):
            serde.consume("transactions", bad, Transaction)

    def test_consume_raises_frame_too_short(self):
        serde = _make_serde()
        with pytest.raises(SerdeError, match="too short"):
            serde.consume("transactions", b"\x00\x00", Transaction)

    def test_consume_raises_unknown_schema_id(self):
        serde = _make_serde()
        # Frame with a schema_id not in the SR cache (999) — mock the HTTP 404
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        with patch.object(serde._sr._session, "get", return_value=mock_resp):
            framed = KafkaSerde._frame(999, b"\x0a\x01x")
            with pytest.raises(UnknownSchemaError, match="schema_id=999"):
                serde.consume("transactions", framed, Transaction)

    def test_consume_raises_for_non_message_type(self):
        serde = _make_serde()
        framed = serde.produce("transactions", _make_transaction())
        with pytest.raises(TypeError):
            serde.consume("transactions", framed, str)  # type: ignore[arg-type]

    def test_consume_raises_on_corrupt_protobuf_payload(self):
        serde = _make_serde()
        # Frame with valid schema_id but garbage payload
        framed = KafkaSerde._frame(SCHEMA_ID, b"\xff\xff\xff\xff")
        with pytest.raises(SerdeError, match="deserialization"):
            serde.consume("transactions", framed, Transaction)


# ---------------------------------------------------------------------------
# _validate_known_type
# ---------------------------------------------------------------------------

class TestValidateKnownType:
    def test_accepts_transaction(self):
        KafkaSerde._validate_known_type(Transaction)  # no raise

    def test_accepts_predictive_analyzer(self):
        KafkaSerde._validate_known_type(PredictiveAnalyzer)  # no raise

    def test_rejects_plain_string(self):
        with pytest.raises(TypeError):
            KafkaSerde._validate_known_type(str)  # type: ignore[arg-type]

    def test_rejects_instance_not_class(self):
        with pytest.raises(TypeError):
            KafkaSerde._validate_known_type(Transaction())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# startup() — mock HTTP to test registration path
# ---------------------------------------------------------------------------

class TestStartup:
    def test_startup_registers_schema_via_sr(self):
        serde = KafkaSerde(sr_url=MOCK_SR_URL)
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"id": 7}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(serde._sr._session, "post", return_value=mock_resp) as mock_post:
            serde.startup({"payments": Transaction})

        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "payments-value" in call_args[0][0]  # subject in URL
        assert serde._topic_to_schema_id["payments"] == 7
        assert serde._topic_to_class["payments"] is Transaction

    def test_startup_caches_schema_id_avoids_double_registration(self):
        serde = KafkaSerde(sr_url=MOCK_SR_URL)
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"id": 3}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(serde._sr._session, "post", return_value=mock_resp) as mock_post:
            serde.startup({"payments": Transaction})
            serde.startup({"payments": Transaction})  # second call — cache hit

        assert mock_post.call_count == 1  # SR called only once

    def test_startup_rejects_non_message_class(self):
        serde = KafkaSerde(sr_url=MOCK_SR_URL)
        with pytest.raises(TypeError):
            serde.startup({"bad": str})  # type: ignore[dict-item]
