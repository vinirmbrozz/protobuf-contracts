"""
Python interop harness — Confluent SR serde contract.

Tests that the Python implementation of the Confluent envelope framing is
byte-for-byte compatible with the Node.js reference harness.

STATUS: EXPECTED TO FAIL until the Senior Python engineer implements the
truther-python-kafka library under docs/confluent-sr-serde-spec.md, and
until `pip install truther-contracts-sdk` is available.

Run: python -m pytest interop/python/test_interop.py -v
"""

import struct
import pytest

MAGIC_BYTE = 0x00


def frame_message(schema_id: int, msg_bytes: bytes) -> bytes:
    """Prepend the Confluent SR 6-byte envelope to serialized proto bytes."""
    header = struct.pack(">bI", MAGIC_BYTE, schema_id)  # 1 magic + 4 schema_id
    msg_index = b"\x00"  # first message in schema (Truther convention)
    return header + msg_index + msg_bytes


def parse_frame(data: bytes) -> tuple[int, bytes]:
    """
    Validate and split a Confluent-framed Kafka value.

    Returns (schema_id, msg_bytes).
    Raises ValueError on invalid magic byte or undersized frame.
    """
    if len(data) < 6:
        raise ValueError(f"Frame too short: {len(data)} bytes (minimum 6)")
    magic = data[0]
    if magic != MAGIC_BYTE:
        raise ValueError(f"Invalid magic byte: 0x{magic:02x} (expected 0x00)")
    schema_id = struct.unpack(">I", data[1:5])[0]
    # data[5] is the message index; per Truther convention it is always 0x00
    msg_bytes = data[6:]
    return schema_id, msg_bytes


# ---------------------------------------------------------------------------
# Tests — framing layer only (no Kafka or Schema Registry required)
# ---------------------------------------------------------------------------

class TestFramingLayer:
    def test_magic_byte(self):
        framed = frame_message(1, b"\x0a\x03abc")
        assert framed[0] == 0x00

    def test_schema_id_big_endian(self):
        framed = frame_message(42, b"\x00")
        schema_id = struct.unpack(">I", framed[1:5])[0]
        assert schema_id == 42

    def test_message_index_byte(self):
        framed = frame_message(1, b"\x00")
        assert framed[5] == 0x00

    def test_frame_length(self):
        payload = b"\x0a\x05hello"
        framed = frame_message(1, payload)
        assert len(framed) == 6 + len(payload)

    def test_round_trip(self):
        payload = b"\x0a\x06499.99\x1a\x08APPROVED"
        framed = frame_message(1, payload)
        schema_id, decoded = parse_frame(framed)
        assert schema_id == 1
        assert decoded == payload

    def test_invalid_magic_byte(self):
        bad = bytes([0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a])
        with pytest.raises(ValueError, match="magic byte"):
            parse_frame(bad)

    def test_undersized_frame(self):
        with pytest.raises(ValueError, match="too short"):
            parse_frame(b"\x00\x00\x00")
        with pytest.raises(ValueError, match="too short"):
            parse_frame(b"")


class TestProtoRoundTrip:
    """
    Round-trip tests using the generated Python SDK.

    SKIPPED until `truther-contracts-sdk` is installed.
    Install with: pip install -e gen/python/
    """

    @pytest.fixture(autouse=True)
    def require_sdk(self):
        try:
            from truther_contracts_sdk import Transaction, PredictiveAnalyzer  # noqa: F401
        except ImportError:
            pytest.skip(
                "truther-contracts-sdk not installed — run: pip install -e gen/python/"
            )

    def test_transaction_round_trip(self):
        from truther_contracts_sdk import Transaction, PredictiveAnalyzer

        pa = PredictiveAnalyzer()
        pa.isAllowed = True
        pa.reason = "approved"
        pa.cardId = "card-123"
        pa.userId = "user-456"

        tx = Transaction()
        tx.transactionAmount = "499.99"
        tx.predictiveAnalyzer.CopyFrom(pa)
        tx.final_decision = "APPROVED"

        framed = frame_message(1, tx.SerializeToString())
        schema_id, msg_bytes = parse_frame(framed)

        tx2 = Transaction()
        tx2.ParseFromString(msg_bytes)

        assert tx2.transactionAmount == "499.99"
        assert tx2.final_decision == "APPROVED"
        assert tx2.predictiveAnalyzer.isAllowed is True
        assert tx2.predictiveAnalyzer.cardId == "card-123"


class TestNodeCompatibility:
    """
    Verifies that Python can deserialize bytes produced by the Node.js harness.

    SKIPPED until fixture bytes are captured from the Node harness and
    truther-contracts-sdk is installed.
    """

    @pytest.mark.skip(reason="TODO: add fixture bytes from Node harness")
    def test_deserialize_node_produced_bytes(self):
        # fixture_hex = "000000000100..."  # captured from node interop/harness.js
        # framed = bytes.fromhex(fixture_hex)
        # schema_id, msg_bytes = parse_frame(framed)
        # tx = Transaction()
        # tx.ParseFromString(msg_bytes)
        # assert tx.transactionAmount == "499.99"
        pass
