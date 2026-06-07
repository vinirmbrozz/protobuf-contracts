"""
Python interop harness — Confluent SR serde contract.

Tests that the Python SDK (sdk/python / truther_contracts) implementation of
the Confluent envelope framing is byte-for-byte compatible with the Node.js
reference harness.

Framing functions come from truther_contracts.serde.KafkaSerde — zero local
reimplementation of the wire format.

Run: python -m pytest interop/python/test_interop.py -v
"""

import struct
import pytest

from truther_contracts import Transaction, PredictiveAnalyzer
from truther_contracts.serde import KafkaSerde, MAGIC_BYTE

# Framing via SDK — no local reimplementation
frame_message = KafkaSerde._frame
parse_frame = KafkaSerde._parse_frame


# ---------------------------------------------------------------------------
# Tests — framing layer only (no Kafka or Schema Registry required)
# ---------------------------------------------------------------------------

class TestFramingLayer:
    def test_magic_byte(self):
        framed = frame_message(1, b"\x0a\x03abc")
        assert framed[0] == MAGIC_BYTE

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
        with pytest.raises(Exception, match="magic byte"):
            parse_frame(bad)

    def test_undersized_frame(self):
        with pytest.raises(Exception, match="too short|short"):
            parse_frame(b"\x00\x00\x00")
        with pytest.raises(Exception, match="too short|short"):
            parse_frame(b"")


class TestProtoRoundTrip:
    """Round-trip tests using the generated Python SDK (truther_contracts)."""

    def test_transaction_round_trip(self):
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

    Fixture bytes were captured from interop/harness.js (Test 1 framed output):

      Transaction{
        transactionAmount: "499.99",
        predictiveAnalyzer: {
          isAllowed: true, reason: "approved by risk engine",
          cardId: "card-abc-123", userId: "user-xyz-456",
          walletAddress: "0xDEADBEEF", allowance: "1000.00"
        },
        finalDecision: "APPROVED"
      }  — schemaId = 1
    """

    FIXTURE_HEX = (
        "0000000001000a063439392e3939124c08011217617070726f766564206279207269736b"
        "20656e67696e651a0c636172642d6162632d313233220c757365722d78797a2d3435362a"
        "0a307844454144424545463207313030302e30301a08415050524f564544"
    )

    def test_deserialize_node_produced_bytes(self):
        framed = bytes.fromhex(self.FIXTURE_HEX)
        schema_id, msg_bytes = parse_frame(framed)

        assert schema_id == 1

        tx = Transaction()
        tx.ParseFromString(msg_bytes)

        assert tx.transactionAmount == "499.99"
        assert tx.final_decision == "APPROVED"
        assert tx.predictiveAnalyzer.isAllowed is True
        assert tx.predictiveAnalyzer.cardId == "card-abc-123"
        assert tx.predictiveAnalyzer.userId == "user-xyz-456"
        assert tx.predictiveAnalyzer.walletAddress == "0xDEADBEEF"
        assert tx.predictiveAnalyzer.allowance == "1000.00"
        assert tx.predictiveAnalyzer.reason == "approved by risk engine"
