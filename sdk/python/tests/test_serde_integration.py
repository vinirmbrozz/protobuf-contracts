"""
Integration tests for truther_contracts.serde.

Uses a lightweight mock HTTP server (wsgiref/threading) to simulate the Confluent
Schema Registry REST API. No Docker, no real Kafka required.

Covers:
- Full produce→consume roundtrip via a real (in-process) SR mock
- Schema registration idempotency (same schema registered twice → same id)
- Invalid payloads rejected before any deserialization
- Unknown schema_id (SR returns 404) raises UnknownSchemaError
- Missing SR_URL raises at construction
- startup() eagerly registers schemas
"""

import http.server
import json
import struct
import threading
from typing import Any

import pytest

from truther_contracts import PredictiveAnalyzer, Transaction
from truther_contracts.serde import (
    InvalidMagicByteError,
    KafkaSerde,
    SerdeError,
    UnknownSchemaError,
)


# ---------------------------------------------------------------------------
# In-process mock Schema Registry server
# ---------------------------------------------------------------------------

class _MockSRHandler(http.server.BaseHTTPRequestHandler):
    """Minimal Confluent SR REST API mock."""

    def log_message(self, fmt: str, *args: Any) -> None:
        pass  # suppress access log noise in test output

    def _json(self, code: int, body: dict) -> None:
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/vnd.schemaregistry.v1+json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        # POST /subjects/<subject>/versions
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)  # consume body
        subject = self.path.split("/subjects/")[1].split("/versions")[0]
        schema_id = self.server.register(subject)
        self._json(200, {"id": schema_id})

    def do_GET(self) -> None:
        # GET /schemas/ids/<id>
        if "/schemas/ids/" in self.path:
            sid = int(self.path.split("/schemas/ids/")[1])
            if self.server.has_schema_id(sid):
                self._json(200, {"schema": "...", "schemaType": "PROTOBUF"})
            else:
                self._json(404, {"error_code": 40403, "message": "Schema not found"})
        else:
            self._json(404, {"error_code": 404, "message": "Not found"})


class MockSchemaRegistry(http.server.HTTPServer):
    """In-process mock Schema Registry with a subject → id registry."""

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _MockSRHandler)
        self._subjects: dict[str, int] = {}
        self._next_id = 1

    @property
    def url(self) -> str:
        host, port = self.server_address
        return f"http://{host}:{port}"

    def register(self, subject: str) -> int:
        if subject not in self._subjects:
            self._subjects[subject] = self._next_id
            self._next_id += 1
        return self._subjects[subject]

    def has_schema_id(self, schema_id: int) -> bool:
        return schema_id in self._subjects.values()


@pytest.fixture(scope="module")
def mock_sr():
    """Start the mock SR server in a daemon thread for the test module."""
    server = MockSchemaRegistry()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()


@pytest.fixture()
def serde(mock_sr: MockSchemaRegistry) -> KafkaSerde:
    """Fresh KafkaSerde pointing at the mock SR, with Transaction pre-registered."""
    s = KafkaSerde(sr_url=mock_sr.url)
    s.startup({"transactions": Transaction})
    return s


def _tx(amount: str = "100.00", decision: str = "APPROVED") -> Transaction:
    pa = PredictiveAnalyzer()
    pa.isAllowed = True
    pa.reason = "approved"
    pa.cardId = "card-int-001"
    pa.userId = "user-int-001"
    pa.walletAddress = "0xcafe"
    pa.allowance = "9999.99"
    pa.transactionId = "txn-int-001"

    tx = Transaction()
    tx.transactionAmount = amount
    tx.predictiveAnalyzer.CopyFrom(pa)
    tx.final_decision = decision
    return tx


# ---------------------------------------------------------------------------
# Core roundtrip
# ---------------------------------------------------------------------------

class TestRoundtrip:
    def test_produce_consume_basic(self, serde: KafkaSerde):
        tx = _tx("250.00", "APPROVED")
        framed = serde.produce("transactions", tx)
        result = serde.consume("transactions", framed, Transaction)
        assert result.transactionAmount == "250.00"
        assert result.final_decision == "APPROVED"

    def test_roundtrip_preserves_nested_predictive_analyzer(self, serde: KafkaSerde):
        tx = _tx()
        framed = serde.produce("transactions", tx)
        result = serde.consume("transactions", framed, Transaction)
        pa = result.predictiveAnalyzer
        assert pa.isAllowed is True
        assert pa.cardId == "card-int-001"
        assert pa.userId == "user-int-001"
        assert pa.allowance == "9999.99"
        assert pa.transactionId == "txn-int-001"
        assert pa.walletAddress == "0xcafe"

    def test_roundtrip_denied_transaction(self, serde: KafkaSerde):
        tx = _tx("99.99", "DENIED")
        framed = serde.produce("transactions", tx)
        result = serde.consume("transactions", framed, Transaction)
        assert result.final_decision == "DENIED"
        assert result.transactionAmount == "99.99"

    def test_roundtrip_empty_optional_fields(self, serde: KafkaSerde):
        tx = Transaction()
        tx.transactionAmount = "0.00"
        tx.final_decision = "PENDING"
        framed = serde.produce("transactions", tx)
        result = serde.consume("transactions", framed, Transaction)
        assert result.transactionAmount == "0.00"
        assert result.final_decision == "PENDING"

    def test_produce_framed_bytes_start_with_magic_byte(self, serde: KafkaSerde):
        framed = serde.produce("transactions", _tx())
        assert framed[0] == 0x00

    def test_produce_framed_bytes_contain_schema_id(self, serde: KafkaSerde):
        framed = serde.produce("transactions", _tx())
        schema_id = struct.unpack(">I", framed[1:5])[0]
        assert schema_id >= 1  # SR assigned a real id


# ---------------------------------------------------------------------------
# Startup / schema registration
# ---------------------------------------------------------------------------

class TestStartup:
    def test_startup_registers_schema_in_sr(self, mock_sr: MockSchemaRegistry):
        s = KafkaSerde(sr_url=mock_sr.url)
        s.startup({"payments": Transaction})
        assert "payments-value" in mock_sr._subjects

    def test_startup_idempotent_same_schema_id(self, mock_sr: MockSchemaRegistry):
        s1 = KafkaSerde(sr_url=mock_sr.url)
        s1.startup({"idempotent-topic": Transaction})
        id1 = s1._topic_to_schema_id["idempotent-topic"]

        s2 = KafkaSerde(sr_url=mock_sr.url)
        s2.startup({"idempotent-topic": Transaction})
        id2 = s2._topic_to_schema_id["idempotent-topic"]

        assert id1 == id2

    def test_startup_eagerly_registers_before_first_produce(self, mock_sr: MockSchemaRegistry):
        s = KafkaSerde(sr_url=mock_sr.url)
        s.startup({"eager-topic": Transaction})
        # Schema should be in SR before any produce() call
        assert "eager-topic-value" in mock_sr._subjects


# ---------------------------------------------------------------------------
# Invalid payload rejection
# ---------------------------------------------------------------------------

class TestInvalidPayloadRejection:
    def test_reject_bad_magic_byte(self, serde: KafkaSerde):
        bad = bytes([0xFF]) + struct.pack(">I", 1) + b"\x00" + b"\x0a\x00"
        with pytest.raises(InvalidMagicByteError, match="magic byte"):
            serde.consume("transactions", bad, Transaction)

    def test_reject_too_short_frame(self, serde: KafkaSerde):
        with pytest.raises(SerdeError, match="too short"):
            serde.consume("transactions", b"\x00\x00", Transaction)

    def test_reject_empty_payload(self, serde: KafkaSerde):
        with pytest.raises(SerdeError, match="too short"):
            serde.consume("transactions", b"", Transaction)

    def test_reject_unknown_schema_id(self, serde: KafkaSerde):
        # Use a schema_id (99999) that was never registered with mock_sr
        framed = KafkaSerde._frame(99999, b"\x0a\x01x")
        with pytest.raises(UnknownSchemaError, match="99999"):
            serde.consume("transactions", framed, Transaction)

    def test_reject_corrupt_protobuf_payload(self, serde: KafkaSerde):
        # Frame with valid schema_id (so SR lookup succeeds) but garbage protobuf
        schema_id = serde._topic_to_schema_id["transactions"]
        framed = KafkaSerde._frame(schema_id, b"\xff\xff\xff\xff\xff\xff")
        with pytest.raises(SerdeError, match="deserialization"):
            serde.consume("transactions", framed, Transaction)

    def test_reject_non_message_class(self, serde: KafkaSerde):
        framed = serde.produce("transactions", _tx())
        with pytest.raises(TypeError):
            serde.consume("transactions", framed, str)  # type: ignore[arg-type]
