"""
Unit tests for protobuf_contracts.serde (thin) against a mock Schema Registry.

The mock is read-only (no register): it serves
  GET /subjects/{subject}/versions/latest  -> {"id": N}
  GET /schemas/ids/{id}/versions           -> [{"subject","version"}]
"""
import json
import struct
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from protobuf_contracts import Transaction, TransactionData, Customer, OnboardingCustomer
from protobuf_contracts.serde import (
    KafkaSerde,
    InvalidMagicByteError,
    FrameTooShortError,
    TopicNotBoundError,
    SchemaForeignError,
    MessageIndexMismatchError,
    DeserializeError,
    _encode_indexes,
)


def _make_mock_sr(subject_id: dict[str, int]):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silence
            pass

        def do_GET(self):
            parts = self.path.split("/")  # ["", "subjects", sub, "versions", "latest"]
            if parts[1:2] == ["subjects"] and parts[3:5] == ["versions", "latest"]:
                sub = parts[2]
                if sub in subject_id:
                    self._json(200, {"id": subject_id[sub]})
                else:
                    self._json(404, {})
            elif parts[1:3] == ["schemas", "ids"] and parts[4:5] == ["versions"]:
                sid = int(parts[3])
                out = [{"subject": s, "version": 1} for s, v in subject_id.items() if v == sid]
                self._json(200 if out else 404, out)
            else:
                self._json(404, {})

        def _json(self, code, body):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    return server, url


@pytest.fixture
def mock_sr():
    servers = []

    def factory(subject_id):
        server, url = _make_mock_sr(subject_id)
        servers.append(server)
        return url

    yield factory
    for s in servers:
        s.shutdown()


def _sample_tx():
    return Transaction(
        transaction=TransactionData(id="tx-1", amount_total="9.99", channel="web", type="PIX"),
        customer=Customer(name="Ada", email="ada@example.com"),
    )


def _frame(schema_id, msg_index_bytes, payload):
    return struct.pack(">BI", 0x00, schema_id) + msg_index_bytes + payload


# ── round-trip ───────────────────────────────────────────────────────────────


def test_roundtrip_transaction_index_0(mock_sr):
    serde = KafkaSerde(sr_url=mock_sr({"transactions-value": 42}))
    serde.bind("transactions", Transaction)

    original = _sample_tx()
    framed = serde.produce("transactions", original)
    assert framed[0] == 0x00
    assert struct.unpack(">I", framed[1:5])[0] == 42
    assert framed[5] == 0x00  # Transaction is the 1st message → single 0x00 byte

    got = serde.consume("transactions", framed)
    assert got == original


def test_roundtrip_variable_index(mock_sr):
    # OnboardingCustomer is the 2nd message in onboarding.proto (index 1).
    serde = KafkaSerde(sr_url=mock_sr({"cust-value": 7}))
    serde.bind("cust", OnboardingCustomer)
    original = OnboardingCustomer(id="c-1")
    framed = serde.produce("cust", original)
    assert framed[5:7] == b"\x02\x02"  # variable msg-index for index 1
    assert serde.consume("cust", framed) == original


# ── bind / not-bound ──────────────────────────────────────────────────────────


def test_bind_fails_when_subject_missing(mock_sr):
    serde = KafkaSerde(sr_url=mock_sr({}))
    with pytest.raises(SchemaForeignError):
        serde.bind("transactions", Transaction)


def test_produce_consume_unbound(mock_sr):
    serde = KafkaSerde(sr_url=mock_sr({"transactions-value": 42}))
    with pytest.raises(TopicNotBoundError):
        serde.produce("transactions", _sample_tx())
    with pytest.raises(TopicNotBoundError):
        serde.consume("transactions", b"\x00\x00\x00\x00\x2a\x00\x0a")


# ── consumer security rejections ──────────────────────────────────────────────


def test_invalid_magic(mock_sr):
    serde = KafkaSerde(sr_url=mock_sr({"transactions-value": 42}))
    serde.bind("transactions", Transaction)
    with pytest.raises(InvalidMagicByteError):
        serde.consume("transactions", b"\x01\x00\x00\x00\x2a\x00\x0a")


def test_frame_too_short(mock_sr):
    serde = KafkaSerde(sr_url=mock_sr({"transactions-value": 42}))
    serde.bind("transactions", Transaction)
    with pytest.raises(FrameTooShortError):
        serde.consume("transactions", b"\x00\x00\x00")


def test_foreign_subject_id(mock_sr):
    serde = KafkaSerde(sr_url=mock_sr({"transactions-value": 42, "other-value": 99}))
    serde.bind("transactions", Transaction)
    bad = _frame(99, _encode_indexes([0]), _sample_tx().SerializeToString())
    with pytest.raises(SchemaForeignError):
        serde.consume("transactions", bad)


def test_message_index_mismatch(mock_sr):
    serde = KafkaSerde(sr_url=mock_sr({"transactions-value": 42}))
    serde.bind("transactions", Transaction)  # expects index 0
    bad = _frame(42, _encode_indexes([1]), _sample_tx().SerializeToString())
    with pytest.raises(MessageIndexMismatchError):
        serde.consume("transactions", bad)


def test_invalid_payload(mock_sr):
    serde = KafkaSerde(sr_url=mock_sr({"transactions-value": 42}))
    serde.bind("transactions", Transaction)
    bad = _frame(42, _encode_indexes([0]), b"\xff\xff\xff")
    with pytest.raises(DeserializeError):
        serde.consume("transactions", bad)
