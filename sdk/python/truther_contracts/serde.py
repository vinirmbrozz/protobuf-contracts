"""
Thin Confluent Schema Registry serde (Decision A).

The SDK never reads a .proto and never registers schemas (that is the
registrador's job, out of band). bind() resolves a topic's schema_id from the
Schema Registry (read-only); produce() stamps the Confluent envelope; consume()
validates it strictly and deserializes into the bound type.

Wire format:
  [0x00 magic] [schema_id: 4 bytes BE] [message-index] [proto3 payload]

The message-index is variable length (Confluent): a single top-level message at
index 0 is the 1-byte 0x00 optimization; otherwise zig-zag varints (count, then
each index). The index is derived natively from the message descriptor.

Usage:
  from truther_contracts import Transaction
  from truther_contracts.serde import KafkaSerde

  serde = KafkaSerde()                       # reads SCHEMA_REGISTRY_URL
  serde.startup({"transactions": Transaction})  # resolves schema_ids
  framed = serde.produce("transactions", tx)
  tx = serde.consume("transactions", framed) # -> Transaction
"""
from __future__ import annotations

import os
import struct
from typing import Type

import requests
from google.protobuf.message import Message

MAGIC_BYTE = 0x00


class SerdeError(Exception):
    """Base for any rejected payload; the adapter routes these to the DLQ."""


class InvalidMagicByteError(SerdeError):
    """First byte is not 0x00."""


class FrameTooShortError(SerdeError):
    """Frame is shorter than the minimum envelope."""


class TopicNotBoundError(SerdeError):
    """bind() was not called for this topic."""


class SchemaForeignError(SerdeError):
    """schema_id is not a registered version of this topic's subject."""


class MessageIndexMismatchError(SerdeError):
    """Envelope message-index does not match the bound type."""


class DeserializeError(SerdeError):
    """Protobuf payload failed to decode."""


# ── Schema Registry REST client (read-only) ──────────────────────────────────


class SchemaRegistryClient:
    """Read-only Confluent SR client. Resolves ids; never registers."""

    def __init__(self, base_url: str, api_key: str | None = None, api_secret: str | None = None):
        self._base_url = base_url.rstrip("/")
        self._session = requests.Session()
        if api_key and api_secret:
            self._session.auth = (api_key, api_secret)
        self._session.headers.update({"Accept": "application/vnd.schemaregistry.v1+json"})
        self._id_subject_ok: set[tuple[int, str]] = set()

    def latest_id(self, subject: str) -> int:
        """Resolve the latest registered schema id for a subject."""
        url = f"{self._base_url}/subjects/{subject}/versions/latest"
        resp = self._session.get(url, timeout=10)
        if resp.status_code == 404:
            raise SchemaForeignError(f"subject '{subject}' is not registered in Schema Registry")
        resp.raise_for_status()
        return int(resp.json()["id"])

    def id_belongs_to_subject(self, schema_id: int, subject: str) -> bool:
        """True iff schema_id is a registered version of subject (cached)."""
        if (schema_id, subject) in self._id_subject_ok:
            return True
        url = f"{self._base_url}/schemas/ids/{schema_id}/versions"
        resp = self._session.get(url, timeout=10)
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        for pair in resp.json():
            if pair.get("subject") == subject:
                self._id_subject_ok.add((schema_id, subject))
                return True
        return False


# ── message-index (Confluent) ────────────────────────────────────────────────


def _message_indexes(msg_class: Type[Message]) -> list[int]:
    """Declaration-order index path of a (top-level) message in its file.

    The Python message Descriptor exposes no ``index`` for top-level types, so we
    read declaration order from the FileDescriptorProto (reliable across the C++
    and upb runtimes).
    """
    from google.protobuf import descriptor_pb2

    desc = msg_class.DESCRIPTOR
    fdp = descriptor_pb2.FileDescriptorProto()
    desc.file.CopyToProto(fdp)
    for i, mt in enumerate(fdp.message_type):
        if mt.name == desc.name:
            return [i]
    raise SerdeError(f"message {desc.full_name} not found in its file descriptor")


def _encode_indexes(indexes: list[int]) -> bytes:
    if len(indexes) == 1 and indexes[0] == 0:
        return b"\x00"
    out = bytearray()
    _append_zigzag(out, len(indexes))
    for idx in indexes:
        _append_zigzag(out, idx)
    return bytes(out)


def _append_zigzag(out: bytearray, value: int) -> None:
    zz = (value << 1) & 0xFFFFFFFF  # indexes are small non-negative
    while zz >= 0x80:
        out.append((zz & 0x7F) | 0x80)
        zz >>= 7
    out.append(zz)


def _read_zigzag(data: bytes, offset: int) -> tuple[int, int]:
    ux = 0
    shift = 0
    n = 0
    while True:
        if offset + n >= len(data):
            raise FrameTooShortError("truncated message-index varint")
        b = data[offset + n]
        n += 1
        ux |= (b & 0x7F) << shift
        if b < 0x80:
            break
        shift += 7
    return (ux >> 1) ^ -(ux & 1), offset + n


def _read_indexes(data: bytes, offset: int) -> tuple[list[int], int]:
    count, offset = _read_zigzag(data, offset)
    if count == 0:  # 1-byte optimization → [0]
        return [0], offset
    indexes: list[int] = []
    for _ in range(count):
        idx, offset = _read_zigzag(data, offset)
        indexes.append(idx)
    return indexes, offset


# ── Serde ─────────────────────────────────────────────────────────────────────


class _Binding:
    __slots__ = ("msg_class", "subject", "schema_id", "msg_index_bytes", "indexes")

    def __init__(self, msg_class, subject, schema_id, msg_index_bytes, indexes):
        self.msg_class = msg_class
        self.subject = subject
        self.schema_id = schema_id
        self.msg_index_bytes = msg_index_bytes
        self.indexes = indexes


class KafkaSerde:
    """Thin Confluent SR serde — resolves ids, frames, and validates strictly."""

    def __init__(
        self,
        sr_url: str | None = None,
        sr_api_key: str | None = None,
        sr_api_secret: str | None = None,
    ):
        url = sr_url or os.environ.get("SCHEMA_REGISTRY_URL")
        if not url:
            raise ValueError("Schema Registry URL required: pass sr_url or set SCHEMA_REGISTRY_URL")
        self._sr = SchemaRegistryClient(
            url,
            sr_api_key or os.environ.get("SCHEMA_REGISTRY_API_KEY"),
            sr_api_secret or os.environ.get("SCHEMA_REGISTRY_API_SECRET"),
        )
        self._bindings: dict[str, _Binding] = {}

    def bind(self, topic: str, msg_class: Type[Message]) -> None:
        """Map topic→type and resolve its schema_id from SR (read-only)."""
        _require_message(msg_class)
        subject = f"{topic}-value"
        schema_id = self._sr.latest_id(subject)
        indexes = _message_indexes(msg_class)
        self._bindings[topic] = _Binding(
            msg_class, subject, schema_id, _encode_indexes(indexes), indexes
        )

    def startup(self, topic_to_class: dict[str, Type[Message]]) -> None:
        """Bind every topic→type pair."""
        for topic, msg_class in topic_to_class.items():
            self.bind(topic, msg_class)

    def produce(self, topic: str, msg: Message) -> bytes:
        """Serialize msg and wrap it in the Confluent envelope."""
        b = self._bindings.get(topic)
        if b is None:
            raise TopicNotBoundError(f"topic '{topic}' not bound; call bind() at startup")
        if not isinstance(msg, b.msg_class):
            raise TypeError(f"expected {b.msg_class.__name__}, got {type(msg).__name__}")
        header = struct.pack(">BI", MAGIC_BYTE, b.schema_id)
        return header + b.msg_index_bytes + msg.SerializeToString()

    def consume(self, topic: str, data: bytes) -> Message:
        """Validate the envelope and deserialize into the bound type (typed errors → DLQ)."""
        b = self._bindings.get(topic)
        if b is None:
            raise TopicNotBoundError(f"topic '{topic}' not bound; call bind() at startup")

        if len(data) < 6:
            raise FrameTooShortError(f"frame too short: {len(data)} bytes (minimum 6)")
        if data[0] != MAGIC_BYTE:
            raise InvalidMagicByteError(f"invalid magic byte: 0x{data[0]:02x} (expected 0x00)")
        schema_id = struct.unpack(">I", data[1:5])[0]
        indexes, offset = _read_indexes(data, 5)
        payload = data[offset:]

        if not self._sr.id_belongs_to_subject(schema_id, b.subject):
            raise SchemaForeignError(
                f"schema_id={schema_id} is not a registered version of '{b.subject}'"
            )
        if indexes != b.indexes:
            raise MessageIndexMismatchError(f"message-index {indexes} != bound {b.indexes}")

        instance = b.msg_class()
        try:
            instance.ParseFromString(payload)
        except Exception as exc:  # noqa: BLE001 — any decode failure → typed error for DLQ
            raise DeserializeError(f"protobuf deserialization failed: {exc}") from exc
        return instance


def _require_message(msg_class: Type[Message]) -> None:
    if not (isinstance(msg_class, type) and issubclass(msg_class, Message)):
        raise TypeError(f"{msg_class!r} is not a protobuf Message subclass")
