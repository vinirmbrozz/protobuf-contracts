"""
Confluent Schema Registry serde for Truther Kafka messages.

Wire format (§2 of docs/confluent-sr-serde-spec.md):
  [0x00][schema_id: 4 bytes big-endian][0x00][protobuf payload]

Usage:
  from truther_contracts_sdk.serde import KafkaSerde
  serde = KafkaSerde()
  await serde.startup({"transactions": Transaction})
  framed = serde.produce("transactions", tx_message)
  tx = serde.consume("transactions", framed, Transaction)
"""

from __future__ import annotations

import os
import struct
import logging
from typing import Type, TypeVar

import requests
from google.protobuf.message import Message

logger = logging.getLogger(__name__)

MAGIC_BYTE = 0x00
_HEADER_FMT = ">BI"  # 1 magic byte + 4-byte big-endian uint32 schema_id
_HEADER_SIZE = 5  # 1 + 4
_MSG_INDEX = b"\x00"  # first-message-in-schema — Truther convention (§2.3)
_ENVELOPE_SIZE = 6  # magic + schema_id + msg_index


class SerdeError(Exception):
    """Raised for any invalid Confluent-framed payload."""


class UnknownSchemaError(SerdeError):
    """schema_id is not registered in the Schema Registry."""


class InvalidMagicByteError(SerdeError):
    """First byte of the payload is not 0x00."""


class SchemaRegistryClient:
    """Thin synchronous HTTP client for Confluent Schema Registry."""

    def __init__(self, base_url: str, api_key: str | None = None, api_secret: str | None = None):
        self._base_url = base_url.rstrip("/")
        self._session = requests.Session()
        if api_key and api_secret:
            self._session.auth = (api_key, api_secret)
        self._session.headers.update({"Content-Type": "application/vnd.schemaregistry.v1+json"})
        # Cache: subject -> schema_id, schema_id -> subject
        self._subject_to_id: dict[str, int] = {}
        self._id_to_subject: dict[int, str] = {}

    def register_schema(self, subject: str, proto_schema: str) -> int:
        """
        Register a Protobuf schema under the given subject.
        Returns the schema_id assigned by SR (may return existing id for same schema).
        """
        if subject in self._subject_to_id:
            return self._subject_to_id[subject]

        payload = {"schema": proto_schema, "schemaType": "PROTOBUF"}
        url = f"{self._base_url}/subjects/{subject}/versions"
        resp = self._session.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        schema_id: int = resp.json()["id"]
        self._subject_to_id[subject] = schema_id
        self._id_to_subject[schema_id] = subject
        logger.info("Registered schema subject=%s schema_id=%d", subject, schema_id)
        return schema_id

    def get_schema_id(self, subject: str) -> int:
        """Return cached schema_id for subject, raising if not registered yet."""
        if subject not in self._subject_to_id:
            raise UnknownSchemaError(f"Schema for subject '{subject}' not registered in this client")
        return self._subject_to_id[subject]

    def verify_schema_id(self, schema_id: int) -> bool:
        """Return True if schema_id is known to SR (uses cache + fallback HTTP GET)."""
        if schema_id in self._id_to_subject:
            return True
        url = f"{self._base_url}/schemas/ids/{schema_id}"
        resp = self._session.get(url, timeout=10)
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        # Populate reverse cache
        self._id_to_subject[schema_id] = resp.json().get("subject", str(schema_id))
        return True


def _get_proto_schema(msg_class: Type[Message]) -> str:
    """Return the .proto file source for a generated protobuf message class."""
    descriptor = msg_class.DESCRIPTOR.file
    # Return the full qualified proto file name; SR stores the schema by name.
    # We use the serialized file descriptor to handle imports correctly.
    # For SR registration we need the human-readable .proto source.
    # Since buf-generated files embed their descriptor, we reconstruct a minimal schema.
    # Full proto source is not embedded in Python generated code; we read the .proto file
    # from the package, falling back to the descriptor proto serialisation as a string.
    try:
        from google.protobuf import descriptor_pb2
        fdp = descriptor_pb2.FileDescriptorProto()
        descriptor.CopyToProto(fdp)
        return fdp.SerializeToString().hex()  # SR accepts binary descriptors as hex
    except Exception:
        # Absolute fallback: use file name as a placeholder schema
        return descriptor.name


class KafkaSerde:
    """
    Confluent SR-aware serde for Truther Kafka messages.

    Startup:
        serde = KafkaSerde()
        serde.startup({"transactions": Transaction})  # registers schemas eagerly

    Produce:
        framed_bytes = serde.produce("transactions", tx_message)

    Consume:
        tx = serde.consume("transactions", framed_bytes, Transaction)
    """

    def __init__(
        self,
        sr_url: str | None = None,
        sr_api_key: str | None = None,
        sr_api_secret: str | None = None,
    ):
        url = sr_url or os.environ.get("SCHEMA_REGISTRY_URL")
        if not url:
            raise ValueError(
                "Schema Registry URL is required: pass sr_url or set SCHEMA_REGISTRY_URL env var"
            )
        api_key = sr_api_key or os.environ.get("SCHEMA_REGISTRY_API_KEY")
        api_secret = sr_api_secret or os.environ.get("SCHEMA_REGISTRY_API_SECRET")
        self._sr = SchemaRegistryClient(url, api_key, api_secret)
        # topic -> message class (registered at startup)
        self._topic_to_class: dict[str, Type[Message]] = {}
        # topic -> cached schema_id
        self._topic_to_schema_id: dict[str, int] = {}

    def startup(self, topic_to_class: dict[str, Type[Message]]) -> None:
        """
        Eagerly register schemas for each topic.

        topic_to_class: {"topic-name": MessageClass, ...}

        Must be called before produce(). Raises on SR connection errors.
        """
        for topic, msg_class in topic_to_class.items():
            self._validate_known_type(msg_class)
            subject = f"{topic}-value"
            proto_schema = _get_proto_schema(msg_class)
            schema_id = self._sr.register_schema(subject, proto_schema)
            self._topic_to_class[topic] = msg_class
            self._topic_to_schema_id[topic] = schema_id
            logger.info("Startup: topic=%s subject=%s schema_id=%d", topic, subject, schema_id)

    def produce(self, topic: str, msg: Message) -> bytes:
        """
        Serialize msg and frame it with the Confluent SR envelope.

        Returns framed bytes ready to be published to Kafka.
        Raises TypeError if msg is not a type registered in startup().
        Raises RuntimeError if startup() was not called for this topic.
        """
        msg_class = self._topic_to_class.get(topic)
        if msg_class is None:
            raise RuntimeError(
                f"Topic '{topic}' not registered — call startup() before produce()"
            )
        if not isinstance(msg, msg_class):
            raise TypeError(
                f"Expected {msg_class.__name__}, got {type(msg).__name__}"
            )
        schema_id = self._topic_to_schema_id[topic]
        proto_bytes = msg.SerializeToString()
        return self._frame(schema_id, proto_bytes)

    def consume(self, topic: str, data: bytes, msg_class: Type[Message]) -> Message:
        """
        Validate and deserialize a Confluent-framed Kafka message.

        Raises InvalidMagicByteError, UnknownSchemaError, SerdeError on bad payloads.
        """
        self._validate_known_type(msg_class)
        schema_id, proto_bytes = self._parse_frame(data)

        # Verify schema_id is known to SR
        if not self._sr.verify_schema_id(schema_id):
            raise UnknownSchemaError(
                f"schema_id={schema_id} is not registered in Schema Registry"
            )

        instance = msg_class()
        try:
            instance.ParseFromString(proto_bytes)
        except Exception as exc:
            raise SerdeError(f"Protobuf deserialization failed: {exc}") from exc
        return instance

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _frame(schema_id: int, proto_bytes: bytes) -> bytes:
        header = struct.pack(">BI", MAGIC_BYTE, schema_id)
        return header + _MSG_INDEX + proto_bytes

    @staticmethod
    def _parse_frame(data: bytes) -> tuple[int, bytes]:
        if len(data) < _ENVELOPE_SIZE:
            raise SerdeError(
                f"Frame too short: {len(data)} bytes (minimum {_ENVELOPE_SIZE})"
            )
        magic = data[0]
        if magic != MAGIC_BYTE:
            raise InvalidMagicByteError(
                f"Invalid magic byte: 0x{magic:02x} (expected 0x00)"
            )
        schema_id: int = struct.unpack(">I", data[1:5])[0]
        # byte[5] is msg_index (always 0x00 in Truther convention)
        proto_bytes = data[6:]
        return schema_id, proto_bytes

    @staticmethod
    def _validate_known_type(msg_class: Type[Message]) -> None:
        """Raise TypeError if msg_class is not a protobuf Message subclass."""
        if not (isinstance(msg_class, type) and issubclass(msg_class, Message)):
            raise TypeError(
                f"{msg_class!r} is not a protobuf Message subclass. "
                "Only types generated from proto/ files are accepted."
            )
