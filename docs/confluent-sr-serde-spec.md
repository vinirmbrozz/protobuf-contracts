# Confluent Schema Registry Serde Specification

**Version:** 1.0  
**Status:** Authoritative  
**Owner:** Platform Engineer (ROD-14)  
**Audience:** Senior Go, Senior Node-TS, Senior Python engineers implementing per-language Kafka libraries

---

## 1. Purpose

This document is the **single source of truth** for how Truther services serialize and deserialize
Protobuf messages over Kafka using Confluent Schema Registry. Every producer and consumer across
Go, Node and Python **must** implement exactly this contract. Deviations require a versioned update
to this spec and CTO sign-off.

---

## 2. Wire Format

Every Kafka message value produced or consumed by a Truther service uses the **Confluent envelope**:

```
┌─────────────┬─────────────────────────────┬──────────────────────┬─────────────────────────────┐
│  Magic Byte │        Schema ID             │   Message Index      │     Protobuf Payload        │
│   1 byte    │        4 bytes               │   1–N bytes          │     variable length         │
│   0x00      │   big-endian int32           │   varint array       │   proto3 binary encoding    │
└─────────────┴─────────────────────────────┴──────────────────────┴─────────────────────────────┘
```

### 2.1 Magic Byte

Always `0x00`. Consumers MUST reject any message whose first byte is not `0x00`.

### 2.2 Schema ID

A 4-byte big-endian unsigned integer assigned by the Schema Registry when the schema is registered.
Producers MUST obtain this ID before publishing. Consumers MUST validate that this ID is registered
in the Schema Registry before deserializing.

### 2.3 Message Index Array

The Confluent Protobuf serializer includes a message-index array after the schema ID to identify
which message type within a `.proto` file is serialized.

Encoding:
- A single `0x00` byte for the **common case**: the message is the first (and usually only) top-level
  message in the registered `.proto` schema.
- For nested or non-first messages: a zigzag-varint array `[count, index0, index1, ...]` where
  `count` is the number of index entries minus one.

**Truther standard:** Each `.proto` file registers **one message per schema subject**. With this
convention the message index is always `0x00`.

### 2.4 Protobuf Payload

Standard proto3 binary encoding of the message. No length prefix is added — the Kafka message
boundary defines the payload end.

### 2.5 Concrete Go framing example

```go
// Frame a serialized Transaction for Kafka.
func FrameMessage(schemaID int32, msgBytes []byte) []byte {
    out := make([]byte, 6+len(msgBytes))
    out[0] = 0x00                                     // magic byte
    binary.BigEndian.PutUint32(out[1:5], uint32(schemaID)) // schema ID
    out[5] = 0x00                                     // message index (first message)
    copy(out[6:], msgBytes)
    return out
}

// Parse the Confluent envelope.
func ParseFrame(data []byte) (schemaID int32, msgBytes []byte, err error) {
    if len(data) < 6 {
        return 0, nil, fmt.Errorf("frame too short: %d bytes", len(data))
    }
    if data[0] != 0x00 {
        return 0, nil, fmt.Errorf("invalid magic byte: 0x%02x", data[0])
    }
    schemaID = int32(binary.BigEndian.Uint32(data[1:5]))
    // data[5] is the message index (0x00 = first message, per Truther convention)
    return schemaID, data[6:], nil
}
```

---

## 3. Subject Naming Strategy

### 3.1 Proposed strategies (pending CTO decision — see §8)

| Strategy | Subject name pattern | Example |
|---|---|---|
| **TopicNameStrategy** | `<topic-name>-value` | `transactions-value` |
| **RecordNameStrategy** | `<package>.<MessageName>` | `truther.transaction.Transaction` |

### 3.2 Recommended: TopicNameStrategy

Until a decision is made, implementations MUST use **TopicNameStrategy** (`<topic>-value`). This
keeps the Schema Registry subject tied to the Kafka topic, making it easy to reason about which
schema governs which topic.

RecordNameStrategy is useful when the same message type is produced to multiple topics — defer to
CTO to decide if/when that pattern applies.

---

## 4. Schema Registration

### 4.1 Producer flow

```
1. At service startup, for each message type M produced to topic T:
   a. subject = "<T>-value"
   b. schema = M.proto file descriptor (the .proto file serialized as a FileDescriptorProto)
   c. Call SR POST /subjects/<subject>/versions with the schema
   d. Cache the returned schema_id in memory
2. On each Produce(msg):
   a. Serialize msg to proto3 binary → msgBytes
   b. Write frame: [0x00][schema_id_be4][0x00][msgBytes]
   c. Publish framed bytes to Kafka
```

Producers MUST register schemas **eagerly at startup**, not lazily on first message. This ensures
the schema is available before the first consumer starts.

### 4.2 Consumer flow

```
1. On each consumed Kafka message value:
   a. Check byte[0] == 0x00; reject if not
   b. schema_id = BigEndian(bytes[1:5])
   c. Verify schema_id is registered (cache SR lookup by ID)
   d. message_type = resolve(schema_id) → known registered type
   e. Deserialize bytes[6:] using the known type's proto3 decoder
   f. Validate: run protovalidate rules on the decoded message
   g. Route to handler; on any error → DLQ
```

### 4.3 Unknown / invalid payloads

- Magic byte mismatch → **drop + DLQ + metric**
- Schema ID not in registry → **drop + DLQ + metric**
- Schema ID registered but unknown to this service → **drop + DLQ + alert** (deployment lag)
- Protobuf deserialization error → **drop + DLQ + metric**
- Protovalidate violation → **drop + DLQ + metric**

Consumers MUST NOT panic or crash on invalid payloads. Every rejection MUST emit a metric.

---

## 5. Compatibility Mode

All subjects use **BACKWARD** compatibility (Schema Registry default).

| Compatibility | Meaning |
|---|---|
| BACKWARD | New schema can read data written with the previous schema |
| FORWARD | Previous schema can read data written with the new schema |
| FULL | Both BACKWARD and FORWARD |

### 5.1 Safe evolution rules (BACKWARD)

These changes are safe without a schema version bump:
- Adding an **optional** field (proto3 field with a new field number)
- Adding a new enum value

These changes REQUIRE a new version/migration (see §6):
- Removing or renaming a field
- Changing a field's type
- Changing a field's field number
- Adding a `required` constraint to an existing field (via protovalidate)

---

## 6. Protovalidate Rules

Validation is enforced **at the consumer** using [protovalidate](https://github.com/bufbuild/protovalidate).

Standard rules for Truther messages:
- All ID fields (`*_id`, `*Id`) — `string.min_len: 1` (non-empty UUID-like)
- Amount fields (`transaction_amount`) — `string.pattern: "^[0-9]+(\\.[0-9]{1,8})?$"`
- Boolean decision fields — no special rule, default false is valid

Example proto annotation (to add after CTO confirms buf dependency):
```protobuf
import "buf/validate/validate.proto";

message PredictiveAnalyzer {
  bool isAllowed = 1;
  string reason = 2;
  string cardId = 3 [(buf.validate.field).string.min_len = 1];
  string userId = 4 [(buf.validate.field).string.min_len = 1];
  // ...
}
```

---

## 7. Schema Registry Configuration

### 7.1 Compatibility setting

Set `BACKWARD` on each subject at registration time:

```
PUT /config/<subject>
{ "compatibility": "BACKWARD" }
```

Or set it globally (preferred during bootstrap):
```
PUT /config
{ "compatibility": "BACKWARD" }
```

### 7.2 Subject auto-creation

Schema Registry auto-creates subjects on first registration. No manual setup is required per-subject.

### 7.3 Connection configuration (per language)

The Schema Registry URL is read from the environment variable `SCHEMA_REGISTRY_URL`.
Credentials (if any) from `SCHEMA_REGISTRY_API_KEY` and `SCHEMA_REGISTRY_API_SECRET`.

---

## 8. Pending Decisions (awaiting CTO approval)

These decisions are **not yet made**. Implementations MUST NOT assume any of these until they are
resolved via an interaction on [ROD-14](/ROD/issues/ROD-14):

| # | Decision | Option A | Option B | Recommendation |
|---|---|---|---|---|
| D1 | **Kafka client per language** | confluent-kafka-go / kafkajs / confluent-kafka-python | sarama (Go) / node-rdkafka / aiokafka | Use official Confluent clients: they ship native SR + serde integration |
| D2 | **Schema Registry location** | docker-compose local for dev | Confluent Cloud managed / self-hosted in prod | docker-compose for dev; managed for prod |
| D3 | **Subject naming strategy** | TopicNameStrategy (`<topic>-value`) | RecordNameStrategy (`pkg.MessageName`) | TopicNameStrategy (simpler, one schema per topic) |

Until D3 is resolved, use **TopicNameStrategy** as the default (see §3.2).

---

## 9. Typed API Contract

Language seniors MUST implement the following interface contract:

```
Producer<M extends KnownMessage>:
  - produce(topic: string, msg: M): void
  - Internally: frame with Confluent envelope, register schema on startup

Consumer<M extends KnownMessage>:
  - subscribe(topic: string, handler: (msg: M) => void): void
  - Internally: validate magic byte, resolve schema_id, deserialize, validate

DLQ routing:
  - Any rejection goes to: <topic>-dead-letter
  - DLQ messages carry original bytes + rejection reason header
```

Only types generated from `proto/` files are valid as `M`. Consumers MUST NOT accept raw `bytes`
without going through the typed deserializer.

---

## 10. Reference Implementation

See [`interop/harness.js`](../interop/harness.js) for a Node.js reference implementation of:
- Confluent envelope framing / unframing
- Round-trip serialize → frame → unframe → deserialize
- Rejection of invalid magic byte
- Rejection of unknown schema_id

The harness is executable today without Kafka or Schema Registry (uses mock schema_id = 1).
Cross-language equivalents in [`interop/go/`](../interop/go/) and [`interop/python/`](../interop/python/)
will pass once the per-language Kafka libs exist.
