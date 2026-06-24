# Confluent Schema Registry Serde Specification

**Version:** 1.0  
**Status:** Authoritative  
**Owner:** Platform Engineer (ROD-14)  
**Audience:** Senior Go, Senior Node-TS, Senior Python engineers implementing per-language Kafka libraries

---

## 1. Purpose

This document is the **single source of truth** for how Protobuf services serialize and deserialize
Protobuf messages over Kafka using Confluent Schema Registry. Every producer and consumer across
Go, Node and Python **must** implement exactly this contract. Deviations require a versioned update
to this spec and CTO sign-off.

---

## 2. Wire Format

Every Kafka message value produced or consumed by a Protobuf service uses the **Confluent envelope**:

```
┌─────────────┬─────────────────────────────┬──────────────────────┬─────────────────────────────┐
│  Magic Byte │        Schema ID            │   Message Index      │     Protobuf Payload        │
│   1 byte    │        4 bytes              │   1–N bytes          │     variable length         │
│   0x00      │   big-endian int32          │   varint array       │   proto3 binary encoding    │
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

Encoding (Confluent):
- A single `0x00` byte is the optimization for the **first** top-level message (index `[0]`).
- Otherwise a zig-zag varint array: `count`, then each index. A top-level message at index `i`
  is encoded as `count=1, index=i`.

The index identifies **which** message of the `.proto` is serialized, by declaration order. The SDKs
**derive it from the type's descriptor** — generic for any message, no hard-coded convention.

> ⚠️ A `.proto` can declare several messages. In `transaction.proto`, `Transaction` is index 0 and
> `Device`, `TransactionData`, … follow — so the index is **not** universally `0x00`. A consumer that
> hard-coded the index would misread non-first messages (and break interop with other Confluent consumers).

### 2.4 Protobuf Payload

Standard proto3 binary encoding of the message. No length prefix is added — the Kafka message
boundary defines the payload end.

### 2.5 Concrete Go framing example

```go
// Frame a serialized message. msgIndex is the Confluent message-index bytes for
// the message's position in its .proto (§2.3); the SDKs derive it from the type
// descriptor (a single 0x00 for the first message). See sdk/go/serde.go.
func FrameMessage(schemaID int32, msgIndex, msgBytes []byte) []byte {
    out := make([]byte, 0, 5+len(msgIndex)+len(msgBytes))
    out = append(out, 0x00)                                  // magic byte
    out = binary.BigEndian.AppendUint32(out, uint32(schemaID))
    out = append(out, msgIndex...)                           // variable-length message-index
    return append(out, msgBytes...)
}

// Parse the Confluent envelope. The payload starts AFTER the variable-length
// message-index, so it must be read with a zig-zag varint reader (see the SDKs);
// it is not a fixed offset.
func ParseFrame(data []byte) (schemaID int32, err error) {
    if len(data) < 6 {
        return 0, fmt.Errorf("frame too short: %d bytes", len(data))
    }
    if data[0] != 0x00 {
        return 0, fmt.Errorf("invalid magic byte: 0x%02x", data[0])
    }
    return int32(binary.BigEndian.Uint32(data[1:5])), nil
}
```

---

## 3. Subject Naming Strategy

### 3.1 Proposed strategies (pending CTO decision — see §8)

| Strategy | Subject name pattern | Example |
|---|---|---|
| **TopicNameStrategy** | `<topic-name>-value` | `transactions-value` |
| **RecordNameStrategy** | `<package>.<MessageName>` | `protobuf.transaction.Transaction` |

### 3.2 Confirmed: TopicNameStrategy

Implementations MUST use **TopicNameStrategy** (`<topic>-value`). This keeps the Schema Registry
subject tied to the Kafka topic, making it easy to reason about which schema governs which topic.
Confirmed by CTO/founder on 2026-06-05 (see §8).

RecordNameStrategy would only apply if the same message type is produced to multiple topics.
That pattern is not in scope now; any future change requires a new CTO decision.

---

## 4. Schema Registration

### 4.0 Who registers (out of band — NOT the SDK)

Schemas are registered by the contracts repo's **registrador** (`scripts/register_schemas.py`),
which reads the real `.proto` and is the **only** writer to the Registry. The language SDKs **never
register** — they only resolve ids. This keeps the SDKs thin and means a service never carries a
`.proto` at runtime.

```
(build/ops) registrador, per topic T in scripts/schemas.json:
  a. subject = "<T>-value"
  b. POST /subjects/<subject>/versions { schemaType: PROTOBUF, schema: <.proto text>, references }
  c. SR assigns schema_id
```

### 4.1 Producer flow (in the service)

```
1. At startup, bind topic T → message type M:
   a. GET /subjects/<T>-value/versions/latest → schema_id   (resolve; read-only)
   b. cache schema_id; precompute M's message-index from its descriptor (§2.3)
2. On each Produce(msg):
   a. Serialize msg to proto3 binary → msgBytes
   b. Write frame: [0x00][schema_id_be4][message-index][msgBytes]
   c. Publish framed bytes to Kafka
```

The producer **resolves** the id at startup (fail-fast if the subject isn't registered yet) — it does
not register.

### 4.2 Consumer flow

```
1. On each consumed Kafka message value:
   a. Check byte[0] == 0x00; reject if not
   b. schema_id = BigEndian(bytes[1:5]); read the variable-length message-index
   c. Verify schema_id is a **registered version of this topic's subject** (GET
      /schemas/ids/<id>/versions; reject ids from another subject or unknown to SR)
   d. Verify the message-index matches the bound type; deserialize into it
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

Standard rules for Protobuf messages:
- Required ID fields (`id`) — `string.min_len: 1` (non-empty)
- Amount fields (`amount_total`) — decimal string, validated `> 0` via CEL
- Enum fields (`pix_key_type`) — `repeated.items.enum.defined_only`

protovalidate is wired (dep in `buf.yaml`, resolved by `buf dep update` in CI). Example, as used in
`transaction.proto`:
```protobuf
import "buf/validate/validate.proto";

message TransactionData {
  string id = 1 [(buf.validate.field).string.min_len = 1];
  string amount_total = 4 [(buf.validate.field).cel = {
    id: "transaction.amount_total.gt_zero"
    message: "amount_total must be a decimal string greater than 0"
    expression: "this.matches('^[0-9]+([.][0-9]+)?$') && double(this) > 0.0"
  }];
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

## 8. Confirmed Decisions (resolved 2026-06-05 by CTO/founder via [ROD-14](/ROD/issues/ROD-14))

| # | Decision | **Resolution** |
|---|---|---|
| D1 | **Kafka client per language** | **Confluent official**: confluent-kafka-go + kafkajs + confluent-kafka-python |
| D2 | **Schema Registry location** | **docker-compose** for local dev + **self-hosted** Confluent Schema Registry for prod |
| D3 | **Subject naming strategy** | **TopicNameStrategy**: `<topic>-value` |

These decisions are now final. Language seniors MUST use the Confluent clients listed in D1 and
MUST NOT introduce other Kafka clients without a new CTO-approved decision.

### 8.1 Local dev Schema Registry (docker-compose)

See [`docker-compose.yml`](../docker-compose.yml) in the repo root. Start with:

```bash
docker-compose up -d schema-registry
# Schema Registry available at http://localhost:8081
```

Set `SCHEMA_REGISTRY_URL=http://localhost:8081` in your local `.env`.

### 8.2 Production Schema Registry

Self-hosted Confluent Schema Registry deployment details are owned by the infrastructure team.
The per-language Kafka libs MUST read `SCHEMA_REGISTRY_URL`, `SCHEMA_REGISTRY_API_KEY`, and
`SCHEMA_REGISTRY_API_SECRET` from environment variables so that the same code works against
both local and production registries without changes.

---

## 9. Typed API Contract

Language seniors MUST implement the following interface contract using the Confluent client
libraries confirmed in §8 (D1).

**Client libraries:**
- Go: `github.com/confluentinc/confluent-kafka-go/kafka` + `github.com/confluentinc/confluent-kafka-go/schemaregistry`
- Node/TS: `kafkajs` + `@confluentinc/schemaregistry`  
- Python: `confluent-kafka` (includes `confluent_kafka.schema_registry`)

```
Producer<M extends KnownMessage>:
  - produce(topic: string, msg: M): void
  - Internally: register schema at startup, frame with Confluent envelope (§2), publish

Consumer<M extends KnownMessage>:
  - subscribe(topic: string, handler: (msg: M) => void): void
  - Internally: validate magic byte, resolve schema_id via SR cache, deserialize, validate

DLQ routing:
  - Any rejection goes to: <topic>-dead-letter
  - DLQ messages carry original bytes + rejection reason header
```

Only types generated from `proto/` files are valid as `M`. Consumers MUST NOT accept raw `bytes`
without going through the typed deserializer.

---

## 10. Reference Implementation

The authoritative reference implementations are the SDKs: [`sdk/go`](../sdk/go/),
[`sdk/node`](../sdk/node/) and [`sdk/python`](../sdk/python/) — each implements framing/unframing,
the variable-length message-index (§2.3), schema_id resolution, subject-scoped validation, and
typed rejections (→ DLQ).

The [`interop/`](../interop/) harness proves they interoperate: each language produces a frame and all
three consume + verify it, against a **real** Schema Registry. Run it with
`node interop/orchestrate.mjs` (after `docker compose up -d` + `scripts/register_schemas.py`); it also
runs in CI (`buf-ci.yml`, job `interop`). The full 3×3 matrix is green — see
[`interop/README.md`](../interop/README.md).

---

## 11. Security Model

The Confluent envelope (§2) is a **governance and correctness** mechanism — **not** an
authentication or authorization mechanism. The wire format is the public Confluent standard:
the magic byte is always `0x00` and the schema-ID layout is well known, so anyone who knows the
format can craft a structurally valid envelope. Do **not** mistake it for access control.

### 11.1 What the envelope guarantees
- The value references a schema **registered in our Schema Registry** (`schema_id`).
- The payload **decodes** into a known generated type and **passes** protovalidate (§6).
- This stops accidental garbage, schema drift, malformed data, and unknown/unversioned schemas.

### 11.2 What the envelope does NOT guarantee
- It does **not** authenticate the producer or authorize the consumer. The magic byte (`0x00`)
  and `schema_id` are public, not secrets — a client with Kafka write access can forge a valid
  envelope.

### 11.3 Where security actually lives (separate, complementary layer)
| Concern | Mechanism |
|---|---|
| Client authentication | Kafka **SASL/SCRAM** or **mTLS** |
| Authorization (who produces/consumes which topic) | Kafka **ACLs** |
| Schema Registry access | Private + authenticated SR (`SCHEMA_REGISTRY_API_KEY`/`SECRET`, §7.3) |
| Exposure | Network isolation — Kafka and SR not publicly reachable |

### 11.4 Rule
**Do not** replace the standard wire format with a "secret" format hoping to gain security: it
would break interop with the Confluent ecosystem and give only a *false* sense of protection.
Security is the auth/ACL layer; the envelope is the structure contract. They are complementary,
not substitutes. (Analogy: the Postgres port `5432` being well known does not protect the
database — passwords, auth and network do. Same here.)
