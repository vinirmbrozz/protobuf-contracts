# Cross-language Interop Harness

Proves that the Go, Node.js and Python SDKs implement the **same Confluent SR wire
format** by having each language **produce** a message and all three **consume +
verify** it — against a **real Schema Registry**. The `schema_id` is resolved at
runtime (no brittle hard-coded fixtures), and all SDKs are used through their thin
public API (`bind` / `produce` / `consume`).

See [`docs/confluent-sr-serde-spec.md`](../docs/confluent-sr-serde-spec.md) for the wire spec.

---

## Structure

```
interop/
  orchestrate.mjs   ← runs the 3×3 produce→consume matrix
  cli.js            ← Node CLI (ESM)        — imports @protobuf/contracts (sdk/node)
  go/main.go        ← Go CLI                — imports sdk/go (serde + proto)
  python/cli.py     ← Python CLI            — imports protobuf_contracts (sdk/python)
```

Each CLI exposes the same contract:

```
<runtime> <produce|consume> <topic> <file>
```

`produce` builds the canonical sample, frames it via the SDK, and writes the bytes to
`<file>`. `consume` reads `<file>`, runs it through the SDK, and verifies the decoded
fields. The canonical sample is identical across all three CLIs, so any producer's
bytes must verify in any consumer.

---

## Running

```bash
# 1. Kafka + Schema Registry
docker compose up -d

# 2. Register the schema (the only writer to SR)
SCHEMA_REGISTRY_URL=http://localhost:8081 python scripts/register_schemas.py

# 3. Build/link the SDKs the CLIs import
cd sdk/node && npm install && npm run build && cd ../..
pip install sdk/python/
cd interop && npm install && cd ..

# 4. Run the full cross-language matrix
SCHEMA_REGISTRY_URL=http://localhost:8081 node interop/orchestrate.mjs
```

The orchestrator builds the Go CLI, then runs every producer × every consumer.

---

## Cross-language matrix (all green)

| Producer ↓ / Consumer → | Node | Go | Python |
|---|---|---|---|
| **Node**   | ✅ | ✅ | ✅ |
| **Go**     | ✅ | ✅ | ✅ |
| **Python** | ✅ | ✅ | ✅ |

All three produce **byte-identical frames** for the same message + schema (same
`schema_id` from SR, same variable-length `message-index`, same proto3 payload).

This same flow runs in CI (`.github/workflows/buf-ci.yml`, job `interop`) against a real
Kafka + Schema Registry brought up via `docker compose`.
