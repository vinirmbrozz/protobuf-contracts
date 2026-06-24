# Canonical Layout & Packaging Standard — protobuf-contracts

**Version:** 1.0  
**Status:** Authoritative  
**Owner:** Platform Engineer ([ROD-21](/ROD/issues/ROD-21))  
**Audience:** Senior Go, Senior Node-TS, Senior Python engineers integrating [ROD-15](/ROD/issues/ROD-15) / [ROD-16](/ROD/issues/ROD-16) / [ROD-17](/ROD/issues/ROD-17)

---

## 1. Goal

Every Protobuf service must install **one dependency per language** and immediately have:

- all generated Protobuf message types, and  
- the typed `produce` / `consume` API wired to Confluent Schema Registry.

This document defines where that one package lives in the repository, how it is named, and what reorganisation each per-language serde branch must carry out when integrated into `main`.

---

## 2. Repository Layout

```
protobuf-contracts/
├── proto/                        # source of truth — never hand-edited except here
│   └── transaction.proto
├── gen/                          # buf codegen output — never hand-edited
│   ├── go/                       # Go generated types (transaction.pb.go)
│   ├── typescript/               # TypeScript generated types
│   ├── node/                     # CommonJS generated types
│   └── python/                   # Python generated types
├── sdk/                          # ← ONE PUBLISHABLE PACKAGE PER LANGUAGE
│   ├── go/                       # Go SDK (types + serde)
│   ├── node/                     # Node/TS SDK (types + serde)
│   └── python/                   # Python SDK (types + serde)
├── interop/                      # cross-language round-trip harness
├── docs/
│   ├── packaging.md              # this document
│   ├── confluent-sr-serde-spec.md
│   └── versioning-policy.md
├── buf.yaml
└── buf.gen.yaml
```

### 2.1 Separation of concerns

| Directory | Content | Hand-edited? |
|-----------|---------|-------------|
| `proto/` | `.proto` files — single source of truth | Yes |
| `gen/` | Generated SDKs — output of `buf generate` | **Never** |
| `sdk/` | Publishable packages — generated types + serde | Serde only |
| `interop/` | Cross-language test harness | Yes |

`gen/` is always safe to regenerate and commit. `sdk/<lang>/` contains the serde implementation;
generated message files inside it are embedded (see §4) and treated the same as `gen/` — never
hand-edited.

---

## 3. Per-language Canonical Layout

### 3.1 Go — `sdk/go/`

```
sdk/go/
├── go.mod            # module github.com/vinirmbrozz/protobuf-contracts/sdk/go
├── go.sum
├── transaction.pb.go # generated (embedded from gen/go — see §4)
├── serde.go          # Confluent SR serde (produce/consume)
└── serde_test.go
```

**Module path:** `github.com/vinirmbrozz/protobuf-contracts/sdk/go`

A consumer in another Go service does:

```go
import contracts "github.com/vinirmbrozz/protobuf-contracts/sdk/go"

producer, _ := contracts.NewTransactionProducer(contracts.ProducerConfig{
    Brokers:        []string{"kafka:9092"},
    RegistryURL:    "http://schema-registry:8081",
    Topic:          "transactions",
})
producer.Produce(ctx, &contracts.Transaction{...})
```

No secondary import from `gen/go` is needed; message types are re-exported from within `sdk/go`.

### 3.2 Node/TS — `sdk/node/`

```
sdk/node/
├── package.json      # "name": "@protobuf/contracts"  ← see §5
├── tsconfig.json
├── src/
│   ├── index.ts      # public API — re-exports types + serde
│   ├── serde.ts      # Confluent SR serde (produce/consume)
│   ├── framing.ts    # Confluent envelope framing
│   ├── schema-registry-client.ts
│   ├── types.ts      # typed produce/consume interfaces
│   └── generated/    # generated TS types (embedded — see §4)
│       └── transaction_pb.ts
└── dist/             # compiled output (gitignored, built on publish)
```

**npm package name (proposed):** `@protobuf/contracts` — see §5 for escalation note.

A consumer does:

```typescript
import { TransactionProducer } from "@protobuf/contracts";

const producer = new TransactionProducer({
  brokers: ["kafka:9092"],
  registryUrl: "http://schema-registry:8081",
  topic: "transactions",
});
await producer.produce({ cardId: "123", amount: 99.0, ... });
```

### 3.3 Python — `sdk/python/`

```
sdk/python/
├── setup.py          # name "protobuf-contracts"  ← see §5
├── MANIFEST.in
├── README.md
└── protobuf_contracts/
    ├── __init__.py   # exports ProduceTransaction, ConsumeTransaction + pb types
    ├── serde.py      # Confluent SR serde (produce/consume)
    └── transaction_pb2.py  # generated (embedded — see §4)
```

**PyPI package name (proposed):** `protobuf-contracts` — see §5 for escalation note.

A consumer does:

```python
from protobuf_contracts import TransactionProducer

producer = TransactionProducer(
    brokers=["kafka:9092"],
    registry_url="http://schema-registry:8081",
    topic="transactions",
)
producer.produce(card_id="123", amount=99.0)
```

---

## 4. Generated Files Inside `sdk/`

`sdk/<lang>/` embeds generated message files so consumers install exactly one package.
There are two supported integration strategies; choose the one that minimises build complexity
for each language:

### Strategy A — direct codegen target (applied, ROD-22)

`buf.gen.yaml` carries dual output entries per language plugin: one to `gen/<lang>/` (canonical
record, never deleted) and one to `sdk/<lang>/`. Running `buf generate` keeps both trees in sync
automatically.

```yaml
# buf.gen.yaml — applied as of ROD-22
plugins:
  - name: go
    out: gen/go          # canonical gen record
    opt: [paths=source_relative]
  - name: go
    out: sdk/go          # SDK embedding
    opt: [paths=source_relative]
  - name: ts_proto
    out: gen/typescript  # canonical gen record
    opt: [esModuleInterop=true, outputServices=false, oneof=unions]
  - name: ts_proto
    out: sdk/node/src/generated  # SDK embedding
    opt: [esModuleInterop=true, outputServices=false, oneof=unions]
  - name: python
    out: gen/python      # canonical gen record
  - name: python
    out: sdk/python/protobuf_contracts  # SDK embedding
```

### Strategy B — copy step in CI

A build script (`make sdk`) copies `gen/<lang>/` into `sdk/<lang>/generated/` after `buf generate`.
Simpler to set up; requires CI discipline to keep them in sync.

**Both strategies produce the same result**: a `sdk/<lang>/` directory that is fully self-contained
and can be tagged and published without reference to `gen/`.

Strategy A was selected (D-3, confirmed by CTO) and applied in ROD-22.

---

## 5. Package Names — Confirmed (ROD-22)

| Language | Package name | Registry | Status |
|----------|-------------|----------|--------|
| Go | `github.com/vinirmbrozz/protobuf-contracts/sdk/go` | GitHub (public) | **Confirmed** |
| Node/TS | `@protobuf/contracts` | npm (public) | **Confirmed** (D-1, ROD-22) |
| Python | `protobuf-contracts` | PyPI | **Confirmed** (D-2, ROD-22) |

The npm org `protobuf` must exist before publish; the founder handles npm/PyPI account setup.
The Node SDK branch ([ROD-16](/ROD/issues/ROD-16)) must rename the package from `@protobuf/kafka-serde`
to `@protobuf/contracts` as part of the conformance step — this is a breaking change for consumers
on the old name.

Similarly, [ROD-17](/ROD/issues/ROD-17) uses `protobuf-contracts-sdk`; conformance must rename it to
`protobuf-contracts`. The founder must verify no external registrations exist for the old names before
publishing under the new ones. **Agents do not publish**.

---

## 6. Consumer Quick-start

After the packages are published, a new Protobuf service integrates as follows.

### Go

```bash
go get github.com/vinirmbrozz/protobuf-contracts/sdk/go
```

```go
import (
    contracts "github.com/vinirmbrozz/protobuf-contracts/sdk/go"
    "context"
)

// Produce
p, _ := contracts.NewTransactionProducer(contracts.ProducerConfig{
    Brokers: []string{os.Getenv("KAFKA_BROKERS")},
    RegistryURL: os.Getenv("SCHEMA_REGISTRY_URL"),
    Topic: "transactions",
})
defer p.Close()
p.Produce(context.Background(), &contracts.Transaction{...})

// Consume
c, _ := contracts.NewTransactionConsumer(contracts.ConsumerConfig{
    Brokers: []string{os.Getenv("KAFKA_BROKERS")},
    RegistryURL: os.Getenv("SCHEMA_REGISTRY_URL"),
    Topic: "transactions",
    GroupID: "my-service",
})
msg, _ := c.Consume(context.Background())
```

### Node/TS

```bash
npm install @protobuf/contracts
```

```typescript
import { TransactionProducer, TransactionConsumer } from "@protobuf/contracts";

// Produce
const producer = new TransactionProducer({
    brokers: [process.env.KAFKA_BROKERS!],
    registryUrl: process.env.SCHEMA_REGISTRY_URL!,
    topic: "transactions",
});
await producer.produce({ cardId: "abc", amount: 100 });

// Consume
const consumer = new TransactionConsumer({
    brokers: [process.env.KAFKA_BROKERS!],
    registryUrl: process.env.SCHEMA_REGISTRY_URL!,
    topic: "transactions",
    groupId: "my-service",
});
const msg = await consumer.consume();
```

### Python

```bash
pip install protobuf-contracts[kafka]
```

```python
from protobuf_contracts import TransactionProducer, TransactionConsumer
import os

# Produce
producer = TransactionProducer(
    brokers=[os.environ["KAFKA_BROKERS"]],
    registry_url=os.environ["SCHEMA_REGISTRY_URL"],
    topic="transactions",
)
producer.produce(card_id="abc", amount=100.0)

# Consume
consumer = TransactionConsumer(
    brokers=[os.environ["KAFKA_BROKERS"]],
    registry_url=os.environ["SCHEMA_REGISTRY_URL"],
    topic="transactions",
    group_id="my-service",
)
msg = consumer.consume()
```

---

## 7. Conformance Plan — ROD-15 / ROD-16 / ROD-17

Each serde branch must carry out the following reorganisation **before merging to `main`**.
No serde logic changes; only file location and package metadata change.

### 7.1 ROD-15 — Go serde

**Current location:** `serde/` (module `github.com/vinirmbrozz/protobuf-contracts/serde`)  
**Target location:** `sdk/go/` (module `github.com/vinirmbrozz/protobuf-contracts/sdk/go`)

Steps:
1. Move `serde/serde.go` → `sdk/go/serde.go`.
2. Move `serde/serde_test.go` → `sdk/go/serde_test.go`.
3. Update `sdk/go/go.mod`:
   - Change module path to `github.com/vinirmbrozz/protobuf-contracts/sdk/go`.
   - Keep existing `require` entries.
4. Embed generated types: copy `gen/go/transaction.pb.go` into `sdk/go/` OR update `go.mod` to
   `replace` the gen module with a local path (temporary, until codegen target change is approved).
5. Update `go.mod` import paths inside `serde.go` if they referenced the old `serde` module.
6. Verify `go test ./sdk/go/...` passes.
7. Delete the now-empty `serde/` directory.

> Note: the `gen/go/` module (`github.com/vinirmbrozz/protobuf-contracts/gen/go`) stays in place as
> the canonical generated-types module. `sdk/go/` may import it as a dependency or embed the
> generated files directly (see §4).

### 7.2 ROD-16 — Node/TS serde

**Current location:** `packages/kafka-serde/` (`@protobuf/kafka-serde`)  
**Target location:** `sdk/node/` (`@protobuf/contracts` — pending CTO name confirmation, see §5)

Steps:
1. Move `packages/kafka-serde/src/` → `sdk/node/src/`.
2. Move `packages/kafka-serde/tsconfig.json` → `sdk/node/tsconfig.json`.
3. Rename the `package.json`:
   - Change `"name"` from `"@protobuf/kafka-serde"` to the CTO-confirmed name (propose `"@protobuf/contracts"`).
   - Update `"description"` to reflect it is the full contracts SDK.
   - Keep all existing `dependencies` / `devDependencies`.
4. Embed generated types: copy `gen/typescript/transaction_pb.ts` into `sdk/node/src/generated/`
   OR update `tsconfig.json` path mappings to reference `../../gen/typescript/`.
5. Export generated types from `sdk/node/src/index.ts` so consumers get everything from one import.
6. Run `npm run build` and `npm test` inside `sdk/node/`.
7. Delete `packages/kafka-serde/`.

### 7.3 ROD-17 — Python serde

**Current location:** `gen/python/protobuf_contracts_sdk/` (package `protobuf-contracts-sdk`)  
**Target location:** `sdk/python/` (package `protobuf-contracts` — pending CTO name confirmation, see §5)

Steps:
1. Move `gen/python/protobuf_contracts_sdk/` → `sdk/python/protobuf_contracts/`.
2. Rename the Python package directory from `protobuf_contracts_sdk` to `protobuf_contracts`
   (drops the `_sdk` suffix to match the pip install name).
3. Move `gen/python/setup.py` → `sdk/python/setup.py`; update:
   - `name` → `"protobuf-contracts"` (or CTO-confirmed name).
   - `packages` → `find_packages(exclude=["tests*"])` pointing at `protobuf_contracts/`.
4. Move or symlink `gen/python/tests/` → `sdk/python/tests/`.
5. Embed generated types: copy `gen/python/transaction_pb2.py` into `sdk/python/protobuf_contracts/`
   OR keep `gen/python/` as a declared dependency (`setup.py` `install_requires` → path dependency
   until proper packaging is finalised).
6. Update `__init__.py` imports from `protobuf_contracts_sdk.*` → `protobuf_contracts.*`.
7. Run `pytest sdk/python/tests/` — all tests must pass.
8. `gen/python/` retains the raw buf output; `sdk/python/` is the publishable SDK.

---

## 8. Invariants

These rules apply to all three languages after conformance:

1. **One install, all you need.** A service that installs the SDK package gets generated message
   types AND the serde API. No secondary import from `gen/` is needed.
2. **Generated files in `sdk/` are never hand-edited.** If a proto changes, `buf generate` (or a
   copy step) regenerates them. The `sdk/<lang>/` file is overwritten.
3. **Serde logic lives only in `sdk/<lang>/`.** No serde code in `gen/`, `proto/`, or `interop/`.
4. **`gen/` remains the canonical codegen record.** It is never removed; it provides the baseline
   for buf breaking checks and for language seniors who only need raw generated types.
5. **`interop/` is unchanged.** The cross-language round-trip harness imports from the SDK packages
   (or their local paths) and is updated by the Platform Engineer once all three branches land.

---

## 9. Decisions — All RESOLVED (ROD-22)

| # | Decision | Resolved value | Status |
|---|----------|---------------|--------|
| D-1 | npm package name | `@protobuf/contracts` | **RESOLVED** |
| D-2 | PyPI package name | `protobuf-contracts` | **RESOLVED** |
| D-3 | Gen embedding strategy | Strategy A — dual `buf.gen.yaml` outputs (applied in ROD-22) | **RESOLVED** |
| D-4 | Go SDK: embed vs import | Embed `transaction.pb.go` directly in `sdk/go/` | **RESOLVED** |

> All four decisions confirmed by CTO. Resolved in [ROD-22](/ROD/issues/ROD-22); applied in this
> commit. No further CTO sign-off is required for these items.

### 9.1 interop/ — future update (post ROD-15/16/17)

`interop/` currently imports generated types from `gen/<lang>/` paths. Once the three language
conformances land ([ROD-15](/ROD/issues/ROD-15), [ROD-16](/ROD/issues/ROD-16),
[ROD-17](/ROD/issues/ROD-17)), the Platform Engineer will update `interop/` to import directly from
`sdk/<lang>/` instead. **Do not update `interop/` until all three conformances are merged to
`main`** — the round-trip harness must continue to pass against the canonical `gen/` outputs in the
meantime.

---

## 10. References

- [Confluent SR Serde Specification](confluent-sr-serde-spec.md) — wire format and SR contract
- [Versioning & Compatibility Policy](versioning-policy.md) — buf governance, breaking changes
- [ROD-15](/ROD/issues/ROD-15) — Go serde implementation (done, pending integration)
- [ROD-16](/ROD/issues/ROD-16) — Node/TS serde implementation (done, pending integration)
- [ROD-17](/ROD/issues/ROD-17) — Python serde implementation (done, pending integration)
