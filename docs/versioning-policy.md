# Proto Versioning & Compatibility Policy

**Version:** 1.0  
**Status:** Authoritative  
**Owner:** Platform Engineer (ROD-14)

---

## 1. Proto Layout Conventions

```
proto/
  <domain>/           # one directory per bounded context
    <entity>.proto    # one .proto per top-level message or small message cluster
```

Current layout (buf-idiomatic: directory path = package, version suffix):
```
proto/
  protobuf/transaction/v1/transaction.proto   # package protobuf.transaction.v1
  protobuf/onboarding/v1/onboarding.proto      # package protobuf.onboarding.v1
  protobuf/type/v1/{address,registration,banking,pix}.proto  # protobuf.type.v1 (shared)
```

### 1.1 Package naming

```protobuf
syntax = "proto3";
package protobuf.<domain>.v1;
option go_package = "github.com/vinirmbrozz/protobuf-contracts/gen/go/protobuf/<domain>/v1;<domain>v1";
```

As new domains are added, create `proto/protobuf/<domain>/v1/<entity>.proto` with package
`protobuf.<domain>.v1`. The buf module roots at `proto/` (`buf.yaml` v2), so the directory matches
the package. A breaking change bumps the version (`v2`) alongside `v1`.

### 1.2 Field naming

**Rule:** All new fields MUST use `snake_case`. The existing `isAllowed`, `cardId`, etc. are a
known exception (pre-standard, tracked in [ROD-14](/ROD/issues/ROD-14)) and will be migrated
in a future version bump.

**Field numbers:** Start at 1, increment sequentially. Never reuse a field number, even after
deleting a field. Reserve deleted field numbers with `reserved` statements.

### 1.3 Enum values

Always include an `_UNSPECIFIED` zero value:
```protobuf
enum Decision {
  DECISION_UNSPECIFIED = 0;
  DECISION_APPROVED    = 1;
  DECISION_DENIED      = 2;
}
```

---

## 2. Backward Compatibility Enforcement

`buf breaking` runs in CI against the last released snapshot (the `main` branch HEAD).
**A PR will not merge if it introduces a buf breaking violation.**

Breaking change definition (buf `FILE` mode):
- Removing or renaming a field
- Changing a field's type or number
- Removing a message or enum type
- Changing a field from `optional` to `required`

### 2.1 Safe changes (no version bump needed)

| Change | Example |
|---|---|
| Add optional field | `string new_field = 9;` |
| Add enum value | `DECISION_PENDING = 3;` |
| Add new message | New top-level message in a new `.proto` |
| Add comment | Any comment change |

### 2.2 Breaking changes (require version bump + migration)

| Change | Migration path |
|---|---|
| Remove field | Add `reserved <number>; reserved "<name>";` and bump schema version |
| Rename field | Add new field + deprecate old (mark with `[deprecated = true]`) |
| Change field type | New field with new number; remove old field in next major version |
| Rename message | Add type alias / new message; migrate consumers |

---

## 3. Schema Version Lifecycle

### 3.1 Versions

`v1` is the **current and only version** of the protobuf-contracts schema. The version lives in the
Schema Registry subject, not in the proto package name.

Proto packages do **not** include a version suffix until a breaking change is required. At that
point, create `proto/transaction/v2/transaction.proto` with package `protobuf.transaction.v2`.

### 3.2 Version bump process

1. File a CTO-reviewed PR with the proposed breaking change and migration plan
2. CTO approves via Paperclip interaction
3. Bump the Schema Registry subject compatibility to `NONE` temporarily for the new version
4. Register the new schema under a new subject (e.g. `transactions-v2-value`)
5. Deploy new producers → deploy new consumers → decommission old producers
6. Set compatibility back to `BACKWARD` on the new subject
7. After full migration, mark old subject as `READONLY` in SR

### 3.3 PR checklist

Before merging any proto change:
- [ ] `buf lint` passes
- [ ] `buf breaking --against .git#branch=main` passes
- [ ] Generated SDKs regenerated (`buf generate`)
- [ ] Interop harness passes
- [ ] If breaking: migration plan documented and CTO-approved

---

## 4. buf Governance

### 4.1 buf lint rules

We use `DEFAULT` rule set with two exceptions (see `buf.yaml`):
- `FIELD_LOWER_SNAKE_CASE` — excluded for existing camelCase fields (legacy)
- `PACKAGE_VERSION_SUFFIX` — excluded until first breaking change requires versioning

### 4.2 Breaking checks

`buf breaking` is run with `--against .git#branch=main` in CI and against the BSR baseline
(once we publish to `buf.build/protobuf/contracts`).

### 4.3 protovalidate

Field-level validation is declared in `.proto` files using `buf.validate` annotations.
The generated validator runs at the consumer side (see §6 of the Serde Spec).

---

## 5. Codegen Pipeline

Generated SDKs are committed to `gen/` and are **never hand-edited**.

```
proto/*.proto
  └──► buf generate (buf.gen.yaml)
        ├── gen/go/        ← Go SDK
        ├── gen/typescript/ ← TypeScript SDK
        ├── gen/node/       ← CommonJS Node SDK
        └── gen/python/     ← Python SDK
```

CI regenerates SDKs on every push to `main` and commits the result. Per-language seniors consume
the generated SDKs as packages published from `gen/`.

---

## 6. Tech Debt

The seed mock's debts are **resolved** by the real versioned contracts:
- camelCase field names → all fields are `snake_case`; the `FIELD_LOWER_SNAKE_CASE` lint exception was
  dropped (`buf.yaml` v2 uses the full `STANDARD` set with no exceptions).
- protovalidate annotations → wired (dep in `buf.yaml`, resolved by `buf dep update` in CI; rules ported
  from the source `Valid()`).
