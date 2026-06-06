# Cross-language Interop Harness

This directory proves the Confluent SR wire format is correctly implemented across
Go, Node.js, and Python using the **`sdk/`** packages as the single source of truth.
No local framing reimplementation — all three harnesses import from `sdk/`.

See [`docs/confluent-sr-serde-spec.md`](../docs/confluent-sr-serde-spec.md) for the full spec.

---

## Structure

```
interop/
  harness.js        ← Node.js harness — imports from sdk/node (@truther/contracts)
  go/
    interop_test.go ← Go harness — imports from sdk/go (serde + proto packages)
    go.mod          ← replace directive pointing to ../../sdk/go
  python/
    test_interop.py ← Python harness — imports from truther_contracts (sdk/python)
```

---

## Running the harness

### Node.js

```bash
node interop/harness.js
```

All 6 test groups pass, including the cross-language fixture.

### Go

```bash
go test ./interop/go/...
```

All 4 tests pass, including `TestNodeCompatibility` (decodes Node-produced bytes).

### Python

```bash
# Install the SDK if not already present:
pip install sdk/python/
# or editable: pip install -e sdk/python/ --break-system-packages

python3 -m pytest interop/python/test_interop.py -v
```

All 9 tests pass. No skipped tests — `TestProtoRoundTrip` and `TestNodeCompatibility`
are fully active.

---

## Cross-language matrix

| Producer | Consumer | Status |
|---|---|---|
| Node.js | Node.js | ✅ `node interop/harness.js` (Test 1) |
| Node.js | Go | ✅ `TestNodeCompatibility` in Go |
| Node.js | Python | ✅ `TestNodeCompatibility` in Python |
| Go | Node.js | ✅ `Test 6` in harness.js (fixture also validates Go output) |
| Go | Python | 🔲 full Produce→Consume path needs live or mock SR per language |
| Python | Node.js | 🔲 full Produce→Consume path needs live or mock SR per language |
| Python | Go | 🔲 full Produce→Consume path needs live or mock SR per language |

---

## Canonical fixture

The canonical cross-language fixture (schemaId=1) encodes:

```
Transaction{
  transactionAmount: "499.99",
  predictiveAnalyzer: {
    isAllowed: true, reason: "approved by risk engine",
    cardId: "card-abc-123", userId: "user-xyz-456",
    walletAddress: "0xDEADBEEF", allowance: "1000.00"
  },
  finalDecision: "APPROVED"
}
```

Framed hex (102 bytes):

```
0000000001000a063439392e3939124c08011217617070726f766564206279207269736b20
656e67696e651a0c636172642d6162632d313233220c757365722d78797a2d3435362a0a30
7844454144424545463207313030302e30301a08415050524f564544
```

This same hex string is embedded directly in each harness — no fixture file needed.
