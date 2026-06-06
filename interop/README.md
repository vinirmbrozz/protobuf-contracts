# Cross-language Interop Harness

This directory contains the test harness that proves the Confluent SR wire format
is correctly implemented across Go, Node.js and Python.

See [`docs/confluent-sr-serde-spec.md`](../docs/confluent-sr-serde-spec.md) for the full spec.

---

## Structure

```
interop/
  harness.js        ← Node.js reference implementation (runnable today, no Kafka needed)
  fixtures/         ← Shared binary fixtures for cross-language byte-level checks
  go/
    interop_test.go ← Go framing unit tests
    go.mod
  python/
    test_interop.py ← Python framing + round-trip tests
```

---

## Running the harness

### Node.js (works today)

```bash
node interop/harness.js
```

Expected output: all tests pass.

### Go

```bash
# Requires Go 1.21+ and the generated Go SDK
go test ./interop/go/...
```

`TestNodeCompatibility` is skipped until fixture bytes are captured from the Node harness.

### Python

```bash
# Requires Python 3.10+ and the generated Python SDK
pip install -e gen/python/
python -m pytest interop/python/test_interop.py -v
```

`TestProtoRoundTrip` and `TestNodeCompatibility` are skipped until the SDK is installed.

---

## Cross-language matrix

| Producer | Consumer | Status |
|---|---|---|
| Node.js | Node.js | ✅ passes (`node interop/harness.js`) |
| Node.js | Go | 🔲 pending Senior Go engineer |
| Node.js | Python | 🔲 pending Senior Python engineer |
| Go | Node.js | 🔲 pending Senior Go engineer |
| Go | Python | 🔲 pending Senior Go + Python engineers |
| Python | Node.js | 🔲 pending Senior Python engineer |
| Python | Go | 🔲 pending Senior Python + Go engineers |

---

## Adding fixture bytes

To enable cross-language deserialization tests:

1. Run `node interop/harness.js` and capture the framed hex bytes for the canonical
   `Transaction{transactionAmount="499.99", finalDecision="APPROVED"}` message
2. Save the hex string to `interop/fixtures/transaction_canonical.hex`
3. Each language test loads this file and asserts it can deserialize it identically

```bash
node -e "
const {Transaction, PredictiveAnalyzer} = require('./gen/node/proto/transaction_pb.js');
const pa = new PredictiveAnalyzer();
pa.setIsallowed(true); pa.setReason('approved'); pa.setCardid('card-123'); pa.setUserid('user-456');
const tx = new Transaction();
tx.setTransactionamount('499.99'); tx.setPredictiveanalyzer(pa); tx.setFinalDecision('APPROVED');
const framed = Buffer.concat([Buffer.from([0x00,0x00,0x00,0x00,0x01,0x00]), Buffer.from(tx.serializeBinary())]);
require('fs').writeFileSync('interop/fixtures/transaction_canonical.hex', framed.toString('hex'));
console.log('fixture written:', framed.toString('hex'));
"
```
