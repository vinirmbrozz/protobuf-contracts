# Protobuf Contracts — Python (generated record)

`gen/python/` is the **canonical codegen record** (raw `buf generate` output). For real use,
import the **publishable SDK** in `sdk/python/` (`protobuf-contracts`), which adds the Confluent
SR serde (`bind`/`produce`/`consume`) — see [`sdk/python`](../../sdk/python).

```python
from protobuf_contracts import Transaction, TransactionData
from protobuf_contracts.serde import KafkaSerde

serde = KafkaSerde()                       # reads SCHEMA_REGISTRY_URL
serde.bind("transactions", Transaction)    # resolves schema_id (read-only)

framed = serde.produce("transactions", Transaction(
    transaction=TransactionData(id="tx-1", amount_total="100.00", channel="web", type="PIX"),
))
tx = serde.consume("transactions", framed) # -> Transaction (or SerdeError → DLQ)
```
