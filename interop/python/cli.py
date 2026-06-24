"""Python side of the cross-language interop harness — uses the protobuf_contracts SDK.

  python interop/python/cli.py produce <topic> <file>   # bind + produce → file
  python interop/python/cli.py consume <topic> <file>    # bind + consume + verify

The canonical SAMPLE is identical across the Go/Node/Python CLIs, so a frame
produced by any language must consume+verify in the others. Reads
SCHEMA_REGISTRY_URL from the environment.
"""
import sys

from protobuf_contracts import Transaction, TransactionData, Customer
from protobuf_contracts.serde import KafkaSerde

# Scalar string fields only → byte-identical wire across Go/Node/Python.
SAMPLE = Transaction(
    transaction=TransactionData(id="tx-1", amount_total="499.99", channel="web", type="PIX"),
    customer=Customer(name="Ada Lovelace", email="ada@example.com"),
)


def main() -> None:
    if len(sys.argv) < 4:
        print("usage: cli.py <produce|consume> <topic> <file>", file=sys.stderr)
        sys.exit(2)
    cmd, topic, file = sys.argv[1], sys.argv[2], sys.argv[3]

    serde = KafkaSerde()
    serde.bind(topic, Transaction)

    if cmd == "produce":
        with open(file, "wb") as f:
            f.write(serde.produce(topic, SAMPLE))
        print(f"python: produced -> {file}")
    elif cmd == "consume":
        with open(file, "rb") as f:
            tx = serde.consume(topic, f.read())
        if tx != SAMPLE:
            print(f"python: MISMATCH {tx}", file=sys.stderr)
            sys.exit(1)
        print("python: consume OK")
    else:
        print(f"unknown command: {cmd}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
