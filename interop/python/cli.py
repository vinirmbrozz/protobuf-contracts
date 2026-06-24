"""Python side of the cross-language interop harness — uses the protobuf_contracts SDK.

  python interop/python/cli.py produce <topic> <file>   # bind + produce → file
  python interop/python/cli.py consume <topic> <file>    # bind + consume + verify

The canonical SAMPLE is identical across the Go/Node/Python CLIs, so a frame
produced by any language must consume+verify in the others. Reads
SCHEMA_REGISTRY_URL from the environment.
"""
import sys

from protobuf_contracts import Transaction, PredictiveAnalyzer
from protobuf_contracts.serde import KafkaSerde

SAMPLE = Transaction(
    transactionAmount="499.99",
    final_decision="APPROVED",
    predictiveAnalyzer=PredictiveAnalyzer(
        isAllowed=True,
        reason="approved",
        cardId="card-1",
        userId="user-1",
        walletAddress="0xABC",
        allowance="1000.00",
        transactionId="tx-1",
        name="n",
    ),
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
