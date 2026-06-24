# Protobuf Contracts SDK - Python

## Installation
```bash
pip install git+https://github.com/vinirmbrozz/protobuf-contracts.git#subdirectory=gen/python
```

## Usage
```python
from transaction_pb2 import Transaction, PredictiveAnalyzer

analyzer = PredictiveAnalyzer(
    isAllowed=True,
    reason="Approved",
    cardId="card-123",
    userId="user-456",
    walletAddress="0x...",
    allowance="5000.00"
)

transaction = Transaction(
    transactionAmount="100.00",
    predictiveAnalyzer=analyzer,
    final_decision="APPROVED"
)

print(transaction)
```
