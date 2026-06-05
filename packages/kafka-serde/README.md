# @truther/kafka-serde

Confluent Schema Registry serde library for Truther Kafka messages.

Implements the wire format defined in [`docs/confluent-sr-serde-spec.md`](../../docs/confluent-sr-serde-spec.md):

```
[0x00 magic] [schema_id: 4 bytes BE] [0x00 msg-index] [proto3 payload]
```

## Stack

- **Kafka client**: KafkaJS (bring your own)
- **Schema Registry**: Confluent SR REST API (via native Node 18+ `fetch`)
- **Subject naming**: TopicNameStrategy — `<topic>-value`
- **Compatibility**: BACKWARD (set globally in docker-compose)

## Configuration

| Env var                    | Default                  | Description           |
|----------------------------|--------------------------|-----------------------|
| `SCHEMA_REGISTRY_URL`      | `http://localhost:8081`  | SR base URL           |
| `SCHEMA_REGISTRY_API_KEY`  | _(none)_                 | SR API key (prod)     |
| `SCHEMA_REGISTRY_API_SECRET` | _(none)_               | SR API secret (prod)  |

## Installation

```bash
npm install file:./packages/kafka-serde
```

## Quick start

```typescript
import { TrutherSerde } from '@truther/kafka-serde';
import { Transaction } from '@truther/contracts-sdk'; // or gen/node JSPB variant
import { readFileSync } from 'fs';

// 1. Instantiate — reads SR URL from SCHEMA_REGISTRY_URL env var
const serde = new TrutherSerde();

// 2. At startup: register schema(s) eagerly
const protoContent = readFileSync('./proto/transaction.proto', 'utf8');
await serde.registerSchema('transactions', protoContent);

// 3. Codec adapter (ts-proto example)
const TransactionCodec = {
  encode: (msg: Transaction) => Transaction.encode(msg).finish(),
  decode: (bytes: Uint8Array) => Transaction.decode(bytes),
};

// 4. Produce: serialize + frame → Buffer
const tx: Transaction = { transactionAmount: '9.99', finalDecision: 'APPROVED', predictiveAnalyzer: undefined };
const buf = serde.produce('transactions', tx, TransactionCodec);
await kafkaProducer.send({ topic: 'transactions', messages: [{ value: buf }] });

// 5. Consume: validate + deframe + deserialize → typed message
const decoded = await serde.consume('transactions', rawKafkaValue, TransactionCodec);
```

## API

### `new TrutherSerde(options?)`

| Option      | Type   | Description                          |
|-------------|--------|--------------------------------------|
| `srUrl`     | string | Override SR URL (env var fallback)   |
| `srApiKey`  | string | Override SR API key                  |
| `srApiSecret` | string | Override SR API secret             |

### `serde.registerSchema(topic, protoContent): Promise<number>`

Register a PROTOBUF schema under `<topic>-value`. Returns the schema ID. Call at startup for each topic you produce to. Idempotent.

### `serde.produce<T>(topic, msg, codec): Buffer`

Serialize `msg` using `codec.encode` and frame it in the Confluent envelope. Throws if `registerSchema` was not called for `topic`.

### `serde.consume<T>(topic, data, codec): Promise<T>`

Validate the Confluent envelope, verify the schema ID against SR, and decode using `codec.decode`. Throws `SerdeError` on any rejection — callers must route to the dead-letter topic `<topic>-dead-letter`.

### `SerdeError`

```typescript
class SerdeError extends Error {
  code: 'INVALID_MAGIC_BYTE' | 'UNKNOWN_SCHEMA_ID' | 'DESERIALIZATION_ERROR';
  rawPayload?: Buffer;
}
```

## Running tests

```bash
# Unit tests (no external services)
npm test

# Integration tests (requires docker-compose stack)
docker-compose up -d schema-registry
npm run test:integration
```

## Adding a new contract

1. Add your message to `proto/`.
2. Run `buf generate` to regenerate `gen/node/` and `gen/typescript/`.
3. Create a codec adapter for the new type.
4. Call `serde.registerSchema('<your-new-topic>', protoContent)` at startup.
5. Add unit tests for the new codec.
