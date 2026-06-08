# @truther/contracts

Generated Protobuf types + Confluent Schema Registry serde for Node/TypeScript.

The SDK is **thin**: it never reads a `.proto` and never registers schemas. It
**resolves** the `schema_id` from the Schema Registry, frames the Confluent
envelope, and validates strictly on consume. Wire format
([spec](../../docs/confluent-sr-serde-spec.md)):

```
[0x00 magic] [schema_id: 4 bytes BE] [message-index] [proto3 payload]
```

`message-index` is variable length (derived from the type's descriptor) — generic
for any message, no per-contract code.

## Configuration

| Env var | Default | Description |
|---|---|---|
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | SR base URL |
| `SCHEMA_REGISTRY_API_KEY` | _(none)_ | SR API key (authenticated SR) |
| `SCHEMA_REGISTRY_API_SECRET` | _(none)_ | SR API secret |

## Install

```bash
npm install @truther/contracts          # published
# or, from a checkout of this repo:
npm install file:../truther-contracts/sdk/node
```

## Quick start

```ts
import { TrutherSerde, Transaction } from '@truther/contracts';

const serde = new TrutherSerde();                  // reads SCHEMA_REGISTRY_URL
await serde.bind('transactions', Transaction);     // resolves the schema_id from SR

// Produce
const framed = serde.produce('transactions', Transaction.fromPartial({
  transactionAmount: '9.99',
  finalDecision: 'APPROVED',
}));
await kafkaProducer.send({ topic: 'transactions', messages: [{ value: framed }] });

// Consume — throws SerdeError on a bad payload → route to DLQ
const tx = await serde.consume('transactions', rawKafkaValue);
```

> The schema must already be registered in the Registry (by the contracts repo's
> registrador). The SDK only reads it.

## API

### `new TrutherSerde(options?)`
`{ srUrl?, srApiKey?, srApiSecret? }` — each falls back to the matching env var.

### `serde.bind(topic, Codec): Promise<void>`
Map a topic to its message type and resolve its `schema_id` from SR (subject
`<topic>-value`, latest version). Read-only; fails fast if the subject isn't
registered. Call once per topic at startup. `serde.startup({ topic: Codec, ... })`
binds many at once.

### `serde.produce(topic, msg): Buffer`
Serialize `msg` and wrap it in the Confluent envelope (cached `schema_id` + correct
`message-index`). Throws if the topic wasn't bound.

### `serde.consume(topic, data): Promise<T>`
Validate the envelope and deserialize into the bound type. Throws `SerdeError` on:
wrong magic byte, short frame, a `schema_id` that isn't a registered version of the
topic's subject, a `message-index` mismatch, or a decode failure. Route to the DLQ.

### `SerdeError`
```ts
class SerdeError extends Error {
  code:
    | 'INVALID_MAGIC_BYTE' | 'FRAME_TOO_SHORT' | 'TOPIC_NOT_BOUND'
    | 'SCHEMA_FOREIGN' | 'MESSAGE_INDEX_MISMATCH' | 'DESERIALIZATION_ERROR';
  rawPayload?: Buffer;
}
```

## Tests

```bash
npm test                 # unit (mock SR, no infra)
# integration (real SR): docker compose up -d && register schemas, then:
SCHEMA_REGISTRY_URL=http://localhost:8081 npm run test:integration
```

## Adding a new contract

Add the message to `proto/` and run `buf generate` — the SDK regenerates (types +
descriptor) and handles the new message generically. Register it in the Registry
(`scripts/register_schemas.py`), then `bind('<topic>', NewType)` in your service.
No per-contract SDK code.
