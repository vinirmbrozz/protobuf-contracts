/**
 * Integration test against a REAL Confluent Schema Registry.
 * Requires the schema registered (scripts/register_schemas.py) and the env var:
 *
 *   docker-compose up -d
 *   SCHEMA_REGISTRY_URL=http://localhost:8081 python scripts/register_schemas.py
 *   SCHEMA_REGISTRY_URL=http://localhost:8081 npm run test:integration
 */
import { ProtobufSerde } from '../serde';
import { encodeMessageIndexes, frameMessage } from '../framing';
import { Transaction } from '../generated/protobuf/transaction/v1/transaction';

const SR_URL = process.env['SCHEMA_REGISTRY_URL'];
const maybe = SR_URL ? describe : describe.skip;

maybe('ProtobufSerde against real SR', () => {
  test('round-trip a Transaction via the real registry', async () => {
    const serde = new ProtobufSerde({ srUrl: SR_URL });
    await serde.bind('transactions', Transaction);

    const original = Transaction.create({
      transaction: { id: 'tx-1', amountTotal: '9.99', channel: 'web', type: 'PIX' },
      customer: { name: 'Ada', email: 'ada@example.com' },
    });
    const framed = serde.produce('transactions', original);
    const got = await serde.consume<Transaction>('transactions', framed);
    expect(got).toEqual(original);
  });

  test('a bogus schema_id is rejected by the real registry', async () => {
    const serde = new ProtobufSerde({ srUrl: SR_URL });
    await serde.bind('transactions', Transaction);
    const bad = frameMessage(987654, encodeMessageIndexes([0]), Transaction.encode(Transaction.create({})).finish());
    await expect(serde.consume('transactions', bad)).rejects.toMatchObject({ code: 'SCHEMA_FOREIGN' });
  });
});
