/**
 * Integration test — requires docker-compose stack:
 *   docker-compose up -d schema-registry
 *
 * Set SCHEMA_REGISTRY_URL=http://localhost:8081 (default).
 * Run with: npm run test:integration
 *
 * Uses the CommonJS JSPB bindings from gen/node/ and the real SR REST API.
 */

// eslint-disable-next-line @typescript-eslint/no-require-imports
const path = require('path') as typeof import('path');
// eslint-disable-next-line @typescript-eslint/no-require-imports
const fs = require('fs') as typeof import('fs');

import { TrutherSerde, SerdeError } from '../serde';
import type { MessageCodec } from '../types';

const SR_URL = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';
const PROTO_PATH = path.resolve(__dirname, '../../../../proto/transaction.proto');

// ── helpers ─────────────────────────────────────────────────────────────────

function srAvailable(): Promise<boolean> {
  return fetch(`${SR_URL}/subjects`)
    .then((r) => r.ok)
    .catch(() => false);
}

// Minimal JSPB codec shim — works without the full gen/node package installed
// by using the CommonJS bindings relative to the repo root.
function loadJspbCodec(): { codec: MessageCodec<unknown>; makeTx: (amount: string) => unknown } {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const pb = require(path.resolve(__dirname, '../../../../gen/node/transaction_pb.js'));
  const { Transaction, PredictiveAnalyzer } = pb as {
    Transaction: {
      new (): {
        setTransactionamount(v: string): void;
        setPredictiveanalyzer(v: unknown): void;
        setFinalDecision(v: string): void;
        serializeBinary(): Uint8Array;
        getTransactionamount(): string;
        getFinalDecision(): string;
      };
      deserializeBinary(b: Uint8Array): unknown;
    };
    PredictiveAnalyzer: {
      new (): {
        setIsallowed(v: boolean): void;
        setReason(v: string): void;
        setCardid(v: string): void;
        setUserid(v: string): void;
      };
    };
  };

  const codec: MessageCodec<unknown> = {
    encode: (msg) => (msg as { serializeBinary(): Uint8Array }).serializeBinary(),
    decode: (bytes) => Transaction.deserializeBinary(bytes),
  };

  function makeTx(amount: string) {
    const pa = new PredictiveAnalyzer();
    pa.setIsallowed(true);
    pa.setReason('integration-test');
    pa.setCardid('card-int-001');
    pa.setUserid('user-int-001');
    const tx = new Transaction();
    tx.setTransactionamount(amount);
    tx.setPredictiveanalyzer(pa);
    tx.setFinalDecision('APPROVED');
    return tx;
  }

  return { codec, makeTx };
}

// ── tests ────────────────────────────────────────────────────────────────────

describe('TrutherSerde — integration (requires docker-compose SR)', () => {
  let serde: TrutherSerde;
  let protoContent: string;
  let available = false;

  beforeAll(async () => {
    available = await srAvailable();
    if (!available) return;
    protoContent = fs.readFileSync(PROTO_PATH, 'utf8');
    serde = new TrutherSerde({ srUrl: SR_URL });
  });

  function skip() {
    if (!available) {
      console.warn(`  [SKIP] Schema Registry not reachable at ${SR_URL}`);
      return true;
    }
    return false;
  }

  it('registers transaction schema and returns a numeric schema_id', async () => {
    if (skip()) return;
    const id = await serde.registerSchema('transactions-integration', protoContent);
    expect(typeof id).toBe('number');
    expect(id).toBeGreaterThan(0);
  });

  it('produce → consume roundtrip with real SR (JSPB codec)', async () => {
    if (skip()) return;
    const { codec, makeTx } = loadJspbCodec();
    await serde.registerSchema('transactions-integration', protoContent);

    const tx = makeTx('499.99');
    const buf = serde.produce('transactions-integration', tx, codec);

    expect(buf[0]).toBe(0x00); // magic byte
    expect(buf[5]).toBe(0x00); // message index

    const decoded = await serde.consume('transactions-integration', buf, codec);
    const decodedTx = decoded as ReturnType<typeof makeTx> & {
      getTransactionamount(): string;
      getFinalDecision(): string;
    };
    expect(decodedTx.getTransactionamount()).toBe('499.99');
    expect(decodedTx.getFinalDecision()).toBe('APPROVED');
  });

  it('rejects invalid magic byte → SerdeError INVALID_MAGIC_BYTE', async () => {
    if (skip()) return;
    const bad = Buffer.from([0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a]);
    const { codec } = loadJspbCodec();
    await expect(serde.consume('transactions-integration', bad, codec)).rejects.toMatchObject({
      code: 'INVALID_MAGIC_BYTE',
    });
  });

  it('rejects unknown schema_id → SerdeError UNKNOWN_SCHEMA_ID', async () => {
    if (skip()) return;
    const { codec } = loadJspbCodec();
    // schema_id=99999 almost certainly does not exist in a fresh SR instance
    const framed = Buffer.allocUnsafe(6 + 4);
    framed[0] = 0x00;
    framed.writeUInt32BE(99999, 1);
    framed[5] = 0x00;
    framed.fill(0x00, 6);
    await expect(serde.consume('transactions-integration', framed, codec)).rejects.toMatchObject({
      code: 'UNKNOWN_SCHEMA_ID',
    });
  });

  it('rejects corrupt payload → SerdeError DESERIALIZATION_ERROR', async () => {
    if (skip()) return;
    const { codec } = loadJspbCodec();
    await serde.registerSchema('transactions-integration', protoContent);
    const registered = new TrutherSerde({ srUrl: SR_URL });
    await registered.registerSchema('transactions-integration', protoContent);
    // Produce real framing but with garbage proto bytes
    const id = await serde.registerSchema('transactions-integration', protoContent);
    const corrupt = Buffer.allocUnsafe(6 + 5);
    corrupt[0] = 0x00;
    corrupt.writeUInt32BE(id, 1);
    corrupt[5] = 0x00;
    corrupt.fill(0xff, 6); // invalid proto3 bytes
    await expect(serde.consume('transactions-integration', corrupt, codec)).rejects.toMatchObject({
      code: 'DESERIALIZATION_ERROR',
    });
  });

  it('schema registration is idempotent (same schema_id on repeat call)', async () => {
    if (skip()) return;
    const id1 = await serde.registerSchema('transactions-integration', protoContent);
    const id2 = await serde.registerSchema('transactions-integration', protoContent);
    expect(id1).toBe(id2);
  });
});
