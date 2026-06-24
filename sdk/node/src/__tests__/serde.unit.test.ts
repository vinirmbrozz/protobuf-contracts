import http from 'node:http';
import type { AddressInfo } from 'node:net';

import { ProtobufSerde } from '../serde';
import { encodeMessageIndexes, frameMessage } from '../framing';
import { Transaction } from '../generated/protobuf/transaction/v1/transaction';
import { OnboardingCustomer } from '../generated/protobuf/onboarding/v1/onboarding';

/**
 * Read-only mock Schema Registry seeded with subject→id. Serves:
 *   GET /subjects/{subject}/versions/latest  → { id }
 *   GET /schemas/ids/{id}/versions           → [{ subject, version }]
 */
async function mockSR(subjectId: Record<string, number>): Promise<{ url: string; close: () => Promise<void> }> {
  const server = http.createServer((req, res) => {
    const parts = (req.url ?? '').split('/'); // ["", "subjects", sub, "versions", "latest"]
    res.setHeader('content-type', 'application/json');
    if (parts[1] === 'subjects' && parts[3] === 'versions' && parts[4] === 'latest') {
      const sub = decodeURIComponent(parts[2] ?? '');
      if (sub in subjectId) return void res.end(JSON.stringify({ id: subjectId[sub] }));
      res.statusCode = 404;
      return void res.end('{}');
    }
    if (parts[1] === 'schemas' && parts[2] === 'ids' && parts[4] === 'versions') {
      const id = Number(parts[3]);
      const out = Object.entries(subjectId)
        .filter(([, v]) => v === id)
        .map(([subject]) => ({ subject, version: 1 }));
      if (out.length === 0) {
        res.statusCode = 404;
        return void res.end('[]');
      }
      return void res.end(JSON.stringify(out));
    }
    res.statusCode = 404;
    res.end('{}');
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const port = (server.address() as AddressInfo).port;
  return {
    url: `http://127.0.0.1:${port}`,
    close: () => new Promise<void>((r) => server.close(() => r())),
  };
}

function sampleTx(): Transaction {
  return Transaction.create({
    transaction: { id: 'tx-1', amountTotal: '9.99', channel: 'web', type: 'PIX' },
    customer: { name: 'Ada', email: 'ada@example.com' },
  });
}

describe('ProtobufSerde (thin, mock SR)', () => {
  test('round-trip Transaction (index 0 → 0x00 msg-index)', async () => {
    const sr = await mockSR({ 'transactions-value': 42 });
    const serde = new ProtobufSerde({ srUrl: sr.url });
    await serde.bind('transactions', Transaction);

    const original = sampleTx();
    const framed = serde.produce('transactions', original);
    expect(framed[0]).toBe(0x00);
    expect(framed.readUInt32BE(1)).toBe(42);
    expect(framed[5]).toBe(0x00); // index 0 → single 0x00 byte

    const got = await serde.consume<Transaction>('transactions', framed);
    expect(got).toEqual(original);
    await sr.close();
  });

  test('round-trip OnboardingCustomer (index 1 → variable msg-index)', async () => {
    const sr = await mockSR({ 'cust-value': 7 });
    const serde = new ProtobufSerde({ srUrl: sr.url });
    await serde.bind('cust', OnboardingCustomer);

    const original = OnboardingCustomer.create({ id: 'c-1' });
    const framed = serde.produce('cust', original);
    expect([...framed.subarray(5, 7)]).toEqual([0x02, 0x02]); // msg-index for index 1

    const got = await serde.consume<OnboardingCustomer>('cust', framed);
    expect(got).toEqual(original);
    await sr.close();
  });

  test('bind fails when the subject is not registered', async () => {
    const sr = await mockSR({});
    const serde = new ProtobufSerde({ srUrl: sr.url });
    await expect(serde.bind('transactions', Transaction)).rejects.toThrow();
    await sr.close();
  });

  test('produce/consume on an unbound topic → TOPIC_NOT_BOUND', async () => {
    const sr = await mockSR({ 'transactions-value': 42 });
    const serde = new ProtobufSerde({ srUrl: sr.url });
    expect(() => serde.produce('transactions', sampleTx())).toThrow(
      expect.objectContaining({ code: 'TOPIC_NOT_BOUND' }),
    );
    await expect(serde.consume('transactions', Buffer.from([0x00, 0, 0, 0, 42, 0x00, 0x0a]))).rejects.toMatchObject({
      code: 'TOPIC_NOT_BOUND',
    });
    await sr.close();
  });

  describe('consumer security rejections', () => {
    test('wrong magic byte → INVALID_MAGIC_BYTE', async () => {
      const sr = await mockSR({ 'transactions-value': 42 });
      const serde = new ProtobufSerde({ srUrl: sr.url });
      await serde.bind('transactions', Transaction);
      await expect(serde.consume('transactions', Buffer.from([0x01, 0, 0, 0, 42, 0x00, 0x0a]))).rejects.toMatchObject(
        { code: 'INVALID_MAGIC_BYTE' },
      );
      await sr.close();
    });

    test('short frame → FRAME_TOO_SHORT', async () => {
      const sr = await mockSR({ 'transactions-value': 42 });
      const serde = new ProtobufSerde({ srUrl: sr.url });
      await serde.bind('transactions', Transaction);
      await expect(serde.consume('transactions', Buffer.from([0x00, 0, 0]))).rejects.toMatchObject({
        code: 'FRAME_TOO_SHORT',
      });
      await sr.close();
    });

    test('schema_id from another subject → SCHEMA_FOREIGN', async () => {
      const sr = await mockSR({ 'transactions-value': 42, 'other-value': 99 });
      const serde = new ProtobufSerde({ srUrl: sr.url });
      await serde.bind('transactions', Transaction);
      const bad = frameMessage(99, encodeMessageIndexes([0]), Transaction.encode(sampleTx()).finish());
      await expect(serde.consume('transactions', bad)).rejects.toMatchObject({ code: 'SCHEMA_FOREIGN' });
      await sr.close();
    });

    test('message-index mismatch → MESSAGE_INDEX_MISMATCH', async () => {
      const sr = await mockSR({ 'transactions-value': 42 });
      const serde = new ProtobufSerde({ srUrl: sr.url });
      await serde.bind('transactions', Transaction); // expects index 0
      const bad = frameMessage(42, encodeMessageIndexes([1]), Transaction.encode(sampleTx()).finish());
      await expect(serde.consume('transactions', bad)).rejects.toMatchObject({ code: 'MESSAGE_INDEX_MISMATCH' });
      await sr.close();
    });

    test('invalid payload → DESERIALIZATION_ERROR', async () => {
      const sr = await mockSR({ 'transactions-value': 42 });
      const serde = new ProtobufSerde({ srUrl: sr.url });
      await serde.bind('transactions', Transaction);
      const bad = frameMessage(42, encodeMessageIndexes([0]), Buffer.from([0xff, 0xff, 0xff]));
      await expect(serde.consume('transactions', bad)).rejects.toMatchObject({ code: 'DESERIALIZATION_ERROR' });
      await sr.close();
    });
  });
});
