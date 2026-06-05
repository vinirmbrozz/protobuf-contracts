import { TrutherSerde, SerdeError } from '../serde';
import { frameMessage } from '../framing';
import type { MessageCodec } from '../types';

// ---------------------------------------------------------------------------
// Minimal test codec — JSON-over-Buffer, no real proto runtime needed.
// ---------------------------------------------------------------------------

interface Msg {
  id: string;
  amount: string;
}

const MsgCodec: MessageCodec<Msg> = {
  encode: (m) => Buffer.from(JSON.stringify(m)),
  decode: (b) => JSON.parse(Buffer.from(b).toString()) as Msg,
};

// ---------------------------------------------------------------------------
// Mock global fetch for Schema Registry calls.
// ---------------------------------------------------------------------------

const mockFetch = jest.fn();
global.fetch = mockFetch as unknown as typeof fetch;

function okJson(body: unknown) {
  return Promise.resolve({
    ok: true,
    status: 200,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  } as Response);
}

function errResponse(status: number, body = '') {
  return Promise.resolve({
    ok: false,
    status,
    json: () => Promise.resolve({}),
    text: () => Promise.resolve(body),
  } as Response);
}

function notFound() {
  return Promise.resolve({
    ok: false,
    status: 404,
    json: () => Promise.resolve({}),
    text: () => Promise.resolve('not found'),
  } as Response);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('TrutherSerde — unit (fetch mocked)', () => {
  let serde: TrutherSerde;

  beforeEach(() => {
    jest.clearAllMocks();
    serde = new TrutherSerde({ srUrl: 'http://localhost:8081' });
  });

  // ---------- registerSchema ----------

  describe('registerSchema', () => {
    it('posts to /subjects/<topic>-value/versions with PROTOBUF schema', async () => {
      mockFetch.mockReturnValueOnce(okJson({ id: 1 }));
      const id = await serde.registerSchema('transactions', 'syntax = "proto3";');
      expect(id).toBe(1);
      expect(mockFetch).toHaveBeenCalledWith(
        'http://localhost:8081/subjects/transactions-value/versions',
        expect.objectContaining({
          method: 'POST',
          body: expect.stringContaining('"schemaType":"PROTOBUF"'),
        }),
      );
    });

    it('caches the returned schema_id', async () => {
      mockFetch.mockReturnValueOnce(okJson({ id: 7 }));
      await serde.registerSchema('payments', '...');
      // produce should work without network now
      const buf = serde.produce('payments', { id: 'x', amount: '1.00' }, MsgCodec);
      expect(buf.readUInt32BE(1)).toBe(7);
    });

    it('throws when SR returns non-OK status', async () => {
      mockFetch.mockReturnValueOnce(errResponse(500, 'Internal Server Error'));
      await expect(serde.registerSchema('bad', '...')).rejects.toThrow(
        "SR registration failed for subject 'bad-value': 500",
      );
    });
  });

  // ---------- produce ----------

  describe('produce', () => {
    beforeEach(async () => {
      mockFetch.mockReturnValueOnce(okJson({ id: 3 }));
      await serde.registerSchema('transactions', '...');
    });

    it('returns Buffer with Confluent envelope magic byte 0x00', () => {
      const buf = serde.produce('transactions', { id: 'a', amount: '9.99' }, MsgCodec);
      expect(buf[0]).toBe(0x00);
    });

    it('encodes schema_id as big-endian uint32 at offset 1', () => {
      const buf = serde.produce('transactions', { id: 'a', amount: '9.99' }, MsgCodec);
      expect(buf.readUInt32BE(1)).toBe(3);
    });

    it('message-index byte at offset 5 is 0x00', () => {
      const buf = serde.produce('transactions', { id: 'a', amount: '9.99' }, MsgCodec);
      expect(buf[5]).toBe(0x00);
    });

    it('throws when topic has no registered schema', () => {
      expect(() => serde.produce('unknown-topic', { id: 'x', amount: '0' }, MsgCodec)).toThrow(
        "No schema registered for topic 'unknown-topic'",
      );
    });

    it('produce → consume roundtrip (same message)', async () => {
      // The registry already has schema_id=3 cached from beforeEach registration,
      // so verifySchemaId will not hit network.
      const msg: Msg = { id: 'roundtrip-1', amount: '42.00' };
      const buf = serde.produce('transactions', msg, MsgCodec);
      const decoded = await serde.consume('transactions', buf, MsgCodec);
      expect(decoded).toEqual(msg);
    });
  });

  // ---------- consume ----------

  describe('consume', () => {
    beforeEach(async () => {
      mockFetch.mockReturnValueOnce(okJson({ id: 5 }));
      await serde.registerSchema('transactions', '...');
    });

    it('decodes a validly framed message (schema_id cached)', async () => {
      const msg: Msg = { id: 'tx-1', amount: '100.00' };
      const buf = serde.produce('transactions', msg, MsgCodec);
      const decoded = await serde.consume('transactions', buf, MsgCodec);
      expect(decoded).toEqual(msg);
    });

    it('verifies unknown schema_id against SR (cache miss → fetch)', async () => {
      // schema_id=99 was never registered locally; SR returns it
      mockFetch.mockReturnValueOnce(okJson({ schema: '...' })); // GET /schemas/ids/99
      const framed = frameMessage(99, Buffer.from(JSON.stringify({ id: 'x', amount: '0' })));
      const decoded = await serde.consume('transactions', framed, MsgCodec);
      expect((decoded as Msg).id).toBe('x');
    });

    it('throws SerdeError INVALID_MAGIC_BYTE on bad first byte', async () => {
      const bad = Buffer.from([0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a]);
      await expect(serde.consume('transactions', bad, MsgCodec)).rejects.toMatchObject({
        code: 'INVALID_MAGIC_BYTE',
      });
    });

    it('throws SerdeError INVALID_MAGIC_BYTE on undersized frame', async () => {
      const short = Buffer.from([0x00, 0x00, 0x01]);
      await expect(serde.consume('transactions', short, MsgCodec)).rejects.toMatchObject({
        code: 'INVALID_MAGIC_BYTE',
      });
    });

    it('throws SerdeError UNKNOWN_SCHEMA_ID when SR returns 404', async () => {
      mockFetch.mockReturnValueOnce(notFound());
      const framed = frameMessage(9999, Buffer.from('payload'));
      await expect(serde.consume('transactions', framed, MsgCodec)).rejects.toMatchObject({
        code: 'UNKNOWN_SCHEMA_ID',
      });
    });

    it('throws SerdeError DESERIALIZATION_ERROR on corrupt bytes', async () => {
      // schema_id=5 is cached from beforeEach; no fetch needed
      const corrupt = frameMessage(5, Buffer.from('not-json-!@#'));
      const badCodec: MessageCodec<Msg> = {
        encode: MsgCodec.encode,
        decode: () => { throw new Error('bad proto'); },
      };
      await expect(serde.consume('transactions', corrupt, badCodec)).rejects.toMatchObject({
        code: 'DESERIALIZATION_ERROR',
      });
    });

    it('SerdeError carries rawPayload for DLQ routing', async () => {
      const bad = Buffer.from([0xff, 0x00, 0x00, 0x00, 0x01, 0x00]);
      try {
        await serde.consume('transactions', bad, MsgCodec);
        fail('should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(SerdeError);
        expect((e as SerdeError).rawPayload).toEqual(bad);
      }
    });
  });

  // ---------- env-var configuration ----------

  describe('constructor — env-var fallbacks', () => {
    const ORIG_ENV = process.env;

    beforeEach(() => {
      process.env = { ...ORIG_ENV };
    });

    afterEach(() => {
      process.env = ORIG_ENV;
    });

    it('reads SR URL from SCHEMA_REGISTRY_URL', async () => {
      process.env['SCHEMA_REGISTRY_URL'] = 'http://sr-from-env:8081';
      const s = new TrutherSerde();
      mockFetch.mockReturnValueOnce(okJson({ id: 1 }));
      await s.registerSchema('t', '...');
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('sr-from-env'),
        expect.anything(),
      );
    });

    it('defaults to http://localhost:8081 when no env var set', async () => {
      delete process.env['SCHEMA_REGISTRY_URL'];
      const s = new TrutherSerde();
      mockFetch.mockReturnValueOnce(okJson({ id: 1 }));
      await s.registerSchema('t', '...');
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('localhost:8081'),
        expect.anything(),
      );
    });
  });
});
