import { frameMessage, parseFrame, MAGIC_BYTE } from '../framing';

describe('framing — unit', () => {
  const PAYLOAD = Buffer.from([0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f]);

  describe('frameMessage', () => {
    it('sets magic byte 0x00 at offset 0', () => {
      const framed = frameMessage(1, PAYLOAD);
      expect(framed[0]).toBe(MAGIC_BYTE);
    });

    it('encodes schema_id as 4-byte big-endian at offset 1', () => {
      const framed = frameMessage(42, PAYLOAD);
      expect(framed.readUInt32BE(1)).toBe(42);
    });

    it('writes message-index 0x00 at offset 5', () => {
      const framed = frameMessage(1, PAYLOAD);
      expect(framed[5]).toBe(0x00);
    });

    it('copies payload starting at offset 6', () => {
      const framed = frameMessage(1, PAYLOAD);
      expect(framed.subarray(6)).toEqual(PAYLOAD);
    });

    it('total length = 6 + payload length', () => {
      const framed = frameMessage(1, PAYLOAD);
      expect(framed.length).toBe(6 + PAYLOAD.length);
    });

    it('handles schema_id = 0', () => {
      const framed = frameMessage(0, PAYLOAD);
      expect(framed.readUInt32BE(1)).toBe(0);
    });

    it('handles max uint32 schema_id (4294967295)', () => {
      const framed = frameMessage(4294967295, PAYLOAD);
      expect(framed.readUInt32BE(1)).toBe(4294967295);
    });

    it('handles empty payload', () => {
      const framed = frameMessage(1, new Uint8Array(0));
      expect(framed.length).toBe(6);
    });

    it('accepts Uint8Array payload (not only Buffer)', () => {
      const u8 = new Uint8Array([0x01, 0x02, 0x03]);
      const framed = frameMessage(5, u8);
      expect(framed.subarray(6)).toEqual(Buffer.from(u8));
    });
  });

  describe('parseFrame', () => {
    it('returns correct schemaId and msgBytes', () => {
      const { schemaId, msgBytes } = parseFrame(frameMessage(99, PAYLOAD));
      expect(schemaId).toBe(99);
      expect(msgBytes).toEqual(PAYLOAD);
    });

    it('throws on frame shorter than 6 bytes', () => {
      expect(() => parseFrame(Buffer.from([0x00, 0x00]))).toThrow('frame too short');
      expect(() => parseFrame(Buffer.alloc(0))).toThrow('frame too short');
      expect(() => parseFrame(Buffer.alloc(5))).toThrow('frame too short');
    });

    it('throws on magic byte 0x01', () => {
      const bad = Buffer.from([0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a]);
      expect(() => parseFrame(bad)).toThrow('invalid magic byte');
      expect(() => parseFrame(bad)).toThrow('0x01');
    });

    it('throws on magic byte 0xFF', () => {
      const bad = Buffer.from([0xff, 0x00, 0x00, 0x00, 0x01, 0x00]);
      expect(() => parseFrame(bad)).toThrow('0xff');
    });

    it('accepts Uint8Array input', () => {
      const framed = frameMessage(7, PAYLOAD);
      const u8 = new Uint8Array(framed);
      const { schemaId } = parseFrame(u8);
      expect(schemaId).toBe(7);
    });
  });

  describe('frameMessage → parseFrame roundtrip', () => {
    it('roundtrips arbitrary payload without mutation', () => {
      const original = Buffer.from('some protobuf bytes 1234');
      const schemaId = 12345;
      const framed = frameMessage(schemaId, original);
      const { schemaId: parsed, msgBytes } = parseFrame(framed);
      expect(parsed).toBe(schemaId);
      expect(msgBytes.equals(original)).toBe(true);
    });

    it('roundtrips large payload', () => {
      const big = Buffer.alloc(65536, 0xab);
      const { schemaId, msgBytes } = parseFrame(frameMessage(1, big));
      expect(schemaId).toBe(1);
      expect(msgBytes.length).toBe(65536);
      expect(msgBytes.every((b) => b === 0xab)).toBe(true);
    });
  });
});
