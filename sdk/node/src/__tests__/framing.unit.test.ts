import {
  encodeMessageIndexes,
  frameMessage,
  parseFrame,
  FrameError,
} from '../framing';

describe('framing', () => {
  test('encodeMessageIndexes [0] uses the single-byte 0x00 optimization', () => {
    expect([...encodeMessageIndexes([0])]).toEqual([0x00]);
  });

  test('encodeMessageIndexes [1] is zig-zag varint count=1, index=1 → 02 02', () => {
    expect([...encodeMessageIndexes([1])]).toEqual([0x02, 0x02]);
  });

  test('frame/parse round-trip at index 1 (variable-length msg-index)', () => {
    const payload = Buffer.from([0x0a, 0x03, 1, 2, 3]);
    const framed = frameMessage(42, encodeMessageIndexes([1]), payload);
    expect(framed[0]).toBe(0x00);
    expect(framed.readUInt32BE(1)).toBe(42);

    const parsed = parseFrame(framed);
    expect(parsed.schemaId).toBe(42);
    expect(parsed.indexes).toEqual([1]);
    expect([...parsed.payload]).toEqual([...payload]);
  });

  test('frame/parse round-trip at index 0', () => {
    const parsed = parseFrame(frameMessage(7, encodeMessageIndexes([0]), Buffer.from([9])));
    expect(parsed.schemaId).toBe(7);
    expect(parsed.indexes).toEqual([0]);
  });

  test('parseFrame rejects a wrong magic byte', () => {
    expect(() => parseFrame(Buffer.from([0x01, 0, 0, 0, 42, 0]))).toThrow(FrameError);
  });

  test('parseFrame rejects an undersized frame', () => {
    expect(() => parseFrame(Buffer.from([0x00, 0, 0]))).toThrow(FrameError);
  });
});
