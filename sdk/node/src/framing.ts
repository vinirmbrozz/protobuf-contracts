/**
 * Confluent Schema Registry wire-format framing — §2 of docs/confluent-sr-serde-spec.md.
 *
 * Wire layout (6-byte header + proto payload):
 *   [0x00] [schema_id: 4 bytes BE] [0x00 msg-index] [proto3 payload]
 */

export const MAGIC_BYTE = 0x00;
const HEADER_LENGTH = 6;

/**
 * Wrap serialized proto bytes in the Confluent SR envelope.
 * The message-index byte is always 0x00 (first/only message per schema subject,
 * per Truther convention — see spec §2.3).
 */
export function frameMessage(schemaId: number, msgBytes: Uint8Array): Buffer {
  const out = Buffer.allocUnsafe(HEADER_LENGTH + msgBytes.length);
  out[0] = MAGIC_BYTE;
  out.writeUInt32BE(schemaId >>> 0, 1); // unsigned 32-bit big-endian
  out[5] = 0x00;                        // message index
  Buffer.from(msgBytes).copy(out, HEADER_LENGTH);
  return out;
}

/** Parsed Confluent SR envelope. */
export interface ParsedFrame {
  schemaId: number;
  msgBytes: Buffer;
}

/**
 * Strip the Confluent SR envelope from a raw Kafka message value.
 * Throws on invalid magic byte or undersized frame — callers must route to DLQ.
 */
export function parseFrame(data: Buffer | Uint8Array): ParsedFrame {
  const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
  if (buf.length < HEADER_LENGTH) {
    throw new Error(`frame too short: ${buf.length} bytes (minimum ${HEADER_LENGTH})`);
  }
  if (buf[0] !== MAGIC_BYTE) {
    throw new Error(
      `invalid magic byte: 0x${buf[0].toString(16).padStart(2, '0')} (expected 0x00)`,
    );
  }
  return {
    schemaId: buf.readUInt32BE(1),
    msgBytes: buf.subarray(HEADER_LENGTH),
  };
}
