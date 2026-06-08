"use strict";
/**
 * Confluent Schema Registry wire-format framing.
 *
 * Wire layout:
 *   [0x00 magic] [schema_id: 4 bytes BE] [message-index array] [proto3 payload]
 *
 * The message-index array is variable length (Confluent encoding): a single
 * top-level message at index 0 is the 1-byte optimization 0x00; otherwise it is
 * zig-zag varints — the count, then each index. The index identifies which
 * message of the schema's .proto this payload is (declaration order).
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.FrameError = exports.MAGIC_BYTE = void 0;
exports.encodeMessageIndexes = encodeMessageIndexes;
exports.frameMessage = frameMessage;
exports.parseFrame = parseFrame;
exports.MAGIC_BYTE = 0x00;
/** Thrown by parseFrame on a malformed envelope; serde maps it to a SerdeError. */
class FrameError extends Error {
    constructor(message, code) {
        super(message);
        this.name = 'FrameError';
        this.code = code;
    }
}
exports.FrameError = FrameError;
/**
 * Encode the Confluent message-index array. The common case [0] (first message)
 * is the 1-byte optimization 0x00.
 */
function encodeMessageIndexes(indexes) {
    if (indexes.length === 1 && indexes[0] === 0) {
        return Buffer.from([0x00]);
    }
    const bytes = [];
    appendZigzag(bytes, indexes.length);
    for (const idx of indexes)
        appendZigzag(bytes, idx);
    return Buffer.from(bytes);
}
/** Build [0x00][schema_id_be4][msg-index][payload]. */
function frameMessage(schemaId, msgIndexBytes, payload) {
    const header = Buffer.allocUnsafe(5);
    header[0] = exports.MAGIC_BYTE;
    header.writeUInt32BE(schemaId >>> 0, 1);
    return Buffer.concat([header, msgIndexBytes, Buffer.from(payload)]);
}
/**
 * Strip and validate the Confluent SR envelope. Throws FrameError on a wrong
 * magic byte or an undersized frame — callers route the raw bytes to the DLQ.
 */
function parseFrame(data) {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (buf.length < 6) {
        throw new FrameError(`frame too short: ${buf.length} bytes (minimum 6)`, 'FRAME_TOO_SHORT');
    }
    if (buf[0] !== exports.MAGIC_BYTE) {
        throw new FrameError(`invalid magic byte: 0x${buf[0].toString(16).padStart(2, '0')} (expected 0x00)`, 'INVALID_MAGIC_BYTE');
    }
    const schemaId = buf.readUInt32BE(1);
    const { indexes, next } = readMessageIndexes(buf, 5);
    return { schemaId, indexes, payload: buf.subarray(next) };
}
function readMessageIndexes(buf, offset) {
    const first = readZigzag(buf, offset);
    offset = first.next;
    if (first.value === 0) {
        // 1-byte optimization: single index [0]
        return { indexes: [0], next: offset };
    }
    const indexes = [];
    for (let i = 0; i < first.value; i++) {
        const r = readZigzag(buf, offset);
        indexes.push(r.value);
        offset = r.next;
    }
    return { indexes, next: offset };
}
function appendZigzag(out, value) {
    let zz = (value << 1) ^ (value >> 31); // 32-bit zig-zag; indexes are small
    zz >>>= 0;
    while (zz >= 0x80) {
        out.push((zz & 0x7f) | 0x80);
        zz >>>= 7;
    }
    out.push(zz);
}
function readZigzag(buf, offset) {
    let ux = 0;
    let shift = 0;
    let n = 0;
    for (;;) {
        if (offset + n >= buf.length) {
            throw new FrameError('truncated message-index varint', 'FRAME_TOO_SHORT');
        }
        const b = buf[offset + n];
        n++;
        ux |= (b & 0x7f) << shift;
        if (b < 0x80)
            break;
        shift += 7;
    }
    ux >>>= 0;
    const value = (ux >>> 1) ^ -(ux & 1);
    return { value, next: offset + n };
}
//# sourceMappingURL=framing.js.map