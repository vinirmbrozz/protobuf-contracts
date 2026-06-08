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
export declare const MAGIC_BYTE = 0;
export type FrameErrorCode = 'INVALID_MAGIC_BYTE' | 'FRAME_TOO_SHORT';
/** Thrown by parseFrame on a malformed envelope; serde maps it to a SerdeError. */
export declare class FrameError extends Error {
    readonly code: FrameErrorCode;
    constructor(message: string, code: FrameErrorCode);
}
/** Parsed Confluent SR envelope. */
export interface ParsedFrame {
    schemaId: number;
    /** message-index path (declaration order); top-level messages → single element */
    indexes: number[];
    payload: Buffer;
}
/**
 * Encode the Confluent message-index array. The common case [0] (first message)
 * is the 1-byte optimization 0x00.
 */
export declare function encodeMessageIndexes(indexes: number[]): Buffer;
/** Build [0x00][schema_id_be4][msg-index][payload]. */
export declare function frameMessage(schemaId: number, msgIndexBytes: Buffer, payload: Uint8Array): Buffer;
/**
 * Strip and validate the Confluent SR envelope. Throws FrameError on a wrong
 * magic byte or an undersized frame — callers route the raw bytes to the DLQ.
 */
export declare function parseFrame(data: Buffer | Uint8Array): ParsedFrame;
//# sourceMappingURL=framing.d.ts.map