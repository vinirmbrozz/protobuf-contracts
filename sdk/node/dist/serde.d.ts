import type { MessageFns, SerdeOptions } from './types';
export type SerdeErrorCode = 'INVALID_MAGIC_BYTE' | 'FRAME_TOO_SHORT' | 'TOPIC_NOT_BOUND' | 'SCHEMA_FOREIGN' | 'MESSAGE_INDEX_MISMATCH' | 'DESERIALIZATION_ERROR';
/**
 * Thrown by consume() on any payload rejection. The adapter routes the raw bytes
 * to the DLQ using `code` — it must never crash on a bad message.
 */
export declare class SerdeError extends Error {
    readonly code: SerdeErrorCode;
    readonly rawPayload?: Buffer;
    constructor(message: string, code: SerdeErrorCode, rawPayload?: Buffer);
}
export declare class TrutherSerde {
    private readonly sr;
    private readonly bindings;
    constructor(options?: SerdeOptions);
    /**
     * Map a topic to its message type and resolve the topic's schema_id from SR
     * (subject "<topic>-value", latest version). Read-only; fails fast if the
     * subject is not registered. Call once per topic at startup.
     */
    bind<T>(topic: string, codec: MessageFns<T>): Promise<void>;
    /** Bind every topic→codec pair (calls bind for each). */
    startup(bindings: Record<string, MessageFns<unknown>>): Promise<void>;
    /** Serialize msg and wrap it in the Confluent envelope (cached schema_id + correct msg-index). */
    produce<T>(topic: string, msg: T): Buffer;
    /**
     * Validate the envelope and deserialize into the bound type. Rejects (SerdeError)
     * on: wrong magic byte, short frame, schema_id not a registered version of this
     * topic's subject, message-index ≠ bound type, or decode failure. Adapter → DLQ.
     */
    consume<T>(topic: string, data: Buffer): Promise<T>;
}
//# sourceMappingURL=serde.d.ts.map