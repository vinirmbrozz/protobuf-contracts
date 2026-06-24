/**
 * ProtobufSerde — thin Confluent SR serde for Kafka (Decision A).
 *
 * The SDK never reads a .proto and never registers schemas. bind() resolves the
 * topic's schema_id from the Schema Registry (read-only); produce() stamps the
 * Confluent envelope; consume() validates it (strictly) and deserializes into
 * the bound type.
 *
 * Usage:
 *   import { ProtobufSerde } from '@protobuf/contracts';
 *   import { Transaction } from '@protobuf/contracts';
 *
 *   const serde = new ProtobufSerde();             // reads SCHEMA_REGISTRY_URL
 *   await serde.bind('transactions', Transaction); // resolves schema_id at startup
 *   const framed = serde.produce('transactions', Transaction.create({ ... }));
 *   const tx = await serde.consume('transactions', rawValue); // -> Transaction
 */
import { encodeMessageIndexes, frameMessage, parseFrame, FrameError } from './framing';
import { messageIndexFor } from './message-index';
import { SchemaRegistryClient } from './schema-registry-client';
import type { MessageFns, SerdeOptions } from './types';

export type SerdeErrorCode =
  | 'INVALID_MAGIC_BYTE'
  | 'FRAME_TOO_SHORT'
  | 'TOPIC_NOT_BOUND'
  | 'SCHEMA_FOREIGN'
  | 'MESSAGE_INDEX_MISMATCH'
  | 'DESERIALIZATION_ERROR';

/**
 * Thrown by consume() on any payload rejection. The adapter routes the raw bytes
 * to the DLQ using `code` — it must never crash on a bad message.
 */
export class SerdeError extends Error {
  readonly code: SerdeErrorCode;
  readonly rawPayload?: Buffer;
  constructor(message: string, code: SerdeErrorCode, rawPayload?: Buffer) {
    super(message);
    this.name = 'SerdeError';
    this.code = code;
    this.rawPayload = rawPayload;
  }
}

interface Binding<T> {
  codec: MessageFns<T>;
  subject: string;
  schemaId: number;
  msgIndexBytes: Buffer;
  indexes: number[];
}

export class ProtobufSerde {
  private readonly sr: SchemaRegistryClient;
  private readonly bindings = new Map<string, Binding<unknown>>();

  constructor(options: SerdeOptions = {}) {
    const srUrl = options.srUrl ?? process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';
    this.sr = new SchemaRegistryClient(
      srUrl,
      options.srApiKey ?? process.env['SCHEMA_REGISTRY_API_KEY'],
      options.srApiSecret ?? process.env['SCHEMA_REGISTRY_API_SECRET'],
    );
  }

  /**
   * Map a topic to its message type and resolve the topic's schema_id from SR
   * (subject "<topic>-value", latest version). Read-only; fails fast if the
   * subject is not registered. Call once per topic at startup.
   */
  async bind<T>(topic: string, codec: MessageFns<T>): Promise<void> {
    const subject = `${topic}-value`;
    const schemaId = await this.sr.latestId(subject);
    const indexes = messageIndexFor(codec.$type);
    this.bindings.set(topic, {
      codec: codec as MessageFns<unknown>,
      subject,
      schemaId,
      msgIndexBytes: encodeMessageIndexes(indexes),
      indexes,
    });
  }

  /** Bind every topic→codec pair (calls bind for each). */
  async startup(bindings: Record<string, MessageFns<unknown>>): Promise<void> {
    for (const [topic, codec] of Object.entries(bindings)) {
      await this.bind(topic, codec);
    }
  }

  /** Serialize msg and wrap it in the Confluent envelope (cached schema_id + correct msg-index). */
  produce<T>(topic: string, msg: T): Buffer {
    const b = this.bindings.get(topic);
    if (!b) {
      throw new SerdeError(`No binding for topic '${topic}'. Call bind() at startup.`, 'TOPIC_NOT_BOUND');
    }
    const payload = b.codec.encode(msg).finish();
    return frameMessage(b.schemaId, b.msgIndexBytes, payload);
  }

  /**
   * Validate the envelope and deserialize into the bound type. Rejects (SerdeError)
   * on: wrong magic byte, short frame, schema_id not a registered version of this
   * topic's subject, message-index ≠ bound type, or decode failure. Adapter → DLQ.
   */
  async consume<T>(topic: string, data: Buffer): Promise<T> {
    const b = this.bindings.get(topic);
    if (!b) {
      throw new SerdeError(`No binding for topic '${topic}'. Call bind() at startup.`, 'TOPIC_NOT_BOUND', data);
    }

    let schemaId: number;
    let indexes: number[];
    let payload: Buffer;
    try {
      ({ schemaId, indexes, payload } = parseFrame(data));
    } catch (err) {
      const code = err instanceof FrameError ? err.code : 'INVALID_MAGIC_BYTE';
      throw new SerdeError(`[${topic}] ${(err as Error).message}`, code, data);
    }

    // Security: the id must be a registered version of THIS topic's subject.
    if (!(await this.sr.idBelongsToSubject(schemaId, b.subject))) {
      throw new SerdeError(
        `[${topic}] schema_id=${schemaId} is not a registered version of '${b.subject}'`,
        'SCHEMA_FOREIGN',
        data,
      );
    }

    if (!indexesEqual(indexes, b.indexes)) {
      throw new SerdeError(
        `[${topic}] message-index ${JSON.stringify(indexes)} != bound ${JSON.stringify(b.indexes)}`,
        'MESSAGE_INDEX_MISMATCH',
        data,
      );
    }

    try {
      return (b.codec as MessageFns<T>).decode(payload);
    } catch (err) {
      throw new SerdeError(`[${topic}] deserialization failed: ${(err as Error).message}`, 'DESERIALIZATION_ERROR', data);
    }
  }
}

function indexesEqual(a: number[], b: number[]): boolean {
  return a.length === b.length && a.every((v, i) => v === b[i]);
}
