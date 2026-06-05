import { frameMessage, parseFrame } from './framing';
import { SchemaRegistryClient } from './schema-registry-client';
import type { MessageCodec, SerdeOptions } from './types';

/**
 * Thrown by TrutherSerde.consume() on any payload rejection.
 * Callers must route to the dead-letter topic instead of crashing.
 */
export class SerdeError extends Error {
  readonly code: string;
  readonly rawPayload?: Buffer;

  constructor(message: string, code: string, rawPayload?: Buffer) {
    super(message);
    this.name = 'SerdeError';
    this.code = code;
    this.rawPayload = rawPayload;
  }
}

export type SerdeErrorCode =
  | 'INVALID_MAGIC_BYTE'
  | 'UNKNOWN_SCHEMA_ID'
  | 'DESERIALIZATION_ERROR';

/**
 * TrutherSerde — Confluent SR-backed serde for Kafka messages.
 *
 * Usage:
 *   const serde = new TrutherSerde();
 *   await serde.registerSchema('transactions', protoFileContent);
 *
 *   // Producer side
 *   const buf = serde.produce('transactions', txMsg, TransactionCodec);
 *   await kafkaProducer.send({ topic: 'transactions', messages: [{ value: buf }] });
 *
 *   // Consumer side
 *   const msg = await serde.consume('transactions', rawValue, TransactionCodec);
 */
export class TrutherSerde {
  private readonly sr: SchemaRegistryClient;
  /** topic → cached schema_id registered at startup */
  private readonly topicSchemaIds = new Map<string, number>();

  constructor(options: SerdeOptions = {}) {
    const srUrl =
      options.srUrl ??
      process.env['SCHEMA_REGISTRY_URL'] ??
      'http://localhost:8081';
    const apiKey = options.srApiKey ?? process.env['SCHEMA_REGISTRY_API_KEY'];
    const apiSecret =
      options.srApiSecret ?? process.env['SCHEMA_REGISTRY_API_SECRET'];
    this.sr = new SchemaRegistryClient(srUrl, apiKey, apiSecret);
  }

  /**
   * Register the proto schema for a topic with Schema Registry (TopicNameStrategy).
   * MUST be called at service startup before the first produce().
   * Idempotent — safe to call on every startup.
   *
   * @param topic        Kafka topic name (subject will be `${topic}-value`)
   * @param protoContent Raw .proto file content (as a string)
   */
  async registerSchema(topic: string, protoContent: string): Promise<number> {
    const subject = `${topic}-value`;
    const id = await this.sr.registerSchema(subject, protoContent);
    this.topicSchemaIds.set(topic, id);
    return id;
  }

  /**
   * Serialize a proto message and wrap it in the Confluent SR envelope.
   * Returns a Buffer ready to be sent as a Kafka message value.
   *
   * Throws if registerSchema() was not called for this topic at startup.
   * Only types with a registered codec are accepted — compile-time safety
   * is enforced by the `MessageCodec<T>` generic parameter.
   */
  produce<T>(topic: string, msg: T, codec: MessageCodec<T>): Buffer {
    const schemaId = this.topicSchemaIds.get(topic);
    if (schemaId === undefined) {
      throw new Error(
        `No schema registered for topic '${topic}'. Call registerSchema() at startup.`,
      );
    }
    const msgBytes = codec.encode(msg);
    return frameMessage(schemaId, msgBytes);
  }

  /**
   * Validate and deserialize a raw Kafka message value.
   *
   * Rejects (throws SerdeError) on:
   *   - INVALID_MAGIC_BYTE   — first byte !== 0x00
   *   - UNKNOWN_SCHEMA_ID    — schema_id not found in Schema Registry
   *   - DESERIALIZATION_ERROR — proto3 decode failure
   *
   * On rejection, callers MUST route the original bytes to the DLQ topic
   * (`${topic}-dead-letter`) and emit a metric. Never crash.
   */
  async consume<T>(
    topic: string,
    data: Buffer,
    codec: MessageCodec<T>,
  ): Promise<T> {
    let schemaId: number;
    let msgBytes: Buffer;

    try {
      ({ schemaId, msgBytes } = parseFrame(data));
    } catch (err) {
      throw new SerdeError(
        `[${topic}] ${(err as Error).message}`,
        'INVALID_MAGIC_BYTE',
        data,
      );
    }

    const knownSubject = await this.sr.verifySchemaId(schemaId);
    if (knownSubject === null) {
      throw new SerdeError(
        `[${topic}] Unknown schema_id=${schemaId} — not registered in Schema Registry`,
        'UNKNOWN_SCHEMA_ID',
        data,
      );
    }

    try {
      return codec.decode(msgBytes);
    } catch (err) {
      throw new SerdeError(
        `[${topic}] Protobuf deserialization failed: ${(err as Error).message}`,
        'DESERIALIZATION_ERROR',
        data,
      );
    }
  }
}
