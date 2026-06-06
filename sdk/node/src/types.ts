/**
 * Core types for the Truther Kafka serde library.
 *
 * KnownMessage: union of all protobuf message types generated from proto/.
 * Extending this union (when a new .proto message is added) is the ONLY
 * change needed here to get compile-time safety across produce/consume.
 */

// Re-exported from the generated JSPB bindings so callers don't need a
// direct dependency on gen/node for types.
export type { Message } from 'google-protobuf';

/**
 * Codec adapts a generated protobuf class to the serde lib.
 * Works with both JSPB (gen/node) and ts-proto (gen/typescript) styles.
 */
export interface MessageCodec<T> {
  encode(msg: T): Uint8Array;
  decode(bytes: Uint8Array): T;
}

/** Options for constructing TrutherSerde. */
export interface SerdeOptions {
  /** Confluent Schema Registry URL. Read from SCHEMA_REGISTRY_URL if omitted. */
  srUrl?: string;
  /** SR API key (for authenticated clusters). Read from SCHEMA_REGISTRY_API_KEY. */
  srApiKey?: string;
  /** SR API secret. Read from SCHEMA_REGISTRY_API_SECRET. */
  srApiSecret?: string;
}

/** Shape of an error thrown by TrutherSerde on bad payloads. */
export interface SerdeErrorOptions {
  code: string;
  rawPayload?: Buffer;
}
