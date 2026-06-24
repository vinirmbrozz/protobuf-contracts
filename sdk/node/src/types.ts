/**
 * Core types for the thin Kafka serde.
 *
 * MessageFns is the structural shape of a ts-proto generated codec (the const
 * exported per message, e.g. `Transaction`). It carries the message's full name
 * ($type, from outputTypeRegistry) plus encode/decode. The SDK uses $type to
 * resolve the message-index generically — no per-contract code.
 */
export interface MessageFns<T> {
  readonly $type: string;
  encode(message: T): { finish(): Uint8Array };
  decode(input: Uint8Array): T;
}

/** Options for constructing ProtobufSerde. */
export interface SerdeOptions {
  /** Schema Registry URL. Falls back to SCHEMA_REGISTRY_URL, then localhost:8081. */
  srUrl?: string;
  /** SR API key (authenticated clusters). Falls back to SCHEMA_REGISTRY_API_KEY. */
  srApiKey?: string;
  /** SR API secret. Falls back to SCHEMA_REGISTRY_API_SECRET. */
  srApiSecret?: string;
}
