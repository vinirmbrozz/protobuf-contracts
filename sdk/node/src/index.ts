export { TrutherSerde, SerdeError } from './serde';
export { frameMessage, parseFrame, MAGIC_BYTE } from './framing';
export { SchemaRegistryClient } from './schema-registry-client';
export type { MessageCodec, SerdeOptions, SerdeErrorOptions } from './types';
export * from './generated/proto/transaction';
