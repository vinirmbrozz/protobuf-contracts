export { ProtobufSerde, SerdeError } from './serde';
export type { SerdeErrorCode } from './serde';
export { frameMessage, parseFrame, encodeMessageIndexes, MAGIC_BYTE, FrameError } from './framing';
export type { ParsedFrame, FrameErrorCode } from './framing';
export { SchemaRegistryClient } from './schema-registry-client';
export type { MessageFns, SerdeOptions } from './types';
export * from './generated/proto/transaction';
