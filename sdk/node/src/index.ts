export { ProtobufSerde, SerdeError } from './serde';
export type { SerdeErrorCode } from './serde';
export { frameMessage, parseFrame, encodeMessageIndexes, MAGIC_BYTE, FrameError } from './framing';
export type { ParsedFrame, FrameErrorCode } from './framing';
export { SchemaRegistryClient } from './schema-registry-client';
export type { MessageFns, SerdeOptions } from './types';

// Generated contracts (versioned packages). Explicit re-exports — a blanket
// `export *` would collide on each file's `protobufPackage` const.
export {
  Transaction,
  TransactionData,
  Device,
  Party,
  CreditCard,
  Boleto,
  Order,
  OrderItem,
  Item,
  Delivery,
  Pos,
  Customer,
} from './generated/protobuf/transaction/v1/transaction';
export { Onboarding, OnboardingCustomer } from './generated/protobuf/onboarding/v1/onboarding';
export { Address } from './generated/protobuf/type/v1/address';
export { RegistrationData } from './generated/protobuf/type/v1/registration';
export { BankingData } from './generated/protobuf/type/v1/banking';
export { PixKeyType } from './generated/protobuf/type/v1/pix';
