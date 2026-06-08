import type { BinaryReader, BinaryWriter } from "@bufbuild/protobuf/wire";
export interface MessageType<Message extends UnknownMessage = UnknownMessage> {
    $type: Message["$type"];
    encode(message: Message, writer?: BinaryWriter): BinaryWriter;
    decode(input: BinaryReader | Uint8Array, length?: number): Message;
    fromJSON(object: any): Message;
    toJSON(message: Message): unknown;
    fromPartial(object: DeepPartial<Message>): Message;
}
export type UnknownMessage = {
    $type: string;
};
export declare const messageTypeRegistry: Map<string, MessageType<UnknownMessage>>;
type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;
export type DeepPartial<T> = T extends Builtin ? T : T extends globalThis.Array<infer U> ? globalThis.Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>> : T extends {
    $case: string;
} ? {
    [K in keyof Omit<T, "$case">]?: DeepPartial<T[K]>;
} & {
    $case: T["$case"];
} : T extends {} ? {
    [K in Exclude<keyof T, "$type">]?: DeepPartial<T[K]>;
} : Partial<T>;
export {};
//# sourceMappingURL=typeRegistry.d.ts.map