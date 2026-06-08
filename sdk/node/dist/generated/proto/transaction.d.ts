import { BinaryReader, BinaryWriter } from "@bufbuild/protobuf/wire";
export declare const protobufPackage = "truther.transaction";
export interface PredictiveAnalyzer {
    $type: "truther.transaction.PredictiveAnalyzer";
    isAllowed: boolean;
    reason: string;
    cardId: string;
    userId: string;
    walletAddress: string;
    allowance: string;
    transactionId: string;
    name: string;
}
export interface Transaction {
    $type: "truther.transaction.Transaction";
    transactionAmount: string;
    predictiveAnalyzer: PredictiveAnalyzer | undefined;
    finalDecision: string;
}
export declare const PredictiveAnalyzer: MessageFns<PredictiveAnalyzer, "truther.transaction.PredictiveAnalyzer">;
export declare const Transaction: MessageFns<Transaction, "truther.transaction.Transaction">;
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
type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P : P & {
    [K in keyof P]: Exact<P[K], I[K]>;
} & {
    [K in Exclude<keyof I, KeysOfUnion<P> | "$type">]: never;
};
export interface MessageFns<T, V extends string> {
    readonly $type: V;
    encode(message: T, writer?: BinaryWriter): BinaryWriter;
    decode(input: BinaryReader | Uint8Array, length?: number): T;
    fromJSON(object: any): T;
    toJSON(message: T): unknown;
    create<I extends Exact<DeepPartial<T>, I>>(base?: I): T;
    fromPartial<I extends Exact<DeepPartial<T>, I>>(object: I): T;
}
export {};
//# sourceMappingURL=transaction.d.ts.map