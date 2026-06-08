"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TrutherSerde = exports.SerdeError = void 0;
/**
 * TrutherSerde — thin Confluent SR serde for Kafka (Decision A).
 *
 * The SDK never reads a .proto and never registers schemas. bind() resolves the
 * topic's schema_id from the Schema Registry (read-only); produce() stamps the
 * Confluent envelope; consume() validates it (strictly) and deserializes into
 * the bound type.
 *
 * Usage:
 *   import { TrutherSerde } from '@truther/contracts';
 *   import { Transaction } from '@truther/contracts';
 *
 *   const serde = new TrutherSerde();             // reads SCHEMA_REGISTRY_URL
 *   await serde.bind('transactions', Transaction); // resolves schema_id at startup
 *   const framed = serde.produce('transactions', Transaction.create({ ... }));
 *   const tx = await serde.consume('transactions', rawValue); // -> Transaction
 */
const framing_1 = require("./framing");
const message_index_1 = require("./message-index");
const schema_registry_client_1 = require("./schema-registry-client");
/**
 * Thrown by consume() on any payload rejection. The adapter routes the raw bytes
 * to the DLQ using `code` — it must never crash on a bad message.
 */
class SerdeError extends Error {
    constructor(message, code, rawPayload) {
        super(message);
        this.name = 'SerdeError';
        this.code = code;
        this.rawPayload = rawPayload;
    }
}
exports.SerdeError = SerdeError;
class TrutherSerde {
    constructor(options = {}) {
        this.bindings = new Map();
        const srUrl = options.srUrl ?? process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';
        this.sr = new schema_registry_client_1.SchemaRegistryClient(srUrl, options.srApiKey ?? process.env['SCHEMA_REGISTRY_API_KEY'], options.srApiSecret ?? process.env['SCHEMA_REGISTRY_API_SECRET']);
    }
    /**
     * Map a topic to its message type and resolve the topic's schema_id from SR
     * (subject "<topic>-value", latest version). Read-only; fails fast if the
     * subject is not registered. Call once per topic at startup.
     */
    async bind(topic, codec) {
        const subject = `${topic}-value`;
        const schemaId = await this.sr.latestId(subject);
        const indexes = (0, message_index_1.messageIndexFor)(codec.$type);
        this.bindings.set(topic, {
            codec: codec,
            subject,
            schemaId,
            msgIndexBytes: (0, framing_1.encodeMessageIndexes)(indexes),
            indexes,
        });
    }
    /** Bind every topic→codec pair (calls bind for each). */
    async startup(bindings) {
        for (const [topic, codec] of Object.entries(bindings)) {
            await this.bind(topic, codec);
        }
    }
    /** Serialize msg and wrap it in the Confluent envelope (cached schema_id + correct msg-index). */
    produce(topic, msg) {
        const b = this.bindings.get(topic);
        if (!b) {
            throw new SerdeError(`No binding for topic '${topic}'. Call bind() at startup.`, 'TOPIC_NOT_BOUND');
        }
        const payload = b.codec.encode(msg).finish();
        return (0, framing_1.frameMessage)(b.schemaId, b.msgIndexBytes, payload);
    }
    /**
     * Validate the envelope and deserialize into the bound type. Rejects (SerdeError)
     * on: wrong magic byte, short frame, schema_id not a registered version of this
     * topic's subject, message-index ≠ bound type, or decode failure. Adapter → DLQ.
     */
    async consume(topic, data) {
        const b = this.bindings.get(topic);
        if (!b) {
            throw new SerdeError(`No binding for topic '${topic}'. Call bind() at startup.`, 'TOPIC_NOT_BOUND', data);
        }
        let schemaId;
        let indexes;
        let payload;
        try {
            ({ schemaId, indexes, payload } = (0, framing_1.parseFrame)(data));
        }
        catch (err) {
            const code = err instanceof framing_1.FrameError ? err.code : 'INVALID_MAGIC_BYTE';
            throw new SerdeError(`[${topic}] ${err.message}`, code, data);
        }
        // Security: the id must be a registered version of THIS topic's subject.
        if (!(await this.sr.idBelongsToSubject(schemaId, b.subject))) {
            throw new SerdeError(`[${topic}] schema_id=${schemaId} is not a registered version of '${b.subject}'`, 'SCHEMA_FOREIGN', data);
        }
        if (!indexesEqual(indexes, b.indexes)) {
            throw new SerdeError(`[${topic}] message-index ${JSON.stringify(indexes)} != bound ${JSON.stringify(b.indexes)}`, 'MESSAGE_INDEX_MISMATCH', data);
        }
        try {
            return b.codec.decode(payload);
        }
        catch (err) {
            throw new SerdeError(`[${topic}] deserialization failed: ${err.message}`, 'DESERIALIZATION_ERROR', data);
        }
    }
}
exports.TrutherSerde = TrutherSerde;
function indexesEqual(a, b) {
    return a.length === b.length && a.every((v, i) => v === b[i]);
}
//# sourceMappingURL=serde.js.map