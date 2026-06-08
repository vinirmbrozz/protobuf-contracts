"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.messageIndexFor = messageIndexFor;
/**
 * Resolves the Confluent message-index of any message from the embedded
 * FileDescriptorSet — generically, by the message's full name ($type).
 *
 * This is what brings Node to parity with Go/Python (whose generated code embeds
 * the descriptor natively): adding a new .proto regenerates the descriptor set,
 * and this resolver handles the new message with ZERO per-contract code.
 */
const protobuf_1 = require("@bufbuild/protobuf");
const wkt_1 = require("@bufbuild/protobuf/wkt");
const descriptor_set_1 = require("./generated/descriptor-set");
let cache = null;
function buildIndex() {
    const bytes = Buffer.from(descriptor_set_1.DESCRIPTOR_SET_B64, 'base64');
    const fds = (0, protobuf_1.fromBinary)(wkt_1.FileDescriptorSetSchema, bytes);
    const map = new Map();
    for (const file of fds.file) {
        const pkg = file.package ?? '';
        // Top-level messages only (Truther messages are top-level). The index path
        // is the declaration order within the file's message_type list.
        file.messageType.forEach((msg, i) => {
            const fullName = pkg ? `${pkg}.${msg.name}` : (msg.name ?? '');
            map.set(fullName, [i]);
        });
    }
    return map;
}
/** Returns the message-index path for a message's full name (e.g. "truther.transaction.Transaction"). */
function messageIndexFor(fullName) {
    if (!cache)
        cache = buildIndex();
    const idx = cache.get(fullName);
    if (!idx) {
        throw new Error(`message-index: '${fullName}' not found in embedded descriptor set (regenerate the SDK?)`);
    }
    return idx;
}
//# sourceMappingURL=message-index.js.map