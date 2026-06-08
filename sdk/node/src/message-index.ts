/**
 * Resolves the Confluent message-index of any message from the embedded
 * FileDescriptorSet — generically, by the message's full name ($type).
 *
 * This is what brings Node to parity with Go/Python (whose generated code embeds
 * the descriptor natively): adding a new .proto regenerates the descriptor set,
 * and this resolver handles the new message with ZERO per-contract code.
 */
import { fromBinary } from '@bufbuild/protobuf';
import { FileDescriptorSetSchema } from '@bufbuild/protobuf/wkt';
import { DESCRIPTOR_SET_B64 } from './generated/descriptor-set';

let cache: Map<string, number[]> | null = null;

function buildIndex(): Map<string, number[]> {
  const bytes = Buffer.from(DESCRIPTOR_SET_B64, 'base64');
  const fds = fromBinary(FileDescriptorSetSchema, bytes);
  const map = new Map<string, number[]>();
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
export function messageIndexFor(fullName: string): number[] {
  if (!cache) cache = buildIndex();
  const idx = cache.get(fullName);
  if (!idx) {
    throw new Error(
      `message-index: '${fullName}' not found in embedded descriptor set (regenerate the SDK?)`,
    );
  }
  return idx;
}
