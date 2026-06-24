/**
 * Node interop CLI — uses the @protobuf/contracts SDK (ESM, no local framing).
 *
 *   node interop/cli.js produce <topic> <file>   # SDK bind + produce → file
 *   node interop/cli.js consume <topic> <file>    # SDK bind + consume + verify
 *
 * The canonical SAMPLE is identical across the Go/Node/Python CLIs, so a frame
 * produced by any language must consume + verify in the others (cross-language).
 * Reads SCHEMA_REGISTRY_URL from the environment.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { ProtobufSerde, Transaction } from '@protobuf/contracts';

const SAMPLE = {
  transactionAmount: '499.99',
  finalDecision: 'APPROVED',
  predictiveAnalyzer: {
    isAllowed: true,
    reason: 'approved',
    cardId: 'card-1',
    userId: 'user-1',
    walletAddress: '0xABC',
    allowance: '1000.00',
    transactionId: 'tx-1',
    name: 'n',
  },
};

function verify(tx) {
  const pa = tx.predictiveAnalyzer ?? {};
  const ok =
    tx.transactionAmount === SAMPLE.transactionAmount &&
    tx.finalDecision === SAMPLE.finalDecision &&
    pa.isAllowed === true &&
    pa.cardId === 'card-1' &&
    pa.walletAddress === '0xABC';
  if (!ok) {
    console.error('node: MISMATCH', JSON.stringify(tx));
    process.exit(1);
  }
}

const [cmd, topic, file] = process.argv.slice(2);
if (!cmd || !topic || !file) {
  console.error('usage: cli.js <produce|consume> <topic> <file>');
  process.exit(2);
}

const serde = new ProtobufSerde();
await serde.bind(topic, Transaction);

if (cmd === 'produce') {
  const framed = serde.produce(topic, Transaction.fromPartial(SAMPLE));
  writeFileSync(file, framed);
  console.log(`node: produced ${framed.length} bytes → ${file}`);
} else if (cmd === 'consume') {
  verify(await serde.consume(topic, readFileSync(file)));
  console.log('node: consume OK');
} else {
  console.error(`unknown command: ${cmd}`);
  process.exit(2);
}
