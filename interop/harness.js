/**
 * Cross-language interop harness — Confluent SR wire format
 *
 * Tests the Confluent envelope framing without requiring a live Kafka cluster
 * or Schema Registry. Uses mock schema_id = 1 and the generated Node.js
 * CommonJS proto bindings.
 *
 * Run: node interop/harness.js
 */

'use strict';

const path = require('path');
const { Transaction, PredictiveAnalyzer } = require(path.join(__dirname, '../gen/node/proto/transaction_pb.js'));

// ---------------------------------------------------------------------------
// Confluent SR wire framing — §2 of docs/confluent-sr-serde-spec.md
// ---------------------------------------------------------------------------

const MAGIC_BYTE = 0x00;

/**
 * Frame a serialized proto message with the Confluent SR envelope.
 *
 * @param {number} schemaId  - 32-bit schema ID from Schema Registry
 * @param {Uint8Array} msgBytes - proto3 binary serialized message
 * @returns {Buffer} framed bytes ready for Kafka
 */
function frameMessage(schemaId, msgBytes) {
  const out = Buffer.allocUnsafe(6 + msgBytes.length);
  out[0] = MAGIC_BYTE;                          // magic byte
  out.writeUInt32BE(schemaId, 1);               // 4-byte schema ID, big-endian
  out[5] = 0x00;                                // message index: 0 = first message (Truther standard)
  Buffer.from(msgBytes).copy(out, 6);
  return out;
}

/**
 * Parse the Confluent SR envelope and return { schemaId, msgBytes }.
 * Throws on invalid magic byte or undersized frame.
 *
 * @param {Buffer} data - raw Kafka message value
 * @returns {{ schemaId: number, msgBytes: Buffer }}
 */
function parseFrame(data) {
  if (!Buffer.isBuffer(data)) data = Buffer.from(data);
  if (data.length < 6) {
    throw new Error(`Frame too short: ${data.length} bytes (minimum 6)`);
  }
  if (data[0] !== MAGIC_BYTE) {
    throw new Error(`Invalid magic byte: 0x${data[0].toString(16).padStart(2, '0')} (expected 0x00)`);
  }
  const schemaId = data.readUInt32BE(1);
  // data[5] is the message index — we ignore the value but its presence is mandatory
  const msgBytes = data.slice(6);
  return { schemaId, msgBytes };
}

// ---------------------------------------------------------------------------
// Mock Schema Registry — in-memory store for testing
// ---------------------------------------------------------------------------

const mockRegistry = new Map();
let nextId = 1;

function registerSchema(subject, schemaDescriptor) {
  const id = nextId++;
  mockRegistry.set(id, { subject, schemaDescriptor });
  return id;
}

function lookupSchema(schemaId) {
  return mockRegistry.get(schemaId) || null;
}

// ---------------------------------------------------------------------------
// Test assertions
// ---------------------------------------------------------------------------

let passed = 0;
let failed = 0;

function assert(condition, name) {
  if (condition) {
    console.log(`  ✓ ${name}`);
    passed++;
  } else {
    console.error(`  ✗ ${name}`);
    failed++;
  }
}

function assertThrows(fn, name) {
  try {
    fn();
    console.error(`  ✗ ${name} — expected an error but none was thrown`);
    failed++;
  } catch (e) {
    console.log(`  ✓ ${name} (threw: ${e.message})`);
    passed++;
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

console.log('\n=== Confluent SR Serde Interop Harness ===\n');

// Register Transaction schema (mock)
const TRANSACTION_SUBJECT = 'transactions-value';
const txSchemaId = registerSchema(TRANSACTION_SUBJECT, 'transaction.proto');
assert(txSchemaId === 1, `Schema registered with id=${txSchemaId}`);

// ---------------------------------------------------------------------------
// Test 1: Node.js round-trip (producer → consumer in same language)
// ---------------------------------------------------------------------------
console.log('Test 1: Node.js round-trip serialize / frame / unframe / deserialize');
{
  // Note: gen/node/proto/transaction_pb.js — regenerated via buf generate (ROD-26).
  // All 8 fields are now present. Tests use fields 1-6 only for brevity.
  const pa = new PredictiveAnalyzer();
  pa.setIsallowed(true);
  pa.setReason('approved by risk engine');
  pa.setCardid('card-abc-123');
  pa.setUserid('user-xyz-456');
  pa.setWalletaddress('0xDEADBEEF');
  pa.setAllowance('1000.00');

  const tx = new Transaction();
  tx.setTransactionamount('499.99');
  tx.setPredictiveanalyzer(pa);
  tx.setFinalDecision('APPROVED');

  // Producer side: serialize + frame
  const msgBytes = tx.serializeBinary();
  const framed = frameMessage(txSchemaId, msgBytes);

  assert(framed[0] === 0x00, 'Magic byte is 0x00');
  assert(framed.readUInt32BE(1) === txSchemaId, `Schema ID is ${txSchemaId}`);
  assert(framed[5] === 0x00, 'Message index byte is 0x00');
  assert(framed.length === 6 + msgBytes.length, 'Frame length = 6 header + payload');

  // Consumer side: unframe + validate schema_id + deserialize
  const { schemaId, msgBytes: decoded } = parseFrame(framed);
  assert(schemaId === txSchemaId, 'Consumer reads correct schema ID');

  const schema = lookupSchema(schemaId);
  assert(schema !== null, 'Schema ID is known to registry');
  assert(schema.subject === TRANSACTION_SUBJECT, 'Schema subject matches');

  const tx2 = Transaction.deserializeBinary(decoded);
  assert(tx2.getTransactionamount() === '499.99', 'Amount round-trips correctly');
  assert(tx2.getFinalDecision() === 'APPROVED', 'final_decision round-trips correctly');
  assert(tx2.getPredictiveanalyzer().getIsallowed() === true, 'isAllowed round-trips correctly');
  assert(tx2.getPredictiveanalyzer().getCardid() === 'card-abc-123', 'cardId round-trips correctly');
  assert(tx2.getPredictiveanalyzer().getUserid() === 'user-xyz-456', 'userId round-trips correctly');
}

// ---------------------------------------------------------------------------
// Test 2: Rejection of invalid magic byte
// ---------------------------------------------------------------------------
console.log('\nTest 2: Invalid magic byte is rejected');
{
  const bad = Buffer.from([0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a, 0x05]);
  assertThrows(() => parseFrame(bad), 'Magic byte 0x01 throws');

  const bad2 = Buffer.from([0xFF, 0x00, 0x00, 0x00, 0x01, 0x00]);
  assertThrows(() => parseFrame(bad2), 'Magic byte 0xFF throws');
}

// ---------------------------------------------------------------------------
// Test 3: Rejection of undersized frame
// ---------------------------------------------------------------------------
console.log('\nTest 3: Undersized frame is rejected');
{
  assertThrows(() => parseFrame(Buffer.from([0x00, 0x00, 0x00])), 'Frame with 3 bytes throws');
  assertThrows(() => parseFrame(Buffer.from([])), 'Empty frame throws');
}

// ---------------------------------------------------------------------------
// Test 4: Rejection of unknown schema ID
// ---------------------------------------------------------------------------
console.log('\nTest 4: Unknown schema ID is detected (consumer responsibility)');
{
  const unknownId = 9999;
  const pa2 = new PredictiveAnalyzer();
  pa2.setIsallowed(false);
  pa2.setReason('test');

  const tx3 = new Transaction();
  tx3.setTransactionamount('0.01');
  tx3.setPredictiveanalyzer(pa2);
  tx3.setFinalDecision('DENIED');

  const framedUnknown = frameMessage(unknownId, tx3.serializeBinary());
  const { schemaId } = parseFrame(framedUnknown);
  const schema = lookupSchema(schemaId);
  assert(schema === null, `schema_id=${unknownId} correctly not found in registry`);
}

// ---------------------------------------------------------------------------
// Test 5: Deterministic encoding — same message, same bytes (proto3 guarantee)
// ---------------------------------------------------------------------------
console.log('\nTest 5: Deterministic proto3 serialization');
{
  function makeTx(amount) {
    const pa = new PredictiveAnalyzer();
    pa.setIsallowed(true);
    pa.setReason('stable');
    pa.setCardid('card-1');
    pa.setUserid('user-1');
    const tx = new Transaction();
    tx.setTransactionamount(amount);
    tx.setPredictiveanalyzer(pa);
    tx.setFinalDecision('APPROVED');
    return tx;
  }

  const bytes1 = Buffer.from(makeTx('50.00').serializeBinary()).toString('hex');
  const bytes2 = Buffer.from(makeTx('50.00').serializeBinary()).toString('hex');
  assert(bytes1 === bytes2, 'Same message always serializes to same bytes');

  const bytes3 = Buffer.from(makeTx('99.99').serializeBinary()).toString('hex');
  assert(bytes1 !== bytes3, 'Different amount produces different bytes');
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
console.log(`\n=== Results: ${passed} passed, ${failed} failed ===\n`);

if (failed > 0) {
  console.error('FAIL — some interop tests failed.\n');
  process.exit(1);
} else {
  console.log('PASS — Node.js round-trip and framing validation complete.\n');
  console.log('Remaining cross-language tests (Go → Node, Node → Python, etc.) require');
  console.log('per-language Kafka libs. See docs/confluent-sr-serde-spec.md for the contract.');
}
