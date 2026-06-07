/**
 * Cross-language interop harness — Confluent SR wire format
 *
 * Tests the Confluent envelope framing using the @truther/contracts SDK
 * (sdk/node). No live Kafka cluster or Schema Registry required — a
 * lightweight in-memory mock is used for schema ID resolution.
 *
 * Run: node interop/harness.js
 */

'use strict';

// ---------------------------------------------------------------------------
// SDK imports — @truther/contracts package (sdk/node); zero local framing
// ---------------------------------------------------------------------------

const {
  Transaction,
  PredictiveAnalyzer,
  frameMessage,
  parseFrame,
  MAGIC_BYTE,
} = require('@truther/contracts');

// ---------------------------------------------------------------------------
// Mock Schema Registry — in-memory store for testing
// ---------------------------------------------------------------------------

const mockRegistry = new Map();
let nextId = 1;

function registerSchema(subject) {
  const id = nextId++;
  mockRegistry.set(id, { subject });
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
const txSchemaId = registerSchema(TRANSACTION_SUBJECT);
assert(txSchemaId === 1, `Schema registered with id=${txSchemaId}`);

// ---------------------------------------------------------------------------
// Test 1: Node.js round-trip (producer → consumer in same language)
// ---------------------------------------------------------------------------
console.log('Test 1: Node.js round-trip serialize / frame / unframe / deserialize');
{
  // ts-proto API: plain objects + Transaction.encode/decode
  const pa = PredictiveAnalyzer.fromPartial({
    isAllowed: true,
    reason: 'approved by risk engine',
    cardId: 'card-abc-123',
    userId: 'user-xyz-456',
    walletAddress: '0xDEADBEEF',
    allowance: '1000.00',
  });

  const tx = Transaction.fromPartial({
    transactionAmount: '499.99',
    predictiveAnalyzer: pa,
    finalDecision: 'APPROVED',
  });

  // Producer side: serialize + frame
  const msgBytes = Transaction.encode(tx).finish();
  const framed = frameMessage(txSchemaId, msgBytes);

  assert(framed[0] === MAGIC_BYTE, 'Magic byte is 0x00');
  assert(framed.readUInt32BE(1) === txSchemaId, `Schema ID is ${txSchemaId}`);
  assert(framed[5] === 0x00, 'Message index byte is 0x00');
  assert(framed.length === 6 + msgBytes.length, 'Frame length = 6 header + payload');

  // Consumer side: unframe + validate schema_id + deserialize
  const { schemaId, msgBytes: decoded } = parseFrame(framed);
  assert(schemaId === txSchemaId, 'Consumer reads correct schema ID');

  const schema = lookupSchema(schemaId);
  assert(schema !== null, 'Schema ID is known to registry');
  assert(schema.subject === TRANSACTION_SUBJECT, 'Schema subject matches');

  const tx2 = Transaction.decode(decoded);
  assert(tx2.transactionAmount === '499.99', 'Amount round-trips correctly');
  assert(tx2.finalDecision === 'APPROVED', 'final_decision round-trips correctly');
  assert(tx2.predictiveAnalyzer.isAllowed === true, 'isAllowed round-trips correctly');
  assert(tx2.predictiveAnalyzer.cardId === 'card-abc-123', 'cardId round-trips correctly');
  assert(tx2.predictiveAnalyzer.userId === 'user-xyz-456', 'userId round-trips correctly');

  // Emit fixture bytes for cross-language harnesses (Go / Python)
  console.log(`\n  [fixture] framed hex: ${framed.toString('hex')}`);
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
  const tx3 = Transaction.fromPartial({
    transactionAmount: '0.01',
    predictiveAnalyzer: PredictiveAnalyzer.fromPartial({ isAllowed: false, reason: 'test' }),
    finalDecision: 'DENIED',
  });

  const framedUnknown = frameMessage(unknownId, Transaction.encode(tx3).finish());
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
    return Transaction.fromPartial({
      transactionAmount: amount,
      predictiveAnalyzer: PredictiveAnalyzer.fromPartial({
        isAllowed: true,
        reason: 'stable',
        cardId: 'card-1',
        userId: 'user-1',
      }),
      finalDecision: 'APPROVED',
    });
  }

  const bytes1 = Buffer.from(Transaction.encode(makeTx('50.00')).finish()).toString('hex');
  const bytes2 = Buffer.from(Transaction.encode(makeTx('50.00')).finish()).toString('hex');
  assert(bytes1 === bytes2, 'Same message always serializes to same bytes');

  const bytes3 = Buffer.from(Transaction.encode(makeTx('99.99')).finish()).toString('hex');
  assert(bytes1 !== bytes3, 'Different amount produces different bytes');
}

// ---------------------------------------------------------------------------
// Test 6: Cross-language fixture — bytes that Go/Python must also decode
// ---------------------------------------------------------------------------
console.log('\nTest 6: Cross-language fixture round-trip');
{
  // These bytes were captured from this harness and must be identical across
  // all three SDK implementations (Node → Go, Node → Python).
  //
  // Transaction{ transactionAmount:"499.99",
  //              predictiveAnalyzer:{ isAllowed:true, reason:"approved by risk engine",
  //                                  cardId:"card-abc-123", userId:"user-xyz-456",
  //                                  walletAddress:"0xDEADBEEF", allowance:"1000.00" },
  //              finalDecision:"APPROVED" }
  // schemaId = 1
  // 204 hex chars = 102 bytes (6-byte SR header + 96-byte proto payload)
  const FIXTURE_HEX =
    '0000000001000a063439392e3939124c08011217617070726f766564206279207269736b20' +
    '656e67696e651a0c636172642d6162632d313233220c757365722d78797a2d3435362a0a30' +
    '7844454144424545463207313030302e30301a08415050524f564544';

  const fixtureBytes = Buffer.from(FIXTURE_HEX, 'hex');
  const { schemaId, msgBytes } = parseFrame(fixtureBytes);
  assert(schemaId === 1, `Fixture schema ID is 1 (got ${schemaId})`);

  const tx = Transaction.decode(msgBytes);
  assert(tx.transactionAmount === '499.99', 'Fixture: transactionAmount correct');
  assert(tx.finalDecision === 'APPROVED', 'Fixture: finalDecision correct');
  assert(tx.predictiveAnalyzer.isAllowed === true, 'Fixture: isAllowed correct');
  assert(tx.predictiveAnalyzer.cardId === 'card-abc-123', 'Fixture: cardId correct');
  assert(tx.predictiveAnalyzer.walletAddress === '0xDEADBEEF', 'Fixture: walletAddress correct');
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
console.log(`\n=== Results: ${passed} passed, ${failed} failed ===\n`);

if (failed > 0) {
  console.error('FAIL — some interop tests failed.\n');
  process.exit(1);
} else {
  console.log('PASS — Node.js round-trip, framing validation, and cross-language fixture complete.\n');
}
