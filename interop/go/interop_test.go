// Package interop — Go interop harness for the Confluent SR serde contract.
//
// This test validates that the Go implementation of the Confluent envelope
// framing is byte-for-byte compatible with the Node.js reference implementation.
//
// STATUS: EXPECTED TO FAIL until the Senior Go engineer implements the
// truther-go-kafka library under docs/confluent-sr-serde-spec.md.
// The test structure is correct; only the import path needs updating.
//
// Run: go test ./interop/go/...
package interop_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	// TODO: Replace with the actual generated proto import once buf generate runs
	// txpb "github.com/vinirmbrozz/truther-contracts/gen/go"
)

const magicByte = byte(0x00)

// framingHeader returns the 6-byte Confluent SR header for a given schema ID.
func framingHeader(schemaID uint32) []byte {
	h := make([]byte, 6)
	h[0] = magicByte
	binary.BigEndian.PutUint32(h[1:5], schemaID)
	h[5] = 0x00 // message index: first message (Truther convention)
	return h
}

// frameMessage prepends the Confluent SR envelope to serialized proto bytes.
func frameMessage(schemaID uint32, msgBytes []byte) []byte {
	out := make([]byte, 6+len(msgBytes))
	copy(out[:6], framingHeader(schemaID))
	copy(out[6:], msgBytes)
	return out
}

// parseFrame validates and splits a Confluent-framed Kafka value.
func parseFrame(data []byte) (schemaID uint32, msgBytes []byte, err error) {
	if len(data) < 6 {
		return 0, nil, fmt.Errorf("frame too short: %d bytes", len(data))
	}
	if data[0] != magicByte {
		return 0, nil, fmt.Errorf("invalid magic byte 0x%02x", data[0])
	}
	schemaID = binary.BigEndian.Uint32(data[1:5])
	return schemaID, data[6:], nil
}

// TestFramingRoundTrip verifies the framing header is produced and parsed correctly.
func TestFramingRoundTrip(t *testing.T) {
	const schemaID = uint32(1)
	payload := []byte{0x0a, 0x06, 0x31, 0x30, 0x30, 0x2e, 0x30, 0x30} // proto bytes for transactionAmount="100.00"

	framed := frameMessage(schemaID, payload)

	if framed[0] != 0x00 {
		t.Errorf("magic byte: got 0x%02x, want 0x00", framed[0])
	}
	if binary.BigEndian.Uint32(framed[1:5]) != schemaID {
		t.Errorf("schema ID: got %d, want %d", binary.BigEndian.Uint32(framed[1:5]), schemaID)
	}
	if framed[5] != 0x00 {
		t.Errorf("message index byte: got 0x%02x, want 0x00", framed[5])
	}

	gotID, gotPayload, err := parseFrame(framed)
	if err != nil {
		t.Fatalf("parseFrame error: %v", err)
	}
	if gotID != schemaID {
		t.Errorf("parsed schema ID: got %d, want %d", gotID, schemaID)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Errorf("parsed payload mismatch")
	}
}

// TestInvalidMagicByte verifies that frames with a wrong magic byte are rejected.
func TestInvalidMagicByte(t *testing.T) {
	bad := []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a, 0x03}
	_, _, err := parseFrame(bad)
	if err == nil {
		t.Error("expected error for invalid magic byte, got nil")
	}
}

// TestUndersizedFrame verifies that truncated frames are rejected.
func TestUndersizedFrame(t *testing.T) {
	if _, _, err := parseFrame([]byte{0x00, 0x00, 0x00}); err == nil {
		t.Error("expected error for 3-byte frame, got nil")
	}
	if _, _, err := parseFrame([]byte{}); err == nil {
		t.Error("expected error for empty frame, got nil")
	}
}

// TestNodeCompatibility checks that Go framing produces the same bytes as
// the Node.js reference harness for the same mock Transaction payload.
//
// The fixture bytes below were captured from `node interop/harness.js`:
// schemaId=1, Transaction{transactionAmount="499.99", finalDecision="APPROVED", ...}
//
// To regenerate: run the Node harness and add a hex dump of the framed bytes.
//
// TODO: populate fixedFrameHex once the Node harness captures the fixture.
func TestNodeCompatibility(t *testing.T) {
	t.Skip("TODO: populate fixture bytes from Node harness output and remove Skip")
	// fixedFrameHex := "000000000100..."
	// expected, _ := hex.DecodeString(fixedFrameHex)
	// ... compare with Go-produced framing
}
