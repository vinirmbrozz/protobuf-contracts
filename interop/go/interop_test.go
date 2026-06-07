// Package interop — Go interop harness for the Confluent SR serde contract.
//
// Validates that the Go SDK (sdk/go) framing and deserialization are
// byte-for-byte compatible with the Node.js reference harness.
//
// Uses net/http/httptest to simulate the Confluent Schema Registry REST API —
// no real Kafka or Schema Registry instance is required.
//
// Run: go test ./interop/go/...
package interop_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	serde "github.com/vinirmbrozz/truther-contracts/sdk/go"
	txpb "github.com/vinirmbrozz/truther-contracts/sdk/go/proto"
	"google.golang.org/protobuf/proto"
)

// ---------------------------------------------------------------------------
// Mock Schema Registry — mirrors the one in sdk/go/serde_test.go
// ---------------------------------------------------------------------------

func mockSchemaRegistry(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	registered := make(map[string]int)
	nextID := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/versions"):
			parts := strings.SplitN(r.URL.Path, "/", 4)
			if len(parts) < 4 {
				http.Error(w, "bad path", http.StatusBadRequest)
				return
			}
			subject := parts[2]
			mu.Lock()
			id, exists := registered[subject]
			if !exists {
				nextID++
				id = nextID
				registered[subject] = id
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]int{"id": id})

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/schemas/ids/"):
			var id int
			if _, err := fmt.Sscanf(r.URL.Path, "/schemas/ids/%d", &id); err != nil {
				http.Error(w, "bad id", http.StatusBadRequest)
				return
			}
			mu.Lock()
			found := false
			for _, regID := range registered {
				if regID == id {
					found = true
					break
				}
			}
			mu.Unlock()
			if found {
				_ = json.NewEncoder(w).Encode(map[string]string{"schema": "stub", "schemaType": "PROTOBUF"})
			} else {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"error_code": 40403, "message": "Schema not found"})
			}

		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestSerde(t *testing.T, srv *httptest.Server) *serde.Serde {
	t.Helper()
	s := serde.NewWithConfig(serde.Config{SRURL: srv.URL})
	s.RegisterType("transactions", &txpb.Transaction{}, "")
	return s
}

// ---------------------------------------------------------------------------
// TestFramingRoundTrip — produce / consume a Transaction via the SDK serde.
// ---------------------------------------------------------------------------

func TestFramingRoundTrip(t *testing.T) {
	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)

	tx := &txpb.Transaction{
		TransactionAmount: "100.00",
		PredictiveAnalyzer: &txpb.PredictiveAnalyzer{
			IsAllowed: true,
			Reason:    "test",
			CardId:    "card-1",
			UserId:    "user-1",
		},
		FinalDecision: "APPROVED",
	}

	framed, err := s.Produce("transactions", tx)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Validate Confluent header structure
	if len(framed) < 6 {
		t.Fatalf("framed len %d < 6", len(framed))
	}
	if framed[0] != 0x00 {
		t.Errorf("magic byte: got 0x%02x, want 0x00", framed[0])
	}
	schemaID := binary.BigEndian.Uint32(framed[1:5])
	if schemaID == 0 {
		t.Error("schema ID is 0; expected non-zero from SR")
	}
	if framed[5] != 0x00 {
		t.Errorf("message index byte: got 0x%02x, want 0x00", framed[5])
	}

	// Round-trip
	msg, err := s.Consume("transactions", framed)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	got, ok := msg.(*txpb.Transaction)
	if !ok {
		t.Fatalf("Consume returned %T, want *txpb.Transaction", msg)
	}
	if !proto.Equal(tx, got) {
		t.Errorf("roundtrip mismatch: got %v, want %v", got, tx)
	}
}

// ---------------------------------------------------------------------------
// TestInvalidMagicByte — consumer must reject frames with wrong magic byte.
// ---------------------------------------------------------------------------

func TestInvalidMagicByte(t *testing.T) {
	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)

	bad := []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a, 0x03}
	_, err := s.Consume("transactions", bad)
	if err == nil {
		t.Fatal("expected error for invalid magic byte, got nil")
	}
	if !errors.Is(err, serde.ErrInvalidMagicByte) {
		t.Errorf("want ErrInvalidMagicByte, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestUndersizedFrame — consumer must reject frames shorter than 6 bytes.
// ---------------------------------------------------------------------------

func TestUndersizedFrame(t *testing.T) {
	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)

	cases := [][]byte{
		{0x00, 0x00, 0x00},
		{},
	}
	for _, tc := range cases {
		_, err := s.Consume("transactions", tc)
		if err == nil {
			t.Errorf("expected error for %d-byte frame, got nil", len(tc))
		}
		if !errors.Is(err, serde.ErrFrameTooShort) {
			t.Errorf("want ErrFrameTooShort for %d-byte frame, got: %v", len(tc), err)
		}
	}
}

// ---------------------------------------------------------------------------
// TestNodeCompatibility — Go SDK must decode bytes produced by sdk/node.
//
// Fixture bytes were captured from interop/harness.js (Test 1 output):
//
//   Transaction{
//     transactionAmount: "499.99",
//     predictiveAnalyzer: {
//       isAllowed: true, reason: "approved by risk engine",
//       cardId: "card-abc-123", userId: "user-xyz-456",
//       walletAddress: "0xDEADBEEF", allowance: "1000.00",
//     },
//     finalDecision: "APPROVED",
//   }  — schemaId = 1
// ---------------------------------------------------------------------------

func TestNodeCompatibility(t *testing.T) {
	const fixtureHex = "0000000001000a063439392e3939124c08011217617070726f766564206279207269736b" +
		"20656e67696e651a0c636172642d6162632d313233220c757365722d78797a2d3435362a" +
		"0a307844454144424545463207313030302e30301a08415050524f564544"

	fixtureBytes, err := hex.DecodeString(fixtureHex)
	if err != nil {
		t.Fatalf("decode fixture hex: %v", err)
	}

	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)

	// Produce one message so that schemaId=1 is registered and cached in the
	// serde's idKnown map — this lets Consume validate the fixture's schema ID.
	seed := &txpb.Transaction{TransactionAmount: "0.01", FinalDecision: "SEED"}
	if _, err := s.Produce("transactions", seed); err != nil {
		t.Fatalf("Produce seed: %v", err)
	}

	// Verify header bytes
	if fixtureBytes[0] != 0x00 {
		t.Errorf("fixture magic byte: 0x%02x, want 0x00", fixtureBytes[0])
	}
	schemaID := binary.BigEndian.Uint32(fixtureBytes[1:5])
	if schemaID != 1 {
		t.Errorf("fixture schema ID: %d, want 1", schemaID)
	}

	// Decode via SDK Consume
	msg, err := s.Consume("transactions", fixtureBytes)
	if err != nil {
		t.Fatalf("Consume Node fixture: %v", err)
	}
	tx, ok := msg.(*txpb.Transaction)
	if !ok {
		t.Fatalf("Consume returned %T, want *txpb.Transaction", msg)
	}

	if tx.TransactionAmount != "499.99" {
		t.Errorf("transactionAmount: got %q, want %q", tx.TransactionAmount, "499.99")
	}
	if tx.FinalDecision != "APPROVED" {
		t.Errorf("finalDecision: got %q, want %q", tx.FinalDecision, "APPROVED")
	}
	if !tx.PredictiveAnalyzer.IsAllowed {
		t.Error("isAllowed: got false, want true")
	}
	if tx.PredictiveAnalyzer.CardId != "card-abc-123" {
		t.Errorf("cardId: got %q, want %q", tx.PredictiveAnalyzer.CardId, "card-abc-123")
	}
	if tx.PredictiveAnalyzer.UserId != "user-xyz-456" {
		t.Errorf("userId: got %q, want %q", tx.PredictiveAnalyzer.UserId, "user-xyz-456")
	}
	if tx.PredictiveAnalyzer.WalletAddress != "0xDEADBEEF" {
		t.Errorf("walletAddress: got %q, want %q", tx.PredictiveAnalyzer.WalletAddress, "0xDEADBEEF")
	}
	if tx.PredictiveAnalyzer.Allowance != "1000.00" {
		t.Errorf("allowance: got %q, want %q", tx.PredictiveAnalyzer.Allowance, "1000.00")
	}
	if tx.PredictiveAnalyzer.Reason != "approved by risk engine" {
		t.Errorf("reason: got %q, want %q", tx.PredictiveAnalyzer.Reason, "approved by risk engine")
	}

	// Prove the payload bytes are identical (proto3 determinism)
	serialized, _ := proto.Marshal(tx)
	reimaged, err := hex.DecodeString(fixtureHex)
	if err != nil {
		t.Fatalf("re-decode fixture hex: %v", err)
	}
	if !bytes.Equal(serialized, reimaged[6:]) {
		t.Error("proto bytes differ from fixture — cross-language encoding diverged")
	}
}
