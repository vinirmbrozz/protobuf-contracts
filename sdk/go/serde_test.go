// Package serde_test — unit tests for the thin Confluent SR serde.
//
// The SDK only READS the Schema Registry (resolve id at Bind, validate id at
// Consume). These tests simulate SR with net/http/httptest serving:
//   - GET /subjects/{subject}/versions/latest      → {"id": N}
//   - GET /schemas/ids/{id}/versions               → [{"subject","version"}]
// No real Kafka/SR required; the end-to-end test against a real SR lives apart.
package serde_test

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	serde "github.com/vinirmbrozz/truther-contracts/sdk/go"
	txpb "github.com/vinirmbrozz/truther-contracts/sdk/go/proto"
	"google.golang.org/protobuf/proto"
)

// mockSR serves a read-only Confluent SR seeded with subject→id. idVersions is
// the optional reverse view (id → subjects) used by the id-validation endpoint;
// when nil it is derived from subjectID. idVersionCalls counts validation hits.
type mockSR struct {
	*httptest.Server
	idVersionCalls atomic.Int32
}

func newMockSR(t *testing.T, subjectID map[string]int) *mockSR {
	t.Helper()
	m := &mockSR{}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		parts := strings.Split(r.URL.Path, "/") // leading "" element

		switch {
		// GET /subjects/{subject}/versions/latest
		case len(parts) == 5 && parts[1] == "subjects" && parts[3] == "versions" && parts[4] == "latest":
			subject := parts[2]
			if id, ok := subjectID[subject]; ok {
				_ = json.NewEncoder(w).Encode(map[string]int{"id": id})
				return
			}
			w.WriteHeader(http.StatusNotFound)

		// GET /schemas/ids/{id}/versions
		case len(parts) == 5 && parts[1] == "schemas" && parts[2] == "ids" && parts[4] == "versions":
			m.idVersionCalls.Add(1)
			id, _ := strconv.Atoi(parts[3])
			var out []map[string]any
			for subject, sid := range subjectID {
				if sid == id {
					out = append(out, map[string]any{"subject": subject, "version": 1})
				}
			}
			if len(out) == 0 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(out)

		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(m.Close)
	return m
}

func newSerde(t *testing.T, m *mockSR) *serde.Serde {
	t.Helper()
	return serde.NewWithConfig(serde.Config{SRURL: m.URL})
}

func sampleTx() *txpb.Transaction {
	return &txpb.Transaction{
		TransactionAmount:  "9.99",
		FinalDecision:      "APPROVED",
		PredictiveAnalyzer: &txpb.PredictiveAnalyzer{IsAllowed: true, Reason: "ok", CardId: "card-1"},
	}
}

// frame builds a Confluent envelope by hand for negative tests.
func frame(id uint32, msgIndex, payload []byte) []byte {
	f := []byte{0x00}
	f = binary.BigEndian.AppendUint32(f, id)
	f = append(f, msgIndex...)
	return append(f, payload...)
}

// ── Round-trip + envelope correctness ───────────────────────────────────────

// Transaction is the 2nd message in the file (index 1) → exercises the
// variable-length message-index path, NOT the 0x00 optimization.
func TestRoundTripTransaction(t *testing.T) {
	m := newMockSR(t, map[string]int{"transactions-value": 42})
	s := newSerde(t, m)
	if err := s.Bind("transactions", &txpb.Transaction{}); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	original := sampleTx()
	framed, err := s.Produce("transactions", original)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	if framed[0] != 0x00 {
		t.Errorf("magic: got 0x%02x want 0x00", framed[0])
	}
	if id := binary.BigEndian.Uint32(framed[1:5]); id != 42 {
		t.Errorf("schema_id: got %d want 42", id)
	}
	// index 1 → zig-zag varint(count=1)=0x02, varint(index=1)=0x02
	if got := framed[5:7]; got[0] != 0x02 || got[1] != 0x02 {
		t.Errorf("msg-index: got % x want 02 02", got)
	}

	got, err := s.Consume("transactions", framed)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if !proto.Equal(original, got) {
		t.Errorf("roundtrip mismatch: got %v", got)
	}
}

// PredictiveAnalyzer is the 1st message (index 0) → the single-byte 0x00 path.
func TestRoundTripIndexZero(t *testing.T) {
	m := newMockSR(t, map[string]int{"predictions-value": 7})
	s := newSerde(t, m)
	if err := s.Bind("predictions", &txpb.PredictiveAnalyzer{}); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	framed, err := s.Produce("predictions", &txpb.PredictiveAnalyzer{IsAllowed: true})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	// index 0 → single 0x00 byte, payload starts at offset 6
	if framed[5] != 0x00 {
		t.Errorf("msg-index byte: got 0x%02x want 0x00", framed[5])
	}
	if _, err := s.Consume("predictions", framed); err != nil {
		t.Fatalf("Consume: %v", err)
	}
}

// ── Bind / not-bound ────────────────────────────────────────────────────────

func TestBindFailsWhenSubjectMissing(t *testing.T) {
	m := newMockSR(t, map[string]int{}) // nothing registered
	s := newSerde(t, m)
	if err := s.Bind("transactions", &txpb.Transaction{}); err == nil {
		t.Fatal("expected Bind to fail when subject is not registered")
	}
}

func TestProduceConsumeUnbound(t *testing.T) {
	m := newMockSR(t, map[string]int{"transactions-value": 42})
	s := newSerde(t, m)
	if _, err := s.Produce("transactions", sampleTx()); !errors.Is(err, serde.ErrTopicNotBound) {
		t.Errorf("Produce unbound: want ErrTopicNotBound, got %v", err)
	}
	if _, err := s.Consume("transactions", []byte{0x00, 0, 0, 0, 42, 0x02, 0x02, 0x00}); !errors.Is(err, serde.ErrTopicNotBound) {
		t.Errorf("Consume unbound: want ErrTopicNotBound, got %v", err)
	}
}

// ── Security: consumer rejections ───────────────────────────────────────────

func TestConsumeInvalidMagicByte(t *testing.T) {
	m := newMockSR(t, map[string]int{"transactions-value": 42})
	s := newSerde(t, m)
	_ = s.Bind("transactions", &txpb.Transaction{})

	bad := []byte{0x01, 0x00, 0x00, 0x00, 0x2a, 0x02, 0x02, 0x0a}
	if _, err := s.Consume("transactions", bad); !errors.Is(err, serde.ErrInvalidMagicByte) {
		t.Errorf("want ErrInvalidMagicByte, got %v", err)
	}
}

func TestConsumeFrameTooShort(t *testing.T) {
	m := newMockSR(t, map[string]int{"transactions-value": 42})
	s := newSerde(t, m)
	_ = s.Bind("transactions", &txpb.Transaction{})

	for _, data := range [][]byte{{}, {0x00}, {0x00, 0, 0, 0}, {0x00, 0, 0, 0, 42}} {
		if _, err := s.Consume("transactions", data); !errors.Is(err, serde.ErrFrameTooShort) {
			t.Errorf("len=%d: want ErrFrameTooShort, got %v", len(data), err)
		}
	}
}

// schema_id that belongs to a DIFFERENT subject must be rejected (the core
// security tightening: not "exists somewhere" but "version of THIS subject").
func TestConsumeForeignSubjectID(t *testing.T) {
	m := newMockSR(t, map[string]int{
		"transactions-value": 42,
		"other-value":        99, // id 99 exists, but under another subject
	})
	s := newSerde(t, m)
	_ = s.Bind("transactions", &txpb.Transaction{})

	payload, _ := proto.Marshal(sampleTx())
	bad := frame(99, []byte{0x02, 0x02}, payload) // valid envelope, wrong subject's id
	if _, err := s.Consume("transactions", bad); !errors.Is(err, serde.ErrSchemaForeign) {
		t.Errorf("want ErrSchemaForeign, got %v", err)
	}
}

func TestConsumeUnknownID(t *testing.T) {
	m := newMockSR(t, map[string]int{"transactions-value": 42})
	s := newSerde(t, m)
	_ = s.Bind("transactions", &txpb.Transaction{})

	payload, _ := proto.Marshal(sampleTx())
	bad := frame(12345, []byte{0x02, 0x02}, payload) // id never registered (404)
	if _, err := s.Consume("transactions", bad); !errors.Is(err, serde.ErrSchemaForeign) {
		t.Errorf("want ErrSchemaForeign, got %v", err)
	}
}

func TestConsumeMessageIndexMismatch(t *testing.T) {
	m := newMockSR(t, map[string]int{"transactions-value": 42})
	s := newSerde(t, m)
	_ = s.Bind("transactions", &txpb.Transaction{}) // expects index 1

	payload, _ := proto.Marshal(sampleTx())
	bad := frame(42, []byte{0x00}, payload) // index 0, but bound type is index 1
	if _, err := s.Consume("transactions", bad); !errors.Is(err, serde.ErrMessageIndexMismatch) {
		t.Errorf("want ErrMessageIndexMismatch, got %v", err)
	}
}

func TestConsumeInvalidPayload(t *testing.T) {
	m := newMockSR(t, map[string]int{"transactions-value": 42})
	s := newSerde(t, m)
	_ = s.Bind("transactions", &txpb.Transaction{})

	bad := frame(42, []byte{0x02, 0x02}, []byte{0xFF, 0xFF, 0xFF}) // invalid proto wire
	if _, err := s.Consume("transactions", bad); !errors.Is(err, serde.ErrDeserialize) {
		t.Errorf("want ErrDeserialize, got %v", err)
	}
}

// ── Caching: id validation hits SR once per id ──────────────────────────────

func TestSchemaIDValidationCached(t *testing.T) {
	m := newMockSR(t, map[string]int{"transactions-value": 42})
	s := newSerde(t, m)
	_ = s.Bind("transactions", &txpb.Transaction{})

	framed, _ := s.Produce("transactions", sampleTx())
	for i := 0; i < 4; i++ {
		if _, err := s.Consume("transactions", framed); err != nil {
			t.Fatalf("Consume[%d]: %v", i, err)
		}
	}
	if n := m.idVersionCalls.Load(); n != 1 {
		t.Errorf("id-validation hit SR %d times, want 1 (cached)", n)
	}
}

// ── New() env wiring ─────────────────────────────────────────────────────────

func TestNewFailsWithoutSRURL(t *testing.T) {
	t.Setenv("SCHEMA_REGISTRY_URL", "")
	if _, err := serde.New(); err == nil {
		t.Fatal("expected error when SCHEMA_REGISTRY_URL is empty")
	}
}

func TestStartupBindsAll(t *testing.T) {
	m := newMockSR(t, map[string]int{"transactions-value": 42, "predictions-value": 7})
	s := newSerde(t, m)
	err := s.Startup(map[string]proto.Message{
		"transactions": &txpb.Transaction{},
		"predictions":  &txpb.PredictiveAnalyzer{},
	})
	if err != nil {
		t.Fatalf("Startup: %v", err)
	}
	if _, err := s.Produce("transactions", sampleTx()); err != nil {
		t.Errorf("Produce transactions: %v", err)
	}
	if _, err := s.Produce("predictions", &txpb.PredictiveAnalyzer{}); err != nil {
		t.Errorf("Produce predictions: %v", err)
	}
}
