// Package serde_test — unit and integration tests for the Confluent SR serde library.
//
// Tests use net/http/httptest to simulate the Confluent Schema Registry REST API.
// No real Kafka or SR instance is required.
package serde_test

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vinirmbrozz/truther-contracts/serde"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// mockSchemaRegistry returns an httptest.Server that simulates Confluent SR.
// It auto-increments schema IDs and returns 404 for IDs not yet registered.
func mockSchemaRegistry(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	registered := make(map[string]int) // subject → ID
	var nextID atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/versions"):
			// POST /subjects/{subject}/versions — register or return existing schema ID
			parts := strings.SplitN(r.URL.Path, "/", 4) // ["", "subjects", "{subject}", "versions"]
			if len(parts) < 4 {
				http.Error(w, "bad path", http.StatusBadRequest)
				return
			}
			subject := parts[2]

			mu.Lock()
			id, exists := registered[subject]
			if !exists {
				id = int(nextID.Add(1))
				registered[subject] = id
			}
			mu.Unlock()

			_ = json.NewEncoder(w).Encode(map[string]int{"id": id})

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/schemas/ids/"):
			// GET /schemas/ids/{id} — return schema or 404
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
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"error_code": 40403,
					"message":    "Schema not found",
				})
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
	return serde.NewWithConfig(serde.Config{SRURL: srv.URL})
}

// ── Framing correctness ──────────────────────────────────────────────────────

// TestFrameRoundTrip is the primary TDD test: encode → decode produces identical
// message, and the Confluent header bytes are correct.
func TestFrameRoundTrip(t *testing.T) {
	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)
	s.RegisterType("test", &wrapperspb.StringValue{}, "")

	original := wrapperspb.String("hello Truther")

	framed, err := s.Produce("test", original)
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
		t.Error("schema ID is 0; expected a non-zero value from SR")
	}
	if framed[5] != 0x00 {
		t.Errorf("message index byte: got 0x%02x, want 0x00 (first message convention)", framed[5])
	}

	// Roundtrip
	msg, err := s.Consume("test", framed)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	got, ok := msg.(*wrapperspb.StringValue)
	if !ok {
		t.Fatalf("Consume returned %T, want *wrapperspb.StringValue", msg)
	}
	if !proto.Equal(original, got) {
		t.Errorf("roundtrip mismatch: got %q, want %q", got.Value, original.Value)
	}
}

// ── Invalid payload rejection (SPEC §4.3) ───────────────────────────────────

func TestConsumeInvalidMagicByte(t *testing.T) {
	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)
	s.RegisterType("test", &wrapperspb.StringValue{}, "")

	bad := []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0a, 0x03}
	_, err := s.Consume("test", bad)
	if err == nil {
		t.Fatal("expected error for invalid magic byte, got nil")
	}
	if !errors.Is(err, serde.ErrInvalidMagicByte) {
		t.Errorf("want ErrInvalidMagicByte, got: %v", err)
	}
}

func TestConsumeFrameTooShort(t *testing.T) {
	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)

	cases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"1 byte", []byte{0x00}},
		{"3 bytes", []byte{0x00, 0x00, 0x00}},
		{"5 bytes", []byte{0x00, 0x00, 0x00, 0x00, 0x01}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Consume("test", tc.data)
			if err == nil {
				t.Fatal("expected error for short frame, got nil")
			}
			if !errors.Is(err, serde.ErrFrameTooShort) {
				t.Errorf("want ErrFrameTooShort, got: %v", err)
			}
		})
	}
}

func TestConsumeUnknownSchemaID(t *testing.T) {
	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)
	s.RegisterType("test", &wrapperspb.StringValue{}, "")

	// Manually build a frame with schemaID=999 which was never registered
	frame := make([]byte, 7)
	frame[0] = 0x00
	binary.BigEndian.PutUint32(frame[1:5], 999)
	frame[5] = 0x00
	frame[6] = 0x00 // zero payload byte

	_, err := s.Consume("test", frame)
	if err == nil {
		t.Fatal("expected error for unknown schema ID, got nil")
	}
	// Must not panic — return an explicit error
}

func TestConsumeNoRegisteredType(t *testing.T) {
	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)

	// Produce on "registered" to create a valid framed payload
	s.RegisterType("registered", &wrapperspb.StringValue{}, "")
	framed, err := s.Produce("registered", wrapperspb.String("x"))
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Consume on a different topic with no RegisterType call → error
	_, err = s.Consume("unregistered", framed)
	if err == nil {
		t.Fatal("expected error for unregistered topic, got nil")
	}
}

func TestConsumeInvalidPayload(t *testing.T) {
	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)
	s.RegisterType("test", &wrapperspb.StringValue{}, "")

	// Produce to register schema and obtain a valid schemaID
	framed, err := s.Produce("test", wrapperspb.String("seed"))
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Replace the proto3 payload with invalid wire bytes (must not panic)
	corrupt := make([]byte, len(framed))
	copy(corrupt, framed)
	for i := 6; i < len(corrupt); i++ {
		corrupt[i] = 0xFF // invalid protobuf wire type 7 on field 31
	}

	_, err = s.Consume("test", corrupt)
	if err == nil {
		t.Fatal("expected unmarshal error for corrupt payload, got nil")
	}
}

// ── Schema Registry integration (mock SR) ───────────────────────────────────

// TestSchemaCachedAfterFirstProduce verifies that the SR is called exactly once
// per topic even across multiple Produce calls (eager-register + cache, SPEC §4.1).
func TestSchemaCachedAfterFirstProduce(t *testing.T) {
	var registerCalls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/versions"):
			registerCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]int{"id": 1})
		case r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]string{"schema": "stub"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	s := serde.NewWithConfig(serde.Config{SRURL: srv.URL})
	s.RegisterType("test", &wrapperspb.StringValue{}, "")

	for i := 0; i < 5; i++ {
		if _, err := s.Produce("test", wrapperspb.String("msg")); err != nil {
			t.Fatalf("Produce[%d]: %v", i, err)
		}
	}

	if n := registerCalls.Load(); n != 1 {
		t.Errorf("SR registration called %d times, want exactly 1", n)
	}
}

// TestTopicNameStrategy verifies that the SR subject is "<topic>-value".
func TestTopicNameStrategy(t *testing.T) {
	var registeredSubjects []string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/versions") {
			parts := strings.SplitN(r.URL.Path, "/", 4)
			if len(parts) >= 3 {
				mu.Lock()
				registeredSubjects = append(registeredSubjects, parts[2])
				mu.Unlock()
			}
			_ = json.NewEncoder(w).Encode(map[string]int{"id": 1})
		} else {
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	s := serde.NewWithConfig(serde.Config{SRURL: srv.URL})
	s.RegisterType("transactions", &wrapperspb.StringValue{}, "")
	if _, err := s.Produce("transactions", wrapperspb.String("x")); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	mu.Lock()
	subjects := registeredSubjects
	mu.Unlock()

	if len(subjects) == 0 {
		t.Fatal("no SR registration call received")
	}
	if subjects[0] != "transactions-value" {
		t.Errorf("subject: got %q, want %q", subjects[0], "transactions-value")
	}
}

// TestProduceConsumeMultipleTopics verifies that distinct topics get distinct
// schema IDs and their messages roundtrip correctly.
func TestProduceConsumeMultipleTopics(t *testing.T) {
	srv := mockSchemaRegistry(t)
	s := newTestSerde(t, srv)
	s.RegisterType("topic-a", &wrapperspb.StringValue{}, "")
	s.RegisterType("topic-b", &wrapperspb.Int32Value{}, "")

	framedA, err := s.Produce("topic-a", wrapperspb.String("hello"))
	if err != nil {
		t.Fatalf("Produce topic-a: %v", err)
	}
	framedB, err := s.Produce("topic-b", wrapperspb.Int32(42))
	if err != nil {
		t.Fatalf("Produce topic-b: %v", err)
	}

	idA := binary.BigEndian.Uint32(framedA[1:5])
	idB := binary.BigEndian.Uint32(framedB[1:5])
	if idA == idB {
		t.Errorf("both topics got the same schema ID %d; expected distinct IDs", idA)
	}

	msgA, err := s.Consume("topic-a", framedA)
	if err != nil {
		t.Fatalf("Consume topic-a: %v", err)
	}
	if v := msgA.(*wrapperspb.StringValue).Value; v != "hello" {
		t.Errorf("topic-a: got %q, want \"hello\"", v)
	}

	msgB, err := s.Consume("topic-b", framedB)
	if err != nil {
		t.Fatalf("Consume topic-b: %v", err)
	}
	if v := msgB.(*wrapperspb.Int32Value).Value; v != 42 {
		t.Errorf("topic-b: got %d, want 42", v)
	}
}

// ── New() env-var wiring ─────────────────────────────────────────────────────

func TestNewFailsWithoutSRURL(t *testing.T) {
	t.Setenv("SCHEMA_REGISTRY_URL", "")
	_, err := serde.New()
	if err == nil {
		t.Fatal("expected error when SCHEMA_REGISTRY_URL is empty, got nil")
	}
}

func TestNewReadsSchemaRegistryURL(t *testing.T) {
	srv := mockSchemaRegistry(t)
	t.Setenv("SCHEMA_REGISTRY_URL", srv.URL)

	s, err := serde.New()
	if err != nil {
		t.Fatalf("New(): %v", err)
	}

	s.RegisterType("test", &wrapperspb.StringValue{}, "")
	if _, err := s.Produce("test", wrapperspb.String("ok")); err != nil {
		t.Fatalf("Produce: %v", err)
	}
}
