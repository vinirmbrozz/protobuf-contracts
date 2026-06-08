//go:build integration

// Integration test against a REAL Confluent Schema Registry (mock is not enough
// to validate registration/resolution). Requires the schema already registered
// (run scripts/register_schemas.py first) and SCHEMA_REGISTRY_URL set.
//
//	docker-compose up -d
//	SCHEMA_REGISTRY_URL=http://localhost:8081 python scripts/register_schemas.py
//	SCHEMA_REGISTRY_URL=http://localhost:8081 go test -tags integration ./...
package serde_test

import (
	"errors"
	"os"
	"testing"

	serde "github.com/vinirmbrozz/truther-contracts/sdk/go"
	txpb "github.com/vinirmbrozz/truther-contracts/sdk/go/proto"
	"google.golang.org/protobuf/proto"
)

func TestIntegrationRealSR(t *testing.T) {
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	if url == "" {
		t.Skip("SCHEMA_REGISTRY_URL not set; skipping real-SR integration test")
	}
	s := serde.NewWithConfig(serde.Config{
		SRURL:     url,
		APIKey:    os.Getenv("SCHEMA_REGISTRY_API_KEY"),
		APISecret: os.Getenv("SCHEMA_REGISTRY_API_SECRET"),
	})

	// Bind resolves the real schema_id assigned by SR (must be registered first).
	if err := s.Bind("transactions", &txpb.Transaction{}); err != nil {
		t.Fatalf("Bind against real SR (did you run register_schemas.py?): %v", err)
	}

	original := sampleTx()
	framed, err := s.Produce("transactions", original)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	got, err := s.Consume("transactions", framed)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if !proto.Equal(original, got) {
		t.Errorf("roundtrip mismatch against real SR: got %v", got)
	}

	// Negative: a schema_id that does not exist in the real SR must be rejected.
	payload, _ := proto.Marshal(original)
	bogus := frame(987654, []byte{0x02, 0x02}, payload)
	if _, err := s.Consume("transactions", bogus); !errors.Is(err, serde.ErrSchemaForeign) {
		t.Errorf("bogus id against real SR: want ErrSchemaForeign, got %v", err)
	}
}
