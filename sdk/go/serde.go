// Package serde implements Confluent Schema Registry-aware serialization and
// deserialization of proto.Message values for Kafka.
//
// Wire format per Confluent SR SPEC §2:
//
//	[0x00] [schema_id_be4] [0x00] [proto3_binary_payload]
//
// Subject naming uses TopicNameStrategy: "<topic>-value" (SPEC §3.2).
// SR connection is configured via environment variables (SPEC §7.3) — no hardcode.
package serde

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	magicByte      = byte(0x00)
	frameHeaderLen = 6 // magic(1) + schemaID(4) + msgIndex(1)
)

// Sentinel errors for invalid payloads. Use errors.Is to check.
var (
	ErrInvalidMagicByte = errors.New("serde: invalid magic byte: expected 0x00")
	ErrFrameTooShort    = errors.New("serde: frame too short: minimum 6 bytes required")
)

// Config holds Schema Registry connection settings.
type Config struct {
	SRURL     string // SCHEMA_REGISTRY_URL
	APIKey    string // SCHEMA_REGISTRY_API_KEY  (optional)
	APISecret string // SCHEMA_REGISTRY_API_SECRET (optional)
}

type typeEntry struct {
	prototype  proto.Message
	schemaText string // proto IDL content for SR registration
}

// Serde serializes/deserializes Confluent-framed Kafka message values.
// Construct with New or NewWithConfig; zero value is not usable.
type Serde struct {
	cfg    Config
	client *http.Client

	mu           sync.RWMutex
	subjectToID  map[string]uint32   // produce cache: subject → schema ID
	idKnown      map[uint32]struct{} // consume validation cache: known schema IDs
	typeRegistry map[string]typeEntry
}

// New creates a Serde reading SR config from environment variables:
//   - SCHEMA_REGISTRY_URL (required)
//   - SCHEMA_REGISTRY_API_KEY (optional)
//   - SCHEMA_REGISTRY_API_SECRET (optional)
func New() (*Serde, error) {
	srURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if srURL == "" {
		return nil, fmt.Errorf("serde: SCHEMA_REGISTRY_URL environment variable is not set")
	}
	return NewWithConfig(Config{
		SRURL:     srURL,
		APIKey:    os.Getenv("SCHEMA_REGISTRY_API_KEY"),
		APISecret: os.Getenv("SCHEMA_REGISTRY_API_SECRET"),
	}), nil
}

// NewWithConfig creates a Serde with an explicit Config.
func NewWithConfig(cfg Config) *Serde {
	return &Serde{
		cfg:          cfg,
		client:       &http.Client{Timeout: 10 * time.Second},
		subjectToID:  make(map[string]uint32),
		idKnown:      make(map[uint32]struct{}),
		typeRegistry: make(map[string]typeEntry),
	}
}

// RegisterType maps a topic to a proto.Message prototype for Consume.
// schemaText is the proto IDL content sent to SR during Produce; pass "" to
// derive a minimal identifier from the message descriptor (sufficient for
// testing with a mock SR — production should pass the actual .proto IDL).
func (s *Serde) RegisterType(topic string, prototype proto.Message, schemaText string) {
	if schemaText == "" {
		schemaText = string(proto.MessageName(prototype))
	}
	s.mu.Lock()
	s.typeRegistry[topicSubject(topic)] = typeEntry{prototype: prototype, schemaText: schemaText}
	s.mu.Unlock()
}

// Produce serializes msg to the Confluent envelope and registers its schema in
// the Schema Registry on the first call per topic (eager registration, SPEC §4.1).
// Subject: "<topic>-value" (TopicNameStrategy, SPEC §3.2).
func (s *Serde) Produce(topic string, msg proto.Message) ([]byte, error) {
	subject := topicSubject(topic)

	schemaID, err := s.ensureRegistered(subject, msg)
	if err != nil {
		return nil, fmt.Errorf("serde produce %q: %w", topic, err)
	}

	payload, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("serde produce %q: marshal: %w", topic, err)
	}

	return frameMessage(schemaID, payload), nil
}

// Consume validates the Confluent envelope, resolves the schema ID via Schema
// Registry, and deserializes the payload into the registered type for topic.
//
// Returns an explicit error (never panics) for:
//   - wrong magic byte
//   - frame shorter than 6 bytes
//   - schema ID not registered in SR
//   - protobuf unmarshal failure
//   - no RegisterType call for topic
func (s *Serde) Consume(topic string, data []byte) (proto.Message, error) {
	schemaID, payload, err := parseFrame(data)
	if err != nil {
		return nil, fmt.Errorf("serde consume %q: %w", topic, err)
	}

	if err := s.validateSchemaID(schemaID); err != nil {
		return nil, fmt.Errorf("serde consume %q: %w", topic, err)
	}

	s.mu.RLock()
	entry, ok := s.typeRegistry[topicSubject(topic)]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("serde consume %q: no registered type; call RegisterType before Consume", topic)
	}

	msg := proto.Clone(entry.prototype)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("serde consume %q: unmarshal: %w", topic, err)
	}
	return msg, nil
}

// topicSubject returns "<topic>-value" per TopicNameStrategy (SPEC §3.2).
func topicSubject(topic string) string { return topic + "-value" }

// frameMessage builds [0x00][schemaID_be4][0x00][proto3_payload].
func frameMessage(schemaID uint32, payload []byte) []byte {
	frame := make([]byte, frameHeaderLen+len(payload))
	frame[0] = magicByte
	binary.BigEndian.PutUint32(frame[1:5], schemaID)
	frame[5] = 0x00 // message index = 0 (first message; Truther single-message-per-file convention)
	copy(frame[frameHeaderLen:], payload)
	return frame
}

// parseFrame validates and splits a Confluent-framed Kafka message value.
func parseFrame(data []byte) (schemaID uint32, payload []byte, err error) {
	if len(data) < frameHeaderLen {
		return 0, nil, ErrFrameTooShort
	}
	if data[0] != magicByte {
		return 0, nil, fmt.Errorf("%w (got 0x%02x)", ErrInvalidMagicByte, data[0])
	}
	return binary.BigEndian.Uint32(data[1:5]), data[frameHeaderLen:], nil
}

// ensureRegistered returns the cached schema ID, registering with SR on first call.
func (s *Serde) ensureRegistered(subject string, msg proto.Message) (uint32, error) {
	s.mu.RLock()
	id, cached := s.subjectToID[subject]
	s.mu.RUnlock()
	if cached {
		return id, nil
	}

	s.mu.RLock()
	entry, hasEntry := s.typeRegistry[subject]
	s.mu.RUnlock()

	schemaText := string(proto.MessageName(msg))
	if hasEntry && entry.schemaText != "" {
		schemaText = entry.schemaText
	}

	id, err := s.srRegister(subject, schemaText)
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	s.subjectToID[subject] = id
	s.idKnown[id] = struct{}{}
	s.mu.Unlock()
	return id, nil
}

// validateSchemaID verifies the schema ID is in SR (with in-memory caching).
func (s *Serde) validateSchemaID(id uint32) error {
	s.mu.RLock()
	_, known := s.idKnown[id]
	s.mu.RUnlock()
	if known {
		return nil
	}

	exists, err := s.srSchemaExists(id)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("schema ID %d is not registered in Schema Registry", id)
	}

	s.mu.Lock()
	s.idKnown[id] = struct{}{}
	s.mu.Unlock()
	return nil
}

// ── Schema Registry REST client (Confluent SR API, no CGo) ──────────────────

type srVersionReq struct {
	SchemaType string `json:"schemaType"`
	Schema     string `json:"schema"`
}

type srVersionResp struct {
	ID int `json:"id"`
}

func (s *Serde) srRegister(subject, schema string) (uint32, error) {
	url := s.cfg.SRURL + "/subjects/" + subject + "/versions"
	body, err := json.Marshal(srVersionReq{SchemaType: "PROTOBUF", Schema: schema})
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	if s.cfg.APIKey != "" {
		req.SetBasicAuth(s.cfg.APIKey, s.cfg.APISecret)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("SR POST /subjects/%s/versions: %w", subject, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("SR register %q: HTTP %d: %s", subject, resp.StatusCode, b)
	}

	var res srVersionResp
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, fmt.Errorf("SR register %q: decode response: %w", subject, err)
	}
	return uint32(res.ID), nil
}

func (s *Serde) srSchemaExists(id uint32) (bool, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", s.cfg.SRURL, id)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	if s.cfg.APIKey != "" {
		req.SetBasicAuth(s.cfg.APIKey, s.cfg.APISecret)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("SR GET /schemas/ids/%d: %w", id, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		b, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("SR lookup schema %d: HTTP %d: %s", id, resp.StatusCode, b)
	}
}
