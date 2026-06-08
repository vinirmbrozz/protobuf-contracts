// Package serde implements Confluent Schema Registry-aware serialization and
// deserialization of proto.Message values for Kafka.
//
// Design (Decision A — "thin SDK"):
//   - The SDK NEVER reads a .proto file and NEVER registers schemas. Schema
//     registration is an out-of-band step (CI/ops in the contracts repo).
//   - At Bind, the SDK RESOLVES the schema_id for a topic from the Schema
//     Registry (it only reads). Produce stamps that id; Consume validates the
//     id against the topic's subject and deserializes into the bound type.
//
// Wire format (Confluent SR):
//
//	[0x00 magic] [schema_id: 4 bytes BE] [message-index array] [proto3 payload]
//
// The message-index array is variable length (Confluent encoding): a single
// top-level message at index 0 is the 1-byte optimization 0x00; otherwise it is
// zig-zag varints (count, then each index). Subject naming uses
// TopicNameStrategy: "<topic>-value".
//
// SECURITY: Kafka accepts arbitrary bytes; this SDK does NOT gate writes at the
// broker. Enforcement lives at the CONSUMER: Consume rejects (typed errors, for
// DLQ routing) anything that is not a valid envelope carrying a schema_id that
// is a registered version of THIS topic's subject and that deserializes into the
// bound type. Sender authentication (ACLs/TLS/signing) is a separate layer.
package serde

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const magicByte = byte(0x00)

// Sentinel errors. Consume wraps these so adapters can route to DLQ via errors.Is.
var (
	ErrInvalidMagicByte   = errors.New("serde: invalid magic byte: expected 0x00")
	ErrFrameTooShort      = errors.New("serde: frame too short")
	ErrTopicNotBound      = errors.New("serde: topic not bound; call Bind before Produce/Consume")
	ErrSchemaForeign      = errors.New("serde: schema_id is not a registered version of this topic's subject")
	ErrMessageIndexMismatch = errors.New("serde: message-index does not match the bound type")
	ErrDeserialize        = errors.New("serde: protobuf deserialization failed")
)

// Config holds Schema Registry connection settings.
type Config struct {
	SRURL     string // SCHEMA_REGISTRY_URL (use https for TLS)
	APIKey    string // SCHEMA_REGISTRY_API_KEY  (optional)
	APISecret string // SCHEMA_REGISTRY_API_SECRET (optional)
}

// binding holds everything resolved for one topic at Bind time.
type binding struct {
	prototype     proto.Message
	subject       string
	schemaID      uint32 // resolved from SR (latest version of subject); used by Produce
	msgIndexBytes []byte // pre-encoded message-index for Produce
	indexes       []int  // expected message-index path, for Consume validation
}

// Serde resolves schema ids from the Schema Registry and frames/unframes
// Confluent envelopes. Construct with New or NewWithConfig.
type Serde struct {
	cfg    Config
	client *http.Client

	mu       sync.RWMutex
	bindings map[string]binding             // topic → binding
	idOK     map[string]struct{}            // consume cache: "<id>|<subject>" confirmed
}

// New creates a Serde reading SR config from environment variables:
//   - SCHEMA_REGISTRY_URL (required)
//   - SCHEMA_REGISTRY_API_KEY / SCHEMA_REGISTRY_API_SECRET (optional)
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
		cfg:      cfg,
		client:   &http.Client{Timeout: 10 * time.Second},
		bindings: make(map[string]binding),
		idOK:     make(map[string]struct{}),
	}
}

// Bind maps a topic to its proto message type and resolves the topic's schema_id
// from the Schema Registry (subject "<topic>-value", latest version). It reads
// only — it never registers. Fails fast if the subject is not registered yet.
func (s *Serde) Bind(topic string, prototype proto.Message) error {
	subject := topicSubject(topic)
	id, err := s.srLatestID(subject)
	if err != nil {
		return fmt.Errorf("serde: bind %q: %w", topic, err)
	}
	indexes := messageIndexes(prototype)
	s.mu.Lock()
	s.bindings[topic] = binding{
		prototype:     prototype,
		subject:       subject,
		schemaID:      id,
		msgIndexBytes: encodeMessageIndexes(indexes),
		indexes:       indexes,
	}
	s.mu.Unlock()
	return nil
}

// Startup binds every topic→type pair (calls Bind for each). Any failure aborts.
func (s *Serde) Startup(bindings map[string]proto.Message) error {
	for topic, prototype := range bindings {
		if err := s.Bind(topic, prototype); err != nil {
			return err
		}
	}
	return nil
}

// Produce serializes msg and wraps it in the Confluent envelope using the
// schema_id resolved at Bind. Fails if the topic was not bound.
func (s *Serde) Produce(topic string, msg proto.Message) ([]byte, error) {
	s.mu.RLock()
	b, ok := s.bindings[topic]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("serde: produce %q: %w", topic, ErrTopicNotBound)
	}

	payload, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("serde: produce %q: marshal: %w", topic, err)
	}

	frame := make([]byte, 0, 5+len(b.msgIndexBytes)+len(payload))
	frame = append(frame, magicByte)
	frame = binary.BigEndian.AppendUint32(frame, b.schemaID)
	frame = append(frame, b.msgIndexBytes...)
	frame = append(frame, payload...)
	return frame, nil
}

// Consume validates the Confluent envelope and deserializes into the bound type.
// Rejects (typed errors, for DLQ) when: magic byte is wrong, frame is too short,
// the schema_id is not a registered version of the topic's subject, the
// message-index does not match the bound type, or the payload fails to decode.
func (s *Serde) Consume(topic string, data []byte) (proto.Message, error) {
	s.mu.RLock()
	b, ok := s.bindings[topic]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("serde: consume %q: %w", topic, ErrTopicNotBound)
	}

	schemaID, indexes, payload, err := parseFrame(data)
	if err != nil {
		return nil, fmt.Errorf("serde: consume %q: %w", topic, err)
	}

	// Security: the id must be a registered version of THIS topic's subject —
	// not merely "exists somewhere in SR". A newer version of the same subject
	// is accepted (forward-compat); an id from another subject is rejected.
	if err := s.validateIDForSubject(schemaID, b.subject); err != nil {
		return nil, fmt.Errorf("serde: consume %q: %w", topic, err)
	}

	if !equalInts(indexes, b.indexes) {
		return nil, fmt.Errorf("serde: consume %q: %w (got %v, want %v)", topic, ErrMessageIndexMismatch, indexes, b.indexes)
	}

	msg := proto.Clone(b.prototype)
	proto.Reset(msg)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("serde: consume %q: %w: %v", topic, ErrDeserialize, err)
	}
	return msg, nil
}

func topicSubject(topic string) string { return topic + "-value" }

// ── Envelope framing ────────────────────────────────────────────────────────

// parseFrame splits [magic][id_be4][msg-index][payload], validating the header.
func parseFrame(data []byte) (schemaID uint32, indexes []int, payload []byte, err error) {
	if len(data) < 6 { // magic(1) + id(4) + at least 1 index byte
		return 0, nil, nil, ErrFrameTooShort
	}
	if data[0] != magicByte {
		return 0, nil, nil, fmt.Errorf("%w (got 0x%02x)", ErrInvalidMagicByte, data[0])
	}
	schemaID = binary.BigEndian.Uint32(data[1:5])
	indexes, off, err := readMessageIndexes(data, 5)
	if err != nil {
		return 0, nil, nil, err
	}
	return schemaID, indexes, data[off:], nil
}

// messageIndexes returns the message-index path of m within its FileDescriptor.
// Truther messages are top-level, so this is a single-element path [index].
func messageIndexes(m proto.Message) []int {
	return []int{int(m.ProtoReflect().Descriptor().Index())}
}

// encodeMessageIndexes encodes the Confluent message-index array. The common
// case [0] (first message) is the 1-byte optimization 0x00.
func encodeMessageIndexes(indexes []int) []byte {
	if len(indexes) == 1 && indexes[0] == 0 {
		return []byte{0x00}
	}
	out := make([]byte, 0, (len(indexes)+1)*2)
	out = appendZigzag(out, int64(len(indexes)))
	for _, idx := range indexes {
		out = appendZigzag(out, int64(idx))
	}
	return out
}

// readMessageIndexes reads the message-index array at offset, returning the
// indexes and the offset where the payload begins.
func readMessageIndexes(data []byte, offset int) (indexes []int, next int, err error) {
	count, n, err := readZigzag(data, offset)
	if err != nil {
		return nil, 0, err
	}
	offset += n
	if count == 0 { // 1-byte optimization: single index [0]
		return []int{0}, offset, nil
	}
	indexes = make([]int, count)
	for i := 0; i < int(count); i++ {
		idx, n, err := readZigzag(data, offset)
		if err != nil {
			return nil, 0, err
		}
		indexes[i] = int(idx)
		offset += n
	}
	return indexes, offset, nil
}

func appendZigzag(b []byte, v int64) []byte {
	zz := uint64((v << 1) ^ (v >> 63))
	for zz >= 0x80 {
		b = append(b, byte(zz)|0x80)
		zz >>= 7
	}
	return append(b, byte(zz))
}

func readZigzag(data []byte, offset int) (val int64, n int, err error) {
	var ux uint64
	var shift uint
	for {
		if offset+n >= len(data) {
			return 0, 0, ErrFrameTooShort
		}
		b := data[offset+n]
		n++
		ux |= uint64(b&0x7f) << shift
		if b < 0x80 {
			break
		}
		shift += 7
	}
	return int64(ux>>1) ^ -int64(ux&1), n, nil
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ── Schema Registry REST client (read-only) ─────────────────────────────────

// srLatestID returns the schema id of the latest registered version of subject.
func (s *Serde) srLatestID(subject string) (uint32, error) {
	endpoint := s.cfg.SRURL + "/subjects/" + url.PathEscape(subject) + "/versions/latest"
	var res struct {
		ID int `json:"id"`
	}
	if err := s.srGet(endpoint, &res); err != nil {
		return 0, err
	}
	return uint32(res.ID), nil
}

// validateIDForSubject confirms (with caching) that schema id is a registered
// version of subject, via GET /schemas/ids/{id}/versions.
func (s *Serde) validateIDForSubject(id uint32, subject string) error {
	key := fmt.Sprintf("%d|%s", id, subject)
	s.mu.RLock()
	_, ok := s.idOK[key]
	s.mu.RUnlock()
	if ok {
		return nil
	}

	endpoint := fmt.Sprintf("%s/schemas/ids/%d/versions", s.cfg.SRURL, id)
	var pairs []struct {
		Subject string `json:"subject"`
		Version int    `json:"version"`
	}
	if err := s.srGet(endpoint, &pairs); err != nil {
		// 404 → id unknown to SR entirely; srGet returns a typed not-found below.
		if errors.Is(err, errSRNotFound) {
			return fmt.Errorf("%w (schema_id=%d unknown to SR)", ErrSchemaForeign, id)
		}
		return err
	}
	for _, p := range pairs {
		if p.Subject == subject {
			s.mu.Lock()
			s.idOK[key] = struct{}{}
			s.mu.Unlock()
			return nil
		}
	}
	return fmt.Errorf("%w (schema_id=%d not under %q)", ErrSchemaForeign, id, subject)
}

var errSRNotFound = errors.New("serde: schema registry: not found")

// srGet performs an authenticated GET and decodes the JSON body into out.
func (s *Serde) srGet(endpoint string, out any) error {
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	if s.cfg.APIKey != "" {
		req.SetBasicAuth(s.cfg.APIKey, s.cfg.APISecret)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("serde: SR GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("serde: SR GET %s: decode: %w", endpoint, err)
		}
		return nil
	case http.StatusNotFound:
		return errSRNotFound
	default:
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("serde: SR GET %s: HTTP %d: %s", endpoint, resp.StatusCode, b)
	}
}
