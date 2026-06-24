// Command interop-cli is the Go side of the cross-language interop harness.
//
//	go run ./interop/go produce <topic> <file>   # SDK Bind + Produce → file
//	go run ./interop/go consume <topic> <file>    # SDK Bind + Consume + verify
//
// The canonical sample is identical across the Go/Node/Python CLIs, so a frame
// produced by any language must consume+verify in the others. Reads
// SCHEMA_REGISTRY_URL from the environment.
package main

import (
	"fmt"
	"os"

	serde "github.com/vinirmbrozz/protobuf-contracts/sdk/go"
	txpb "github.com/vinirmbrozz/protobuf-contracts/sdk/go/protobuf/transaction/v1"
)

// sample is the canonical interop Transaction — keep byte-identical with the
// Node and Python CLIs (scalar string fields only, so the wire bytes match).
func sample() *txpb.Transaction {
	return &txpb.Transaction{
		Transaction: &txpb.TransactionData{
			Id:          "tx-1",
			AmountTotal: "499.99",
			Channel:     "web",
			Type:        "PIX",
		},
		Customer: &txpb.Customer{
			Name:  "Ada Lovelace",
			Email: "ada@example.com",
		},
	}
}

func verify(tx *txpb.Transaction) error {
	d := tx.GetTransaction()
	c := tx.GetCustomer()
	if d.GetId() != "tx-1" || d.GetAmountTotal() != "499.99" ||
		d.GetChannel() != "web" || d.GetType() != "PIX" ||
		c.GetName() != "Ada Lovelace" || c.GetEmail() != "ada@example.com" {
		return fmt.Errorf("mismatch: %+v", tx)
	}
	return nil
}

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: cli <produce|consume> <topic> <file>")
		os.Exit(2)
	}
	cmd, topic, file := os.Args[1], os.Args[2], os.Args[3]

	s, err := serde.New()
	if err != nil {
		fatal(err)
	}
	if err := s.Bind(topic, &txpb.Transaction{}); err != nil {
		fatal(err)
	}

	switch cmd {
	case "produce":
		framed, err := s.Produce(topic, sample())
		if err != nil {
			fatal(err)
		}
		if err := os.WriteFile(file, framed, 0o644); err != nil {
			fatal(err)
		}
		fmt.Printf("go: produced %d bytes → %s\n", len(framed), file)
	case "consume":
		data, err := os.ReadFile(file)
		if err != nil {
			fatal(err)
		}
		msg, err := s.Consume(topic, data)
		if err != nil {
			fatal(err)
		}
		if err := verify(msg.(*txpb.Transaction)); err != nil {
			fatal(err)
		}
		fmt.Println("go: consume OK")
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		os.Exit(2)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "go:", err)
	os.Exit(1)
}
