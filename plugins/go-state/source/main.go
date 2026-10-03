// A managed Go service: durable chat delivery and state advance in one commit.
package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	neomega "github.com/OmineDev/neomega-sdk-go"
)

//go:embed bus-contract.json
var contractJSON []byte

type settings struct {
	Label string `json:"label"`
}

func readCount(ctx context.Context, storage neomega.Storage) (neomega.Snapshot, uint64, error) {
	snapshot, err := storage.Snapshot(ctx, []string{"count"})
	if err != nil {
		return snapshot, 0, err
	}
	var count uint64
	if raw, exists := snapshot.Values["count"]; exists {
		if string(raw) == "null" {
			return snapshot, 0, errors.New("stored count is null")
		}
		if err := json.Unmarshal(raw, &count); err != nil {
			return snapshot, 0, err
		}
	}
	return snapshot, count, nil
}

func run(ctx context.Context) error {
	peer, err := neomega.Open(ctx)
	if err != nil {
		return err
	}
	storage := neomega.Storage{Peer: peer}
	config := settings{Label: "Go chat counter"}
	worker := neomega.NewWorker(peer, neomega.Callbacks{
		Validate: func(ctx context.Context, _ *neomega.Peer, _ map[string]any) (map[string]any, error) {
			if err := neomega.LoadConfig(&config); err != nil {
				return nil, err
			}
			_, _, err := readCount(ctx, storage)
			if err != nil {
				return nil, err
			}
			var contract map[string]any
			if err := json.Unmarshal(contractJSON, &contract); err != nil {
				return nil, err
			}
			declarations := []any{}
			for _, spec := range []struct{ name, kind string }{{"count", "read"}, {"inspect", "service"}} {
				declarations = append(declarations, map[string]any{"name": spec.name, "member": "go_state." + spec.name, "kind": spec.kind, "major": 1, "shared": false, "candidate_readable": spec.kind == "read", "contract": contract})
			}
			return map[string]any{"bus_exports": declarations}, nil
		},
		Bus: func(ctx context.Context, _ *neomega.Peer, body map[string]any) (any, error) {
			args, ok := body["arguments"].([]any)
			if !ok || len(args) != 2 || args[0] != "struct" {
				return nil, errors.New("expected EmptyRequest")
			}
			fields, ok := args[1].([]any)
			if !ok || len(fields) != 0 {
				return nil, errors.New("expected empty fields")
			}
			_, count, err := readCount(ctx, storage)
			if err != nil {
				return nil, err
			}
			return []any{"uint", strconv.FormatUint(count, 10)}, nil
		},
		Activate: func(context.Context, *neomega.Peer, map[string]any) error { return nil },
		Drain:    func(context.Context, *neomega.Peer, map[string]any) error { return nil },
		Event: func(ctx context.Context, _ *neomega.Peer, body map[string]any) error {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			raw, err := json.Marshal(body)
			if err != nil {
				return err
			}
			var event neomega.EventDelivery
			if err := json.Unmarshal(raw, &event); err != nil {
				return err
			}
			if event.SubscriptionID == "" || event.EventID == "" || event.DeliveryToken == "" {
				return errors.New("missing durable delivery identity")
			}
			snapshot, count, err := readCount(ctx, storage)
			if err != nil {
				return err
			}
			if count == math.MaxUint64 {
				return errors.New("counter exhausted")
			}
			mutation, err := neomega.SetValue("count", count+1)
			if err != nil {
				return err
			}
			digest := sha256.Sum256([]byte(event.DeliveryToken))
			commitID := "go_" + hex.EncodeToString(digest[:])
			intent, err := neomega.PrepareCommit(commitID, snapshot, []neomega.Mutation{mutation}, nil, &event)
			if err != nil {
				return err
			}
			if _, err := storage.Submit(ctx, intent); err != nil {
				// Retain this ID for state.receipt reconciliation; never replay here.
				return fmt.Errorf("commit %s: %w", commitID, err)
			}
			return nil
		},
		Service: func(ctx context.Context, _ *neomega.Peer, body map[string]any) (map[string]any, error) {
			if !strings.HasSuffix(fmt.Sprint(body["service"]), ".count") {
				return nil, errors.New("unknown service")
			}
			_, count, err := readCount(ctx, storage)
			if err != nil {
				return nil, err
			}
			return map[string]any{"label": config.Label, "count": count}, nil
		},
	})
	return worker.Run(ctx)
}

func main() {
	// stdout belongs exclusively to IPC. Diagnostics go to stderr.
	log.SetOutput(os.Stderr)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
