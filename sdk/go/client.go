package neomega

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// Caller is the managed peer's RPC surface. Facades do not own its lifetime.
type Caller interface {
	Call(context.Context, string, any) (map[string]any, error)
}

// LoadConfig reads Host's selected configuration into dst. Prepopulate dst with
// defaults. Validation is read-only; a missing file leaves those defaults intact.
func LoadConfig(dst any) error {
	path := os.Getenv("NEOMEGA_CONFIG_OVERRIDE_FILE")
	if path == "" {
		dir := os.Getenv("NEOMEGA_INSTALLATION_DATA_DIR")
		if dir == "" {
			return errors.New("Host did not provide an installation data directory")
		}
		path = filepath.Join(dir, "config.json")
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.New("configuration read failed")
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (256<<10)+1))
	if err != nil {
		return errors.New("configuration read failed")
	}
	if len(raw) > 256<<10 {
		return errors.New("configuration exceeds 256 KiB")
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("configuration must be a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid configuration JSON or fields")
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("configuration contains trailing JSON")
	}
	return nil
}

var clientIdentifier = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// Intent is an immutable JSON request. Payload returns an independent copy for
// durable reconciliation; it never changes IDs, deadlines or unknown outcomes.
type Intent struct {
	method string
	raw    string
}

func (i Intent) Method() string           { return i.method }
func (i Intent) Payload() json.RawMessage { return json.RawMessage(i.raw) }
func freezeIntent(method string, payload any) (Intent, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Intent{}, err
	}
	return Intent{method, string(raw)}, nil
}

// Uncertain retains the original request after a transport or receipt failure.
// It never implies the request was unsent or safe to replay.
type Uncertain struct {
	Intent Intent
	ID     string
	Err    error
}

func (e *Uncertain) Error() string {
	return "request outcome unresolved; retain original intent and ID"
}
func (e *Uncertain) Unwrap() error { return e.Err }
func submitIntent(ctx context.Context, p Caller, i Intent) (map[string]any, error) {
	row, err := p.Call(ctx, i.method, i.Payload())
	if err != nil {
		var rejected *IPCRejected
		if errors.As(err, &rejected) {
			return nil, err
		}
		return nil, &Uncertain{Intent: i, Err: err}
	}
	return row, nil
}

// Storage commits explicit global revision CAS transactions. It never retries.
type Storage struct{ Peer Caller }
type Snapshot struct {
	Revision int64                      `json:"revision"`
	Values   map[string]json.RawMessage `json:"values"`
	Session  string                     `json:"session"`
}

func (s Storage) Snapshot(ctx context.Context, keys []string) (Snapshot, error) {
	body := map[string]any{}
	if keys != nil {
		body["keys"] = keys
	}
	row, err := s.Peer.Call(ctx, "state.get", body)
	if err != nil {
		return Snapshot{}, err
	}
	raw, err := json.Marshal(row)
	if err != nil {
		return Snapshot{}, err
	}
	var snap Snapshot
	err = json.Unmarshal(raw, &snap)
	if err != nil {
		return Snapshot{}, err
	}
	if _, ok := row["revision"]; !ok || snap.Revision < 0 || snap.Values == nil {
		return Snapshot{}, errors.New("invalid state snapshot")
	}
	return snap, nil
}

// Mutation distinguishes deletion from writing JSON null. Use SetValue/DeleteKey.
type Mutation struct {
	Key    string          `json:"key"`
	Value  json.RawMessage `json:"value,omitempty"`
	Delete bool            `json:"delete,omitempty"`
}

func SetValue(key string, value any) (Mutation, error) {
	raw, err := json.Marshal(value)
	return Mutation{Key: key, Value: raw}, err
}
func DeleteKey(key string) Mutation { return Mutation{Key: key, Delete: true} }

// EventDelivery contains the exact durable delivery identity from Host. Merely
// receiving it never advances the subscription cursor.
type EventDelivery struct {
	SubscriptionID string `json:"subscription_id"`
	EventID        string `json:"event_id"`
	DeliveryToken  string `json:"delivery_token"`
}

func PrepareCommit(id string, snapshot Snapshot, mutations []Mutation, actions []Intent, event *EventDelivery) (Intent, error) {
	if !clientIdentifier.MatchString(id) || snapshot.Revision < 0 {
		return Intent{}, errors.New("invalid commit identifier or revision")
	}
	if mutations == nil {
		mutations = []Mutation{}
	}
	converted := make([]map[string]any, 0, len(actions))
	for _, a := range actions {
		if a.method != "operations.submit" {
			return Intent{}, errors.New("expected operation intent")
		}
		var body map[string]any
		if err := decodeClientJSON(a.Payload(), &body); err != nil {
			return Intent{}, err
		}
		if body["session"] != snapshot.Session {
			return Intent{}, errors.New("action and snapshot sessions differ")
		}
		delete(body, "session")
		converted = append(converted, body)
	}
	body := map[string]any{"commit_id": id, "expected_state_revision": snapshot.Revision, "mutations": mutations, "actions": converted}
	if len(actions) > 0 {
		body["session"] = snapshot.Session
	}
	method := "state.commit"
	if event != nil {
		method = "events.commit"
		body["subscription_id"] = event.SubscriptionID
		body["event_id"] = event.EventID
		body["delivery_token"] = event.DeliveryToken
	}
	i, err := freezeIntent(method, body)
	if err == nil && len(i.raw) > 256<<10 {
		return Intent{}, errors.New("commit exceeds 256 KiB")
	}
	return i, err
}
func (s Storage) Submit(ctx context.Context, i Intent) (map[string]any, error) {
	if i.method != "state.commit" && i.method != "events.commit" {
		return nil, errors.New("expected commit intent")
	}
	row, err := submitIntent(ctx, s.Peer, i)
	if err != nil {
		return nil, err
	}
	var body map[string]any
	_ = decodeClientJSON(i.Payload(), &body)
	if row["commit_id"] != body["commit_id"] || !validCommitReceipt(row, body) {
		return row, &Uncertain{Intent: i, Err: errors.New("invalid commit receipt")}
	}
	return row, nil
}
func (s Storage) Receipt(ctx context.Context, id string) (map[string]any, error) {
	return s.Peer.Call(ctx, "state.receipt", map[string]any{"commit_id": id})
}

// Ack creates one event-only CAS commit. Retain the returned intent even on error.
func (s Storage) Ack(ctx context.Context, id string, snapshot Snapshot, event EventDelivery) (Intent, map[string]any, error) {
	i, err := PrepareCommit(id, snapshot, nil, nil, &event)
	if err != nil {
		return i, nil, err
	}
	row, err := s.Submit(ctx, i)
	return i, row, err
}

// Operations refers to the game session from capabilities.get, not hello's
// installation metadata. Context cancellation does not cancel a world action.
type Operations struct {
	Peer    Caller
	Session string
}

func (o Operations) Prepare(operation string, arguments map[string]any, key string, deadline time.Time) (Intent, error) {
	if operation == "" || o.Session == "" || !clientIdentifier.MatchString(key) || deadline.IsZero() || arguments == nil {
		return Intent{}, errors.New("invalid operation intent")
	}
	return freezeIntent("operations.submit", map[string]any{"operation": operation, "arguments": arguments, "session": o.Session, "idempotency_key": key, "deadline": deadline.UTC().Format(time.RFC3339Nano)})
}
func (o Operations) Submit(ctx context.Context, i Intent) (map[string]any, error) {
	if i.method != "operations.submit" {
		return nil, errors.New("expected operation intent")
	}
	var body map[string]any
	_ = decodeClientJSON(i.Payload(), &body)
	if body["session"] != o.Session {
		return nil, errors.New("operation session mismatch")
	}
	row, err := submitIntent(ctx, o.Peer, i)
	if err != nil {
		return nil, err
	}
	id, _ := row["operation_id"].(string)
	if !clientIdentifier.MatchString(id) || !validOperationAdmission(row) {
		return row, &Uncertain{Intent: i, ID: id, Err: errors.New("invalid operation admission")}
	}
	return row, nil
}
func (o Operations) Get(ctx context.Context, id string) (map[string]any, error) {
	return o.Peer.Call(ctx, "operations.get", map[string]any{"operation_id": id})
}
func (o Operations) Cancel(ctx context.Context, id string) (map[string]any, error) {
	i, _ := freezeIntent("operations.cancel", map[string]any{"operation_id": id})
	row, err := submitIntent(ctx, o.Peer, i)
	if u, ok := err.(*Uncertain); ok {
		u.ID = id
	}
	return row, err
}

// Bus exposes the existing tagged-value contract without pretending that a Go
// struct implies a schema digest. Caller supplies catalog-derived schema/args.
// Receipts retain wire bulk references; Result/Call decode them separately.
type Bus struct{ Peer Caller }
type BusRequest struct {
	Target       string         `json:"target"`
	Major        uint32         `json:"major"`
	SchemaDigest string         `json:"schema_digest"`
	Args         any            `json:"args"`
	Deadline     time.Time      `json:"deadline"`
	Kind         string         `json:"kind"`
	Provider     map[string]any `json:"provider,omitempty"`
	Cause        string         `json:"cause,omitempty"`
}

func (b Bus) Catalog(ctx context.Context) (map[string]any, error) {
	return b.Peer.Call(ctx, "bus.catalog", map[string]any{})
}
func (b Bus) Submit(ctx context.Context, request BusRequest) (map[string]any, error) {
	if request.Kind != "read" && request.Kind != "service" {
		return nil, errors.New("Bus Submit supports read/service only")
	}
	if request.Deadline.IsZero() {
		return nil, errors.New("explicit bus deadline required")
	}
	encoded, err := EncodeBusValue(ctx, b.Peer, request.Args, request.Deadline)
	if err != nil {
		return nil, err
	}
	request.Args = encoded
	i, err := freezeIntent("bus.call", request)
	if err != nil {
		return nil, err
	}
	row, err := submitIntent(ctx, b.Peer, i)
	if err != nil {
		return nil, err
	}
	if !validBusReceipt(row, "") {
		id, _ := row["call_id"].(string)
		return row, &Uncertain{Intent: i, ID: id, Err: errors.New("invalid bus receipt")}
	}
	return row, nil
}
func (b Bus) Get(ctx context.Context, id string) (map[string]any, error) {
	return b.Peer.Call(ctx, "bus.call.get", map[string]any{"call_id": id})
}
func (b Bus) Cancel(ctx context.Context, id string) (map[string]any, error) {
	i, _ := freezeIntent("bus.call.cancel", map[string]any{"call_id": id})
	row, err := submitIntent(ctx, b.Peer, i)
	if u, ok := err.(*Uncertain); ok {
		u.ID = id
	}
	return row, err
}

// Wait consumes receipts until terminal or the caller's context expires. It
// returns the last receipt with errors and never implicitly cancels or replays.
// To request cancellation use Cancel with an explicit cleanup context.
func (b Bus) Wait(ctx context.Context, receipt map[string]any, poll time.Duration) (map[string]any, error) {
	if poll <= 0 {
		return receipt, errors.New("positive polling interval required")
	}
	id, _ := receipt["call_id"].(string)
	if !validBusReceipt(receipt, id) {
		return receipt, errors.New("invalid bus call receipt")
	}
	for {
		state, _ := receipt["state"].(string)
		switch state {
		case "succeeded", "failed", "cancelled", "deadline_exceeded", "unavailable":
			return receipt, nil
		case "running":
		default:
			return receipt, fmt.Errorf("invalid bus receipt state %q", state)
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return receipt, ctx.Err()
		case <-timer.C:
		}
		row, err := b.Get(ctx, id)
		if err != nil {
			return receipt, err
		}
		if !validBusReceipt(row, id) {
			return receipt, errors.New("bus receipt identity mismatch")
		}
		receipt = row
	}
}

func decodeClientJSON(raw []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return d.Decode(dst)
}

func validCommitReceipt(row, body map[string]any) bool {
	raw, err := json.Marshal(row)
	if err != nil {
		return false
	}
	var receipt struct {
		Revision     *int64   `json:"revision"`
		Duplicate    *bool    `json:"duplicate"`
		OperationIDs []string `json:"operation_ids"`
	}
	if json.Unmarshal(raw, &receipt) != nil || receipt.Revision == nil || *receipt.Revision < 0 || receipt.Duplicate == nil || receipt.OperationIDs == nil {
		return false
	}
	actions, ok := body["actions"].([]any)
	if !ok || len(receipt.OperationIDs) != len(actions) {
		return false
	}
	for _, id := range receipt.OperationIDs {
		if !clientIdentifier.MatchString(id) {
			return false
		}
	}
	return true
}
func validOperationAdmission(row map[string]any) bool {
	if _, ok := row["duplicate"].(bool); !ok {
		return false
	}
	switch row["state"] {
	case "accepted", "dispatching", "running", "succeeded", "failed", "cancelled", "unknown":
		return true
	}
	return false
}

// RestoreIntent restores previously persisted request bytes without replacing
// identity or deadlines. Submission remains an explicit caller decision.
func RestoreIntent(method string, payload json.RawMessage) (Intent, error) {
	switch method {
	case "state.commit", "events.commit", "operations.submit":
	default:
		return Intent{}, errors.New("unsupported durable intent method")
	}
	var object map[string]any
	if err := decodeClientJSON(payload, &object); err != nil || object == nil || !json.Valid(payload) {
		return Intent{}, errors.New("invalid retained intent")
	}
	return Intent{method: method, raw: string(payload)}, nil
}

func validBusReceipt(row map[string]any, id string) bool {
	actual, ok := row["call_id"].(string)
	if !ok || actual == "" || (id != "" && id != actual) {
		return false
	}
	if _, ok := row["actual_done"].(bool); !ok {
		return false
	}
	switch row["state"] {
	case "running", "failed", "cancelled", "deadline_exceeded", "unavailable":
		return true
	case "succeeded":
		_, ok := row["result"]
		return ok
	}
	return false
}

// Result decodes a successful receipt without modifying its original evidence.
func (b Bus) Result(ctx context.Context, receipt map[string]any) (any, error) {
	if !validBusReceipt(receipt, "") || receipt["state"] != "succeeded" {
		return nil, errors.New("bus receipt has no successful result")
	}
	return DecodeBusValue(ctx, receipt["result"])
}

// Call submits once, consumes receipts, and decodes the result. Receipt and error
// are returned together on terminal failure. Context cancellation stops waiting;
// explicit Cancel is required to request remote cancellation.
func (b Bus) Call(ctx context.Context, request BusRequest, poll time.Duration) (map[string]any, any, error) {
	if poll <= 0 {
		return nil, nil, errors.New("positive polling interval required")
	}
	ctx, cancel := context.WithDeadline(ctx, request.Deadline)
	defer cancel()
	receipt, err := b.Submit(ctx, request)
	if err != nil {
		return receipt, nil, err
	}
	receipt, err = b.Wait(ctx, receipt, poll)
	if err != nil {
		return receipt, nil, err
	}
	if receipt["state"] != "succeeded" {
		return receipt, nil, errors.New("bus call terminated without success; inspect receipt")
	}
	value, err := b.Result(ctx, receipt)
	return receipt, value, err
}
