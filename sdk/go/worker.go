package neomega

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"
)

// Callbacks run with cooperative cancellation. Event runs serially and never
// acknowledges durable delivery automatically. Bus callbacks may run concurrently.
// Activate and Drain are required. Validate may return bus_exports, but Go stream
// providers are not supported by this SDK. Maintenance may be omitted.
type Callbacks struct {
	Validate    func(context.Context, *Peer, map[string]any) (map[string]any, error)
	Activate    func(context.Context, *Peer, map[string]any) error
	Drain       func(context.Context, *Peer, map[string]any) error
	Event       func(context.Context, *Peer, map[string]any) error
	Maintenance func(context.Context, *Peer, map[string]any) (map[string]any, error)
	Bus         func(context.Context, *Peer, map[string]any) (any, error)
	Service     func(context.Context, *Peer, map[string]any) (map[string]any, error)
}

var legacyDeadlineExpired = errors.New("service deadline expired")
var workerCallID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type workerCall struct {
	cancel      context.CancelFunc
	drainCancel context.CancelCauseFunc
	done        chan struct{}
	bus         bool
	reason      string // protected by Worker.mu
}
type earlyCancellation struct {
	id    string
	until time.Time
}

// Worker owns the peer and all Spawn activities until actual completion. Run is
// single-use. Detached goroutines are outside this ownership; uncooperative
// callbacks require Host process termination, not an artificial successful drain.
type Worker struct {
	peer                                 *Peer
	callbacks                            Callbacks
	mu                                   sync.Mutex
	started, active, accepting, spawning bool
	ctx                                  context.Context
	cancel                               context.CancelFunc
	session                              map[string]any
	candidates                           map[string]bool
	exports                              map[string]map[string]any
	calls                                map[string]*workerCall
	early                                []earlyCancellation
	background                           map[*workerCall]struct{}
	failure                              error
	failureCh                            chan struct{}
	legacy                               sync.Mutex
}

func NewWorker(peer *Peer, callbacks Callbacks) *Worker {
	return &Worker{peer: peer, callbacks: callbacks, candidates: make(map[string]bool), calls: make(map[string]*workerCall), background: make(map[*workerCall]struct{}), failureCh: make(chan struct{})}
}

// Spawn starts at most 32 owned activities during activation or active delivery.
// A non-cancellation error fails Run even if another caller observes that error.
func (w *Worker) Spawn(fn func(context.Context) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if fn == nil || !w.spawning || w.failure != nil {
		return errors.New("worker background admission closed")
	}
	if len(w.background) >= 32 {
		return errors.New("worker background limit reached")
	}
	ctx, cancelCause := context.WithCancelCause(w.ctx)
	cancel := func() { cancelCause(context.Canceled) }
	record := &workerCall{cancel: cancel, drainCancel: cancelCause, done: make(chan struct{})}
	w.background[record] = struct{}{}
	go func() {
		err := fn(ctx)
		if err != nil && !(ctx.Err() != nil && errors.Is(err, context.Canceled)) {
			w.fail(err)
		}
		cancel()
		w.mu.Lock()
		delete(w.background, record)
		close(record.done)
		w.mu.Unlock()
	}()
	return nil
}

func (w *Worker) fail(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	if w.failure == nil {
		w.failure = err
		w.spawning = false
		w.accepting = false
		close(w.failureCh)
	}
	w.mu.Unlock()
}

// Run handshakes if needed, serves lifecycle and events, and closes and joins
// every owned activity on cancellation or failure. It never replays failed calls.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return errors.New("worker is single use")
	}
	w.started = true
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.mu.Unlock()
	if w.peer == nil {
		w.cancel()
		return errors.New("worker requires peer")
	}
	defer w.cancel()
	if w.callbacks.Validate == nil || w.callbacks.Activate == nil || w.callbacks.Drain == nil || w.callbacks.Event == nil {
		_ = w.peer.Close()
		return errors.New("worker requires lifecycle and event callbacks")
	}
	session := w.peer.Session()
	var err error
	if session == nil {
		session, err = w.peer.Handshake(w.ctx)
	}
	if err != nil {
		_ = w.peer.Close()
		return err
	}
	w.session = session
	loops := sync.WaitGroup{}
	start := func(fn func() error) { loops.Add(1); go func() { defer loops.Done(); w.fail(fn()) }() }
	start(w.events)
	start(w.controls)
	start(func() error {
		_, err := w.peer.NextNotification(w.ctx)
		if err != nil {
			return err
		}
		return errors.New("unsupported worker notification")
	})
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-w.failureCh:
		w.mu.Lock()
		err = w.failure
		w.mu.Unlock()
	case <-w.peer.Done():
		err = w.peer.Err()
		if err == nil {
			err = errors.New("worker peer closed")
		}
	}
	// Close IPC before cancelling callback contexts: cleanup cannot emit replies.
	w.mu.Lock()
	w.spawning = false
	w.accepting = false
	w.mu.Unlock()
	_ = w.peer.Close()
	w.cancel()
	w.cancelOwned(true)
	loops.Wait()
	w.joinOwned()
	w.mu.Lock()
	if w.failure != nil && !errors.Is(w.failure, context.Canceled) {
		err = w.failure
	}
	w.mu.Unlock()
	return err
}

func (w *Worker) events() error {
	for {
		frame, err := w.peer.NextEvent(w.ctx)
		if err != nil {
			return err
		}
		w.mu.Lock()
		active := w.active
		w.mu.Unlock()
		if !active {
			return errors.New("event before activation")
		}
		if err = w.callbacks.Event(w.ctx, w.peer, frame.Payload); err != nil {
			return err
		}
		w.peer.EventDone()
	}
}
func (w *Worker) controls() error {
	for {
		frame, err := w.peer.NextRequest(w.ctx)
		if err != nil {
			return err
		}
		if err = w.control(frame); err != nil {
			return err
		}
	}
}

func workerInteger(value any) (int64, bool) {
	switch value := value.(type) {
	case json.Number:
		n, e := value.Int64()
		return n, e == nil
	case int:
		return int64(value), true
	case int64:
		return value, true
	case float64:
		if value >= 0 && value < 9223372036854775808 && value == float64(int64(value)) {
			return int64(value), true
		}
	}
	return 0, false
}
func (w *Worker) identity(body map[string]any, numbers ...string) (map[string]any, error) {
	generation, ok := workerInteger(body["generation"])
	expected, valid := workerInteger(w.session["generation"])
	if !ok || !valid || generation != expected || body["installation_id"] != w.session["installation_id"] {
		return nil, errors.New("invalid lifecycle identity")
	}
	reply := map[string]any{"installation_id": w.session["installation_id"], "generation": generation}
	for _, key := range numbers {
		n, ok := workerInteger(body[key])
		if !ok || n < 0 {
			return nil, errors.New("invalid lifecycle version")
		}
		reply[key] = n
	}
	return reply, nil
}
func cloneBody(body map[string]any) map[string]any {
	result := make(map[string]any, len(body))
	for k, v := range body {
		result[k] = v
	}
	return result
}
func memberKey(body map[string]any) string {
	member, ok := body["member"].(string)
	major, valid := workerInteger(body["major"])
	if !ok || !valid {
		return ""
	}
	return fmt.Sprintf("%s/%d", member, major)
}
func (w *Worker) control(frame Frame) error {
	body := frame.Payload
	w.mu.Lock()
	active := w.active
	w.mu.Unlock()
	switch frame.Method {
	case "bus.invoke", "services.online.invoke":
		return w.startService(frame, frame.Method == "bus.invoke")
	case "bus.cancel":
		id, ok := body["call_id"].(string)
		if !ok || len(id) < 1 || len(id) > 256 {
			return w.peer.Reject(frame.ID, "invalid_call", false)
		}
		w.mu.Lock()
		record := w.calls[id]
		cancelled := record != nil
		if record != nil {
			if record.reason == "" {
				record.reason = "cancelled"
				record.cancel()
			}
		} else {
			now := time.Now()
			kept := w.early[:0]
			for _, item := range w.early {
				if item.until.After(now) && item.id != id {
					kept = append(kept, item)
				}
			}
			w.early = append(kept, earlyCancellation{id, now.Add(30 * time.Second)})
			if len(w.early) > 32 {
				w.early = w.early[len(w.early)-32:]
			}
		}
		w.mu.Unlock()
		return w.peer.Respond(frame.ID, map[string]any{"call_id": id, "cancelled": cancelled})
	case "lifecycle.maintenance":
		if !active {
			return errors.New("unsupported lifecycle transition")
		}
		reply, err := w.identity(body)
		if err != nil {
			return err
		}
		operation, _ := body["operation"].(string)
		token, _ := body["token"].(string)
		if (operation != "seal" && operation != "release") || len(token) < 1 || len(token) > 256 {
			return w.peer.Reject(frame.ID, "invalid_maintenance", false)
		}
		request := cloneBody(reply)
		request["operation"] = operation
		request["token"] = token
		result := map[string]any{"status": "unsupported", "token": token}
		if w.callbacks.Maintenance != nil {
			result, err = w.callbacks.Maintenance(w.ctx, w.peer, request)
		}
		status, _ := result["status"].(string)
		if err != nil || result["token"] != token || !(status == "unsupported" || status == "busy" || operation == "seal" && status == "sealed" || operation == "release" && status == "released") {
			return w.peer.Reject(frame.ID, "maintenance_failed", false)
		}
		reply["status"] = status
		reply["token"] = token
		return w.peer.Respond(frame.ID, reply)
	case "lifecycle.validate":
		if active {
			return errors.New("unsupported lifecycle transition")
		}
		reply, err := w.identity(body, "revision")
		if err != nil {
			return err
		}
		validation, err := w.callbacks.Validate(w.ctx, w.peer, cloneBody(reply))
		if err != nil {
			return err
		}
		candidates := make(map[string]bool)
		exported := make(map[string]map[string]any)
		if validation != nil {
			exports, ok := validation["bus_exports"].([]any)
			if !ok {
				return errors.New("validation bus_exports must be []any")
			}
			if streams, exists := validation["bus_streams"]; exists && streams != false {
				return errors.New("Go stream providers are unsupported")
			}
			// Freeze descriptors so subsequent author mutation cannot change dispatch.
			encoded, err := json.Marshal(exports)
			if err != nil {
				return err
			}
			decoded, err := DecodeJSON(encoded)
			if err != nil {
				return err
			}
			exports = decoded.([]any)
			reply["bus_exports"] = exports
			for _, entry := range exports {
				item, ok := entry.(map[string]any)
				if !ok {
					return errors.New("invalid bus export")
				}
				key := memberKey(item)
				if key == "" || exported[key] != nil {
					return errors.New("invalid or duplicate bus export")
				}
				exported[key] = item
				if item["kind"] == "read" && item["candidate_readable"] == true {
					key := memberKey(item)
					if key == "" {
						return errors.New("invalid candidate read export")
					}
					candidates[key] = true
				}
			}
		}
		w.mu.Lock()
		w.candidates = candidates
		w.exports = exported
		w.accepting = true
		w.mu.Unlock()
		reply["validated"] = true
		return w.peer.Respond(frame.ID, reply)
	case "lifecycle.activate":
		if active {
			return errors.New("unsupported lifecycle transition")
		}
		reply, err := w.identity(body, "revision", "state_version")
		if err != nil {
			return err
		}
		w.mu.Lock()
		w.spawning = true
		w.mu.Unlock()
		if err = w.callbacks.Activate(w.ctx, w.peer, cloneBody(reply)); err != nil {
			return err
		}
		w.mu.Lock()
		w.active = true
		w.accepting = true
		failure := w.failure
		w.mu.Unlock()
		if failure != nil {
			return failure
		}
		reply["activated"] = true
		return w.peer.Respond(frame.ID, reply)
	case "lifecycle.drain":
		if !active {
			return errors.New("unsupported lifecycle transition")
		}
		reply, err := w.identity(body)
		if err != nil {
			return err
		}
		if err = w.peer.WaitEvents(w.ctx); err != nil {
			return err
		}
		w.mu.Lock()
		w.accepting = false
		w.mu.Unlock()
		// Legacy services finish before owned background cancellation, so a
		// legacy callback can still await or spawn its owned cleanup child.
		w.mu.Lock()
		legacyDone := make([]chan struct{}, 0)
		for _, record := range w.calls {
			if record.bus {
				if record.reason == "" {
					record.reason = "cancelled"
					w.peer.cancelForDrain(record.drainCancel)
				}
			} else {
				legacyDone = append(legacyDone, record.done)
			}
		}
		w.mu.Unlock()
		for _, done := range legacyDone {
			<-done
		}
		w.cancelOwned(false)
		w.joinOwned()
		w.mu.Lock()
		failure := w.failure
		w.mu.Unlock()
		if failure != nil {
			return failure
		}
		if err = w.callbacks.Drain(w.ctx, w.peer, cloneBody(reply)); err != nil {
			return err
		}
		w.mu.Lock()
		w.spawning = true
		w.accepting = true
		failure = w.failure
		w.mu.Unlock()
		if failure != nil {
			return failure
		}
		reply["drained"] = true
		return w.peer.Respond(frame.ID, reply)
	case "bus.streams.start":
		return w.peer.Reject(frame.ID, "provider_unavailable", false)
	default:
		return errors.New("unsupported lifecycle transition")
	}
}

func (w *Worker) startService(frame Frame, bus bool) error {
	body := frame.Payload
	callbackPresent := w.callbacks.Service != nil
	if bus {
		callbackPresent = w.callbacks.Bus != nil
	}
	w.mu.Lock()
	candidate := bus && body["kind"] == "read" && w.candidates[memberKey(body)]
	if !(w.active || candidate) || !w.accepting || !callbackPresent {
		w.mu.Unlock()
		return w.peer.Reject(frame.ID, "provider_unavailable", false)
	}
	id := "legacy:" + frame.ID
	if bus {
		id, _ = body["call_id"].(string)
	}
	if len(id) < 1 || len(id) > 256 || w.calls[id] != nil {
		w.mu.Unlock()
		return w.peer.Reject(frame.ID, "invalid_call", false)
	}
	for i, item := range w.early {
		if item.id == id {
			w.early = append(w.early[:i], w.early[i+1:]...)
			if bus && item.until.After(time.Now()) {
				w.mu.Unlock()
				return w.peer.Reject(frame.ID, "cancelled", false)
			}
			break
		}
	}
	if len(w.calls) >= 16 {
		w.mu.Unlock()
		return w.peer.Reject(frame.ID, "provider_busy", false)
	}
	ctx, cancel := context.WithCancel(w.ctx)
	if bus {
		if !w.validBusContext(body) {
			cancel()
			w.mu.Unlock()
			return w.peer.Reject(frame.ID, "invalid_call", false)
		}
		callContext, _ := body["context"].(map[string]any)
		deadline, valid := workerInteger(callContext["deadline_unix_ms"])
		if !valid {
			cancel()
			w.mu.Unlock()
			return w.peer.Reject(frame.ID, "invalid_call", false)
		}
		until := time.UnixMilli(deadline)
		maximum := time.Now().Add(30 * time.Second)
		if until.After(maximum) {
			until = maximum
		}
		if !until.After(time.Now()) {
			cancel()
			w.mu.Unlock()
			return w.peer.Reject(frame.ID, "deadline_exceeded", false)
		}
		cancel()
		ctx, cancel = context.WithDeadline(w.ctx, until)
	}
	parentCancel := cancel
	ctx, cancelCause := context.WithCancelCause(ctx)
	cancel = func() { cancelCause(context.Canceled); parentCancel() }
	record := &workerCall{cancel: cancel, drainCancel: cancelCause, done: make(chan struct{}), bus: bus}
	w.calls[id] = record
	w.mu.Unlock()
	go func() {
		defer func() { cancel(); w.mu.Lock(); delete(w.calls, id); close(record.done); w.mu.Unlock() }()
		var result any
		var err error
		if !bus {
			w.legacy.Lock()
			defer w.legacy.Unlock()
		}
		if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			if bus {
				decoded, decodeErr := DecodeBusValue(ctx, body["arguments"])
				if decodeErr != nil {
					err = decodeErr
				} else {
					input := cloneBody(body)
					input["arguments"] = decoded
					result, err = w.callbacks.Bus(ctx, w.peer, input)
					if err == nil {
						deadline, _ := ctx.Deadline()
						encoded, encodeErr := EncodeBusValue(ctx, w.peer, result, deadline)
						if encodeErr != nil {
							err = encodeErr
						} else {
							result = encoded
						}
					}
				}
			} else {
				err = validLegacyContext(body)
				if err == nil {
					result, err = w.callbacks.Service(ctx, w.peer, body)
				}
			}
		}
		w.mu.Lock()
		reason := record.reason
		w.mu.Unlock()
		if reason == "" {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				reason = "deadline_exceeded"
			} else if ctx.Err() != nil {
				reason = "cancelled"
			}
		}
		if reason != "" {
			err = w.peer.Reject(frame.ID, reason, false)
		} else if err != nil {
			if !bus && !errors.Is(err, legacyDeadlineExpired) {
				w.fail(err)
				return
			}
			code := "provider_failed"
			if errors.Is(err, legacyDeadlineExpired) {
				code = "service_rejected"
			}
			err = w.peer.Reject(frame.ID, code, false)
		} else {
			err = w.peer.Respond(frame.ID, result)
		}
		if err != nil {
			w.fail(err)
		}
	}()
	return nil
}

func (w *Worker) cancelOwned(all bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.accepting = false
	w.spawning = false
	for _, record := range w.calls {
		if record.bus || all {
			if record.reason == "" {
				record.reason = "cancelled"
				if all {
					record.cancel()
				} else {
					w.peer.cancelForDrain(record.drainCancel)
				}
			}
		}
	}
	for record := range w.background {
		if all {
			record.cancel()
		} else {
			w.peer.cancelForDrain(record.drainCancel)
		}
	}
}
func (w *Worker) joinOwned() {
	w.mu.Lock()
	done := make([]chan struct{}, 0, len(w.calls)+len(w.background))
	for _, record := range w.calls {
		done = append(done, record.done)
	}
	for record := range w.background {
		done = append(done, record.done)
	}
	w.mu.Unlock()
	for _, ch := range done {
		<-ch
	}
}

// validBusContext checks Host metadata against the frozen registration, before
// business data is decoded or the author's callback is entered. w.mu is held.
func (w *Worker) validBusContext(body map[string]any) bool {
	raw, ok := body["context"].(map[string]any)
	if !ok {
		return false
	}
	descriptor := w.exports[memberKey(body)]
	if descriptor == nil {
		return false
	}
	contract, _ := descriptor["contract"].(map[string]any)
	kind, kindOK := body["kind"].(string)
	digest, digestOK := contract["schema_digest"].(string)
	if !kindOK || (kind != "read" && kind != "service") || !digestOK || digest == "" {
		return false
	}
	if raw["call_id"] != body["call_id"] || raw["member"] != body["member"] || raw["member"] != descriptor["member"] || raw["kind"] != body["kind"] || raw["kind"] != descriptor["kind"] || raw["schema_digest"] != contract["schema_digest"] {
		return false
	}
	major, ok := workerInteger(raw["major"])
	bodyMajor, bok := workerInteger(body["major"])
	if !ok || !bok || major != bodyMajor {
		return false
	}
	for _, key := range []string{"caller", "provider"} {
		identity, ok := raw[key].(map[string]any)
		if !ok {
			return false
		}
		installation, ok := identity["installation"].(string)
		if !ok || len(installation) < 1 || len(installation) > 256 {
			return false
		}
		generation, ok := workerInteger(identity["generation"])
		if !ok || generation < 1 {
			return false
		}
		if key == "provider" {
			expected, _ := workerInteger(w.session["generation"])
			if installation != w.session["installation_id"] || generation != expected {
				return false
			}
		}
	}
	accepted, aok := workerInteger(raw["accepted_at_unix_ms"])
	deadline, dok := workerInteger(raw["deadline_unix_ms"])
	return aok && dok && accepted >= 0 && deadline > accepted && deadline-accepted <= 30000
}
func validLegacyContext(body map[string]any) error {
	value, exists := body["call_context"]
	if !exists {
		return nil
	}
	raw, ok := value.(map[string]any)
	if !ok || len(raw) != 5 {
		return errors.New("invalid Host service call context")
	}
	installation, ok := raw["installation_id"].(string)
	generation, gok := workerInteger(raw["generation"])
	id, iok := raw["call_id"].(string)
	acceptedText, aok := raw["accepted_at"].(string)
	deadlineText, dok := raw["deadline"].(string)
	if !ok || len(installation) < 1 || len(installation) > 256 || !gok || generation < 1 || !iok || !workerCallID.MatchString(id) || id != body["call_id"] || !aok || !dok {
		return errors.New("invalid Host service call context")
	}
	accepted, aerr := time.Parse(time.RFC3339Nano, acceptedText)
	deadline, derr := time.Parse(time.RFC3339Nano, deadlineText)
	if aerr != nil || derr != nil || !deadline.After(accepted) || deadline.Sub(accepted) > 30*time.Second {
		return errors.New("invalid Host service call context")
	}
	if !deadline.After(time.Now()) {
		return legacyDeadlineExpired
	}
	return nil
}
