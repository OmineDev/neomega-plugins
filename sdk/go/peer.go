package neomega

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxFrameBytes = 1 << 20

var ErrClosed = errors.New("IPC closed")

var normalDrain = errors.New("worker normal drain")

// IPCRejected is a definite Host rejection. Retryable is metadata, never a
// directive to retry an admitted mutation automatically.
type IPCRejected struct {
	Code, RequestID string
	Retryable       bool
}

func (e *IPCRejected) Error() string { return "host rejected IPC request: " + e.Code }

type Frame struct {
	Type    string         `json:"type"`
	ID      string         `json:"id,omitempty"`
	Method  string         `json:"method,omitempty"`
	Payload map[string]any `json:"payload"`
}
type response struct {
	result map[string]any
	err    error
}
type pendingCall struct {
	ch       chan response
	method   string
	answered bool
}

// Peer owns both streams. Their Close methods must unblock outstanding Read and
// Write calls. Close and Done join both I/O goroutines; they do not prove that
// any remote mutation has finished.
type Peer struct {
	reader                          io.ReadCloser
	writer                          io.WriteCloser
	mu                              sync.Mutex
	pending                         map[string]*pendingCall
	inbound                         map[string]bool
	sequence                        uint64
	hello                           string
	handshaking                     bool
	session                         map[string]any
	limit, pendingLimit             int
	err                             error
	stop                            chan struct{}
	done                            chan struct{}
	once                            sync.Once
	wg                              sync.WaitGroup
	out                             chan []byte
	requests, events, notifications chan Frame
	eventIn, eventOut               uint64
	eventChanged                    chan struct{}
}

func NewPeer(reader io.ReadCloser, writer io.WriteCloser) *Peer {
	p := &Peer{reader: reader, writer: writer, pending: map[string]*pendingCall{}, inbound: map[string]bool{}, limit: maxFrameBytes, pendingLimit: 128, stop: make(chan struct{}), done: make(chan struct{}), out: make(chan []byte, 128), requests: make(chan Frame, 32), events: make(chan Frame, 32), notifications: make(chan Frame, 32), eventChanged: make(chan struct{})}
	p.wg.Add(2)
	go p.readLoop()
	go p.writeLoop()
	go func() { p.wg.Wait(); close(p.done) }()
	return p
}
func (p *Peer) fail(err error) {
	p.once.Do(func() {
		p.mu.Lock()
		p.err = err
		close(p.stop)
		p.mu.Unlock()
		_ = p.reader.Close()
		_ = p.writer.Close()
	})
}
func (p *Peer) Err() error            { p.mu.Lock(); defer p.mu.Unlock(); return p.err }
func (p *Peer) Done() <-chan struct{} { return p.done }
func (p *Peer) Close() error {
	p.fail(ErrClosed)
	<-p.done
	err := p.Err()
	if errors.Is(err, ErrClosed) {
		return nil
	}
	return err
}
func (p *Peer) Session() map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.session == nil {
		return nil
	}
	data, _ := json.Marshal(p.session)
	v, _ := DecodeJSON(data)
	return v.(map[string]any)
}

// Open takes the managed transport from the environment. Socket authentication
// and hello share one ten-second budget; no connection is retried.
func Open(ctx context.Context) (*Peer, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var p *Peer
	transport := os.Getenv("NEOMEGA_RUNTIME_TRANSPORT")
	if transport == "" || transport == "stdio" {
		p = NewPeer(os.Stdin, os.Stdout)
	} else {
		endpoint := os.Getenv("NEOMEGA_RUNTIME_ENDPOINT")
		token := os.Getenv("NEOMEGA_RUNTIME_TOKEN")
		if len(token) != 64 || strings.Trim(token, "0123456789abcdef") != "" {
			return nil, errors.New("invalid socket authentication token")
		}
		network := "unix"
		switch transport {
		case "unix":
			if !strings.HasPrefix(endpoint, "/") {
				return nil, errors.New("unix endpoint must be absolute")
			}
		case "tcp-loopback":
			network = "tcp"
			host, _, err := net.SplitHostPort(endpoint)
			if err != nil {
				return nil, err
			}
			ip := net.ParseIP(host)
			if ip == nil || !ip.IsLoopback() {
				return nil, errors.New("TCP endpoint must be a loopback IP")
			}
		default:
			return nil, errors.New("unsupported managed transport")
		}
		conn, err := (&net.Dialer{}).DialContext(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		deadline, _ := ctx.Deadline()
		_ = conn.SetDeadline(deadline)
		stopCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
		auth, _ := json.Marshal(map[string]string{"token": token})
		auth = append(auth, '\n')
		_, err = io.Copy(conn, strings.NewReader(string(auth)))
		if err != nil {
			stopCancel()
			_ = conn.Close()
			return nil, err
		}
		p = NewPeer(conn, conn)
		_, err = p.Handshake(ctx)
		stopCancel()
		if err != nil {
			_ = p.Close()
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			_ = p.Close()
			return nil, err
		}
		_ = conn.SetDeadline(time.Time{})
		return p, nil
	}
	if _, err := p.Handshake(ctx); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}
func (p *Peer) Handshake(ctx context.Context) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p.mu.Lock()
	if p.sequence != 0 || p.handshaking || p.session != nil {
		p.mu.Unlock()
		return nil, errors.New("hello must be the first operation")
	}
	p.handshaking = true
	p.mu.Unlock()
	result, err := p.call(ctx, "hello", map[string]any{"protocol_major": 1, "protocol_minor": 0}, true)
	p.mu.Lock()
	p.handshaking = false
	p.mu.Unlock()
	if err != nil {
		p.fail(errors.New("IPC handshake failed"))
	}
	return result, err
}
func (p *Peer) Call(ctx context.Context, method string, payload any) (map[string]any, error) {
	return p.call(ctx, method, payload, false)
}
func (p *Peer) call(ctx context.Context, method string, payload any, hello bool) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if method == "" {
		return nil, errors.New("invalid IPC method")
	}
	raw, err := EncodeJSON(payload)
	if err != nil {
		return nil, err
	}
	decoded, err := DecodeJSON(raw)
	if err != nil {
		return nil, err
	}
	body, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("IPC payload must be an object")
	}
	p.mu.Lock()
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if p.err != nil {
		err = p.err
		p.mu.Unlock()
		return nil, err
	}
	if p.handshaking && !hello {
		p.mu.Unlock()
		return nil, errors.New("hello negotiation in progress")
	}
	if len(p.pending) >= p.pendingLimit {
		p.mu.Unlock()
		return nil, errors.New("too many pending IPC requests")
	}
	p.sequence++
	id := "w" + strconv.FormatUint(p.sequence, 10)
	wire, err := p.encodeLocked(Frame{Type: "request", ID: id, Method: method, Payload: body})
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	call := &pendingCall{ch: make(chan response, 1), method: method}
	p.pending[id] = call
	if hello {
		p.hello = id
	}
	select {
	case p.out <- wire:
	default:
		delete(p.pending, id)
		p.mu.Unlock()
		p.fail(errors.New("IPC writer queue overflow"))
		return nil, p.Err()
	}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.pending, id); p.mu.Unlock() }()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	// Preserve the caller's original deadline even if normal Drain cancels its
	// context. A later timeout is still an uncertain admitted request.
	if deadline, ok := ctx.Deadline(); ok {
		timer.Stop()
		timer.Reset(time.Until(deadline))
	}
	select {
	case reply := <-call.ch:
		return reply.result, reply.err
	case <-p.stop:
		return nil, p.Err()
	case <-timer.C:
		p.fail(errors.New("IPC request timed out after admission"))
		return nil, context.DeadlineExceeded
	case <-ctx.Done():
		protected := !hello && context.Cause(ctx) == normalDrain
		switch method {
		case "players.observe.open", "players.events.open", "packets.observe.open":
			protected = false
		}
		if protected && errors.Is(ctx.Err(), context.Canceled) {
			select {
			case <-call.ch:
				return nil, ctx.Err()
			case <-p.stop:
				return nil, p.Err()
			case <-timer.C:
				p.fail(errors.New("IPC request timed out during drain"))
				return nil, context.DeadlineExceeded
			}
		}
		p.fail(errors.New("IPC request interrupted after admission"))
		return nil, ctx.Err()
	}
}
func (p *Peer) encodeLocked(frame Frame) ([]byte, error) {
	wire, err := EncodeJSON(frame)
	if err != nil {
		return nil, err
	}
	if len(wire) > p.limit {
		return nil, errors.New("IPC frame exceeds limit")
	}
	return append(wire, '\n'), nil
}
func (p *Peer) Respond(id string, payload any) error {
	return p.reply(id, map[string]any{"result": payload})
}
func (p *Peer) Reject(id, code string, retryable bool) error {
	if code == "" {
		return errors.New("empty rejection code")
	}
	return p.reply(id, map[string]any{"error": map[string]any{"code": code, "message": "provider rejected the business request", "request_id": id, "retryable": retryable}})
}
func (p *Peer) reply(id string, payload map[string]any) error {
	p.mu.Lock()
	if p.err != nil {
		err := p.err
		p.mu.Unlock()
		return err
	}
	if !p.inbound[id] {
		p.mu.Unlock()
		return errors.New("unknown host request")
	}
	wire, err := p.encodeLocked(Frame{Type: "response", ID: id, Payload: payload})
	if err != nil {
		p.mu.Unlock()
		return err
	}
	select {
	case p.out <- wire:
		delete(p.inbound, id)
		p.mu.Unlock()
		return nil
	default:
		p.mu.Unlock()
		p.fail(errors.New("IPC writer queue overflow"))
		return p.Err()
	}
}
func (p *Peer) take(ctx context.Context, queue <-chan Frame) (Frame, error) {
	if err := p.Err(); err != nil {
		return Frame{}, err
	}
	select {
	case f := <-queue:
		if err := ctx.Err(); err != nil {
			p.fail(errors.New("IPC receive interrupted"))
			return Frame{}, err
		}
		if err := p.Err(); err != nil {
			return Frame{}, err
		}
		return f, nil
	case <-p.stop:
		return Frame{}, p.Err()
	case <-ctx.Done():
		p.fail(errors.New("IPC receive interrupted"))
		return Frame{}, ctx.Err()
	}
}
func (p *Peer) NextRequest(ctx context.Context) (Frame, error) { return p.take(ctx, p.requests) }
func (p *Peer) NextEvent(ctx context.Context) (Frame, error)   { return p.take(ctx, p.events) }
func (p *Peer) NextNotification(ctx context.Context) (Frame, error) {
	return p.take(ctx, p.notifications)
}

// cancelForDrain linearizes owned cancellation with RPC admission. Context
// causes identify exactly the cancelled activity (including derived contexts),
// so an independently cancelled call never acquires Drain protection.
func (p *Peer) cancelForDrain(cancel context.CancelCauseFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cancel(normalDrain)
}

func (p *Peer) EventDone() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.eventOut++
	close(p.eventChanged)
	p.eventChanged = make(chan struct{})
}
func (p *Peer) WaitEvents(ctx context.Context) error {
	p.mu.Lock()
	target := p.eventIn
	for p.eventOut < target {
		changed := p.eventChanged
		p.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-p.stop:
			return p.Err()
		}
		p.mu.Lock()
	}
	p.mu.Unlock()
	return p.Err()
}
func (p *Peer) writeLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.stop:
			return
		case wire := <-p.out:
			for len(wire) > 0 {
				n, err := p.writer.Write(wire)
				if err != nil {
					p.fail(errors.New("IPC write failed"))
					return
				}
				if n <= 0 {
					p.fail(io.ErrShortWrite)
					return
				}
				wire = wire[n:]
			}
		}
	}
}
func (p *Peer) readLoop() {
	defer p.wg.Done()
	reader := bufio.NewReaderSize(p.reader, maxFrameBytes+2)
	for {
		line, err := reader.ReadSlice('\n')
		if err != nil {
			p.fail(errors.New("IPC read/protocol failure"))
			return
		}
		if err = p.accept(line); err != nil {
			p.fail(fmt.Errorf("IPC read/protocol failure: %w", err))
			return
		}
	}
}
func (p *Peer) accept(line []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(line) < 2 || len(line)-1 > p.limit || strings.ContainsRune(string(line), '\r') {
		return errors.New("invalid IPC frame")
	}
	v, err := DecodeJSON(line[:len(line)-1])
	if err != nil {
		return err
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return errors.New("invalid IPC frame")
	}
	kind, ok := obj["type"].(string)
	if !ok {
		return errors.New("invalid IPC type")
	}
	id, method := "", ""
	if value, exists := obj["id"]; exists {
		id, ok = value.(string)
		if !ok {
			return errors.New("invalid IPC id")
		}
	}
	if value, exists := obj["method"]; exists {
		method, ok = value.(string)
		if !ok {
			return errors.New("invalid IPC method")
		}
	}
	body, ok := obj["payload"].(map[string]any)
	if !ok {
		return errors.New("invalid IPC payload")
	}
	f := Frame{Type: kind, ID: id, Method: method, Payload: body}
	switch {
	case kind == "response" && id != "" && method == "":
		call := p.pending[id]
		_, resultPresent := body["result"]
		ev, errorPresent := body["error"]
		if call == nil || call.answered || resultPresent == errorPresent {
			return errors.New("invalid IPC response")
		}
		r := response{}
		if errorPresent {
			e, ok := ev.(map[string]any)
			if !ok {
				return errors.New("invalid IPC error")
			}
			code, c := e["code"].(string)
			message, m := e["message"].(string)
			retry, re := e["retryable"].(bool)
			if !c || code == "" || !m || message == "" || !re || e["request_id"] != id {
				return errors.New("invalid IPC error")
			}
			if d, present := e["details"]; present {
				if _, ok := d.(map[string]any); !ok {
					return errors.New("invalid IPC error details")
				}
			}
			r.err = &IPCRejected{code, id, retry}
		} else {
			r.result, ok = body["result"].(map[string]any)
			if !ok {
				return errors.New("IPC result must be object")
			}
			if id == p.hello {
				limit, pending, err := validateSession(r.result)
				if err != nil {
					return err
				}
				p.limit = limit
				p.pendingLimit = pending
				if len(line)-1 > limit {
					return errors.New("host hello exceeds advertised frame limit")
				}
				snapshot, _ := EncodeJSON(r.result)
				copied, _ := DecodeJSON(snapshot)
				p.session = copied.(map[string]any)
				p.hello = ""
			}
		}
		call.answered = true
		call.ch <- r
		return nil
	case kind == "request" && id != "" && method != "":
		if p.inbound[id] || len(p.inbound) >= 32 {
			return errors.New("duplicate/overflow host request")
		}
		p.inbound[id] = true
		select {
		case p.requests <- f:
			return nil
		default:
			return errors.New("host request queue overflow")
		}
	case kind == "notification" && id == "" && method != "":
		queue := p.notifications
		if method == "event" {
			queue = p.events
		}
		select {
		case queue <- f:
			if method == "event" {
				p.eventIn++
			}
			return nil
		default:
			return errors.New("notification queue overflow")
		}
	default:
		return errors.New("invalid IPC frame")
	}
}
func validateSession(value map[string]any) (int, int, error) {
	integer := func(v any, low, high int64) (int64, bool) {
		n, ok := v.(json.Number)
		if !ok {
			return 0, false
		}
		i, e := n.Int64()
		return i, e == nil && i >= low && i <= high
	}
	text := func(v any) bool { s, ok := v.(string); return ok && len(s) > 0 && len(s) <= 256 }
	bad := errors.New("invalid host hello")
	if _, ok := integer(value["protocol_major"], 1, 1); !ok {
		return 0, 0, bad
	}
	if _, ok := integer(value["protocol_minor"], 0, 0); !ok {
		return 0, 0, bad
	}
	if !text(value["installation_id"]) {
		return 0, 0, bad
	}
	if _, ok := integer(value["generation"], 1, 1<<63-1); !ok {
		return 0, 0, bad
	}
	caps, ok := value["capabilities"].([]any)
	if !ok || len(caps) > 128 {
		return 0, 0, bad
	}
	seen := map[string]bool{}
	for _, v := range caps {
		if !text(v) {
			return 0, 0, bad
		}
		s := v.(string)
		if seen[s] {
			return 0, 0, bad
		}
		seen[s] = true
	}
	limits, ok := value["limits"].(map[string]any)
	if !ok {
		return 0, 0, bad
	}
	frame, ok := integer(limits["max_frame_bytes"], 2, maxFrameBytes)
	if !ok {
		return 0, 0, bad
	}
	pending, ok := integer(limits["inbound_requests"], 1, 32)
	if !ok {
		return 0, 0, bad
	}
	if _, ok = integer(limits["outgoing_requests"], 1, 128); !ok {
		return 0, 0, bad
	}
	if _, ok = integer(limits["queue_frames"], 1, 32); !ok {
		return 0, 0, bad
	}
	return int(frame), int(pending), nil
}
