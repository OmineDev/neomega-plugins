package neomega

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var bulkIdentity = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var bulkDigest = regexp.MustCompile(`^(sha256:)?[0-9a-f]{64}$`)

type bulkDescriptor struct {
	url, token, digest string
	length             int64
	ref                map[string]any
}

func parseBulk(value map[string]any, collection string) (bulkDescriptor, error) {
	bad := errors.New("invalid bulk descriptor")
	raw, err := EncodeJSON(value)
	if err != nil {
		return bulkDescriptor{}, bad
	}
	normalized, err := DecodeJSON(raw)
	if err != nil {
		return bulkDescriptor{}, bad
	}
	var objectOK bool
	value, objectOK = normalized.(map[string]any)
	if !objectOK {
		return bulkDescriptor{}, bad
	}
	ref, ok := value["ref"].(map[string]any)
	if !ok {
		return bulkDescriptor{}, bad
	}
	id, ok := ref["id"].(string)
	if !ok || !bulkIdentity.MatchString(id) {
		return bulkDescriptor{}, bad
	}
	digest, ok := ref["digest"].(string)
	if !ok || !bulkDigest.MatchString(digest) {
		return bulkDescriptor{}, bad
	}
	number, ok := ref["length"].(json.Number)
	if !ok {
		return bulkDescriptor{}, bad
	}
	length, err := number.Int64()
	if err != nil || length < 0 || length > MaxValueBytes {
		return bulkDescriptor{}, bad
	}
	format, ok := ref["format"].(string)
	if !ok || format == "" {
		return bulkDescriptor{}, bad
	}
	address, ok := value["url"].(string)
	if !ok {
		return bulkDescriptor{}, bad
	}
	for _, c := range address {
		if c <= 32 || c >= 127 {
			return bulkDescriptor{}, bad
		}
	}
	parsed, err := url.Parse(address)
	if err != nil {
		return bulkDescriptor{}, bad
	}
	ip := net.ParseIP(parsed.Hostname())
	port, e := strconv.Atoi(parsed.Port())
	if parsed.Scheme != "http" || ip == nil || !ip.IsLoopback() || e != nil || port < 1 || port > 65535 || parsed.User != nil || strings.ContainsAny(address, "?#") || parsed.EscapedPath() != "/bulk/v1/"+collection+"/"+id {
		return bulkDescriptor{}, bad
	}
	token, ok := value["token"].(string)
	if !ok || len(token) < 1 || len(token) > 4096 {
		return bulkDescriptor{}, bad
	}
	for _, c := range token {
		if c < 33 || c > 126 {
			return bulkDescriptor{}, bad
		}
	}
	return bulkDescriptor{address, token, strings.TrimPrefix(digest, "sha256:"), length, ref}, nil
}
func bulkClient() (*http.Client, *http.Transport) {
	// Fresh connections prevent transport retries; no proxy or redirect receives tokens.
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, DisableCompression: true, MaxResponseHeaderBytes: 32 << 10}
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, transport
}
func releaseBulk(d bulkDescriptor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, tr := bulkClient()
	defer tr.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, d.url, nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+d.token)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
}
func bulkFraming(resp *http.Response, length int64) bool {
	lengths := resp.Header.Values("Content-Length")
	encoding := resp.Header.Get("Content-Encoding")
	return len(lengths) == 1 && lengths[0] == strconv.FormatInt(length, 10) && resp.ContentLength == length && len(resp.TransferEncoding) == 0 && (encoding == "" || encoding == "identity")
}

// UploadBulk transfers at most 16 MiB. Success retains the grant for Host
// consumption; failure releases it without retrying the upload.
func UploadBulk(ctx context.Context, descriptor map[string]any, data []byte) (map[string]any, error) {
	d, err := parseBulk(descriptor, "uploads")
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	success := false
	defer func() {
		if !success {
			releaseBulk(d)
		}
	}()
	data = append([]byte(nil), data...)
	digest := sha256.Sum256(data)
	if int64(len(data)) != d.length || hex.EncodeToString(digest[:]) != d.digest {
		return nil, errors.New("bulk source length or digest mismatch")
	}
	client, tr := bulkClient()
	defer tr.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, d.url, bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("invalid bulk request")
	}
	req.Header.Set("Authorization", "Bearer "+d.token)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept-Encoding", "identity")
	req.ContentLength = d.length
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("bulk transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("bulk HTTP status %d", resp.StatusCode)
	}
	if resp.ContentLength < 0 || resp.ContentLength > 4096 || !bulkFraming(resp, resp.ContentLength) {
		return nil, errors.New("invalid bulk response framing")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil || int64(len(body)) != resp.ContentLength {
		return nil, errors.New("truncated bulk response")
	}
	value, err := DecodeJSON(body)
	if err != nil {
		return nil, errors.New("invalid bulk response JSON")
	}
	actual, _ := EncodeJSON(value)
	expected, _ := EncodeJSON(d.ref)
	if !bytes.Equal(actual, expected) {
		return nil, errors.New("bulk response reference mismatch")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	success = true
	return d.ref, nil
}

// DownloadBulk returns bytes only after length and SHA-256 verification. The
// authenticated loopback grant is released after either success or failure.
func DownloadBulk(ctx context.Context, descriptor map[string]any) ([]byte, error) {
	d, err := parseBulk(descriptor, "objects")
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	defer releaseBulk(d)
	client, tr := bulkClient()
	defer tr.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.url, nil)
	if err != nil {
		return nil, errors.New("invalid bulk request")
	}
	req.Header.Set("Authorization", "Bearer "+d.token)
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("bulk transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bulk HTTP status %d", resp.StatusCode)
	}
	if !bulkFraming(resp, d.length) {
		return nil, errors.New("invalid bulk response framing")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, d.length+1))
	if err != nil || int64(len(data)) != d.length {
		return nil, errors.New("truncated bulk response")
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != d.digest {
		return nil, errors.New("bulk digest mismatch")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

// EncodeBusValue preserves tagged values, using bulk for values above 32 KiB.
func EncodeBusValue(ctx context.Context, peer Caller, value any, deadline time.Time) (any, error) {
	data, err := EncodeJSON(value)
	if err != nil {
		return nil, err
	}
	if len(data) <= 32<<10 {
		return DecodeJSON(data)
	}
	digest := sha256.Sum256(data)
	descriptor, err := peer.Call(ctx, "bus.blob.upload", map[string]any{"length": len(data), "digest": hex.EncodeToString(digest[:]), "format": "bus-value-v1", "deadline": deadline.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return nil, err
	}
	ref, err := UploadBulk(ctx, descriptor, data)
	if err != nil {
		return nil, err
	}
	return map[string]any{"bulk": ref}, nil
}
func DecodeBusValue(ctx context.Context, value any) (any, error) {
	wrapper, ok := value.(map[string]any)
	if !ok || len(wrapper) != 1 {
		return value, nil
	}
	descriptor, exists := wrapper["bulk"]
	if !exists {
		return value, nil
	}
	d, ok := descriptor.(map[string]any)
	if !ok {
		return nil, errors.New("invalid bulk value")
	}
	data, err := DownloadBulk(ctx, d)
	if err != nil {
		return nil, err
	}
	return DecodeJSON(data)
}
