package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llm-gateway/internal/config"
)

func postClaude(t *testing.T, client *http.Client, gwURL, model string, stream bool, apiKey string) (int, []byte) {
	t.Helper()
	payload := map[string]any{
		"model": model, "max_tokens": 100,
		"messages": []any{map[string]any{"role": "user", "content": "hello"}},
	}
	if stream {
		payload["stream"] = true
	}
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, gwURL+"/v1/messages", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-version", "2023-06-01")
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("claude do: %v", err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func claudeOKJSON() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		text := ""
		if json.Unmarshal(body, &req) == nil && len(req.Messages) > 0 {
			text = req.Messages[0].Content
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "msg_test123", "type": "message", "role": "assistant",
			"model":       "claude-sonnet-4-5",
			"content":     []any{map[string]any{"type": "text", "text": "echo: " + text}},
			"stop_reason": "end_turn", "stop_sequence": nil,
			"usage": map[string]any{"input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 4},
		})
	}
}

func claudeOKStream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		writeEvt := func(event, data string) {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
			if fl != nil {
				fl.Flush()
			}
		}
		writeEvt("message_start", `{"type":"message_start","message":{"id":"msg_s1","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":10,"output_tokens":0}}}`)
		writeEvt("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		writeEvt("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`)
		writeEvt("content_block_stop", `{"type":"content_block_stop","index":0}`)
		writeEvt("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}`)
		writeEvt("message_stop", `{"type":"message_stop"}`)
	}
}

func TestClaudeMessagesJSON(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-good"] = claudeOKJSON()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}
	status, body := postClaude(t, client, gw.URL, "claude-sonnet-4-5", false, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	if !strings.Contains(string(body), "echo: hello") {
		t.Fatalf("body = %s, want upstream echo proving the request was forwarded as-is", body)
	}
	logs := readAllLogs(t, st)
	if len(logs) != 1 || logs[0].ModelRequested != "claude-sonnet-4-5" || logs[0].PromptTokens != 10 || logs[0].CachedTokens != 4 || logs[0].CompletionTokens != 5 || logs[0].TotalTokens != 15 {
		t.Fatalf("log = %+v, want 1 line 10/4/5/15 tokens", logs)
	}
}

func TestClaudeMessagesStream(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-good"] = claudeOKStream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}
	status, body := postClaude(t, client, gw.URL, "claude-sonnet-4-5", true, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	for _, want := range []string{"event: message_start", "text_delta", "event: message_stop"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("body = %s, want %q", body, want)
		}
	}
	logs := readAllLogs(t, st)
	if len(logs) != 1 || !logs[0].Stream || logs[0].PromptTokens != 10 || logs[0].CompletionTokens != 5 || logs[0].TotalTokens != 15 {
		t.Fatalf("log = %+v, want 1 stream line 10/5/15 tokens", logs)
	}
}

func TestClaudeMessagesFailover(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-bad"] = errJSON(429, `{"type":"error","error":{"type":"rate_limit_error","message":"limited"}}`, nil)
	up.behaviors["tok-good"] = claudeOKJSON()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "bad", APIKey: "tok-bad", Priority: 1, Enabled: true},
		{ID: "good", APIKey: "tok-good", Priority: 2, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}
	status, body := postClaude(t, client, gw.URL, "claude-sonnet-4-5", false, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	if !strings.Contains(string(body), "echo: hello") {
		t.Fatalf("body = %s, want success via good key after failover", body)
	}
	logs := readAllLogs(t, st)
	if len(logs) != 2 || logs[0].KeyID != "bad" || logs[0].Status != 429 || logs[1].KeyID != "good" {
		t.Fatalf("logs = %+v, want bad/429 then good/200", logs)
	}
}

func TestClaudeMessagesAllFail(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-a"] = errJSON(429, `{"type":"error","error":{"type":"rate_limit_error","message":"limited-a"}}`, nil)
	up.behaviors["tok-b"] = errJSON(429, `{"type":"error","error":{"type":"rate_limit_error","message":"limited-b"}}`, nil)
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, _ := newTestGateway(t, srv.URL, []config.Key{
		{ID: "a", APIKey: "tok-a", Priority: 1, Enabled: true},
		{ID: "b", APIKey: "tok-b", Priority: 2, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}
	status, body := postClaude(t, client, gw.URL, "claude-sonnet-4-5", false, "")
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", status)
	}
	if !strings.Contains(string(body), "limited-b") || strings.Contains(string(body), "limited-a") {
		t.Fatalf("body = %s, want the LAST upstream Claude error verbatim", body)
	}
}

func TestClaudeMessagesAuth(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-good"] = claudeOKJSON()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, _ := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true},
	}, nil, "test-secret")
	client := &http.Client{Timeout: 10 * time.Second}

	call := func(key, bearer string) (int, []byte) {
		payload := []byte(`{"model":"claude-sonnet-4-5","max_tokens":100,"messages":[{"role":"user","content":"hello"}]}`)
		req, _ := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("x-api-key", key)
		}
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, out
	}
	if status, _ := call("test-secret", ""); status != http.StatusOK {
		t.Fatalf("x-api-key ok: status = %d, want 200", status)
	}
	if status, body := call("wrong", ""); status != http.StatusUnauthorized || !strings.Contains(string(body), "authentication_error") {
		t.Fatalf("x-api-key wrong: status = %d body = %s, want 401 authentication_error", status, body)
	}
	if status, _ := call("", "test-secret"); status != http.StatusOK {
		t.Fatalf("bearer ok: status = %d, want 200", status)
	}
	if status, _ := call("", ""); status != http.StatusUnauthorized {
		t.Fatalf("no auth: status = %d, want 401", status)
	}
}

func TestClaudeMessagesNoKey(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, _ := newTestGateway(t, srv.URL, []config.Key{
		{ID: "off", APIKey: "tok-off", Priority: 1, Enabled: false},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}
	status, body := postClaude(t, client, gw.URL, "claude-sonnet-4-5", false, "")
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", status)
	}
	if !strings.Contains(string(body), "rate_limit_error") {
		t.Fatalf("body = %s, want gateway error in Anthropic shape", body)
	}
}
