package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var seen int64

func main() {
	http.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&seen, 1)
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		model, _ := body["model"].(string)
		stream, _ := body["stream"].(bool)
		log.Printf("upstream #%d key=%s model=%s stream=%v", n, key, model, stream)

		switch key {
		case "key-429":
			w.Header().Set("Retry-After", "60")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(429)
			fmt.Fprint(w, `{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}`)
			return
		case "key-500":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			fmt.Fprint(w, `{"error":{"message":"internal error","type":"server_error"}}`)
			return
		case "key-quota":
			w.Header().Set("Retry-After", "5")
			w.Header().Set("x-ratelimit-reset-requests", "5s")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(429)
			fmt.Fprint(w, `{"error":{"message":"You exceeded your current quota","type":"insufficient_quota","code":"insufficient_quota"}}`)
			return
		}

		if stream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(200)
			fl := w.(http.Flusher)
			for _, chunk := range []string{
				`{"id":"chatcmpl-mock","object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"},"index":0}]}`,
				`{"id":"chatcmpl-mock","object":"chat.completion.chunk","choices":[{"delta":{"content":" world"},"index":0}]}`,
				`{"id":"chatcmpl-mock","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"prompt_tokens_details":{"cached_tokens":4}}}`,
			} {
				fmt.Fprintf(w, "data: %s\n\n", chunk)
				fl.Flush()
				time.Sleep(20 * time.Millisecond)
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
			fl.Flush()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"id":      "chatcmpl-mock",
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"model":   model,
			"choices": []any{map[string]any{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "Hello from mock upstream, model=" + model,
				},
				"finish_reason": "stop",
			}},
			"usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
				"prompt_tokens_details": map[string]any{"cached_tokens": 4}},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	http.HandleFunc("/v1/messages", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&seen, 1)
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		model, _ := body["model"].(string)
		stream, _ := body["stream"].(bool)
		log.Printf("upstream #%d key=%s model=%s stream=%v path=/v1/messages", n, key, model, stream)

		switch key {
		case "key-429":
			w.Header().Set("Retry-After", "60")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(429)
			fmt.Fprint(w, `{"type":"error","error":{"type":"rate_limit_error","message":"Rate limit reached"}}`)
			return
		case "key-quota":
			w.Header().Set("Retry-After", "5")
			w.Header().Set("x-ratelimit-reset-requests", "5s")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(429)
			fmt.Fprint(w, `{"type":"error","error":{"type":"rate_limit_error","message":"You exceeded your current quota"}}`)
			return
		}

		if stream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(200)
			fl := w.(http.Flusher)
			for _, evt := range []string{
				"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_mock\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"" + model + "\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}",
				"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}",
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}",
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}",
				"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}",
				"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}",
				"event: message_stop\ndata: {\"type\":\"message_stop\"}",
			} {
				fmt.Fprintf(w, "%s\n\n", evt)
				fl.Flush()
				time.Sleep(20 * time.Millisecond)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"id":    "msg_mock",
			"type":  "message",
			"role":  "assistant",
			"model": model,
			"content": []any{map[string]any{
				"type": "text",
				"text": "Hello from mock upstream, model=" + model,
			}},
			"stop_reason":   "end_turn",
			"stop_sequence": nil,
			"usage": map[string]any{
				"input_tokens": 10, "output_tokens": 5,
				"cache_read_input_tokens": 4,
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	log.Println("mock upstream on :18081")
	log.Fatal(http.ListenAndServe(":18081", nil))
}
