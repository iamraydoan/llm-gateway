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
	log.Println("mock upstream on :18081")
	log.Fatal(http.ListenAndServe(":18081", nil))
}
