package main

import (
	"io"
	"log"
	"net/http"
	"strings"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("x-relay-target")
		path := r.Header.Get("x-relay-path")
		if path == "" {
			path = "/"
		}
		if target == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"Missing x-relay-target header"}`))
			return
		}
		targetURL := strings.TrimSuffix(target, "/") + path
		up, err := http.NewRequest(r.Method, targetURL, r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		for k, vs := range r.Header {
			lk := strings.ToLower(k)
			if lk == "x-relay-target" || lk == "x-relay-path" || lk == "host" {
				continue
			}
			for _, v := range vs {
				up.Header.Add(k, v)
			}
		}
		resp, err := http.DefaultClient.Do(up)
		if err != nil {
			log.Printf("relay error: %v", err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		fl, _ := w.(http.Flusher)
		buf := make([]byte, 32*1024)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				_, _ = w.Write(buf[:n])
				if fl != nil {
					fl.Flush()
				}
			}
			if err != nil {
				break
			}
		}
		log.Printf("relayed %s %s -> %s %d", r.Method, path, target, resp.StatusCode)
		_ = io.Discard
	})
	log.Fatal(http.ListenAndServe(":18082", nil))
}
