package gateway

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Hub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}

	keysMu       sync.Mutex
	keysDirty    bool
	keysSnapshot func() []byte
}

func NewHub(keysSnapshot func() []byte) *Hub {
	h := &Hub{
		clients:      map[chan []byte]struct{}{},
		keysSnapshot: keysSnapshot,
	}
	go h.keysLoop()
	return h
}

func (h *Hub) Subscribe() chan []byte {
	ch := make(chan []byte, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) Unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
}

func (h *Hub) Broadcast(event string, payload []byte) {
	frame := []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", event, payload))
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- frame:
		default:

		}
	}
}

func (h *Hub) NotifyKeys() {
	h.keysMu.Lock()
	h.keysDirty = true
	h.keysMu.Unlock()
}

func (h *Hub) keysLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		h.keysMu.Lock()
		dirty := h.keysDirty
		h.keysDirty = false
		h.keysMu.Unlock()
		if !dirty || h.keysSnapshot == nil {
			continue
		}
		h.Broadcast("keys", h.keysSnapshot())
	}
}

func (g *Gateway) handleAPIEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "streaming not supported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	ch := g.hub.Subscribe()
	defer g.hub.Unsubscribe(ch)

	if payload := g.keysSnapshot(); len(payload) > 0 {
		g.sendSSE(w, flusher, "keys", payload)
	}
	if payload := g.statsSnapshot(24); len(payload) > 0 {
		g.sendSSE(w, flusher, "stats", payload)
	}

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = w.Write([]byte(": heartbeat\n\n"))
			flusher.Flush()
		case frame, ok := <-ch:
			if !ok {
				return
			}
			_, _ = w.Write(frame)
			flusher.Flush()
		}
	}
}

func (g *Gateway) sendSSE(w http.ResponseWriter, flusher http.Flusher, event string, payload []byte) {
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload)
	flusher.Flush()
}
