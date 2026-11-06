package gateway

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestHubBroadcast(t *testing.T) {
	h := NewHub(nil)
	ch1 := h.Subscribe()
	ch2 := h.Subscribe()
	defer h.Unsubscribe(ch1)
	defer h.Unsubscribe(ch2)

	h.Broadcast("log", []byte(`{"id":1}`))
	for _, ch := range []chan []byte{ch1, ch2} {
		select {
		case frame := <-ch:
			if !strings.Contains(string(frame), "event: log") || !strings.Contains(string(frame), `"id":1`) {
				t.Fatalf("bad frame: %s", frame)
			}
		case <-time.After(time.Second):
			t.Fatal("no frame received")
		}
	}

	h.Unsubscribe(ch2)
	h.Broadcast("log", []byte(`{"id":2}`))
	select {
	case frame := <-ch1:
		if !strings.Contains(string(frame), `"id":2`) {
			t.Fatalf("bad frame: %s", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("ch1 got nothing after unsubscribe of ch2")
	}
	select {
	case frame := <-ch2:
		t.Fatalf("unsubscribed ch2 still got a frame: %s", frame)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHubSlowClientDropped(t *testing.T) {
	h := NewHub(nil)

	slow := h.Subscribe()
	fast := h.Subscribe()
	defer h.Unsubscribe(slow)
	defer h.Unsubscribe(fast)
	for i := 0; i < 70; i++ {
		h.Broadcast("log", []byte(`{"id":1}`))
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 64; i++ {
			<-fast
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast blocked on slow client")
	}
}

func TestHubNotifyKeysDebounced(t *testing.T) {
	var calls int
	h := NewHub(func() []byte {
		calls++
		b, _ := json.Marshal(map[string]any{"keys": []any{}})
		return b
	})
	ch := h.Subscribe()
	defer h.Unsubscribe(ch)

	for i := 0; i < 10; i++ {
		h.NotifyKeys()
	}
	received := 0
	timeout := time.After(1500 * time.Millisecond)
loop:
	for {
		select {
		case frame := <-ch:
			if strings.Contains(string(frame), "event: keys") {
				received++
			}
		case <-timeout:
			break loop
		}
	}
	if received == 0 {
		t.Fatal("no keys broadcast received")
	}
	if received > 3 {
		t.Fatalf("debounce failed: %d keys broadcasts in 1.5s", received)
	}
	if calls == 0 {
		t.Fatal("snapshot func never called")
	}
}
