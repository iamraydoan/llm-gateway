package gateway

import (
	"strings"
	"testing"
)

func TestExtractUpstreamError(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{`{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}`, "rate_limit_exceeded: Rate limit reached"},
		{`{"error":{"message":"internal error","type":"server_error"}}`, "server_error: internal error"},
		{`{"error":{"message":"You exceeded your current quota","type":"insufficient_quota","code":"insufficient_quota"}}`, "insufficient_quota: You exceeded your current quota"},
		{`{"error":"plain string error"}`, "plain string error"},
		{`not json at all`, "not json at all"},
		{``, ""},
		{`{"error":{"message":""}}`, ""},
	}
	for _, c := range cases {
		if got := extractUpstreamError([]byte(c.body)); got != c.want {
			t.Fatalf("body %q: got %q, want %q", c.body, got, c.want)
		}
	}
	long := strings.Repeat("x", 300)
	if got := extractUpstreamError([]byte(long)); len([]rune(got)) != 201 || !strings.HasSuffix(got, "…") {
		t.Fatalf("long body not truncated to 200 runes + ellipsis: %q", got)
	}
}
