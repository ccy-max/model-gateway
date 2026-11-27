package gateway

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestHeartbeatWhileWaitingUpstreamHeaders 上游迟迟不返回响应头时（商汤等），
// 网关应在等待期间就向客户端发心跳，而不是等拿到响应头才发。
func TestHeartbeatWhileWaitingUpstreamHeaders(t *testing.T) {
	// 假上游：1.5 秒后才开始写响应
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/models") {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"data":[{"id":"m1"}]}`))
			return
		}
		time.Sleep(1500 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	t.Cleanup(slow.Close)

	st := newTestStore(t)
	st.Update(func(c *Config) { c.Settings.PreCommitHeartbeatSec = 1 })
	s := NewServer(st, "test")
	seedTwoProviders(s, slow.URL, slow.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	start := time.Now()
	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	defer resp.Body.Close()

	br := bufio.NewReader(resp.Body)
	var heartbeatAt time.Duration
	sawData := false
	for {
		line, err := br.ReadString('\n')
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, ":") && heartbeatAt == 0 {
			heartbeatAt = time.Since(start)
		}
		if strings.HasPrefix(trimmed, "data:") && strings.Contains(trimmed, "hi") {
			sawData = true
		}
		if err != nil {
			break
		}
	}
	if heartbeatAt == 0 {
		t.Fatal("等待上游响应头期间应收到心跳注释帧")
	}
	if heartbeatAt > 1300*time.Millisecond {
		t.Fatalf("心跳应在约 1s 到达（早于上游响应头），实际 %.0fms", float64(heartbeatAt.Milliseconds()))
	}
	if !sawData {
		t.Fatal("心跳后仍应转发上游内容")
	}
}
