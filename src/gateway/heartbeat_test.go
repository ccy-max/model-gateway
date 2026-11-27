package gateway

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestPreCommitHeartbeat 上游迟迟不吐首字时，应先收到 SSE 注释帧让连接保持活跃，
// 随后数据照常到达；且发出心跳后视为已提交（不可再重试）。
func TestPreCommitHeartbeat(t *testing.T) {
	st := newTestStore(t)
	st.Update(func(c *Config) { c.Settings.PreCommitHeartbeatSec = 1 })
	s := NewServer(st, "test")

	// 上游 1.6 秒后才吐第一块
	resp := sseResp(1600*time.Millisecond,
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
		"data: [DONE]\n\n",
	)
	h := s.timingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _, _, _, _, _ = s.streamToClient(w, resp, time.Now())
	}))
	srv := httptest.NewServer(h)
	defer srv.Close()

	start := time.Now()
	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer res.Body.Close()

	br := bufio.NewReader(res.Body)
	var heartbeatAt time.Duration
	sawData := false
	for {
		line, rerr := br.ReadString('\n')
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, ":") && heartbeatAt == 0 {
			heartbeatAt = time.Since(start)
		}
		if strings.HasPrefix(trimmed, "data:") && strings.Contains(trimmed, "hi") {
			sawData = true
		}
		if rerr != nil {
			break
		}
	}
	if heartbeatAt == 0 {
		t.Fatal("应在首字前收到心跳注释帧")
	}
	if heartbeatAt > 1300*time.Millisecond {
		t.Fatalf("心跳应在约 1s 时到达，实际 %.0fms", float64(heartbeatAt.Milliseconds()))
	}
	if !sawData {
		t.Fatal("心跳之后应正常收到内容帧")
	}
}

// TestSSEWriterHeartbeatCommits 心跳会提交响应（客户端已收到字节 → 不可重试）。
func TestSSEWriterHeartbeatCommits(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := newSSEWriter(rec, "text/event-stream")
	sw.Line("data: {\"x\":1}")
	if sw.Committed() {
		t.Fatal("未提交前不应标记 committed")
	}
	sw.StartHeartbeat(10 * time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	sw.StopHeartbeat()
	if !sw.Committed() {
		t.Fatal("心跳后应视为已提交")
	}
	if !strings.Contains(rec.Body.String(), ": keepalive") {
		t.Fatalf("应写出心跳注释帧，实际: %q", rec.Body.String())
	}
	if !rec.Flushed {
		t.Fatal("心跳必须 flush，否则客户端仍收不到")
	}
}
