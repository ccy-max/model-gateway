package gateway

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestStatusWriterKeepsFlusher 回归：timingMiddleware 包装后的 ResponseWriter 必须仍可 Flush，
// 否则 SSE 无法逐行刷写（表现为客户端「一顿一顿」地成块接收）。
func TestStatusWriterKeepsFlusher(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec, status: 200}
	f, ok := any(sw).(http.Flusher)
	if !ok {
		t.Fatal("statusWriter 未实现 http.Flusher，流式响应会被整体缓冲")
	}
	f.Flush()
	if !rec.Flushed {
		t.Fatal("statusWriter.Flush 未透传到底层 ResponseWriter")
	}
}

// TestStreamFlushesIncrementally 端到端：经完整中间件链后，客户端应「边到边收」，
// 而不是等上游结束后一次性收到全部片段。
func TestStreamFlushesIncrementally(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")

	const n = 6
	chunks := make([]string, 0, n)
	for i := 0; i < n; i++ {
		chunks = append(chunks, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
	}
	resp := sseResp(60*time.Millisecond, chunks...)

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
	var firstLineAt time.Duration
	lines := 0
	for {
		line, rerr := br.ReadString('\n')
		if strings.HasPrefix(strings.TrimSpace(line), "data:") {
			if lines == 0 {
				firstLineAt = time.Since(start)
			}
			lines++
		}
		if rerr != nil {
			break
		}
	}
	total := time.Since(start)
	if lines != n {
		t.Fatalf("应收到 %d 个 data 行，实际 %d", n, lines)
	}
	// 上游总时长约 360ms；若被缓冲，首行会在接近末尾时才到。
	if firstLineAt > 250*time.Millisecond {
		t.Fatalf("首个 data 行 %.0fms 才到达（总耗时 %.0fms），说明流被缓冲未逐行刷写",
			float64(firstLineAt.Milliseconds()), float64(total.Milliseconds()))
	}
}
