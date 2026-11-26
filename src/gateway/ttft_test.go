package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// chunkReader 依次吐出预置的 SSE 块，每块之前阻塞 delay，
// 可精确模拟「首字前等待」以及块与块之间的间隔。
type chunkReader struct {
	chunks [][]byte
	delay  time.Duration
	i      int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.i >= len(c.chunks) {
		return 0, io.EOF
	}
	time.Sleep(c.delay)
	n := copy(p, c.chunks[c.i])
	c.i++
	return n, nil
}

func sseResp(delay time.Duration, chunks ...string) *http.Response {
	raw := make([][]byte, len(chunks))
	for i, s := range chunks {
		raw[i] = []byte(s)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(&chunkReader{chunks: raw, delay: delay}),
	}
}

// TestStreamTTFTCaptured 首块即内容 → ttft 约为一个块间隔（120ms）。
func TestStreamTTFTCaptured(t *testing.T) {
	resp := sseResp(120*time.Millisecond,
		"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n",
		"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\n",
	)
	st := newTestStore(t)
	s := NewServer(st, "test")
	w := httptest.NewRecorder()
	p, c, _, ttft, committed, serr := s.streamToClient(w, resp, time.Now())
	if serr != nil || !committed {
		t.Fatalf("应正常提交: committed=%v err=%v", committed, serr)
	}
	if p != 10 || c != 2 {
		t.Fatalf("usage 解析错误: %d/%d", p, c)
	}
	if ttft < 100 || ttft > 1500 {
		t.Fatalf("首块即内容时 ttft 应约 120ms，实际 %dms", ttft)
	}
}

// TestStreamTTFTIgnoresRoleOnlyChunk 首块只有 role（无内容）→ 首字应算在第二块（约 240ms），
// 而不是第一块（约 120ms），验证「只认内容块」。
func TestStreamTTFTIgnoresRoleOnlyChunk(t *testing.T) {
	resp := sseResp(120*time.Millisecond,
		"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
	)
	st := newTestStore(t)
	s := NewServer(st, "test")
	w := httptest.NewRecorder()
	_, _, _, ttft, _, _ := s.streamToClient(w, resp, time.Now())
	if ttft < 190 {
		t.Fatalf("role-only 块不应计首字，ttft 应约 240ms，实际 %dms", ttft)
	}
}

// TestStreamTTFTReasoningCounts 推理块（reasoning_content）也算首字前的内容输出。
func TestStreamTTFTReasoningCounts(t *testing.T) {
	resp := sseResp(120*time.Millisecond,
		"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"想\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"答\"}}]}\n\n",
	)
	st := newTestStore(t)
	s := NewServer(st, "test")
	w := httptest.NewRecorder()
	_, _, _, ttft, _, _ := s.streamToClient(w, resp, time.Now())
	if ttft > 260 {
		t.Fatalf("推理块应算首字（约 120ms），实际 %dms", ttft)
	}
}
