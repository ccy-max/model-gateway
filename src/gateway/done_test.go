package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// errAfterDoneBody 模拟真实客户端：收到 [DONE] 后立刻断开连接，
// 网关继续读上游时就会拿到 context canceled。
type errAfterDoneBody struct {
	chunks []string
	i      int
}

func (b *errAfterDoneBody) Read(p []byte) (int, error) {
	if b.i >= len(b.chunks) {
		return 0, context.Canceled
	}
	n := copy(p, b.chunks[b.i])
	b.i++
	return n, nil
}

func (b *errAfterDoneBody) Close() error { return nil }

// TestStreamDoneNotRecordedAsCancel 客户端在 [DONE] 之后关闭连接，
// 不能被记成「客户端已断开」——此时本轮输出已完整送达，属于成功。
func TestStreamDoneNotRecordedAsCancel(t *testing.T) {
	st := newTestStore(t)
	st.Update(func(c *Config) { c.Settings.PreCommitHeartbeatSec = -1 })
	s := NewServer(st, "test")

	body := &errAfterDoneBody{chunks: []string{
		"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
		"data: [DONE]\n\n",
	}}
	resp := &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       body,
	}
	rec := httptest.NewRecorder()
	// start 放到 50ms 前，避免毫秒取整后 ttft 变成 0
	_, _, _, ttft, committed, err := s.streamToClient(rec, resp, time.Now().Add(-50*time.Millisecond))

	if err != nil {
		t.Fatalf("[DONE] 之后读到 context canceled 不应算错误，实际: %v", err)
	}
	if !committed {
		t.Fatal("应标记为已提交")
	}
	if ttft == 0 {
		t.Fatal("应记录到首字时间")
	}
	if !strings.Contains(rec.Body.String(), "hello") {
		t.Fatal("内容应转发给客户端")
	}
	if !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatal("[DONE] 应转发给客户端")
	}
}

// TestStreamTruncatedWithoutDoneIsCancel 没收到 [DONE] 就被掐断，
// 仍应如实报告错误（不能被上面的修复掩盖）。
func TestStreamTruncatedWithoutDoneIsCancel(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")

	body := &errAfterDoneBody{chunks: []string{
		"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
	}}
	resp := &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       body,
	}
	rec := httptest.NewRecorder()
	_, _, _, _, committed, err := s.streamToClient(rec, resp, time.Now())

	if !committed {
		t.Fatal("已输出内容，应标记为已提交")
	}
	if err == nil {
		t.Fatal("没有 [DONE] 却中断，必须报错")
	}
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("错误应保留原始原因，实际: %v", err)
	}
}
