package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// timeoutSetup 构造单供应商配置，可指定超时覆盖。
func timeoutSetup(t *testing.T, baseURL string, firstByte, stall, retries int) *Server {
	t.Helper()
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedOne(t, s, baseURL)
	s.store.Update(func(c *Config) {
		c.Providers[0].FirstByteTimeoutSec = firstByte
		c.Providers[0].StallTimeoutSec = stall
		c.Providers[0].TimeoutRetries = retries
	})
	return s
}

// TestFirstByteTimeoutRetry 首轮上游迟迟不回（超首字阈值）→ 同 key 自动重试并成功。
func TestFirstByteTimeoutRetry(t *testing.T) {
	var hits int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&hits, 1)
		if n == 1 {
			time.Sleep(2500 * time.Millisecond) // 超过 1s 首字阈值
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer up.Close()

	s := timeoutSetup(t, up.URL, 1, 0, 0) // 首字 1s，停滞关闭，重试用默认(1)
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	start := time.Now()
	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("重试后应 200，实际 %d: %s", resp.StatusCode, body)
	}
	if got := atomic.LoadInt64(&hits); got != 2 {
		t.Fatalf("应命中上游 2 次（超时+重试），实际 %d", got)
	}
	if elapsed > 4*time.Second {
		t.Fatalf("超时重试耗时过长: %v", elapsed)
	}
}

// TestNoRetryWhenDisabled TimeoutRetries=-1 → 首字超时后不再重试。
func TestNoRetryWhenDisabled(t *testing.T) {
	var hits int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		time.Sleep(2500 * time.Millisecond)
	}))
	defer up.Close()

	s := timeoutSetup(t, up.URL, 1, 0, -1)
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("应失败，实际 200")
	}
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Fatalf("关闭重试后应只命中 1 次，实际 %d", got)
	}
	if !strings.Contains(string(body), "首字超时") {
		t.Fatalf("错误信息应说明首字超时: %s", body)
	}
}

// TestStallTimeoutAborts 流式输出中途停滞：已提交给客户端，只中止不重试。
func TestStallTimeoutAborts(t *testing.T) {
	var hits int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n"))
		if fl != nil {
			fl.Flush()
		}
		time.Sleep(4 * time.Second) // 超过 1s 停滞阈值
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n"))
	}))
	defer up.Close()

	s := timeoutSetup(t, up.URL, 0, 1, 0) // 停滞 1s
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	start := time.Now()
	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","stream":true,"messages":[]}`)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)

	if !strings.Contains(string(body), "\"a\"") {
		t.Fatalf("客户端应已收到首块内容: %s", body)
	}
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Fatalf("停滞超时不应重试，命中 %d 次", got)
	}
	if elapsed > 3500*time.Millisecond {
		t.Fatalf("应在停滞阈值附近中止，实际耗时 %v", elapsed)
	}
}

// TestModelTimeoutOverridesProvider 平台模型覆盖优先于供应商配置。
func TestModelTimeoutOverridesProvider(t *testing.T) {
	if got := resolveTimeout(5, 30, 120); got != 5 {
		t.Fatalf("模型覆盖应优先，got %d", got)
	}
	if got := resolveTimeout(0, 30, 120); got != 30 {
		t.Fatalf("模型未设时应跟随供应商，got %d", got)
	}
	if got := resolveTimeout(0, 0, 120); got != 120 {
		t.Fatalf("都未设时应跟随全局，got %d", got)
	}
	if got := resolveTimeout(0, -1, 120); got != -1 {
		t.Fatalf("供应商 -1 表示关闭，got %d", got)
	}

	// 端到端：模型层设 1s 首字超时，供应商层设 5s（模型应胜出）
	var hits int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&hits, 1)
		if n == 1 {
			time.Sleep(2200 * time.Millisecond)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
	}))
	defer up.Close()

	s := timeoutSetup(t, up.URL, 5, 0, 0)
	s.store.Update(func(c *Config) { c.PlatformModels[0].FirstByteTimeoutSec = 1 })
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("模型级 1s 超时应触发重试并成功，实际 %d: %s", resp.StatusCode, body)
	}
	if got := atomic.LoadInt64(&hits); got != 2 {
		t.Fatalf("应命中 2 次，实际 %d", got)
	}
}

// TestTimeoutDefaults 全局默认 120/300/1，可在设置里改为 -1 关闭。
func TestTimeoutDefaults(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")
	s.store.View(func(c *Config) {
		if c.Settings.FirstByteTimeoutSec != 120 || c.Settings.StallTimeoutSec != 300 || c.Settings.TimeoutRetries != 1 {
			t.Fatalf("默认超时配置异常: %d/%d/%d",
				c.Settings.FirstByteTimeoutSec, c.Settings.StallTimeoutSec, c.Settings.TimeoutRetries)
		}
	})
}
