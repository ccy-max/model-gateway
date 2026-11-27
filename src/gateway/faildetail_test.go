package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestBuildFailureDiagnostic 失败诊断信息应包含 HTTP 状态码与上游响应片段，
// 且不依赖调试开关（buildFailureDiagnostic 无条件调用）。
func TestBuildFailureDiagnostic(t *testing.T) {
	body := `{"error":{"message":"Rate limit exceeded","type":"rate_limit"}}`
	raw := buildFailureDiagnostic(429, []byte(body), "https://up.example/v1/chat/completions", "m1", "sk-abc***")
	if raw == "" {
		t.Fatal("诊断信息不应为空")
	}
	var d map[string]any
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatalf("诊断信息应是合法 JSON: %v / %s", err, raw)
	}
	if d["http_status"] != float64(429) {
		t.Fatalf("应记录 HTTP 429，实际 %v", d["http_status"])
	}
	rp, ok := d["upstream_response"].(map[string]any)
	if !ok {
		t.Fatalf("应含 upstream_response: %v", d)
	}
	if !strings.Contains(rp["body"].(string), "Rate limit exceeded") {
		t.Fatalf("应保留上游错误 JSON，实际 %v", rp["body"])
	}
	ur, ok := d["upstream_request"].(map[string]any)
	if !ok || ur["model"] != "m1" || ur["key"] != "sk-abc***" {
		t.Fatalf("应记录请求目标/模型/密钥，实际 %v", d["upstream_request"])
	}
}

// TestBuildFailureDiagnosticTruncates 超长上游响应应被截断，避免撑爆日志。
func TestBuildFailureDiagnosticTruncates(t *testing.T) {
	raw := buildFailureDiagnostic(500, []byte(strings.Repeat("x", 20*1024)), "u", "m", "k")
	var d map[string]any
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatalf("截断后仍应是合法 JSON: %v", err)
	}
	rp := d["upstream_response"].(map[string]any)
	if len(rp["body"].(string)) > debugCap+32 {
		t.Fatalf("响应体应截断到约 %d 字节，实际 %d", debugCap, len(rp["body"].(string)))
	}
}

// TestFailureLogHasDiagnostic 关闭调试模式时，上游 4xx 失败仍写入诊断信息。
func TestFailureLogHasDiagnostic(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/models") {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"data":[{"id":"real-a"}]}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":{"message":"quota exceeded","type":"rate_limit"}}`))
	}))
	t.Cleanup(up.Close)

	st.Update(func(c *Config) {
		c.Settings.DebugPayloads = false // 明确关闭调试模式
		c.APIKeys = []APIKey{{Key: "sk-test", CreatedAt: 1}}
		c.Providers = []Provider{{ID: "p1", Name: "上游", Kind: "openai-compat", BaseURL: up.URL, Enabled: true,
			Keys: []ProviderKey{{Key: "k1"}}}}
		c.ProviderModels = []ProviderModel{{ID: "pm", ProviderID: "p1", Model: "real-a", Enabled: true}}
		c.PlatformModels = []PlatformModel{{
			ID: "plat", Name: "m1", Enabled: true, Strategy: "round-robin",
			Members: []Member{{ProviderModelID: "pm", Enabled: true}},
		}}
	})

	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"m1","messages":[{"role":"user","content":"hi"}]}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("上游全失败应返回 502，实际 %d", resp.StatusCode)
	}

	logs := s.db.QueryLogsRecent(10)
	if len(logs) == 0 {
		t.Fatal("应记录失败尝试")
	}
	found := false
	for _, l := range logs {
		if l.Debug == "" {
			continue
		}
		var d map[string]any
		if json.Unmarshal([]byte(l.Debug), &d) != nil {
			continue
		}
		if d["http_status"] == float64(429) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("即使关闭调试模式，失败日志也应含 HTTP 429 诊断。实际日志: %+v", logs)
	}
}
