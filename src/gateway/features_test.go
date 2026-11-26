package gateway

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countingUpstream 可控状态码 + 命中计数的假上游（单模型 model-a）。
func countingUpstream(t *testing.T, status int, hits *int64) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"model-a"}]}`))
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(hits, 1)
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"message":"upstream says no"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":5,"completion_tokens":7}}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// seedOne 单供应商 + 单平台模型的极简配置。
func seedOne(t *testing.T, s *Server, baseURL string) {
	t.Helper()
	s.store.Update(func(c *Config) {
		c.Password = "test-pass"
		c.APIKeys = []APIKey{{Key: "sk-test", Note: "t", CreatedAt: 1}}
		c.Providers = []Provider{{ID: "p1", Name: "单家", Kind: "openai-compat", BaseURL: baseURL, APIKey: "up-key", Enabled: true}}
		c.ProviderModels = []ProviderModel{{ID: "prm1", ProviderID: "p1", Model: "model-a", Enabled: true}}
		c.PlatformModels = []PlatformModel{{
			ID: "plat1", Name: "smart", Strategy: "round-robin", Enabled: true, MaxRetry: 3,
			Members: []Member{{ProviderModelID: "prm1", Weight: 1, Enabled: true}},
		}}
	})
}

func loginCookie(t *testing.T, srvURL string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srvURL+"/admin/api/login", strings.NewReader(`{"password":"test-pass"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	for _, c := range resp.Cookies() {
		if c.Name == sessionCookie {
			return c.Value
		}
	}
	t.Fatalf("login failed: %d", resp.StatusCode)
	return ""
}

// TestProviderExhaustedOn402 上游 402 → 供应商标记额度耗尽，后续请求直接 402 跳过（不再打上游）。
func TestProviderExhaustedOn402(t *testing.T) {
	var hits int64
	up := countingUpstream(t, http.StatusPaymentRequired, &hits)
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedOne(t, s, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	// 第一次：真正打到上游（返回 402），供应商被标记耗尽
	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Fatalf("首次应命中上游 1 次，实际 %d", got)
	}

	// 第二次：直接跳过（供应商额度耗尽），上游零命中
	resp2 := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	body, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusPaymentRequired {
		t.Fatalf("额度耗尽应返回 402，实际 %d: %s", resp2.StatusCode, body)
	}
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Fatalf("额度耗尽后不应再打上游，命中 %d", got)
	}

	// 面板接口应带上 exhausted_left
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/api/providers", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: loginCookie(t, ts.URL)})
	pr, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer pr.Body.Close()
	var list []map[string]any
	if err := json.NewDecoder(pr.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("供应商数量异常: %d", len(list))
	}
	if v, _ := list[0]["exhausted_left"].(float64); v <= 0 {
		t.Fatalf("exhausted_left 应 >0，实际 %v", list[0]["exhausted_left"])
	}

	// 「恢复」按钮：清除标记后可再次调用上游
	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/providers/p1/recover", nil)
	req2.AddCookie(&http.Cookie{Name: sessionCookie, Value: loginCookie(t, ts.URL)})
	rr, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	rr.Body.Close()
	if rr.StatusCode != http.StatusOK {
		t.Fatalf("recover 应 200，实际 %d", rr.StatusCode)
	}
	resp3 := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()
	if got := atomic.LoadInt64(&hits); got != 2 {
		t.Fatalf("恢复后应再次命中上游（2），实际 %d", got)
	}
}

// TestProviderBreakerOpens 连续 3 次 5xx → 熔断，后续请求跳过上游。
func TestProviderBreakerOpens(t *testing.T) {
	var hits int64
	up := countingUpstream(t, http.StatusServiceUnavailable, &hits)
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedOne(t, s, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	for i := 0; i < 3; i++ {
		resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if got := atomic.LoadInt64(&hits); got != 3 {
		t.Fatalf("前 3 次每次应命中 1 次，实际 %d", got)
	}
	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if got := atomic.LoadInt64(&hits); got != 3 {
		t.Fatalf("熔断后不应再打上游，命中 %d", got)
	}
	if !strings.Contains(string(body), "provider_breaker_open") {
		t.Fatalf("应返回熔断错误，实际: %s", body)
	}
	if s.router.BreakerLeft("p1") <= 0 {
		t.Fatal("供应商应处于熔断状态")
	}
}

// flakyUpstream 首次请求直接断开连接（网络错误），之后正常返回。
func flakyUpstream(t *testing.T, hits *int64) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(hits, 1)
		if n == 1 {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("no hijacker")
			}
			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.(*net.TCPConn).Close()
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":5,"completion_tokens":7}}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestRetryOnceOnNetworkError 首次网络错误后同 key 自动重试一次，业务请求成功。
func TestRetryOnceOnNetworkError(t *testing.T) {
	var hits int64
	up := flakyUpstream(t, &hits)
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedOne(t, s, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("重试后应 200，实际 %d: %s", resp.StatusCode, body)
	}
	if got := atomic.LoadInt64(&hits); got != 2 {
		t.Fatalf("应命中上游 2 次（失败+重试），实际 %d", got)
	}
}

// TestClientRequestStats 业务请求口径：重试/轮换只计 1 次请求；最终失败计 1 次错误。
func TestClientRequestStats(t *testing.T) {
	var okHits, badHits int64
	upOK := countingUpstream(t, http.StatusOK, &okHits)
	upBad := countingUpstream(t, http.StatusServiceUnavailable, &badHits)
	st := newTestStore(t)
	s := NewServer(st, "test")

	// 两家都是 500：一次客户端请求 = 2 次尝试，业务请求=1、业务失败=1
	s.store.Update(func(c *Config) {
		c.Password = "test-pass"
		c.APIKeys = []APIKey{{Key: "sk-test", Note: "t", CreatedAt: 1}}
		c.Providers = []Provider{
			{ID: "p1", Name: "坏一", Kind: "openai-compat", BaseURL: upBad.URL, APIKey: "k", Enabled: true},
			{ID: "p2", Name: "坏二", Kind: "openai-compat", BaseURL: upBad.URL, APIKey: "k", Enabled: true},
		}
		c.ProviderModels = []ProviderModel{
			{ID: "prm1", ProviderID: "p1", Model: "model-a", Enabled: true},
			{ID: "prm2", ProviderID: "p2", Model: "model-a", Enabled: true},
		}
		c.PlatformModels = []PlatformModel{{
			ID: "plat1", Name: "smart", Strategy: "round-robin", Enabled: true, MaxRetry: 3,
			Members: []Member{
				{ProviderModelID: "prm1", Weight: 1, Enabled: true},
				{ProviderModelID: "prm2", Weight: 1, Enabled: true},
			},
		}}
	})
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	day := time.Now().Format("2006-01-02")
	rows, _, _, _, _, _ := s.db.QueryStats(day, day)
	var creq, cfail, reqs int64
	for _, r := range rows {
		creq += r.CReq
		cfail += r.CFail
		reqs += r.Requests
	}
	if creq != 1 || cfail != 1 {
		t.Fatalf("业务口径应为 1 请求 / 1 失败，实际 %d/%d", creq, cfail)
	}
	if reqs != 2 {
		t.Fatalf("尝试数应为 2，实际 %d", reqs)
	}

	// 把两家都换成正常上游后，再发一次 → 业务请求 +1、错误不变
	s.store.Update(func(c *Config) {
		c.Providers[0].BaseURL = upOK.URL
		c.Providers[1].BaseURL = upOK.URL
	})
	s.invalidateUpstreams()
	s.router.MarkProviderSuccess("p1")
	s.router.MarkProviderSuccess("p2")
	resp2 := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("应 200，实际 %d", resp2.StatusCode)
	}
	rows, _, _, _, _, _ = s.db.QueryStats(day, day)
	creq, cfail = 0, 0
	for _, r := range rows {
		creq += r.CReq
		cfail += r.CFail
	}
	if creq != 2 || cfail != 1 {
		t.Fatalf("成功一次后应为 2 请求 / 1 失败，实际 %d/%d", creq, cfail)
	}
}

// TestLoginThrottle 连续 5 次密码错误 → 锁定，正确密码也被拒。
func TestLoginThrottle(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedOne(t, s, "http://127.0.0.1:1")
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	login := func(pw string) int {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/login", strings.NewReader(`{"password":"`+pw+`"}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	for i := 0; i < 4; i++ {
		if code := login("bad"); code != http.StatusUnauthorized {
			t.Fatalf("第 %d 次错误密码应 401，实际 %d", i+1, code)
		}
	}
	if code := login("bad"); code != http.StatusTooManyRequests {
		t.Fatalf("第 5 次失败应触发锁定 429，实际 %d", code)
	}
	if code := login("test-pass"); code != http.StatusTooManyRequests {
		t.Fatalf("锁定期内正确密码也应 429，实际 %d", code)
	}
}

// TestMetricsEndpoint 指标端点输出请求计数与供应商 gauge。
func TestMetricsEndpoint(t *testing.T) {
	var hits int64
	up := countingUpstream(t, http.StatusOK, &hits)
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedOne(t, s, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[]}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	mr, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Body.Close()
	b, _ := io.ReadAll(mr.Body)
	text := string(b)
	for _, want := range []string{
		"gw_requests_total{provider=\"单家\",model=\"smart\",status=\"ok\"}",
		"gw_tokens_total{provider=\"单家\",model=\"smart\",type=\"completion\"}",
		"gw_request_duration_seconds_count 1",
		"gw_provider_up{provider=\"单家\"} 1",
		"gw_provider_exhausted{provider=\"单家\"} 0",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("指标缺少 %q，实际输出:\n%s", want, text)
		}
	}
}

// TestConfigExportImport 导出后导入到新实例，配置完整还原。
func TestConfigExportImport(t *testing.T) {
	var hits int64
	up := countingUpstream(t, http.StatusOK, &hits)
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedOne(t, s, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	ck := loginCookie(t, ts.URL)
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/api/config/export", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: ck})
	er, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	exported, _ := io.ReadAll(er.Body)
	er.Body.Close()
	if er.StatusCode != http.StatusOK || len(exported) < 50 {
		t.Fatalf("导出失败: %d %s", er.StatusCode, exported)
	}

	// 新实例导入
	st2 := newTestStore(t)
	s2 := NewServer(st2, "test")
	ts2 := httptest.NewServer(s2.Routes())
	t.Cleanup(ts2.Close)
	// 新实例默认密码不同，先设成同一密码以便登录
	s2.store.Update(func(c *Config) { c.Password = "test-pass" })
	ck2 := loginCookie(t, ts2.URL)

	ir, err := http.NewRequest(http.MethodPost, ts2.URL+"/admin/api/config/import", strings.NewReader(string(exported)))
	if err != nil {
		t.Fatal(err)
	}
	ir.AddCookie(&http.Cookie{Name: sessionCookie, Value: ck2})
	ir.Header.Set("Content-Type", "application/json")
	irr, err := http.DefaultClient.Do(ir)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(irr.Body)
	irr.Body.Close()
	if irr.StatusCode != http.StatusOK {
		t.Fatalf("导入失败: %d %s", irr.StatusCode, body)
	}
	s2.store.View(func(c *Config) {
		if len(c.Providers) != 1 || c.Providers[0].Name != "单家" {
			t.Fatalf("导入后供应商不一致: %+v", c.Providers)
		}
		if len(c.PlatformModels) != 1 || c.PlatformModels[0].Name != "smart" {
			t.Fatalf("导入后平台模型不一致: %+v", c.PlatformModels)
		}
		if len(c.APIKeys) != 1 || c.APIKeys[0].Key != "sk-test" {
			t.Fatalf("导入后客户端密钥不一致: %+v", c.APIKeys)
		}
	})
}

// TestSettingsTimeoutClamp 超时上下限钳制落在 10~1800。
func TestSettingsTimeoutClamp(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedOne(t, s, "http://127.0.0.1:1")
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)
	ck := loginCookie(t, ts.URL)

	put := func(v int) {
		body := `{"chat_timeout_seconds":` + strconv.Itoa(v) + `,"debug_payloads":false}`
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/admin/api/settings", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: ck})
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	put(9999)
	s.store.View(func(c *Config) {
		if c.Settings.ChatTimeoutSec != 1800 {
			t.Fatalf("9999 应被钳制为 1800，实际 %d", c.Settings.ChatTimeoutSec)
		}
	})
	put(3)
	s.store.View(func(c *Config) {
		if c.Settings.ChatTimeoutSec != 10 {
			t.Fatalf("3 应被钳制为 10，实际 %d", c.Settings.ChatTimeoutSec)
		}
	})
}


