package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	st := NewStore(filepath.Join(t.TempDir(), "data.json"))
	if err := st.Load(); err != nil {
		t.Fatal(err)
	}
	return st
}

// fakeUpstream 假 OpenAI-compatible 上游：/v1/models 目录 + /v1/chat/completions。
func fakeUpstream(t *testing.T, chatStatus int, chatBody string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"model-a"},{"id":"model-b"},{"id":"text-embedding-x"}]}`))
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if m, _ := body["model"].(string); m != "model-a" && m != "model-b" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"message":"unknown model"}}`))
			return
		}
		wantStream, _ := body["stream"].(bool)
		if wantStream {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
			_, _ = w.Write([]byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7}}\n\n"))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			return
		}
		if chatStatus != http.StatusOK {
			w.WriteHeader(chatStatus)
			_, _ = w.Write([]byte(chatBody))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":5,"completion_tokens":7}}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func seedTwoProviders(s *Server, up1, up2 string) {
	s.store.Update(func(c *Config) {
		c.Password = "test-pass"
		c.APIKeys = []APIKey{{Key: "sk-test", Note: "t", CreatedAt: 1}}
		c.Providers = []Provider{
			{ID: "p1", Name: "上游一号", Kind: "openai-compat", BaseURL: up1, APIKey: "up-key", Enabled: true},
			{ID: "p2", Name: "上游二号", Kind: "openai-compat", BaseURL: up2, APIKey: "up-key", Enabled: true},
		}
		c.ProviderModels = []ProviderModel{
			{ID: "prm1", ProviderID: "p1", Model: "model-a", Enabled: true},
			{ID: "prm2", ProviderID: "p2", Model: "model-b", Enabled: true},
		}
		c.PlatformModels = []PlatformModel{{
			ID: "plat1", Name: "smart", Strategy: "round-robin", Enabled: true, MaxRetry: 3,
			Members: []Member{
				{ProviderModelID: "prm1", Weight: 1, Priority: 0, Enabled: true},
				{ProviderModelID: "prm2", Weight: 1, Priority: 0, Enabled: true},
			},
		}}
	})
}

func chatRequest(t *testing.T, url, key, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestChatAuth(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	resp := chatRequest(t, ts.URL, "sk-wrong", `{"model":"smart","messages":[]}`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expect 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = chatRequest(t, ts.URL, "sk-test", `{"model":"no-such","messages":[]}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown model expect 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestChatNonStream(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[{"role":"user","content":"hi"}]}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expect 200, got %d: %s", resp.StatusCode, b)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "hi") {
		t.Fatalf("response should pass through, got %s", b)
	}
}

func TestChatStream(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expect 200, got %d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "hi") || !strings.Contains(string(b), "[DONE]") {
		t.Fatalf("stream passthrough broken: %s", b)
	}
}

func TestChatFailover(t *testing.T) {
	bad := fakeUpstream(t, 500, `{"error":{"message":"boom"}}`)
	good := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	s.store.Update(func(c *Config) {
		c.Password = "test-pass"
		c.APIKeys = []APIKey{{Key: "sk-test", CreatedAt: 1}}
		c.Providers = []Provider{
			{ID: "p1", Name: "坏上游", Kind: "openai-compat", BaseURL: bad.URL, APIKey: "k", Enabled: true},
			{ID: "p2", Name: "好上游", Kind: "openai-compat", BaseURL: good.URL, APIKey: "k", Enabled: true},
		}
		c.ProviderModels = []ProviderModel{
			{ID: "prm1", ProviderID: "p1", Model: "model-a", Enabled: true},
			{ID: "prm2", ProviderID: "p2", Model: "model-b", Enabled: true},
		}
		c.PlatformModels = []PlatformModel{{
			ID: "plat1", Name: "smart", Strategy: "round-robin", Enabled: true, MaxRetry: 3,
			Members: []Member{
				{ProviderModelID: "prm1", Weight: 1, Priority: 0, Enabled: true},
				{ProviderModelID: "prm2", Weight: 1, Priority: 1, Enabled: true},
			},
		}}
	})
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[{"role":"user","content":"hi"}]}`)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), "hi") {
		t.Fatalf("failover should reach healthy member, got %d: %s", resp.StatusCode, b)
	}
}

func TestV1ModelsCatalog(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if len(out.Data) == 0 || out.Data[0].ID != "smart" {
		t.Fatalf("expect platform model smart first, got %+v", out)
	}
}

func TestAdminFlow(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)
	client := &http.Client{}

	// 未登录 401
	resp, err := client.Get(ts.URL + "/admin/api/providers")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthed admin expect 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 错误密码
	resp, err = client.Post(ts.URL+"/admin/api/login", "application/json", strings.NewReader(`{"password":"wrong"}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad password expect 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 正确登录拿 cookie
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/login", strings.NewReader(`{"password":"test-pass"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || len(resp.Cookies()) == 0 {
		t.Fatalf("login should set cookie, got %d", resp.StatusCode)
	}
	cookie := resp.Cookies()[0]
	resp.Body.Close()

	get := func(path string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		req.AddCookie(cookie)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// 供应商列表
	resp = get("/admin/api/providers")
	var provs []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&provs)
	resp.Body.Close()
	if len(provs) != 2 {
		t.Fatalf("expect 2 providers, got %d", len(provs))
	}

	// 远程目录（3 个，含启发式能力标注）
	resp = get("/admin/api/providers/p1/models/remote")
	var remote []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&remote)
	resp.Body.Close()
	if len(remote) != 3 {
		t.Fatalf("expect 3 remote models, got %d", len(remote))
	}

	// 导入 2 个：model-a 已存在（去重跳过），实际只新增 text-embedding-x 1 个
	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/providers/p1/models/import", strings.NewReader(`{"models":["model-a","text-embedding-x"]}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.AddCookie(cookie)
	resp, err = client.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	var imp map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&imp)
	resp.Body.Close()
	if imp["imported"] != float64(1) {
		t.Fatalf("expect import 1 (model-a deduped), got %v", imp)
	}

	// 平台模型列表
	resp = get("/admin/api/platform-models")
	var plats []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&plats)
	resp.Body.Close()
	if len(plats) != 1 {
		t.Fatalf("expect 1 platform model, got %d", len(plats))
	}

	// 新建密钥
	req3, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/keys", strings.NewReader(`{"note":"ci"}`))
	req3.Header.Set("Content-Type", "application/json")
	req3.AddCookie(cookie)
	resp, err = client.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	var key map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&key)
	resp.Body.Close()
	if ks, _ := key["key"].(string); !strings.HasPrefix(ks, "sk-") {
		t.Fatalf("created key should start with sk-, got %v", key)
	}

	// 统计
	resp = get("/admin/api/stats")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats expect 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestHealthAndPanel(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var h map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&h)
	if h["status"] != "ok" {
		t.Fatalf("health should be ok, got %v", h)
	}

	resp2, err := http.Get(ts.URL + "/admin")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	b, _ := io.ReadAll(resp2.Body)
	if !strings.Contains(string(b), "模型网关") {
		t.Fatalf("panel should contain brand, got %d bytes", len(b))
	}
}

// ── 供应商密钥池：多 key 负载均衡 / 失败轮换 ──

func keyPoolSetup(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *httptest.Server) {
	t.Helper()
	up := httptest.NewServer(handler)
	t.Cleanup(up.Close)
	st := newTestStore(t)
	s := NewServer(st, "test")
	s.store.Update(func(c *Config) {
		c.Password = "test-pass"
		c.APIKeys = []APIKey{{Key: "sk-test", CreatedAt: 1}}
		c.Providers = []Provider{{
			ID: "p1", Name: "多Key供应商", Kind: "openai-compat", BaseURL: up.URL, Enabled: true,
			Keys: []ProviderKey{
				{Key: "sk-bad", Weight: 1},
				{Key: "sk-good", Weight: 1},
			},
		}}
		c.ProviderModels = []ProviderModel{{ID: "prm1", ProviderID: "p1", Model: "model-a", Enabled: true}}
		c.PlatformModels = []PlatformModel{{
			ID: "plat1", Name: "smart", Strategy: "round-robin", Enabled: true, MaxRetry: 3,
			Members: []Member{{ProviderModelID: "prm1", Weight: 1, Priority: 0, Enabled: true}},
		}}
	})
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)
	return up, ts
}

func TestKeyPoolFailoverAndCooldown(t *testing.T) {
	var mu sync.Mutex
	counts := map[string]int{}
	_, ts := keyPoolSetup(t, func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		counts[key]++
		mu.Unlock()
		if key == "sk-bad" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"invalid key"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	})
	_ = ts

	for i := 0; i < 3; i++ {
		resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[{"role":"user","content":"hi"}]}`)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d should succeed via key rotation, got %d: %s", i, resp.StatusCode, b)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	// 坏 key 首轮被尝试并失败冷却，之后 2 次直接走好 key
	if counts["sk-good"] != 3 || counts["sk-bad"] != 1 {
		t.Fatalf("expect good=3 bad=1 (cooldown), got %v", counts)
	}
}

func TestKeyPoolRoundRobin(t *testing.T) {
	var mu sync.Mutex
	counts := map[string]int{}
	_, ts := keyPoolSetup(t, func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		counts[key]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
	})
	_ = ts

	for i := 0; i < 4; i++ {
		resp := chatRequest(t, ts.URL, "sk-test", `{"model":"smart","messages":[{"role":"user","content":"hi"}]}`)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d failed: %d", i, resp.StatusCode)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if counts["sk-bad"] != 2 || counts["sk-good"] != 2 {
		t.Fatalf("two valid keys should split 2/2, got %v", counts)
	}
}

func TestProviderKeysAdmin(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/login", strings.NewReader(`{"password":"test-pass"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	cookie := resp.Cookies()[0]
	resp.Body.Close()

	post := func(path, body string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(cookie)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// 新增两把 key
	for _, k := range []string{"sk-k1", "sk-k2"} {
		resp := post("/admin/api/providers/p1/keys", `{"key":"`+k+`","note":"池"}`)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("add key %s failed: %d", k, resp.StatusCode)
		}
		resp.Body.Close()
	}
	// 重复添加 → 409
	resp = post("/admin/api/providers/p1/keys", `{"key":"sk-k1"}`)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate key expect 409, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 列表含运行态字段；旧 APIKey 字段已自动迁移为第一把 key（up-key），共 3 把
	req2, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/api/providers/p1/keys", nil)
	req2.AddCookie(cookie)
	resp, err = http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	var keys []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&keys)
	resp.Body.Close()
	if len(keys) != 3 {
		t.Fatalf("expect 3 keys (incl. migrated legacy api_key), got %d", len(keys))
	}
	if _, ok := keys[0]["cooldown_left"]; !ok {
		t.Fatalf("keys list should expose cooldown_left")
	}

	// 删除一把
	req3, _ := http.NewRequest(http.MethodDelete, ts.URL+"/admin/api/providers/p1/keys/sk-k1", nil)
	req3.AddCookie(cookie)
	resp, err = http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	req4, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/api/providers/p1/keys", nil)
	req4.AddCookie(cookie)
	resp, err = http.DefaultClient.Do(req4)
	if err != nil {
		t.Fatal(err)
	}
	keys = nil
	_ = json.NewDecoder(resp.Body).Decode(&keys)
	resp.Body.Close()
	if len(keys) != 2 {
		t.Fatalf("expect 2 keys left, got %v", keys)
	}
	found := false
	for _, k := range keys {
		if k["key"] == "sk-k2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("sk-k2 should remain, got %v", keys)
	}
}

// ── 统计日期过滤 + SQLite 日志持久化/回放 ──

func TestStatsDateFilterAndLogReplay(t *testing.T) {
	dir := t.TempDir()
	st := NewStore(filepath.Join(dir, "data.json"))
	if err := st.Load(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(st, "test")

	// 注入两条日志：10 天前 1 条失败、今天 1 条成功
	now := time.Now()
	old := now.AddDate(0, 0, -10)
	s.db.InsertLog(RequestLog{Time: old.Unix(), Model: "smart", Provider: "p1", Status: 1, Error: "500 boom"})
	s.db.InsertLog(RequestLog{Time: now.Unix(), Model: "smart", Provider: "p1", Status: 0})

	// 新实例应从 SQLite 读到日志（重启不丢）
	st2 := NewStore(filepath.Join(dir, "data.json"))
	if err := st2.Load(); err != nil {
		t.Fatal(err)
	}
	s2 := NewServer(st2, "test")

	call := func(url string) map[string]any {
		w := httptest.NewRecorder()
		s2.handleStats(w, httptest.NewRequest(http.MethodGet, url, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("stats %s -> %d", url, w.Code)
		}
		var out map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return out
	}
	logsOf := func(m map[string]any) []any {
		return m["logs"].([]any)
	}

	// 缺省：近 7 天 → 只有今天那条
	def := call("/admin/api/stats")
	if len(logsOf(def)) != 1 {
		t.Fatalf("default range expect 1 log, got %d", len(logsOf(def)))
	}
	// range=all → 两条
	all := call("/admin/api/stats?range=all")
	if len(logsOf(all)) != 2 {
		t.Fatalf("range=all expect 2 logs, got %d", len(logsOf(all)))
	}
	// 指定 10 天前当天 → 只有旧那条
	oldDay := old.Format("2006-01-02")
	oldOnly := call("/admin/api/stats?from="+oldDay+"&to="+oldDay)
	if len(logsOf(oldOnly)) != 1 {
		t.Fatalf("old-day filter expect 1 log, got %d", len(logsOf(oldOnly)))
	}
	// 非法日期 → 回退缺省（近7天）
	bad := call("/admin/api/stats?from=not-a-date&to=also-bad")
	if len(logsOf(bad)) != 1 {
		t.Fatalf("invalid dates expect fallback to 1 log, got %d", len(logsOf(bad)))
	}
}

// 旧 requests.jsonl 应一次性迁移进 SQLite 并改名 .imported。
func TestJSONLMigration(t *testing.T) {
	dir := t.TempDir()
	line := `{"time":1795093073,"key_note":"默认密钥","model":"smart","provider":"p1","status":0}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "requests.jsonl"), []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	st := NewStore(filepath.Join(dir, "data.json"))
	if err := st.Load(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(st, "test")

	if _, err := os.Stat(filepath.Join(dir, "requests.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("jsonl should be renamed after migration")
	}
	if _, err := os.Stat(filepath.Join(dir, "requests.jsonl.imported")); err != nil {
		t.Fatalf("imported marker missing: %v", err)
	}
	w := httptest.NewRecorder()
	s.handleStats(w, httptest.NewRequest(http.MethodGet, "/admin/api/stats?range=all", nil))
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	if len(out["logs"].([]any)) != 1 {
		t.Fatalf("expect 1 migrated log, got %v", out["logs"])
	}
}

// ── usage 缓存命中 tokens 提取（OpenAI 风格 / DeepSeek 风格 / 流式） ──

func TestUsageCacheExtraction(t *testing.T) {
	s := &Server{}

	// 非流式：OpenAI 风格 prompt_tokens_details.cached_tokens
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"usage":{"prompt_tokens":100,"completion_tokens":10,"prompt_tokens_details":{"cached_tokens":64}}}`)),
	}
	w := httptest.NewRecorder()
	p, c, ce, err := s.jsonToClient(w, resp)
	if err != nil || p != 100 || c != 10 || ce != 64 {
		t.Fatalf("openai-style expect 100/10/64, got %d/%d/%d err=%v", p, c, ce, err)
	}

	// 非流式：DeepSeek 风格 prompt_cache_hit_tokens
	resp2 := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"usage":{"prompt_tokens":100,"completion_tokens":10,"prompt_cache_hit_tokens":80,"prompt_cache_miss_tokens":20}}`)),
	}
	w2 := httptest.NewRecorder()
	p2, c2, ce2, err := s.jsonToClient(w2, resp2)
	if err != nil || p2 != 100 || c2 != 10 || ce2 != 80 {
		t.Fatalf("deepseek-style expect 100/10/80, got %d/%d/%d err=%v", p2, c2, ce2, err)
	}

	// 流式：最后一个 usage 块生效
	sse := "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n" +
		"data: {\"usage\":{\"prompt_tokens\":50,\"completion_tokens\":5,\"prompt_cache_hit_tokens\":30}}\n\n" +
		"data: [DONE]\n\n"
	resp3 := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(sse)),
	}
	w3 := httptest.NewRecorder()
	p3, c3, ce3 := s.streamToClient(w3, resp3)
	if p3 != 50 || c3 != 5 || ce3 != 30 {
		t.Fatalf("stream expect 50/5/30, got %d/%d/%d", p3, c3, ce3)
	}
}

func TestModelTestEndpoint(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)
	client := &http.Client{}

	// 未登录 → 401
	resp, err := client.Post(ts.URL+"/admin/api/models/test", "application/json", strings.NewReader(`{"model":"smart"}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthed model test expect 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 登录拿 cookie
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/login", strings.NewReader(`{"password":"test-pass"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || len(resp.Cookies()) == 0 {
		t.Fatalf("login should set cookie, got %d", resp.StatusCode)
	}
	cookie := resp.Cookies()[0]
	resp.Body.Close()

	post := func(body string) *http.Response {
		r, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/models/test", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.AddCookie(cookie)
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// 缺 model → 400
	resp = post(`{}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing model expect 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 正常测试 → ok=true，带时延、usage 与回复内容
	resp = post(`{"model":"smart","prompt":"hi","max_tokens":16}`)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("model test expect 200, got %d: %s", resp.StatusCode, b)
	}
	var out struct {
		OK         bool           `json:"ok"`
		Model      string         `json:"model"`
		LatencyMs  int64          `json:"latency_ms"`
		HTTPStatus int            `json:"http_status"`
		Content    string         `json:"content"`
		Usage      map[string]any `json:"usage"`
		Error      string         `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !out.OK || out.Content != "hi" || out.LatencyMs < 0 || out.Usage == nil || out.Model != "smart" {
		t.Fatalf("unexpected test result: %+v", out)
	}

	// 未知模型 → HTTP 200 内 ok=false，带错误信息
	resp = post(`{"model":"no-such"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("test endpoint should always answer 200, got %d", resp.StatusCode)
	}
	var out2 struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out2)
	resp.Body.Close()
	if out2.OK || out2.Error == "" {
		t.Fatalf("unknown model should produce ok=false with error, got %+v", out2)
	}
}

func TestLogsAndDebugPayload(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)
	client := &http.Client{}

	// 未登录 → 401
	resp, err := client.Get(ts.URL + "/admin/api/logs")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthed logs expect 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 登录
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/login", strings.NewReader(`{"password":"test-pass"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || len(resp.Cookies()) == 0 {
		t.Fatalf("login should set cookie, got %d", resp.StatusCode)
	}
	cookie := resp.Cookies()[0]
	resp.Body.Close()

	// 开启调试模式
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/admin/api/settings",
		strings.NewReader(`{"chat_timeout_seconds":30,"models_timeout_seconds":20,"cooldown_base_seconds":5,"cooldown_max_seconds":60,"debug_payloads":true}`))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("settings PUT expect 200, got %d", resp.StatusCode)
	}

	// 确认 GET settings 回读 debug_payloads=true
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/admin/api/settings", nil)
	req.AddCookie(cookie)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var stOut struct {
		DebugPayloads bool `json:"debug_payloads"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&stOut)
	resp.Body.Close()
	if !stOut.DebugPayloads {
		t.Fatal("debug_payloads should persist as true")
	}

	// 发起一次真实对话（应产生带 debug 报文的日志）
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"smart","messages":[{"role":"user","content":"hi"}],"max_tokens":8}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// 查日志
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/admin/api/logs?page=1&page_size=10", nil)
	req.AddCookie(cookie)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logs expect 200, got %d", resp.StatusCode)
	}
	var out struct {
		Logs []struct {
			Model         string `json:"model"`
			UpstreamModel string `json:"upstream_model"`
			Debug         string `json:"debug"`
		} `json:"logs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, l := range out.Logs {
		if l.Model != "smart" {
			continue
		}
		if l.UpstreamModel != "model-a" && l.UpstreamModel != "model-b" {
			t.Fatalf("upstream_model should be recorded, got %q", l.UpstreamModel)
		}
		if !strings.Contains(l.Debug, "client_request") ||
			!strings.Contains(l.Debug, "upstream_request") ||
			!strings.Contains(l.Debug, "upstream_response") ||
			!strings.Contains(l.Debug, `"status":200`) {
			t.Fatalf("debug payload incomplete: %s", l.Debug)
		}
		found = true
	}
	if !found {
		t.Fatalf("expected a smart-model log with debug payload, got %+v", out.Logs)
	}
}

func TestLogsPaginationAndRetention(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	// 直接写入 25 条日志
	now := time.Now().Unix()
	for i := 0; i < 25; i++ {
		s.db.InsertLog(RequestLog{Time: now - int64(i), Model: "m" + strconv.Itoa(i), UpModel: "up-" + strconv.Itoa(i)})
	}
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)
	client := &http.Client{}

	// 登录
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/login", strings.NewReader(`{"password":"test-pass"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	cookie := resp.Cookies()[0]
	resp.Body.Close()

	get := func(q string) (int, int64, int, []RequestLog) {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/api/logs?"+q, nil)
		req.AddCookie(cookie)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Logs     []RequestLog `json:"logs"`
			Total    int64        `json:"total"`
			Page     int          `json:"page"`
			PageSize int          `json:"page_size"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out.Page, out.Total, out.PageSize, out.Logs
	}

	page, total, size, logs := get("page=2&page_size=10")
	if page != 2 || total != 25 || size != 10 || len(logs) != 10 {
		t.Fatalf("page=2 unexpected: page=%d total=%d size=%d len=%d", page, total, size, len(logs))
	}
	if logs[0].Model != "m10" {
		t.Fatalf("page2 first should be m10, got %s", logs[0].Model)
	}
	_, _, _, logs = get("page=3&page_size=10")
	if len(logs) != 5 {
		t.Fatalf("page3 expect 5 rows, got %d", len(logs))
	}
	// 越界页 → 服务端收敛到最后一页
	page, _, _, logs = get("page=9&page_size=10")
	if page != 3 || len(logs) != 5 {
		t.Fatalf("overflow page should clamp to 3 with 5 rows, got page=%d len=%d", page, len(logs))
	}

	// 日志保存天数设置：写入并回读
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/admin/api/settings",
		strings.NewReader(`{"chat_timeout_seconds":30,"log_retention_days":7}`))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/admin/api/settings", nil)
	req.AddCookie(cookie)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var st2 struct {
		LogRetentionDays int `json:"log_retention_days"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&st2)
	resp.Body.Close()
	if st2.LogRetentionDays != 7 {
		t.Fatalf("log_retention_days expect 7, got %d", st2.LogRetentionDays)
	}
}

func TestStatsErrorAccounting(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)
	client := &http.Client{}

	// 两次成功对话
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions",
			strings.NewReader(`{"model":"smart","messages":[{"role":"user","content":"hi"}],"max_tokens":8}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer sk-test")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	// 直接记一次失败尝试（另一供应商维度）
	s.db.UpsertStat(time.Now().Format("2006-01-02"), "p_fail", "fail-model", true, 0, 0, 0, 10)

	// 登录查统计
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/login", strings.NewReader(`{"password":"test-pass"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	cookie := resp.Cookies()[0]
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/admin/api/stats", nil)
	req.AddCookie(cookie)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Totals struct {
			Requests int64 `json:"requests"`
			Errors   int64 `json:"errors"`
		} `json:"totals"`
		Rows []struct {
			ProviderID string `json:"provider_id"`
			Requests   int64  `json:"requests"`
			Errors     int64  `json:"errors"`
		} `json:"rows"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Totals.Requests != 3 || out.Totals.Errors != 1 {
		t.Fatalf("expect requests=3 errors=1, got %+v", out.Totals)
	}
	for _, r := range out.Rows {
		if r.Errors > r.Requests {
			t.Fatalf("errors must never exceed requests, got %+v", r)
		}
	}
}

func TestProviderStatusToggle(t *testing.T) {
	up := fakeUpstream(t, 200, "")
	st := newTestStore(t)
	s := NewServer(st, "test")
	seedTwoProviders(s, up.URL, up.URL)
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)
	client := &http.Client{}

	// 登录
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/login", strings.NewReader(`{"password":"test-pass"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	cookie := resp.Cookies()[0]
	resp.Body.Close()

	toggle := func(id string, enabled bool, want int) {
		t.Helper()
		body := fmt.Sprintf(`{"enabled":%v}`, enabled)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/api/providers/"+id+"/status", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(cookie)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("toggle %s=%v: HTTP %d", id, enabled, resp.StatusCode)
		}
	}

	// 停用 p1
	toggle("p1", false, 200)

	// 列表反映停用状态
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/admin/api/providers", nil)
	req.AddCookie(cookie)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var provs []Provider
	if err := json.NewDecoder(resp.Body).Decode(&provs); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	for _, p := range provs {
		if p.ID == "p1" && p.Enabled {
			t.Fatal("p1 should be disabled")
		}
		if p.ID == "p2" && !p.Enabled {
			t.Fatal("p2 should stay enabled")
		}
	}

	// 平台模型仍可经 p2 成员成功对话（故障转移）
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"smart","messages":[{"role":"user","content":"hi"}],"max_tokens":8}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("chat after disable: HTTP %d", resp.StatusCode)
	}

	// 不存在的供应商 → 404
	toggle("p_missing", false, 404)

	// 重新启用
	toggle("p1", true, 200)
}
