package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// adminLogin 测试用：登录取会话 cookie。
func adminLogin(t *testing.T, baseURL string) *http.Cookie {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/admin/api/login",
		strings.NewReader(`{"password":"test-pass"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(resp.Cookies()) == 0 {
		t.Fatalf("登录应返回 cookie，实际 %d", resp.StatusCode)
	}
	return resp.Cookies()[0]
}

// TestAPIKeyModelWhitelist 密钥限定模型后，白名单内可用、白名单外 403，/v1/models 同步过滤。
func TestAPIKeyModelWhitelist(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/models") {
			w.Write([]byte(`{"data":[{"id":"real-a"}]}`))
			return
		}
		w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	t.Cleanup(up.Close)

	st.Update(func(c *Config) {
		c.APIKeys = []APIKey{
			{Key: "sk-limited", Note: "受限", CreatedAt: 1, Models: []string{"only-a"}},
			{Key: "sk-open", Note: "不限", CreatedAt: 1},
		}
		c.Providers = []Provider{{ID: "p1", Name: "上", Kind: "openai-compat", BaseURL: up.URL, Enabled: true,
			Keys: []ProviderKey{{Key: "k1"}}}}
		c.ProviderModels = []ProviderModel{
			{ID: "pm-a", ProviderID: "p1", Model: "real-a", Enabled: true},
			{ID: "pm-b", ProviderID: "p1", Model: "real-b", Enabled: true},
		}
		c.PlatformModels = []PlatformModel{
			{ID: "plat-a", Name: "only-a", Enabled: true, Strategy: "round-robin",
				Members: []Member{{ProviderModelID: "pm-a", Enabled: true}}},
			{ID: "plat-b", Name: "other-b", Enabled: true, Strategy: "round-robin",
				Members: []Member{{ProviderModelID: "pm-b", Enabled: true}}},
		}
	})

	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	// ① 白名单内的模型：放行
	resp := chatRequest(t, ts.URL, "sk-limited", `{"model":"only-a","messages":[{"role":"user","content":"hi"}]}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("白名单内应放行，实际 %d", resp.StatusCode)
	}

	// ② 白名单外的模型：403
	resp2 := chatRequest(t, ts.URL, "sk-limited", `{"model":"other-b","messages":[{"role":"user","content":"hi"}]}`)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("白名单外应返回 403，实际 %d", resp2.StatusCode)
	}

	// ③ 未限定模型的密钥：两个都能用
	for _, m := range []string{"only-a", "other-b"} {
		r := chatRequest(t, ts.URL, "sk-open", `{"model":"`+m+`","messages":[{"role":"user","content":"hi"}]}`)
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("不限密钥调用 %s 应放行，实际 %d", m, r.StatusCode)
		}
	}

	// ④ /v1/models 按密钥过滤
	req := httptest.NewRequest(http.MethodGet, ts.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk-limited")
	rec := httptest.NewRecorder()
	ts.Config.Handler.ServeHTTP(rec, req)
	body := rec.Body.String()
	if strings.Contains(body, "other-b") {
		t.Fatalf("受限密钥的模型列表不应包含 other-b，实际: %s", body)
	}
	if !strings.Contains(body, "only-a") {
		t.Fatalf("受限密钥的模型列表应包含 only-a，实际: %s", body)
	}
}

// TestAPIKeyEditNoteAndModels PUT 可改备注与可用模型。
func TestAPIKeyEditNoteAndModels(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")
	st.Update(func(c *Config) {
		c.Password = "test-pass"
		c.APIKeys = []APIKey{{Key: "sk-x", Note: "旧", CreatedAt: 1}}
	})
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)

	login := adminLogin(t, ts.URL)

	req := httptest.NewRequest(http.MethodPut, ts.URL+"/admin/api/keys/sk-x",
		strings.NewReader(`{"note":"新备注","models":[" m1 ","m1","m2",""]}`))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(login)
	rec := httptest.NewRecorder()
	ts.Config.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("改密钥应成功，实际 %d: %s", rec.Code, rec.Body.String())
	}

	st.View(func(c *Config) {
		k := c.APIKeys[0]
		if k.Note != "新备注" {
			t.Fatalf("备注应更新，实际 %q", k.Note)
		}
		if len(k.Models) != 2 || k.Models[0] != "m1" || k.Models[1] != "m2" {
			t.Fatalf("模型应去空白去重后为 [m1 m2]，实际 %v", k.Models)
		}
	})

	// 清空 models 表示不限制
	req2 := httptest.NewRequest(http.MethodPut, ts.URL+"/admin/api/keys/sk-x",
		strings.NewReader(`{"models":[]}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.AddCookie(login)
	rec2 := httptest.NewRecorder()
	ts.Config.Handler.ServeHTTP(rec2, req2)
	st.View(func(c *Config) {
		if len(c.APIKeys[0].Models) != 0 {
			t.Fatalf("清空后应为不限制，实际 %v", c.APIKeys[0].Models)
		}
		if c.APIKeys[0].Note != "新备注" {
			t.Fatalf("只传 models 时不应改动备注，实际 %q", c.APIKeys[0].Note)
		}
	})
}
