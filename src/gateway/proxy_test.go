package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// startFakeProxy 起一个 HTTP 正向代理：对绝对 URI 请求直接返回
// 指定的完成报文并记一次命中（不真正转发到上游）。
func startFakeProxy(t *testing.T, reply string) (*httptest.Server, *int) {
	t.Helper()
	hits := 0
	px := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, reply)
	}))
	t.Cleanup(px.Close)
	return px, &hits
}

func TestProxyUsedForChat(t *testing.T) {
	// 上游：一旦直连到达即失败——证明流量必须走代理
	upHits := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upHits++
		http.Error(w, "direct hit should not happen", http.StatusBadGateway)
	}))
	defer up.Close()

	px, pxHits := startFakeProxy(t, `{"choices":[{"message":{"content":"via-proxy"}}]}`)

	u := newUpstream(Provider{ID: "p1", Name: "t", BaseURL: up.URL, Proxy: px.URL}, 5*time.Second, 5*time.Second)
	resp, err := u.Chat(context.Background(), "m", map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, false, "k")
	if err != nil {
		t.Fatalf("Chat 经代理失败: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "via-proxy") {
		t.Fatalf("代理路径响应异常: %d %s", resp.StatusCode, body)
	}
	if *pxHits != 1 {
		t.Fatalf("代理命中 %d 次，应为 1", *pxHits)
	}
	if upHits != 0 {
		t.Fatalf("上游被直连 %d 次，代理未生效", upHits)
	}
}

func TestProxyUsedForModels(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no direct", 502)
	}))
	defer up.Close()
	px, pxHits := startFakeProxy(t, `{"data":[{"id":"m-x"}]}`)

	u := newUpstream(Provider{ID: "p1", Name: "t", BaseURL: up.URL, Proxy: px.URL}, 5*time.Second, 5*time.Second)
	models, err := u.Models(context.Background(), "k")
	if err != nil {
		t.Fatalf("Models 经代理失败: %v", err)
	}
	if len(models) != 1 || models[0].ID != "m-x" || *pxHits != 1 {
		t.Fatalf("Models 代理路径异常: models=%v hits=%d", models, *pxHits)
	}
}

func TestProxyInvalidFallsBackDirect(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"content":"direct"}}]}`)
	}))
	defer up.Close()

	u := newUpstream(Provider{ID: "p1", Name: "t", BaseURL: up.URL, Proxy: "://not-a-url"}, 5*time.Second, 5*time.Second)
	resp, err := u.Chat(context.Background(), "m", map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, false, "k")
	if err != nil {
		t.Fatalf("非法代理地址应降级直连: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "direct") {
		t.Fatalf("降级直连未生效: %s", body)
	}
}

func TestProxyEmptyDirect(t *testing.T) {
	upHits := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upHits++
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"content":"direct"}}]}`)
	}))
	defer up.Close()

	u := newUpstream(Provider{ID: "p1", Name: "t", BaseURL: up.URL}, 5*time.Second, 5*time.Second)
	if _, err := u.Chat(context.Background(), "m", map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, false, "k"); err != nil {
		t.Fatal(err)
	}
	if upHits != 1 {
		t.Fatalf("留空代理应直连，上游命中 %d", upHits)
	}
}
