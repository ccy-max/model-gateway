package gateway

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestPanelGzipAndETag 面板 gzip 压缩与 ETag/304。
func TestPanelGzipAndETag(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	// 1) 带 gzip：应压缩且可解压出完整 HTML
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	enc := resp.Header.Get("Content-Encoding")
	etag := resp.Header.Get("ETag")
	if enc != "gzip" {
		t.Fatalf("应返回 gzip 编码，实际 %q", enc)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	gr, err := gzip.NewReader(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("解压失败: %v", err)
	}
	plain, _ := io.ReadAll(gr)
	if len(plain) < 50000 || !strings.Contains(string(plain), "<html") {
		t.Fatalf("解压内容异常，长度 %d", len(plain))
	}
	if len(raw) >= len(plain)/2 {
		t.Fatalf("压缩效果不足: %d vs %d", len(raw), len(plain))
	}

	// 2) 不带 gzip：明文
	req2, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req2.Header.Set("Accept-Encoding", "identity")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	if enc2 := resp2.Header.Get("Content-Encoding"); enc2 != "" {
		t.Fatalf("不支持 gzip 的客户端不应压缩，实际 %q", enc2)
	}

	// 3) ETag 命中 → 304
	req3, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req3.Header.Set("If-None-Match", etag)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusNotModified {
		t.Fatalf("ETag 命中应 304，实际 %d", resp3.StatusCode)
	}
}
