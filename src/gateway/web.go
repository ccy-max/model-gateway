package gateway

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"sync"
	_ "embed"
)

//go:embed web/index.html
var indexHTML []byte

var (
	panelOnce sync.Once
	panelGz   []byte
	panelETag string
)

// initPanelAssets 预压缩面板（一次性，之后只读）。
func initPanelAssets() {
	panelOnce.Do(func() {
		sum := sha1.Sum(indexHTML)
		panelETag = `"` + hex.EncodeToString(sum[:8]) + `"`
		var buf bytes.Buffer
		gz, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		if err != nil {
			return
		}
		if _, err := gz.Write(indexHTML); err != nil {
			return
		}
		if err := gz.Close(); err != nil {
			return
		}
		panelGz = buf.Bytes()
	})
}

// panelHandler 内嵌中文管理面板（单页应用，无外部依赖）。
// 支持 gzip（面板约 108KB → ~22KB，显著改善手机/弱网加载）与 ETag/304。
func (s *Server) panelHandler(w http.ResponseWriter, r *http.Request) {
	initPanelAssets()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache") // 可缓存但每次需校验：ETag 不变即 304 秒开
	w.Header().Set("ETag", panelETag)
	w.Header().Add("Vary", "Accept-Encoding")
	if match := r.Header.Get("If-None-Match"); match != "" && strings.Contains(match, panelETag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	body := indexHTML
	if len(panelGz) > 0 && acceptsGzip(r.Header.Get("Accept-Encoding")) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", strconv.Itoa(len(panelGz)))
		body = panelGz
	} else {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	}
	_, _ = w.Write(body)
}

// acceptsGzip 简易 Accept-Encoding 判定。
func acceptsGzip(ae string) bool {
	for _, part := range strings.Split(ae, ",") {
		p := strings.TrimSpace(part)
		if p == "" {
			continue
		}
		if strings.HasPrefix(p, "gzip") {
			return !strings.Contains(p, "q=0")
		}
	}
	return false
}
