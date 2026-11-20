package gateway

import (
	"net/http"
	_ "embed"
)

//go:embed web/index.html
var indexHTML []byte

// panelHandler 内嵌中文管理面板（单页应用，无外部依赖）。
func (s *Server) panelHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store") // 面板更新后浏览器必须立即拿到新版本
	_, _ = w.Write(indexHTML)
}
