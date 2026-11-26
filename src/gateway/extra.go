package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// handleMetrics Prometheus 文本指标：进程内计数器 + 供应商运行态 gauge。
// 说明：不含密钥级指标，避免把密钥写进监控系统。
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	var b strings.Builder
	b.WriteString(s.metrics.Render())

	var provs []Provider
	s.store.View(func(c *Config) {
		provs = append(provs, c.Providers...)
	})
	snap := s.router.ProviderSnapshot()

	b.WriteString("# HELP gw_provider_exhausted 供应商额度耗尽中（1=整体跳过）\n# TYPE gw_provider_exhausted gauge\n")
	for _, p := range provs {
		v := 0
		if snap[p.ID][0] > 0 {
			v = 1
		}
		fmt.Fprintf(&b, "gw_provider_exhausted{provider=\"%s\"} %d\n", escLabel(p.Name), v)
	}
	b.WriteString("# HELP gw_provider_breaker_open 供应商熔断中（1=整体跳过）\n# TYPE gw_provider_breaker_open gauge\n")
	for _, p := range provs {
		v := 0
		if snap[p.ID][1] > 0 {
			v = 1
		}
		fmt.Fprintf(&b, "gw_provider_breaker_open{provider=\"%s\"} %d\n", escLabel(p.Name), v)
	}
	b.WriteString("# HELP gw_provider_up 供应商启用状态（1=启用）\n# TYPE gw_provider_up gauge\n")
	for _, p := range provs {
		v := 0
		if p.Enabled {
			v = 1
		}
		fmt.Fprintf(&b, "gw_provider_up{provider=\"%s\"} %d\n", escLabel(p.Name), v)
	}
	_, _ = w.Write([]byte(b.String()))
}

// handleProviderRecover 手动恢复：清除额度耗尽与熔断标记（面板「恢复」按钮）。
func (s *Server) handleProviderRecover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
		return
	}
	id := r.PathValue("id")
	exists := false
	s.store.View(func(c *Config) {
		for _, p := range c.Providers {
			if p.ID == id {
				exists = true
				break
			}
		}
	})
	if !exists {
		writeErr(w, http.StatusNotFound, "not_found", "供应商不存在")
		return
	}
	s.router.ClearExhausted(id)
	s.router.ClearBreaker(id)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleConfigExport 导出完整配置（含密钥，仅管理员可取）。
func (s *Server) handleConfigExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
		return
	}
	var cfg Config
	s.store.View(func(c *Config) {
		cfg = *c
		cfg.DailyStats = nil // 旧统计已在 SQLite，不重复导出
	})
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="model-gateway-config-%s.json"`, time.Now().Format("20060102-150405")))
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, cfg)
}

// handleConfigImport 导入配置：整体替换密码/密钥/供应商/平台模型/设置（密钥同样在内）。
func (s *Server) handleConfigImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
		return
	}
	var body Config
	if err := jsonDecodeLimited(r, &body, 16<<20); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "配置文件不合法: "+err.Error())
		return
	}
	if body.Password == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "配置缺少管理密码，已拒绝导入（防止导入后无法登录）")
		return
	}
	s.store.Update(func(c *Config) {
		c.Password = body.Password
		c.APIKeys = body.APIKeys
		c.Providers = body.Providers
		c.ProviderModels = body.ProviderModels
		c.PlatformModels = body.PlatformModels
		c.Settings = body.Settings
	})
	s.invalidateUpstreams()
	_ = s.store.Flush()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "providers": len(body.Providers), "platform_models": len(body.PlatformModels)})
}

// jsonDecodeLimited 按上限读取并解析 JSON。
func jsonDecodeLimited(r *http.Request, v any, limit int64) error {
	return json.NewDecoder(http.MaxBytesReader(nil, r.Body, limit)).Decode(v)
}
