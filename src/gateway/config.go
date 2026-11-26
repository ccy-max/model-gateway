// Package gateway 实现公共模型网关：
//
//	平台模型(对外 OpenAI 协议) → 多供应商成员路由(加权/优先级/粘性/冷却/故障转移)
//	→ OpenAI-compatible 上游适配(现有 Qoder 服务即一个兼容端点)。
//
// 设计要点：
//   - 配置持久化为单个 JSON(原子写 tmp+rename)，向后兼容字段全部 omitempty；
//   - 运行时统计/日志存内存，定期合并落盘；
//   - 上游统一按 OpenAI-compatible 处理，本包不依赖仓库内旧包(旧服务独立运行)；
//   - 对外端点：/v1/chat/completions、/v1/models、/health、/admin(面板+API)。
package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ---------- 配置结构 ----------

// APIKey 客户端调用密钥。
type APIKey struct {
	Key       string `json:"key"`
	Note      string `json:"note,omitempty"`
	CreatedAt int64  `json:"created_at"`
	Disabled  bool   `json:"disabled,omitempty"`
}

// ProviderKey 供应商密钥池中的一把 key：同平台多 key 按 key 负载均衡。
type ProviderKey struct {
	Key           string `json:"key"`
	Note          string `json:"note,omitempty"`
	Weight        int    `json:"weight,omitempty"`   // 分流权重，默认 1
	Priority      int    `json:"priority,omitempty"` // 备用序，数字小者优先，默认 0
	CreatedAt     int64  `json:"created_at,omitempty"`
	Disabled      bool   `json:"disabled,omitempty"`
	MaxInflight   int    `json:"max_inflight,omitempty"`    // 最大并发数（同时处理的上限），0=不限
	RateLimit     int    `json:"rate_limit,omitempty"`      // 时间窗内最多请求数，0=不限
	RateWindowSec int    `json:"rate_window_sec,omitempty"` // 时间窗长度（秒），与 RateLimit 配合使用
}

// Provider 上游供应商。MVP 阶段 Kind 恒为 openai-compat；
// 预留 anthropic / gemini 原生适配。
// Keys 为密钥池：请求在该供应商的模型上执行时按 key 轮询/权重分流，
// 单 key 失败(401/403/429/5xx/网络)自动冷却并轮换下一把。
type Provider struct {
	ID      string        `json:"id"`
	Name    string        `json:"name"`
	Kind    string        `json:"kind"`
	BaseURL string        `json:"base_url"`
	APIKey  string        `json:"api_key,omitempty"` // 旧字段：仅作兼容，Normalize 时并入 Keys
	Keys    []ProviderKey `json:"keys,omitempty"`
	Note    string        `json:"note,omitempty"`
	Enabled bool          `json:"enabled"`
	// Proxy 上游调用代理地址（http/https/socks5，如 http://mihomo:7890）；
	// 留空 = 直连。注意：须为容器网络可达地址（127.0.0.1 是容器自身，不可用）。
	Proxy string `json:"proxy,omitempty"`
}

// EnabledKeys 返回启用的非空密钥（旧单 Key 字段已由 Normalize 并入）。
func (p Provider) EnabledKeys() []ProviderKey {
	out := make([]ProviderKey, 0, len(p.Keys))
	for _, k := range p.Keys {
		if !k.Disabled && k.Key != "" {
			out = append(out, k)
		}
	}
	return out
}

// MaskKey 密钥掩码（日志/面板展示用）。
func MaskKey(k string) string {
	if len(k) <= 14 {
		return k
	}
	return k[:8] + "..." + k[len(k)-4:]
}

// ProviderModel 供应商侧的具体模型（可被平台模型引用为成员）。
type ProviderModel struct {
	ID         string   `json:"id"`
	ProviderID string   `json:"provider_id"`
	Model      string   `json:"model"` // 上游真实模型名
	Caps       []string `json:"caps,omitempty"`
	Note       string   `json:"note,omitempty"`
	Enabled    bool     `json:"enabled"`
}

// Member 平台模型成员：引用一个供应商模型并声明权重/优先级。
type Member struct {
	ProviderModelID string `json:"provider_model_id"`
	Weight          int    `json:"weight"`
	Priority        int    `json:"priority"` // 越小越优先，故障转移按此顺序
	Enabled         bool   `json:"enabled"`
}

// PlatformModel 对外模型：聚合多个成员，按策略负载均衡与故障转移。
// Name 与 Aliases 都可作为客户端请求的 model 名。
type PlatformModel struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Aliases     []string `json:"aliases,omitempty"`
	Strategy    string   `json:"strategy,omitempty"` // sticky|round-robin|least-inflight
	StickyLimit int      `json:"sticky_limit,omitempty"`
	MaxRetry    int      `json:"max_retry_members,omitempty"`
	Enabled     bool     `json:"enabled"`
	Members     []Member `json:"members"`
}

// Settings 全局设置。
type Settings struct {
	ChatTimeoutSec       int  `json:"chat_timeout_seconds"`        // 上游对话超时；10~1800，默认 600
	ModelsTimeoutSec     int  `json:"models_timeout_seconds"`      // 拉取上游模型目录超时
	CooldownBaseSec      int  `json:"cooldown_base_seconds"`       // 密钥失败冷却基数（指数退避）
	CooldownMaxSec       int  `json:"cooldown_max_seconds"`        // 密钥冷却上限
	ExhaustedCooldownSec int  `json:"exhausted_cooldown_seconds"`  // 供应商额度耗尽（402）后的整体跳过时长，默认 1800
	DebugPayloads        bool `json:"debug_payloads"`              // 开启后请求日志额外记录请求/响应报文
	LogRetentionDays     int  `json:"log_retention_days"`          // 请求日志保留天数（<=0 视为 30）
}

// DailyStat 单日按 供应商×平台模型 聚合的用量（含失败尝试）。
type DailyStat struct {
	Day        string `json:"day"`
	ProviderID string `json:"provider_id"`
	Model      string `json:"model"` // 平台模型名
	Requests   int64  `json:"requests"`
	Errors     int64  `json:"errors"`
	PromptTok  int64  `json:"prompt_tokens"`
	ComplTok   int64  `json:"completion_tokens"`
	CachedTok  int64  `json:"cached_tokens,omitempty"` // 输入侧缓存命中 tokens
	CReq       int64  `json:"client_requests,omitempty"` // 业务请求数（一次客户端请求只计 1，attempt 级见 requests）
	CFail      int64  `json:"client_errors,omitempty"`   // 业务失败数（该客户端请求最终失败才计 1）
	LatencySum int64  `json:"latency_ms_sum"`
	TTFTSum    int64  `json:"ttft_ms_sum,omitempty"` // 流式首字时间累计（非流式/失败为 0）
	TTFTN      int64  `json:"ttft_n,omitempty"`      // 有首字记录的请求数（仅流式），用于平均
}

// RequestLog 单次请求日志。
type RequestLog struct {
	Time       int64  `json:"time"`
	ReqID      string `json:"req_id,omitempty"`  // 业务请求 ID（一次客户端请求内多个尝试共享）
	Final      bool   `json:"final,omitempty"`   // 该次尝试是否为业务请求的最后一次（成功或最终失败）
	KeyNote    string `json:"key_note,omitempty"`
	Model      string `json:"model"`
	Provider   string `json:"provider,omitempty"`
	ProviderID string `json:"provider_id,omitempty"`
	UpModel    string `json:"upstream_model,omitempty"`
	UpKey      string `json:"upstream_key,omitempty"`
	Status    int    `json:"status"`
	Latency   int64  `json:"latency_ms"`
	Stream    bool   `json:"stream"`
	PromptTok int64  `json:"prompt_tokens,omitempty"`
	ComplTok  int64  `json:"completion_tokens,omitempty"`
	CachedTok int64  `json:"cached_tokens,omitempty"` // 输入侧缓存命中 tokens
	TTFT      int64  `json:"ttft_ms,omitempty"`       // 流式首字时间（毫秒）；非流式或失败为 0
	Error     string `json:"error,omitempty"`
	Debug     string `json:"debug,omitempty"` // 调试模式下的请求/响应报文（JSON 字符串）
}

// Config 持久化配置根。
type Config struct {
	Password       string          `json:"password"`
	APIKeys        []APIKey        `json:"api_keys"`
	Providers      []Provider      `json:"providers"`
	ProviderModels []ProviderModel `json:"provider_models"`
	PlatformModels []PlatformModel `json:"platform_models"`
	Settings       Settings        `json:"settings"`
	DailyStats     []DailyStat     `json:"daily_stats,omitempty"`
}

// Normalize 补齐默认值，保证零值可用。
func (c *Config) Normalize() {
	// 对话超时：默认 600s，允许 10~1800s（过大值会让挂起请求长期占用密钥并发槽）
	if c.Settings.ChatTimeoutSec <= 0 {
		c.Settings.ChatTimeoutSec = 600
	}
	if c.Settings.ChatTimeoutSec < 10 {
		c.Settings.ChatTimeoutSec = 10
	}
	if c.Settings.ChatTimeoutSec > 1800 {
		c.Settings.ChatTimeoutSec = 1800
	}
	if c.Settings.ExhaustedCooldownSec <= 0 {
		c.Settings.ExhaustedCooldownSec = 1800
	}
	if c.Settings.ModelsTimeoutSec <= 0 {
		c.Settings.ModelsTimeoutSec = 15
	}
	if c.Settings.CooldownBaseSec <= 0 {
		c.Settings.CooldownBaseSec = 10
	}
	if c.Settings.CooldownMaxSec <= 0 {
		c.Settings.CooldownMaxSec = 600
	}
	for i := range c.PlatformModels {
		pm := &c.PlatformModels[i]
		if pm.Strategy == "" {
			pm.Strategy = "sticky"
		}
		if pm.MaxRetry <= 0 {
			pm.MaxRetry = 3
		}
		for j := range pm.Members {
			if pm.Members[j].Weight <= 0 {
				pm.Members[j].Weight = 1
			}
		}
	}
	// 旧单 Key 字段迁移进密钥池
	for i := range c.Providers {
		p := &c.Providers[i]
		if len(p.Keys) == 0 && p.APIKey != "" {
			p.Keys = []ProviderKey{{Key: p.APIKey, CreatedAt: time.Now().Unix()}}
		}
		for j := range p.Keys {
			if p.Keys[j].Weight <= 0 {
				p.Keys[j].Weight = 1
			}
		}
	}
}

// FindProvider 按 ID 查供应商。
func (c *Config) FindProvider(id string) (Provider, bool) {
	for _, p := range c.Providers {
		if p.ID == id {
			return p, true
		}
	}
	return Provider{}, false
}

// FindProviderModel 按 ID 查供应商模型。
func (c *Config) FindProviderModel(id string) (ProviderModel, bool) {
	for _, m := range c.ProviderModels {
		if m.ID == id {
			return m, true
		}
	}
	return ProviderModel{}, false
}

// ResolvePlatform 按对外名（Name 或 Alias）解析平台模型。
func (c *Config) ResolvePlatform(name string) (PlatformModel, bool) {
	for _, pm := range c.PlatformModels {
		if !pm.Enabled {
			continue
		}
		if pm.Name == name {
			return pm, true
		}
		for _, a := range pm.Aliases {
			if a == name {
				return pm, true
			}
		}
	}
	return PlatformModel{}, false
}

// ---------- 存储 ----------

// Store 配置存储：内存读写 + 合并落盘。
type Store struct {
	mu    sync.RWMutex
	path  string
	cfg   Config
	dirty bool
	stop  chan struct{}
	once  sync.Once
}

// NewStore 创建存储（不读盘，需再调 Load）。
func NewStore(path string) *Store {
	return &Store{path: path, stop: make(chan struct{})}
}

// Path 返回配置文件所在路径（数据目录派生用）。
func (s *Store) Path() string { return s.path }

// Load 读取磁盘配置；文件不存在视为空配置。
func (s *Store) Load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.mu.Lock()
			s.cfg.Normalize()
			s.mu.Unlock()
			return nil
		}
		return err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return err
	}
	cfg.Normalize()
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	return nil
}

// View 读锁访问配置；fn 内不得修改配置或持有引用逃逸。
func (s *Store) View(fn func(*Config)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn(&s.cfg)
}

// Update 写锁变更配置并标记脏；落盘由 flusher 合并执行。
func (s *Store) Update(fn func(*Config)) {
	s.mu.Lock()
	fn(&s.cfg)
	s.cfg.Normalize()
	s.dirty = true
	s.mu.Unlock()
}

// Flush 脏时立即原子落盘。
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *Store) flushLocked() error {
	if !s.dirty {
		return nil
	}
	b, err := json.MarshalIndent(&s.cfg, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

// RunFlusher 周期落盘直到 Close。
func (s *Store) RunFlusher(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			_ = s.Flush()
		}
	}
}

// Close 停止 flusher 并做最终落盘。
func (s *Store) Close() {
	s.once.Do(func() { close(s.stop) })
	_ = s.Flush()
}

// NewID 生成带前缀的随机 ID。
func NewID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}
