package gateway

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	maxBodyBytes  = 32 << 20 // 与上游补丁一致的安全水位
	sessionCookie = "gw_session"
	sessionTTL    = 7 * 24 * time.Hour
)

// Server 网关 HTTP 服务。
type Server struct {
	store   *Store
	router  *Router
	version string

	sessMu   sync.Mutex
	sessions map[string]time.Time

	db *DB // SQLite：请求日志 + 每日统计

	upMu sync.RWMutex
	ups  map[string]*upstream
}

// NewServer 创建服务；打开 SQLite 并迁移历史数据（requests.jsonl / 配置内统计）。
func NewServer(store *Store, version string) *Server {
	s := &Server{
		store:    store,
		router:   NewRouter(),
		version:  version,
		sessions: map[string]time.Time{},
		ups:      map[string]*upstream{},
	}
	dataDir := filepath.Dir(store.Path())
	db, err := OpenDB(dataDir)
	if err != nil {
		log.Fatalf("[gateway] SQLite 打开失败: %v", err)
	}
	s.db = db
	db.MigrateJSONL(dataDir)
	db.MigrateConfigStats(store)
	go db.CleanupLoop(30, 90) // 日志留 30 天，统计留 90 天
	return s
}

// ---------- 上游客户端缓存 ----------

func (s *Server) getUpstream(p Provider) *upstream {
	s.upMu.RLock()
	u, ok := s.ups[p.ID]
	s.upMu.RUnlock()
	if ok {
		return u
	}
	s.upMu.Lock()
	defer s.upMu.Unlock()
	if u, ok := s.ups[p.ID]; ok {
		return u
	}
	var settings Settings
	s.store.View(func(c *Config) { settings = c.Settings })
	u = newUpstream(p, time.Duration(settings.ChatTimeoutSec)*time.Second, time.Duration(settings.ModelsTimeoutSec)*time.Second)
	s.ups[p.ID] = u
	return u
}

func (s *Server) invalidateUpstreams() {
	s.upMu.Lock()
	s.ups = map[string]*upstream{}
	s.upMu.Unlock()
}

// ---------- 通用工具 ----------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{"message": msg, "type": code, "code": status},
	})
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	return dec.Decode(v)
}

// checkAPIKey 校验 Bearer 密钥（常数时间比较）。
func (s *Server) checkAPIKey(r *http.Request) (APIKey, bool) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return APIKey{}, false
	}
	key := strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	if key == "" {
		return APIKey{}, false
	}
	var out APIKey
	found := false
	s.store.View(func(c *Config) {
		for _, k := range c.APIKeys {
			if k.Disabled {
				continue
			}
			if subtle.ConstantTimeCompare([]byte(k.Key), []byte(key)) == 1 {
				out, found = k, true
				return
			}
		}
	})
	return out, found
}

// stickyKeyFromRequest 会话粘性键：优先 X-Session-Id，否则取最后一条 user 消息哈希。
func stickyKeyFromRequest(r *http.Request, payload map[string]any) string {
	if v := strings.TrimSpace(r.Header.Get("X-Session-Id")); v != "" {
		return "h:" + v
	}
	msgs, _ := payload["messages"].([]any)
	for i := len(msgs) - 1; i >= 0; i-- {
		m, ok := msgs[i].(map[string]any)
		if !ok {
			continue
		}
		if role, _ := m["role"].(string); role == "user" {
			var content string
			if c, ok := m["content"].(string); ok {
				content = c
			} else {
				b, _ := json.Marshal(m["content"])
				content = string(b)
			}
			sum := sha256.Sum256([]byte(content))
			return "m:" + hex.EncodeToString(sum[:8])
		}
	}
	return ""
}

// ---------- 推理端点 ----------

type attemptInfo struct {
	cand Candidate
	prov Provider
	pmod ProviderModel
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	key, ok := s.checkAPIKey(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "invalid_api_key", "无效的 API Key")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "读取请求体失败: "+err.Error())
		return
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不是合法 JSON: "+err.Error())
		return
	}
	modelName, _ := payload["model"].(string)
	if modelName == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "缺少 model 字段")
		return
	}
	s.dispatchChat(w, r, payload, key)
}

// dispatchChat 对话主流程：平台模型解析 → 成员候选构建 → 策略选路 → 密钥池轮换与故障转移。
// 供 /v1/chat/completions 与管理端「模型测试」共用（测试经 httptest.Recorder 承接响应）。
func (s *Server) dispatchChat(w http.ResponseWriter, r *http.Request, payload map[string]any, key APIKey) {
	modelName, _ := payload["model"].(string)
	var pm PlatformModel
	found := false
	s.store.View(func(c *Config) { pm, found = c.ResolvePlatform(modelName) })
	if !found {
		writeErr(w, http.StatusNotFound, "model_not_found", "未知模型: "+modelName)
		return
	}
	wantStream := false
	if b, ok := payload["stream"].(bool); ok {
		wantStream = b
	}
	stickyKey := stickyKeyFromRequest(r, payload)

	// 构建候选（成员→供应商模型→供应商 全链路启用才可用）
	var cands []attemptInfo
	s.store.View(func(c *Config) {
		provByID := make(map[string]Provider, len(c.Providers))
		for _, p := range c.Providers {
			provByID[p.ID] = p
		}
		pmodByID := make(map[string]ProviderModel, len(c.ProviderModels))
		for _, m := range c.ProviderModels {
			pmodByID[m.ID] = m
		}
		for _, mem := range pm.Members {
			if !mem.Enabled {
				continue
			}
			pmod, ok1 := pmodByID[mem.ProviderModelID]
			if !ok1 || !pmod.Enabled {
				continue
			}
			prov, ok2 := provByID[pmod.ProviderID]
			if !ok2 || !prov.Enabled || prov.Kind != "openai-compat" {
				continue
			}
			weight := mem.Weight
			if weight <= 0 {
				weight = 1
			}
			cands = append(cands, attemptInfo{
				cand: Candidate{Key: pm.ID + "|" + pmod.ID, Weight: weight, Priority: mem.Priority, Available: true},
				prov: prov,
				pmod: pmod,
			})
		}
	})
	if len(cands) == 0 {
		writeErr(w, http.StatusServiceUnavailable, "no_available_member", "平台模型没有可用成员")
		return
	}

	var settings Settings
	s.store.View(func(c *Config) { settings = c.Settings })

	candSlice := make([]Candidate, len(cands))
	attemptByKey := make(map[string]attemptInfo, len(cands))
	for i, a := range cands {
		candSlice[i] = a.cand
		attemptByKey[a.cand.Key] = a
	}
	order := s.router.Pick(candSlice, pm.Strategy, stickyKey, pm.StickyLimit)
	maxRetry := pm.MaxRetry
	if maxRetry <= 0 {
		maxRetry = 3
	}
	if maxRetry > len(order) {
		maxRetry = len(order)
	}

	tried := 0
	lastErr := "无可用成员"
	for _, cd := range order {
		if tried >= maxRetry {
			break
		}
		a, ok := attemptByKey[cd.Key]
		if !ok {
			continue
		}
		tried++
		res := s.tryMember(r, w, a.prov, a.pmod, pm, payload, wantStream, key.Note, settings)
		if res.handled {
			if res.status == http.StatusOK && stickyKey != "" && pm.Strategy == "sticky" {
				s.router.RecordSticky(stickyKey, cd.Key)
			}
			return
		}
		lastErr = res.errMsg
	}
	writeErr(w, http.StatusBadGateway, "all_members_failed", fmt.Sprintf("尝试 %d 个成员均失败: %s", tried, lastErr))
}

// attemptResult 单次成员尝试结果。
type attemptResult struct {
	handled bool // 响应已写回客户端（成功，或不可转移的错误）
	status  int
	prompt  int64
	compl   int64
	errMsg  string
}

// tryMember 对单个成员发起调用：先在供应商密钥池内做第二轮负载均衡
// （按权重轮询 + 失败冷却），key 级失败(401/402/403/408/429/5xx/网络)自动轮换下一把；
// 全部 key 失败才把该成员判为失败（handled=false，交回成员级故障转移）。
// 非 key 级 4xx（如 400/404 请求本身有误）直接写回客户端，不再轮换。
func (s *Server) tryMember(r *http.Request, w http.ResponseWriter, prov Provider, pmod ProviderModel, pm PlatformModel, payload map[string]any, wantStream bool, keyNote string, settings Settings) attemptResult {
	memberKey := pm.ID + "|" + pmod.ID
	s.router.Inflight(memberKey, 1)
	defer s.router.Inflight(memberKey, -1)

	keys := prov.EnabledKeys()
	if len(keys) == 0 {
		s.recordAttempt(prov, pm.Name, pmod.Model, keyNote, "", wantStream, 0, 0, 0, 0, true, "无可用密钥")
		return attemptResult{handled: false, errMsg: prov.Name + ": 无可用密钥"}
	}

	cands := make([]Candidate, len(keys))
	byCand := make(map[string]ProviderKey, len(keys))
	for i, k := range keys {
		weight := k.Weight
		if weight <= 0 {
			weight = 1
		}
		ck := prov.ID + "|k|" + k.Key
		cands[i] = Candidate{Key: ck, Weight: weight, Priority: k.Priority, Available: true}
		byCand[ck] = k
	}
	order := s.router.Pick(cands, "round-robin", "", 0)
	maxKeyTries := len(order)
	if maxKeyTries > 5 {
		maxKeyTries = 5
	}

	up := s.getUpstream(prov)
	lastErr := ""
	for i, cd := range order {
		if i >= maxKeyTries {
			break
		}
		pk := byCand[cd.Key]
		keyMask := MaskKey(pk.Key)

		s.router.Inflight(cd.Key, 1)
		start := time.Now()
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(settings.ChatTimeoutSec)*time.Second)
		resp, err := up.Chat(ctx, pmod.Model, payload, wantStream, pk.Key)
		latency := time.Since(start).Milliseconds()

		if err != nil {
			cancel()
			s.router.Inflight(cd.Key, -1)
			s.router.MarkFailure(cd.Key, settings.CooldownBaseSec, settings.CooldownMaxSec)
			s.recordAttempt(prov, pm.Name, pmod.Model, keyNote, keyMask, wantStream, latency, 0, 0, 0, true, err.Error())
			lastErr = fmt.Sprintf("%s(%s): %v", prov.Name, keyMask, err)
			continue
		}

		if resp.StatusCode == http.StatusOK {
			var prompt, compl, cached int64
			if wantStream {
				prompt, compl, cached = s.streamToClient(w, resp)
			} else {
				var err2 error
				prompt, compl, cached, err2 = s.jsonToClient(w, resp)
				if err2 != nil {
					resp.Body.Close()
					cancel()
					s.router.Inflight(cd.Key, -1)
					s.router.MarkFailure(cd.Key, settings.CooldownBaseSec, settings.CooldownMaxSec)
					s.recordAttempt(prov, pm.Name, pmod.Model, keyNote, keyMask, wantStream, latency, 0, 0, 0, true, err2.Error())
					lastErr = fmt.Sprintf("%s(%s): 响应解析失败: %v", prov.Name, keyMask, err2)
					continue
				}
			}
			s.router.MarkSuccess(cd.Key)
			s.router.Inflight(cd.Key, -1)
			s.recordAttempt(prov, pm.Name, pmod.Model, keyNote, keyMask, wantStream, time.Since(start).Milliseconds(), prompt, compl, cached, false, "")
			cancel()
			return attemptResult{handled: true, status: http.StatusOK, prompt: prompt, compl: compl}
		}

		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		cancel()
		s.router.Inflight(cd.Key, -1)
		msg := upstreamError(resp.StatusCode, body)
		failoverable := resp.StatusCode >= 500 || resp.StatusCode == 429 ||
			resp.StatusCode == 401 || resp.StatusCode == 402 || resp.StatusCode == 403 || resp.StatusCode == 408
		s.recordAttempt(prov, pm.Name, pmod.Model, keyNote, keyMask, wantStream, latency, 0, 0, 0, true, fmt.Sprintf("%d %s", resp.StatusCode, msg))
		if !failoverable {
			writeUpstreamError(w, resp.StatusCode, msg)
			return attemptResult{handled: true, status: resp.StatusCode, errMsg: msg}
		}
		s.router.MarkFailure(cd.Key, settings.CooldownBaseSec, settings.CooldownMaxSec)
		lastErr = fmt.Sprintf("%s(%s): %d %s", prov.Name, keyMask, resp.StatusCode, msg)
	}
	return attemptResult{handled: false, errMsg: lastErr}
}

// usageExtras 从 usage 对象里提取 缓存命中 tokens：
// 兼容 OpenAI 系 prompt_tokens_details.cached_tokens 与 DeepSeek 系 prompt_cache_hit_tokens。
type usageExtras struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	PromptTokensDetails *struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	PromptCacheHitTokens int64 `json:"prompt_cache_hit_tokens"`
}

func (u *usageExtras) cached() int64 {
	if u.PromptTokensDetails != nil && u.PromptTokensDetails.CachedTokens > 0 {
		return u.PromptTokensDetails.CachedTokens
	}
	return u.PromptCacheHitTokens
}

// streamToClient SSE 透传（逐行刷写），并从带 usage 的块提取 token 数。
func (s *Server) streamToClient(w http.ResponseWriter, resp *http.Response) (int64, int64, int64) {
	flusher, _ := w.(http.Flusher)
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "text/event-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if flusher != nil {
		flusher.Flush()
	}
	var prompt, compl, cached int64
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data:") {
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload != "" && payload != "[DONE]" {
				var chunk struct {
					Usage *usageExtras `json:"usage"`
				}
				if json.Unmarshal([]byte(payload), &chunk) == nil && chunk.Usage != nil {
					prompt, compl, cached = chunk.Usage.PromptTokens, chunk.Usage.CompletionTokens, chunk.Usage.cached()
				}
			}
		}
		_, _ = fmt.Fprintln(w, line)
		if flusher != nil {
			flusher.Flush()
		}
	}
	return prompt, compl, cached
}

// jsonToClient 非流式透传并提取 usage。
func (s *Server) jsonToClient(w http.ResponseWriter, resp *http.Response) (int64, int64, int64, error) {
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return 0, 0, 0, err
	}
	var parsed struct {
		Usage *usageExtras `json:"usage"`
	}
	_ = json.Unmarshal(body, &parsed)
	var prompt, compl, cached int64
	if parsed.Usage != nil {
		prompt, compl, cached = parsed.Usage.PromptTokens, parsed.Usage.CompletionTokens, parsed.Usage.cached()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
	return prompt, compl, cached, nil
}

func writeUpstreamError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{"message": msg, "type": "upstream_error", "code": status},
	})
}

// handleModels 平台模型目录（含别名）。
// ---------- 模型连通性测试（管理端） ----------

type modelTestReq struct {
	Model     string `json:"model"`
	Prompt    string `json:"prompt"`
	MaxTokens int    `json:"max_tokens"`
}

// handleModelTest 「测试模型是否能用」：按真实调用链（成员选择→密钥池轮换→故障转移）
// 发一条小对话请求，返回是否可用、时延、tokens 与回复摘要；结果会照常计入请求日志与统计。
func (s *Server) handleModelTest(w http.ResponseWriter, r *http.Request) {
	var req modelTestReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不是合法 JSON: "+err.Error())
		return
	}
	if req.Model == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "缺少 model 字段")
		return
	}
	if strings.TrimSpace(req.Prompt) == "" {
		req.Prompt = "你好，请回复：测试成功"
	}
	if req.MaxTokens < 0 {
		req.MaxTokens = 0
	}
	if req.MaxTokens > 1024 {
		req.MaxTokens = 1024
	}
	var key APIKey
	hasKey := false
	s.store.View(func(c *Config) {
		if len(c.APIKeys) > 0 {
			key = c.APIKeys[0]
			hasKey = true
		}
	})
	if !hasKey {
		writeErr(w, http.StatusServiceUnavailable, "no_api_key", "尚未配置客户端 API Key，请先在「密钥」页创建")
		return
	}
	payload := map[string]any{
		"model":    req.Model,
		"messages": []map[string]string{{"role": "user", "content": req.Prompt}},
		"stream":   false,
	}
	if req.MaxTokens > 0 {
		payload["max_tokens"] = req.MaxTokens
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	req2, err := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	rec := httptest.NewRecorder()
	start := time.Now()
	s.dispatchChat(rec, req2, payload, key)
	latency := time.Since(start).Milliseconds()

	out := map[string]any{"model": req.Model, "latency_ms": latency, "http_status": rec.Code}
	var resp map[string]any
	if rec.Code == http.StatusOK && json.Unmarshal(rec.Body.Bytes(), &resp) == nil {
		out["ok"] = true
		out["content"] = truncateRunes(replyContent(resp), 2000)
		if u, ok := resp["usage"].(map[string]any); ok {
			out["usage"] = u
		}
	} else {
		out["ok"] = false
		msg := strings.TrimSpace(rec.Body.String())
		if len(msg) > 500 {
			msg = msg[:500] + "…"
		}
		if msg == "" {
			msg = fmt.Sprintf("上游返回 HTTP %d", rec.Code)
		}
		out["error"] = msg
	}
	writeJSON(w, http.StatusOK, out)
}

// replyContent 从 OpenAI 兼容响应中提取回复文本（兼容 reasoning_content / text 字段）。
func replyContent(resp map[string]any) string {
	choices, _ := resp["choices"].([]any)
	if len(choices) == 0 {
		return ""
	}
	c0, _ := choices[0].(map[string]any)
	if c0 == nil {
		return ""
	}
	if msg, ok := c0["message"].(map[string]any); ok {
		if s, ok := msg["content"].(string); ok && strings.TrimSpace(s) != "" {
			return s
		}
		if s, ok := msg["reasoning_content"].(string); ok && strings.TrimSpace(s) != "" {
			return "[思考] " + s
		}
	}
	if s, ok := c0["text"].(string); ok {
		return s
	}
	return ""
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.checkAPIKey(r); !ok {
		writeErr(w, http.StatusUnauthorized, "invalid_api_key", "无效的 API Key")
		return
	}
	type entry struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	now := time.Now().Unix()
	data := make([]entry, 0, 8)
	s.store.View(func(c *Config) {
		for _, pm := range c.PlatformModels {
			if !pm.Enabled {
				continue
			}
			data = append(data, entry{ID: pm.Name, Object: "model", Created: now, OwnedBy: "model-gateway"})
			for _, a := range pm.Aliases {
				data = append(data, entry{ID: a, Object: "model", Created: now, OwnedBy: "model-gateway"})
			}
		}
	})
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

// handleHealth 健康检查（免鉴权）。
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	providers, platforms := 0, 0
	s.store.View(func(c *Config) {
		for _, p := range c.Providers {
			if p.Enabled {
				providers++
			}
		}
		for _, pm := range c.PlatformModels {
			if pm.Enabled {
				platforms++
			}
		}
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "time": time.Now().Format(time.RFC3339),
		"providers": providers, "platform_models": platforms, "version": s.version,
	})
}

// ---------- 统计与日志 ----------

func (s *Server) recordAttempt(prov Provider, platformName, upModel, keyNote, upKey string, stream bool, latency int64, prompt, compl, cached int64, isErr bool, errMsg string) {
	rec := RequestLog{
		Time: time.Now().Unix(), KeyNote: keyNote, Model: platformName,
		Provider: prov.Name, UpModel: upModel, UpKey: upKey, Status: map[bool]int{true: 1, false: 0}[isErr],
		Latency: latency, Stream: stream, PromptTok: prompt, ComplTok: compl, CachedTok: cached, Error: errMsg,
	}
	s.db.InsertLog(rec)
	s.db.UpsertStat(time.Now().Format("2006-01-02"), prov.ID, platformName, isErr, prompt, compl, cached, latency)
}

// ---------- 管理端鉴权 ----------

func (s *Server) newPasswordToken() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Password string `json:"password"`
	}
	if err := readJSON(r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不合法")
		return
	}
	var stored string
	s.store.View(func(c *Config) { stored = c.Password })
	if stored == "" || subtle.ConstantTimeCompare([]byte(body.Password), []byte(stored)) != 1 {
		writeErr(w, http.StatusUnauthorized, "bad_password", "密码错误")
		return
	}
	token := s.newPasswordToken()
	s.sessMu.Lock()
	s.sessions[token] = time.Now().Add(sessionTTL)
	s.sessMu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: token, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteLaxMode, MaxAge: int(sessionTTL.Seconds()),
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		s.sessMu.Lock()
		delete(s.sessions, c.Value)
		s.sessMu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(sessionCookie)
		if err != nil {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "未登录")
			return
		}
		s.sessMu.Lock()
		exp, ok := s.sessions[c.Value]
		if ok && time.Now().After(exp) {
			delete(s.sessions, c.Value)
			ok = false
		}
		s.sessMu.Unlock()
		if !ok {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "登录已过期")
			return
		}
		next(w, r)
	}
}

func (s *Server) handleAuthCheck(w http.ResponseWriter, r *http.Request) {
	providers, pmods, platforms, keys := 0, 0, 0, 0
	s.store.View(func(c *Config) {
		providers, pmods, platforms, keys = len(c.Providers), len(c.ProviderModels), len(c.PlatformModels), len(c.APIKeys)
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "version": s.version,
		"providers": providers, "provider_models": pmods, "platform_models": platforms, "keys": keys,
	})
}

func (s *Server) handlePassword(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := readJSON(r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不合法")
		return
	}
	var stored string
	s.store.View(func(c *Config) { stored = c.Password })
	if stored == "" || subtle.ConstantTimeCompare([]byte(body.CurrentPassword), []byte(stored)) != 1 {
		writeErr(w, http.StatusUnauthorized, "bad_password", "当前密码错误")
		return
	}
	if len(body.NewPassword) < 6 {
		writeErr(w, http.StatusBadRequest, "weak_password", "新密码至少 6 位")
		return
	}
	s.store.Update(func(c *Config) { c.Password = body.NewPassword })
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// ---------- 供应商管理 ----------

func (s *Server) handleProviders(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		type pv struct {
			Provider
			ModelCount int `json:"model_count"`
			KeyCount   int `json:"key_count"`
		}
		out := []pv{}
		s.store.View(func(c *Config) {
			for _, p := range c.Providers {
				n := 0
				for _, m := range c.ProviderModels {
					if m.ProviderID == p.ID {
						n++
					}
				}
				out = append(out, pv{Provider: p, ModelCount: n, KeyCount: len(p.EnabledKeys())})
			}
		})
		writeJSON(w, http.StatusOK, out)
	case http.MethodPost:
		var body Provider
		if err := readJSON(r, &body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不合法")
			return
		}
		if body.Name == "" || body.BaseURL == "" {
			writeErr(w, http.StatusBadRequest, "invalid_request", "name 与 base_url 必填")
			return
		}
		if body.Kind == "" {
			body.Kind = "openai-compat"
		}
		body.ID = NewID("p")
		body.Enabled = true
		s.store.Update(func(c *Config) { c.Providers = append(c.Providers, body) })
		s.invalidateUpstreams()
		writeJSON(w, http.StatusOK, body)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
	}
}

func (s *Server) handleProviderByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	switch r.Method {
	case http.MethodPut:
		var body Provider
		if err := readJSON(r, &body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不合法")
			return
		}
		updated := false
		s.store.Update(func(c *Config) {
			for i := range c.Providers {
				if c.Providers[i].ID == id {
					body.ID = id
					if body.Kind == "" {
						body.Kind = c.Providers[i].Kind
					}
					c.Providers[i] = body
					updated = true
				}
			}
		})
		if !updated {
			writeErr(w, http.StatusNotFound, "not_found", "供应商不存在")
			return
		}
		s.invalidateUpstreams()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		s.store.Update(func(c *Config) {
			provs := c.Providers[:0]
			for _, p := range c.Providers {
				if p.ID != id {
					provs = append(provs, p)
				}
			}
			c.Providers = provs
			pmods := c.ProviderModels[:0]
			removed := map[string]bool{}
			for _, m := range c.ProviderModels {
				if m.ProviderID == id {
					removed[m.ID] = true
					continue
				}
				pmods = append(pmods, m)
			}
			c.ProviderModels = pmods
			for i := range c.PlatformModels {
				mems := c.PlatformModels[i].Members[:0]
				for _, mem := range c.PlatformModels[i].Members {
					if !removed[mem.ProviderModelID] {
						mems = append(mems, mem)
					}
				}
				c.PlatformModels[i].Members = mems
			}
		})
		s.invalidateUpstreams()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
	}
}

// handleProviderModels 供应商模型列表（含路由运行态）。
func (s *Server) handleProviderModels(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	type mv struct {
		ProviderModel
		UsedBy []string `json:"used_by"`
	}
	out := []mv{}
	s.store.View(func(c *Config) {
		for _, m := range c.ProviderModels {
			if m.ProviderID != id {
				continue
			}
			used := []string{}
			for _, pm := range c.PlatformModels {
				for _, mem := range pm.Members {
					if mem.ProviderModelID == m.ID {
						used = append(used, pm.Name)
					}
				}
			}
			out = append(out, mv{ProviderModel: m, UsedBy: used})
		}
	})
	writeJSON(w, http.StatusOK, out)
}

// handleRemoteModels 实时拉取上游模型目录（不落库），标注能力与已导入状态。
func (s *Server) handleRemoteModels(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var prov Provider
	exists := false
	s.store.View(func(c *Config) { prov, exists = c.FindProvider(id) })
	if !exists {
		writeErr(w, http.StatusNotFound, "not_found", "供应商不存在")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	keys := prov.EnabledKeys()
	if len(keys) == 0 {
		writeErr(w, http.StatusBadRequest, "no_key", "该供应商尚未配置密钥，请先在「密钥」中添加")
		return
	}
	models, err := s.getUpstream(prov).Models(ctx, keys[0].Key)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "upstream_error", "拉取模型目录失败: "+err.Error())
		return
	}
	type rm struct {
		ID       string   `json:"id"`
		Caps     []string `json:"caps"`
		Imported bool     `json:"imported"`
	}
	importedSet := map[string]bool{}
	s.store.View(func(c *Config) {
		for _, m := range c.ProviderModels {
			if m.ProviderID == id {
				importedSet[m.Model] = true
			}
		}
	})
	out := make([]rm, 0, len(models))
	for _, m := range models {
		out = append(out, rm{ID: m.ID, Caps: m.Caps, Imported: importedSet[m.ID]})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleImportModels 批量导入勾选的模型为供应商模型（按 provider+model 去重）。
func (s *Server) handleImportModels(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Models []string `json:"models"`
	}
	if err := readJSON(r, &body); err != nil || len(body.Models) == 0 {
		writeErr(w, http.StatusBadRequest, "invalid_request", "请求体需含非空 models 数组")
		return
	}
	var prov Provider
	exists := false
	s.store.View(func(c *Config) { prov, exists = c.FindProvider(id) })
	if !exists || prov.ID == "" {
		writeErr(w, http.StatusNotFound, "not_found", "供应商不存在")
		return
	}
	imported := 0
	s.store.Update(func(c *Config) {
		existing := map[string]bool{}
		for _, m := range c.ProviderModels {
			if m.ProviderID == id {
				existing[m.Model] = true
			}
		}
		for _, name := range body.Models {
			name = strings.TrimSpace(name)
			if name == "" || existing[name] {
				continue
			}
			c.ProviderModels = append(c.ProviderModels, ProviderModel{
				ID: NewID("prm"), ProviderID: id, Model: name,
				Caps: guessCaps(name), Enabled: true,
			})
			existing[name] = true
			imported++
		}
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "imported": imported})
}

// handleProviderModelByID 供应商模型编辑/删除（同步清理平台模型成员引用）。
func (s *Server) handleProviderModelByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	switch r.Method {
	case http.MethodPut:
		var body ProviderModel
		if err := readJSON(r, &body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不合法")
			return
		}
		s.store.Update(func(c *Config) {
			for i := range c.ProviderModels {
				if c.ProviderModels[i].ID == id {
					if body.Model != "" {
						c.ProviderModels[i].Model = body.Model
					}
					if body.Caps != nil {
						c.ProviderModels[i].Caps = body.Caps
					}
					if body.Note != "" {
						c.ProviderModels[i].Note = body.Note
					}
					c.ProviderModels[i].Enabled = body.Enabled
				}
			}
		})
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		s.store.Update(func(c *Config) {
			pmods := c.ProviderModels[:0]
			for _, m := range c.ProviderModels {
				if m.ID != id {
					pmods = append(pmods, m)
				}
			}
			c.ProviderModels = pmods
			for i := range c.PlatformModels {
				mems := c.PlatformModels[i].Members[:0]
				for _, mem := range c.PlatformModels[i].Members {
					if mem.ProviderModelID != id {
						mems = append(mems, mem)
					}
				}
				c.PlatformModels[i].Members = mems
			}
		})
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
	}
}

// ---------- 供应商密钥池管理 ----------

// handleProviderKeys 供应商密钥池：GET 列表（含冷却/并发运行态）、POST 新增。
func (s *Server) handleProviderKeys(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	switch r.Method {
	case http.MethodGet:
		type kv struct {
			ProviderKey
			CooldownLeft int `json:"cooldown_left"`
			Inflight     int `json:"inflight"`
		}
		out := []kv{}
		snap := s.router.Snapshot()
		s.store.View(func(c *Config) {
			p, ok := c.FindProvider(id)
			if !ok {
				return
			}
			for _, k := range p.Keys {
				st := snap[p.ID+"|k|"+k.Key]
				out = append(out, kv{ProviderKey: k, CooldownLeft: st[2], Inflight: st[0]})
			}
		})
		writeJSON(w, http.StatusOK, out)
	case http.MethodPost:
		var body ProviderKey
		if err := readJSON(r, &body); err != nil || strings.TrimSpace(body.Key) == "" {
			writeErr(w, http.StatusBadRequest, "invalid_request", "请求体需含 key 字段")
			return
		}
		body.Key = strings.TrimSpace(body.Key)
		exists, added := false, false
		s.store.Update(func(c *Config) {
			p, ok := c.FindProvider(id)
			if !ok {
				return
			}
			exists = true
			for _, k := range p.Keys {
				if k.Key == body.Key {
					return
				}
			}
			body.CreatedAt = time.Now().Unix()
			p.Keys = append(p.Keys, body)
			for i := range c.Providers {
				if c.Providers[i].ID == id {
					c.Providers[i] = p
				}
			}
			added = true
		})
		if !exists {
			writeErr(w, http.StatusNotFound, "not_found", "供应商不存在")
			return
		}
		if !added {
			writeErr(w, http.StatusConflict, "duplicate", "该密钥已存在")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
	}
}

// handleProviderKeyByID 供应商单把密钥：PUT 更新 / DELETE 删除（按 key 值定位）。
func (s *Server) handleProviderKeyByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	key := r.PathValue("key")
	switch r.Method {
	case http.MethodPut:
		var body ProviderKey
		if err := readJSON(r, &body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不合法")
			return
		}
		updated := false
		s.store.Update(func(c *Config) {
			p, ok := c.FindProvider(id)
			if !ok {
				return
			}
			for i := range p.Keys {
				if p.Keys[i].Key != key {
					continue
				}
				p.Keys[i].Note = body.Note
				if body.Weight > 0 {
					p.Keys[i].Weight = body.Weight
				}
				p.Keys[i].Priority = body.Priority
				p.Keys[i].Disabled = body.Disabled
				updated = true
			}
			if updated {
				for i := range c.Providers {
					if c.Providers[i].ID == id {
						c.Providers[i] = p
					}
				}
			}
		})
		if !updated {
			writeErr(w, http.StatusNotFound, "not_found", "密钥不存在")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		deleted := false
		s.store.Update(func(c *Config) {
			p, ok := c.FindProvider(id)
			if !ok {
				return
			}
			keys := p.Keys[:0]
			for _, k := range p.Keys {
				if k.Key == key {
					deleted = true
					continue
				}
				keys = append(keys, k)
			}
			p.Keys = keys
			for i := range c.Providers {
				if c.Providers[i].ID == id {
					c.Providers[i] = p
				}
			}
		})
		if !deleted {
			writeErr(w, http.StatusNotFound, "not_found", "密钥不存在")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
	}
}

// ---------- 平台模型管理 ----------

func (s *Server) handlePlatformModels(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		type memberView struct {
			Member
			ProviderName string `json:"provider_name"`
			Model        string `json:"model"`
		}
		type pmv struct {
			PlatformModel
			MembersView []memberView `json:"members_view"`
		}
		out := []pmv{}
		s.store.View(func(c *Config) {
			for _, pm := range c.PlatformModels {
				v := pmv{PlatformModel: pm, MembersView: []memberView{}}
				for _, mem := range pm.Members {
					pmod, ok1 := c.FindProviderModel(mem.ProviderModelID)
					if !ok1 {
						continue
					}
					prov, _ := c.FindProvider(pmod.ProviderID)
					v.MembersView = append(v.MembersView, memberView{
						Member: mem, ProviderName: prov.Name, Model: pmod.Model,
					})
				}
				out = append(out, v)
			}
		})
		writeJSON(w, http.StatusOK, out)
	case http.MethodPost:
		var body PlatformModel
		if err := readJSON(r, &body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不合法")
			return
		}
		if body.Name == "" {
			writeErr(w, http.StatusBadRequest, "invalid_request", "name 必填")
			return
		}
		body.ID = NewID("plat")
		body.Enabled = true
		s.store.Update(func(c *Config) { c.PlatformModels = append(c.PlatformModels, body) })
		writeJSON(w, http.StatusOK, body)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
	}
}

func (s *Server) handlePlatformModelByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	switch r.Method {
	case http.MethodPut:
		var body PlatformModel
		if err := readJSON(r, &body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不合法")
			return
		}
		updated := false
		s.store.Update(func(c *Config) {
			for i := range c.PlatformModels {
				if c.PlatformModels[i].ID == id {
					body.ID = id
					c.PlatformModels[i] = body
					updated = true
				}
			}
		})
		if !updated {
			writeErr(w, http.StatusNotFound, "not_found", "平台模型不存在")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		s.store.Update(func(c *Config) {
			out := c.PlatformModels[:0]
			for _, pm := range c.PlatformModels {
				if pm.ID != id {
					out = append(out, pm)
				}
			}
			c.PlatformModels = out
		})
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
	}
}

// ---------- 密钥管理 ----------

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		out := []APIKey{}
		s.store.View(func(c *Config) { out = append(out, c.APIKeys...) })
		writeJSON(w, http.StatusOK, out)
	case http.MethodPost:
		var body struct {
			Note string `json:"note"`
		}
		_ = readJSON(r, &body)
		var raw [16]byte
		_, _ = rand.Read(raw[:])
		key := "sk-" + hex.EncodeToString(raw[:])
		nk := APIKey{Key: key, Note: body.Note, CreatedAt: time.Now().Unix()}
		s.store.Update(func(c *Config) { c.APIKeys = append(c.APIKeys, nk) })
		writeJSON(w, http.StatusOK, nk)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
	}
}

func (s *Server) handleKeyByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
		return
	}
	key := r.PathValue("key")
	s.store.Update(func(c *Config) {
		out := c.APIKeys[:0]
		for _, k := range c.APIKeys {
			if k.Key != key {
				out = append(out, k)
			}
		}
		c.APIKeys = out
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// ---------- 统计 / 探测 / 设置 ----------

// handleStats 统计 + 日志。支持按日期范围筛选：
//   - ?range=all            全部
//   - ?from=YYYY-MM-DD&to=  指定范围（闭区间）
//   - 缺省                  近 7 天
//
// 汇总数据来自 DailyStats（随配置落盘，保留 90 天）；请求日志来自 requests.jsonl 回放。
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	today := time.Now().Format("2006-01-02")
	q := r.URL.Query()
	from, to := q.Get("from"), q.Get("to")
	if q.Get("range") == "all" {
		from, to = "0000-01-01", today
	}
	if from == "" && to == "" {
		from = time.Now().AddDate(0, 0, -6).Format("2006-01-02")
		to = today
	}
	validDay := func(d string) bool { _, err := time.Parse("2006-01-02", d); return err == nil }
	if !validDay(from) {
		from = ""
	}
	if !validDay(to) {
		to = ""
	}
	if from == "" && to == "" {
		// 缺省或参数非法：近 7 天
		from = time.Now().AddDate(0, 0, -6).Format("2006-01-02")
		to = today
	} else {
		if from == "" {
			from = "0000-01-01"
		}
		if to == "" {
			to = today
		}
	}
	if from > to {
		from, to = to, from
	}

	rows, reqs, errs, ptok, ctok, cktok := s.db.QueryStats(from, to)

	fromTs := dayStartUnix(from)
	toTs := dayStartUnix(to) + 86400 - 1 // 当天 23:59:59
	logs := s.db.QueryLogs(fromTs, toTs, 300)
	writeJSON(w, http.StatusOK, map[string]any{
		"from": from, "to": to, "rows": rows,
		"totals": map[string]any{"requests": reqs, "errors": errs, "prompt_tokens": ptok, "completion_tokens": ctok, "cached_tokens": cktok, "total_tokens": ptok + ctok},
		"logs":   logs,
	})
}

// dayStartUnix 当地时区某日 00:00:00 的 Unix 秒。
func dayStartUnix(day string) int64 {
	t, err := time.ParseInLocation("2006-01-02", day, time.Local)
	if err != nil {
		return 0
	}
	return t.Unix()
}

func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	type keyResult struct {
		Key     string `json:"key"`
		Note    string `json:"note,omitempty"`
		OK      bool   `json:"ok"`
		Latency int64  `json:"latency_ms"`
		Models  int    `json:"models"`
		Error   string `json:"error,omitempty"`
	}
	type pr struct {
		ProviderID string      `json:"provider_id"`
		Name       string      `json:"name"`
		OK         bool        `json:"ok"`
		Keys       []keyResult `json:"keys"`
	}
	provs := []Provider{}
	s.store.View(func(c *Config) { provs = append(provs, c.Providers...) })
	out := []pr{}
	for _, p := range provs {
		if !p.Enabled {
			continue
		}
		item := pr{ProviderID: p.ID, Name: p.Name, Keys: []keyResult{}}
		keys := p.EnabledKeys()
		if len(keys) > 6 {
			keys = keys[:6]
		}
		for _, k := range keys {
			ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
			start := time.Now()
			models, err := s.getUpstream(p).Models(ctx, k.Key)
			cancel()
			kr := keyResult{Key: MaskKey(k.Key), Note: k.Note, Latency: time.Since(start).Milliseconds()}
			if err != nil {
				kr.Error = err.Error()
			} else {
				kr.OK = true
				kr.Models = len(models)
				item.OK = true
			}
			item.Keys = append(item.Keys, kr)
		}
		out = append(out, item)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		var st Settings
		s.store.View(func(c *Config) { st = c.Settings })
		writeJSON(w, http.StatusOK, st)
	case http.MethodPut:
		var body Settings
		if err := readJSON(r, &body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不合法")
			return
		}
		s.store.Update(func(c *Config) {
			if body.ChatTimeoutSec > 0 {
				c.Settings.ChatTimeoutSec = body.ChatTimeoutSec
			}
			if body.ModelsTimeoutSec > 0 {
				c.Settings.ModelsTimeoutSec = body.ModelsTimeoutSec
			}
			if body.CooldownBaseSec > 0 {
				c.Settings.CooldownBaseSec = body.CooldownBaseSec
			}
			if body.CooldownMaxSec > 0 {
				c.Settings.CooldownMaxSec = body.CooldownMaxSec
			}
		})
		s.invalidateUpstreams()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
	}
}

// ---------- 播种 ----------

// Seed 首次启动时播种：默认密钥 + Qoder 现有服务作为首个供应商 + 异步 1:1 建平台模型。
func (s *Server) Seed(qoderURL, qoderKey, apiKey, password string) {
	shouldSeed := false
	s.store.Update(func(c *Config) {
		if len(c.Providers) == 0 && len(c.PlatformModels) == 0 {
			shouldSeed = true
		}
		if c.Password == "" && password != "" {
			c.Password = password
		}
		if len(c.APIKeys) == 0 && apiKey != "" {
			c.APIKeys = append(c.APIKeys, APIKey{Key: apiKey, CreatedAt: time.Now().Unix(), Note: "默认密钥"})
		}
		if shouldSeed && qoderURL != "" && qoderKey != "" {
			c.Providers = append(c.Providers, Provider{
				ID: "p_qoder", Name: "Qoder 现有服务", Kind: "openai-compat",
				BaseURL: qoderURL, APIKey: qoderKey, Enabled: true, Note: "首启自动播种",
			})
		}
	})
	_ = s.store.Flush()
	needBootstrap := false
	s.store.View(func(c *Config) { needBootstrap = len(c.PlatformModels) == 0 })
	if (shouldSeed && qoderURL != "" && qoderKey != "") || needBootstrap {
		go s.autoImportAll()
	}
}

// autoImportAll 为尚无模型目录的启用供应商拉取目录，导入并 1:1 建平台模型。
func (s *Server) autoImportAll() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	provs := []Provider{}
	s.store.View(func(c *Config) {
		for _, p := range c.Providers {
			if p.Enabled {
				provs = append(provs, p)
			}
		}
	})
	for _, p := range provs {
		pkeys := p.EnabledKeys()
		if len(pkeys) == 0 {
			log.Printf("[seed] 跳过 %s：尚未配置密钥", p.Name)
			continue
		}
		models, err := s.getUpstream(p).Models(ctx, pkeys[0].Key)
		if err != nil {
			log.Printf("[seed] 拉取 %s 模型目录失败（可在面板手动获取）: %v", p.Name, err)
			continue
		}
		names := make([]string, 0, len(models))
		for _, m := range models {
			names = append(names, m.ID)
		}
		s.store.Update(func(c *Config) {
			existing := map[string]bool{}
			for _, m := range c.ProviderModels {
				if m.ProviderID == p.ID {
					existing[m.Model] = true
				}
			}
			newIDs := map[string]bool{}
			for _, name := range names {
				if existing[name] {
					continue
				}
				pm := ProviderModel{ID: NewID("prm"), ProviderID: p.ID, Model: name, Caps: guessCaps(name), Enabled: true}
				c.ProviderModels = append(c.ProviderModels, pm)
				newIDs[pm.ID] = true
			}
			// 1:1 平台模型：未被任何平台模型引用的新供应商模型各建一个同名平台模型
			referenced := map[string]bool{}
			for _, plat := range c.PlatformModels {
				for _, mem := range plat.Members {
					referenced[mem.ProviderModelID] = true
				}
			}
			for _, m := range c.ProviderModels {
				if m.ProviderID == p.ID && !referenced[m.ID] {
					c.PlatformModels = append(c.PlatformModels, PlatformModel{
						ID: NewID("plat"), Name: m.Model, Strategy: "sticky", Enabled: true,
						Members: []Member{{ProviderModelID: m.ID, Weight: 1, Priority: 0, Enabled: true}},
					})
				}
			}
		})
		log.Printf("[seed] %s 已导入 %d 个模型并建立同名平台模型", p.Name, len(names))
	}
	_ = s.store.Flush()
}

// ---------- 路由注册 ----------

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Session-Id")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Routes 组装全部路由。
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// 推理端点（OpenAI 协议）
	mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("GET /health", s.handleHealth)

	// 管理端鉴权
	mux.HandleFunc("POST /admin/api/login", s.handleLogin)
	mux.HandleFunc("POST /admin/api/logout", s.requireAuth(s.handleLogout))
	mux.HandleFunc("GET /admin/api/auth", s.requireAuth(s.handleAuthCheck))
	mux.HandleFunc("POST /admin/api/password", s.requireAuth(s.handlePassword))

	// 供应商
	mux.HandleFunc("/admin/api/providers", s.requireAuth(s.handleProviders))
	mux.HandleFunc("/admin/api/providers/{id}", s.requireAuth(s.handleProviderByID))
	mux.HandleFunc("/admin/api/providers/{id}/models", s.requireAuth(s.handleProviderModels))
	mux.HandleFunc("/admin/api/providers/{id}/models/remote", s.requireAuth(s.handleRemoteModels))
	mux.HandleFunc("/admin/api/providers/{id}/models/import", s.requireAuth(s.handleImportModels))
	mux.HandleFunc("/admin/api/provider-models/{id}", s.requireAuth(s.handleProviderModelByID))
	mux.HandleFunc("/admin/api/providers/{id}/keys", s.requireAuth(s.handleProviderKeys))
	mux.HandleFunc("/admin/api/providers/{id}/keys/{key}", s.requireAuth(s.handleProviderKeyByID))

	// 平台模型
	mux.HandleFunc("/admin/api/platform-models", s.requireAuth(s.handlePlatformModels))
	mux.HandleFunc("/admin/api/platform-models/{id}", s.requireAuth(s.handlePlatformModelByID))

	// 密钥 / 统计 / 探测 / 设置
	mux.HandleFunc("/admin/api/keys", s.requireAuth(s.handleKeys))
	mux.HandleFunc("/admin/api/keys/{key}", s.requireAuth(s.handleKeyByID))
	mux.HandleFunc("GET /admin/api/stats", s.requireAuth(s.handleStats))
	mux.HandleFunc("POST /admin/api/probe", s.requireAuth(s.handleProbe))
	mux.HandleFunc("POST /admin/api/models/test", s.requireAuth(s.handleModelTest))
	mux.HandleFunc("/admin/api/settings", s.requireAuth(s.handleSettings))

	// 内嵌面板
	mux.HandleFunc("/admin", s.panelHandler)
	mux.HandleFunc("/admin/", s.panelHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			s.panelHandler(w, r)
			return
		}
		http.NotFound(w, r)
	})

	return s.corsMiddleware(mux)
}
