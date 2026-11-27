package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
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

	metrics *Metrics // Prometheus 指标（进程内）

	loginMu   sync.Mutex
	loginFail map[string]*loginFails // 按 IP 的登录失败计数（防爆破）
}

// loginFails 登录失败节流状态。
type loginFails struct {
	fails int
	until time.Time
}

// NewServer 创建服务；打开 SQLite 并迁移历史数据（requests.jsonl / 配置内统计）。
func NewServer(store *Store, version string) *Server {
	s := &Server{
		store:    store,
		router:   NewRouter(),
		version:  version,
		sessions:  map[string]time.Time{},
		ups:       map[string]*upstream{},
		metrics:   NewMetrics(),
		loginFail: map[string]*loginFails{},
	}
	dataDir := filepath.Dir(store.Path())
	db, err := OpenDB(dataDir)
	if err != nil {
		log.Fatalf("[gateway] SQLite 打开失败: %v", err)
	}
	s.db = db
	db.MigrateJSONL(dataDir)
	db.MigrateConfigStats(store)
	// 日志按设置保留天数清理（默认 30 天，每小时巡一次）；统计固定留 90 天
	go func() {
		retention := func() int {
			days := 0
			store.View(func(c *Config) { days = c.Settings.LogRetentionDays })
			if days <= 0 {
				days = 30
			}
			if days > 3650 {
				days = 3650
			}
			return days
		}
		db.Cleanup(retention(), 90)
		for range time.Tick(time.Hour) {
			db.Cleanup(retention(), 90)
		}
	}()
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
	// 密钥级模型白名单：该密钥未被授权使用此模型时直接拒绝（空列表 = 不限）
	if !key.AllowsModel(modelName, pm) {
		writeErr(w, http.StatusForbidden, "model_forbidden",
			"该 API Key 无权使用模型: "+modelName)
		return
	}
	wantStream := false
	if b, ok := payload["stream"].(bool); ok {
		wantStream = b
	}
	stickyKey := stickyKeyFromRequest(r, payload)
	reqID := newReqID() // 业务请求 ID：同一次客户端请求内的所有尝试共享，用于按「业务请求」统计

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
	// 供应商级过滤：额度耗尽（402）与连续失败熔断的供应商整体跳过
	var usable []attemptInfo
	skippedExhausted, skippedBreaker := 0, 0
	for _, a := range cands {
		if s.router.ExhaustedLeft(a.prov.ID) > 0 {
			skippedExhausted++
			continue
		}
		if s.router.BreakerLeft(a.prov.ID) > 0 {
			skippedBreaker++
			continue
		}
		usable = append(usable, a)
	}
	if len(usable) == 0 {
		switch {
		case skippedExhausted > 0:
			writeErr(w, http.StatusPaymentRequired, "provider_exhausted", fmt.Sprintf("供应商额度耗尽，已暂停调用（%d 个供应商）", skippedExhausted))
		case skippedBreaker > 0:
			writeErr(w, http.StatusServiceUnavailable, "provider_breaker_open", fmt.Sprintf("供应商连续失败，熔断中（%d 个供应商），请稍后重试", skippedBreaker))
		default:
			writeErr(w, http.StatusServiceUnavailable, "no_available_member", "平台模型没有可用成员")
		}
		return
	}
	cands = usable

	var settings Settings
	s.store.View(func(c *Config) { settings = c.Settings })

	// 调试模式：记录客户端原始请求体（供日志 debug 字段使用）
	debugReq := ""
	if settings.DebugPayloads {
		if b, err := json.Marshal(payload); err == nil {
			debugReq = truncate(string(b), debugCap)
		}
	}

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
	allSaturated := true
	for _, cd := range order {
		if tried >= maxRetry {
			break
		}
		a, ok := attemptByKey[cd.Key]
		if !ok {
			continue
		}
		tried++
		res := s.tryMember(r, w, a.prov, a.pmod, pm, payload, wantStream, key.Note, settings, debugReq, reqID)
		if res.handled {
			if res.status == http.StatusOK {
				if stickyKey != "" && pm.Strategy == "sticky" {
					s.router.RecordSticky(stickyKey, cd.Key)
				}
				return
			}
			// 不可转移的 4xx：本次业务请求以失败收尾
			s.db.FinalizeAttempt(reqID)
			return
		}
		if !res.saturated {
			allSaturated = false
		}
		lastErr = res.errMsg
	}
	if allSaturated && tried > 0 {
		// 所有成员的密钥并发/限流都已满：按约定返回 429，由客户端稍后重试
		s.db.FinalizeAttempt(reqID)
		writeErr(w, http.StatusTooManyRequests, "rate_limited", "所有密钥并发/限流已满，请稍后重试")
		return
	}
	s.db.FinalizeAttempt(reqID)
	writeErr(w, http.StatusBadGateway, "all_members_failed", fmt.Sprintf("尝试 %d 个成员均失败: %s", tried, lastErr))
}

// attemptResult 单次成员尝试结果。
type attemptResult struct {
	handled   bool // 响应已写回客户端（成功，或不可转移的错误）
	saturated bool // 该成员因并发/限流满被跳过（未真正发起调用）
	status    int
	prompt    int64
	compl     int64
	errMsg    string
}

// tryMember 对单个成员发起调用：先在供应商密钥池内做第二轮负载均衡
// （按权重轮询 + 失败冷却），key 级失败(401/402/403/408/429/5xx/网络/超时)自动轮换下一把；
// 全部 key 失败才把该成员判为失败（handled=false，交回成员级故障转移）。
// 超时策略：首字超时/网络错误按配置在同 key 上重试，重试用尽再换 key、换供应商。
// 非 key 级 4xx（如 400/404 请求本身有误）直接写回客户端，不再轮换。
func (s *Server) tryMember(r *http.Request, w http.ResponseWriter, prov Provider, pmod ProviderModel, pm PlatformModel, payload map[string]any, wantStream bool, keyNote string, settings Settings, clientReq, reqID string) attemptResult {
	memberKey := pm.ID + "|" + pmod.ID
	s.router.Inflight(memberKey, 1)
	defer s.router.Inflight(memberKey, -1)

	keys := prov.EnabledKeys()
	if len(keys) == 0 {
		s.recordAttempt(prov, pm.Name, pmod.Model, keyNote, "", wantStream, 0, 0, 0, 0, 0, true, "无可用密钥", "", reqID, false)
		return attemptResult{handled: false, errMsg: prov.Name + ": 无可用密钥"}
	}

	// 生效的超时配置：平台模型 > 供应商 > 全局（0=继承，负数=关闭）
	firstByteSec := resolveTimeout(pm.FirstByteTimeoutSec, prov.FirstByteTimeoutSec, settings.FirstByteTimeoutSec)
	stallSec := resolveTimeout(pm.StallTimeoutSec, prov.StallTimeoutSec, settings.StallTimeoutSec)
	retries := prov.TimeoutRetries
	if retries == 0 {
		retries = settings.TimeoutRetries
	}
	if retries < 0 {
		retries = 0
	}
	if retries > 3 {
		retries = 3
	}

	// 每把 key 的最大并发数（MaxInflight，0=不限）：已满载的 key 直接跳过
	cands := make([]Candidate, 0, len(keys))
	byCand := make(map[string]ProviderKey, len(keys))
	for _, k := range keys {
		ck := prov.ID + "|k|" + k.Key
		if k.MaxInflight > 0 && s.router.InflightOf(ck) >= k.MaxInflight {
			continue // 该 key 并发已满，跳到下一把
		}
		weight := k.Weight
		if weight <= 0 {
			weight = 1
		}
		cands = append(cands, Candidate{Key: ck, Weight: weight, Priority: k.Priority, Available: true})
		byCand[ck] = k
	}
	if len(cands) == 0 {
		s.recordAttempt(prov, pm.Name, pmod.Model, keyNote, "", wantStream, 0, 0, 0, 0, 0, true, "限流：全部密钥并发已满", "", reqID, false)
		return attemptResult{handled: false, saturated: true, errMsg: prov.Name + ": 全部密钥并发已满"}
	}
	order := s.router.Pick(cands, "round-robin", "", 0)
	maxKeyTries := len(order)
	if maxKeyTries > 5 {
		maxKeyTries = 5
	}

	up := s.getUpstream(prov)
	lastErr := ""
	attempted := false // 是否真正发起过调用（被限流跳过的不算）
	for i, cd := range order {
		if i >= maxKeyTries {
			break
		}
		pk := byCand[cd.Key]
		keyMask := MaskKey(pk.Key)

		// 时间窗限流（滑动窗口）：窗口内次数已满则跳到下一把 key（不算失败、不冷却）
		if !s.router.RateCheck(cd.Key, pk.RateLimit, pk.RateWindowSec) {
			lastErr = fmt.Sprintf("%s(%s): 时间窗限流已满（%d 次/%d 秒）", prov.Name, keyMask, pk.RateLimit, pk.RateWindowSec)
			continue
		}
		attempted = true
		s.router.RateRecord(cd.Key, pk.RateLimit, pk.RateWindowSec)

		s.router.Inflight(cd.Key, 1)
		call := &keyCall{
			r: r, w: w, prov: prov, pmod: pmod, pm: pm, up: up, pk: pk, keyMask: keyMask, candKey: cd.Key,
			payload: payload, wantStream: wantStream, keyNote: keyNote, settings: settings,
			clientReq: clientReq, reqID: reqID,
			firstByteSec: firstByteSec, stallSec: stallSec,
		}
		res := attemptResult{}
		for try := 0; try <= retries; try++ {
			if try > 0 {
				time.Sleep(200 * time.Millisecond) // 重试前短暂退避
			}
			var retryable bool
			res, retryable = s.attemptOnce(call)
			if !retryable || try == retries {
				break
			}
		}
		s.router.Inflight(cd.Key, -1)
		if res.handled {
			return res
		}
		lastErr = res.errMsg
	}
	// 一把都没真正发出去（全部被并发/限流跳过）→ 标记 saturated，供上层返回 429
	return attemptResult{handled: false, saturated: !attempted, errMsg: lastErr}
}

// keyCall 单把 key 的一次调用上下文。
type keyCall struct {
	r      *http.Request
	w      http.ResponseWriter
	prov   Provider
	pmod   ProviderModel
	pm     PlatformModel
	up     *upstream
	pk      ProviderKey
	keyMask string
	candKey string // 路由候选键：供应商ID|k|密钥，冷却/成功状态都以它为准
	payload    map[string]any
	wantStream bool
	keyNote    string
	settings   Settings
	clientReq  string
	reqID      string
	firstByteSec, stallSec int
}

// attemptOnce 用一把 key 发起一次上游调用并处理响应。
// 返回 retryable=true 表示「未向客户端写出任何内容」的失败，可在同 key 上重试。
func (s *Server) attemptOnce(c *keyCall) (attemptResult, bool) {
	start := time.Now()
	lastAct := start.UnixNano()
	var gotData, timeoutKind int32
	ctx, cancel := context.WithTimeout(c.r.Context(), time.Duration(c.settings.ChatTimeoutSec)*time.Second)
	watch := startIdleWatch(ctx, cancel, c.firstByteSec, c.stallSec, &lastAct, &gotData, &timeoutKind)
	defer watch.Stop()
	defer cancel()

	// 流式请求：先建好 SSE 写出口并在等待上游期间发心跳，
	// 因为部分上游（如商汤）连响应头都要等到首字才返回，不心跳客户端会先超时断开。
	var sw *sseWriter
	if c.wantStream {
		sw = newSSEWriter(c.w, "text/event-stream")
		if hb := s.heartbeatSec(); hb > 0 {
			sw.StartHeartbeat(time.Duration(hb) * time.Second)
			defer sw.StopHeartbeat()
		}
	}

	var upReqBody []byte
	resp, err := c.up.Chat(ctx, c.pmod.Model, c.payload, c.wantStream, c.pk.Key, &upReqBody)
	if err == nil {
		resp.Body = &activityBody{rc: resp.Body, last: &lastAct, got: &gotData}
	}
	latency := time.Since(start).Milliseconds()

	if err != nil {
		// 没拿到响应：网络错误 / 首字超时 / 客户端取消
		msg := err.Error()
		if lbl := timeoutLabel(timeoutKind, c.firstByteSec, c.stallSec); lbl != "" {
			msg = lbl
		}
		if timeoutKind == 0 && clientGone(c.r, err) {
			// 客户端自己断开：不是上游的锅，不冷却密钥/供应商，也不计入错误率
			dbg := buildDebugPayload(c.clientReq, upReqBody, c.up.baseURL+"/chat/completions", c.pmod.Model, c.keyMask, 0, nil)
			s.recordAttemptStatus(c.prov, c.pm.Name, c.pmod.Model, c.keyNote, c.keyMask, c.wantStream, latency, 0, 0, 0, 0, statusCanceled, cancelMsg(false, err), dbg, c.reqID, false)
			return attemptResult{handled: false, errMsg: fmt.Sprintf("%s(%s): %s", c.prov.Name, c.keyMask, cancelMsg(false, err))}, false
		}
		s.router.MarkFailure(c.candKey, c.settings.CooldownBaseSec, c.settings.CooldownMaxSec)
		s.router.MarkProviderFailure(c.prov.ID, 3, 60)
		dbg := buildDebugPayload(c.clientReq, upReqBody, c.up.baseURL+"/chat/completions", c.pmod.Model, c.keyMask, 0, nil)
		s.recordAttempt(c.prov, c.pm.Name, c.pmod.Model, c.keyNote, c.keyMask, c.wantStream, latency, 0, 0, 0, 0, true, msg, dbg, c.reqID, false)
		if sw != nil && sw.Committed() {
			// 已发过心跳（客户端已收到 200）：只能把错误写进流里，无法再重试
			sw.ErrorEvent(msg)
			return attemptResult{handled: true, status: http.StatusOK, errMsg: msg}, false
		}
		return attemptResult{handled: false, errMsg: fmt.Sprintf("%s(%s): %s", c.prov.Name, c.keyMask, msg)}, true
	}

	if resp.StatusCode == http.StatusOK {
		var prompt, compl, cached, ttft int64
		var respHead []byte
		committed := true
		if c.wantStream {
			var buf *limitedBuffer
			if c.settings.DebugPayloads {
				buf = &limitedBuffer{max: debugCap}
				resp.Body = newTeeBody(resp.Body, buf)
			}
			var serr error
			prompt, compl, cached, ttft, committed, serr = s.streamWith(sw, resp, start)
			if buf != nil {
				respHead = buf.b
			}
			if serr != nil {
				msg := serr.Error()
				if lbl := timeoutLabel(timeoutKind, c.firstByteSec, c.stallSec); lbl != "" {
					msg = lbl
				}
				dbg := buildDebugPayload(c.clientReq, upReqBody, c.up.baseURL+"/chat/completions", c.pmod.Model, c.keyMask, resp.StatusCode, respHead)
				if timeoutKind == 0 && clientGone(c.r, serr) {
					resp.Body.Close()
					s.recordAttemptStatus(c.prov, c.pm.Name, c.pmod.Model, c.keyNote, c.keyMask, c.wantStream, time.Since(start).Milliseconds(), ttft, prompt, compl, cached, statusCanceled, cancelMsg(committed, serr), dbg, c.reqID, false)
					return attemptResult{handled: committed, status: http.StatusOK, errMsg: cancelMsg(committed, serr)}, false
				}
				s.recordAttempt(c.prov, c.pm.Name, c.pmod.Model, c.keyNote, c.keyMask, c.wantStream, time.Since(start).Milliseconds(), 0, 0, 0, 0, true, msg, dbg, c.reqID, false)
				if committed {
					// 已经向客户端输出过内容：只能中止，不能重试/转移
					resp.Body.Close()
					return attemptResult{handled: true, status: http.StatusOK, errMsg: msg}, false
				}
				s.router.MarkFailure(c.candKey, c.settings.CooldownBaseSec, c.settings.CooldownMaxSec)
				s.router.MarkProviderFailure(c.prov.ID, 3, 60)
				return attemptResult{handled: false, errMsg: fmt.Sprintf("%s(%s): %s", c.prov.Name, c.keyMask, msg)}, true
			}
		} else {
			if c.settings.DebugPayloads {
				raw, rerr := io.ReadAll(resp.Body)
				if rerr == nil {
					respHead = raw
					resp.Body = io.NopCloser(bytes.NewReader(raw))
				}
			}
			var err2 error
			prompt, compl, cached, err2 = s.jsonToClient(c.w, resp)
			if err2 != nil {
				msg := err2.Error()
				if lbl := timeoutLabel(timeoutKind, c.firstByteSec, c.stallSec); lbl != "" {
					msg = lbl
				}
				resp.Body.Close()
				dbg := buildDebugPayload(c.clientReq, upReqBody, c.up.baseURL+"/chat/completions", c.pmod.Model, c.keyMask, resp.StatusCode, respHead)
				s.recordAttempt(c.prov, c.pm.Name, c.pmod.Model, c.keyNote, c.keyMask, c.wantStream, time.Since(start).Milliseconds(), 0, 0, 0, 0, true, msg, dbg, c.reqID, false)
				s.router.MarkFailure(c.candKey, c.settings.CooldownBaseSec, c.settings.CooldownMaxSec)
				s.router.MarkProviderFailure(c.prov.ID, 3, 60)
				return attemptResult{handled: false, errMsg: fmt.Sprintf("%s(%s): %s", c.prov.Name, c.keyMask, msg)}, true
			}
		}
		s.router.MarkSuccess(c.candKey)
		s.router.MarkProviderSuccess(c.prov.ID)
		dbg := buildDebugPayload(c.clientReq, upReqBody, c.up.baseURL+"/chat/completions", c.pmod.Model, c.keyMask, resp.StatusCode, respHead)
		// 成功即本次业务请求的终态，final 恒为 true（与是否为最后一个成员无关）
		s.recordAttempt(c.prov, c.pm.Name, c.pmod.Model, c.keyNote, c.keyMask, c.wantStream, time.Since(start).Milliseconds(), ttft, prompt, compl, cached, false, "", dbg, c.reqID, true)
		return attemptResult{handled: true, status: http.StatusOK, prompt: prompt, compl: compl}, false
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	msg := upstreamError(resp.StatusCode, body)
	failoverable := resp.StatusCode >= 500 || resp.StatusCode == 429 ||
		resp.StatusCode == 401 || resp.StatusCode == 402 || resp.StatusCode == 403 || resp.StatusCode == 408
	dbg := buildDebugPayload(c.clientReq, upReqBody, c.up.baseURL+"/chat/completions", c.pmod.Model, c.keyMask, resp.StatusCode, body)
	s.recordAttempt(c.prov, c.pm.Name, c.pmod.Model, c.keyNote, c.keyMask, c.wantStream, latency, 0, 0, 0, 0, true, fmt.Sprintf("%d %s", resp.StatusCode, msg), dbg, c.reqID, false)
	switch {
	case resp.StatusCode == http.StatusPaymentRequired:
		// 余额/额度耗尽（402）：整个供应商跳过一段时间，避免每次请求都白打一圈密钥
		s.router.MarkExhausted(c.prov.ID, c.settings.ExhaustedCooldownSec)
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
		s.router.MarkProviderFailure(c.prov.ID, 3, 60)
	}
	if !failoverable {
		writeUpstreamError(c.w, resp.StatusCode, msg)
		return attemptResult{handled: true, status: resp.StatusCode, errMsg: msg}, false
	}
	s.router.MarkFailure(c.candKey, c.settings.CooldownBaseSec, c.settings.CooldownMaxSec)
	return attemptResult{handled: false, errMsg: fmt.Sprintf("%s(%s): %d %s", c.prov.Name, c.keyMask, resp.StatusCode, msg)}, false
}

// resolveTimeout 取第一个非零值：0 表示继承下一级；负数表示关闭。
func resolveTimeout(vals ...int) int {
	for _, v := range vals {
		if v != 0 {
			if v < 0 {
				return -1
			}
			return v
		}
	}
	return 0
}

// ---------- 调试报文捕获 ----------

const debugCap = 8 * 1024 // 单段报文截断上限

// limitedBuffer 只保留前 max 字节的写入缓冲（丢弃后续，不阻塞 tee）。
type limitedBuffer struct {
	b   []byte
	max int
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	if len(l.b) < l.max {
		room := l.max - len(l.b)
		if len(p) > room {
			l.b = append(l.b, p[:room]...)
		} else {
			l.b = append(l.b, p...)
		}
	}
	return len(p), nil
}

// teeReadCloser 边读边把内容留底到 limitedBuffer（流式响应捕获用）。
type teeReadCloser struct {
	io.Reader
	io.Closer
	buf *limitedBuffer
}

func newTeeBody(rc io.ReadCloser, buf *limitedBuffer) io.ReadCloser {
	return &teeReadCloser{Reader: io.TeeReader(rc, buf), Closer: rc, buf: buf}
}

// buildDebugPayload 汇总一条调试记录：客户端请求 + 上游请求 + 上游响应（均截断）。
// clientReq 仅在调试模式开启时非空，据此保证「关闭即不记录」。
func buildDebugPayload(clientReq string, upReq []byte, upURL, model, keyMask string, status int, respBody []byte) string {
	if clientReq == "" {
		return ""
	}
	d := map[string]any{
		"client_request": clientReq,
		"upstream_request": map[string]any{
			"url":   upURL,
			"model": model,
			"key":   keyMask,
			"body":  truncate(string(upReq), debugCap),
		},
		"upstream_response": map[string]any{
			"status": status,
			"body":   truncate(string(respBody), debugCap),
		},
	}
	b, err := json.Marshal(d)
	if err != nil {
		return ""
	}
	return truncate(string(b), 2*debugCap)
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

// sseChunk 流式块：只关心 usage 与首个内容 delta（用于测首字时间 TTFT）。
type sseChunk struct {
	Usage   *usageExtras `json:"usage"`
	Choices []struct {
		Delta struct {
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
			// 部分上游（如商汤 SenseNova）用非标准字段名输出思考内容
			Reasoning string `json:"reasoning"`
			Text      string `json:"text"`
		} `json:"delta"`
	} `json:"choices"`
}

// streamToClient SSE 透传（逐行刷写），并从带 usage 的块提取 token 数；
// 同时记录首个内容块时间（TTFT，相对 start）。
// 重要：在收到第一条有效 data 之前不向客户端写任何内容（响应头也不写），
// 这样首字超时可以安全重试；committed=true 表示已写出内容，此后的错误只能中止。
func (s *Server) streamToClient(w http.ResponseWriter, resp *http.Response, start time.Time) (prompt, compl, cached, ttft int64, committed bool, err error) {
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "text/event-stream"
	}
	sw := newSSEWriter(w, ct)
	// 首字前心跳：上游思考/排队期间定期发 SSE 注释帧，避免客户端因「长时间无数据」而断开。
	// 代价：一旦发出心跳就已向客户端输出字节，本次请求不再具备「首字超时重试」能力。
	if hbSec := s.heartbeatSec(); hbSec > 0 {
		sw.StartHeartbeat(time.Duration(hbSec) * time.Second)
		defer sw.StopHeartbeat()
	}
	return s.streamWith(sw, resp, start)
}

// heartbeatSec 读取「首字前心跳」间隔；<=0 表示关闭。
func (s *Server) heartbeatSec() int {
	hb := 0
	if s.store != nil {
		s.store.View(func(c *Config) { hb = c.Settings.PreCommitHeartbeatSec })
	}
	return hb
}

// streamWith 把上游 SSE 逐行转给客户端（sw 可能已经因心跳提交过响应头）。
func (s *Server) streamWith(sw *sseWriter, resp *http.Response, start time.Time) (prompt, compl, cached, ttft int64, committed bool, err error) {
	// sawDone：上游已发 [DONE]（或客户端已达流末尾），此后一律视为成功。
	// OpenAI 客户端收到 [DONE] 就会关闭连接，网关此时再读上游必然拿到
	// "context canceled"；那不是「客户端中途放弃」，不能记成中断/失败。
	sawDone := false
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		payload := ""
		isData := strings.HasPrefix(line, "data:")
		if isData {
			payload = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload != "" && payload != "[DONE]" {
				var chunk sseChunk
				if json.Unmarshal([]byte(payload), &chunk) == nil {
					if chunk.Usage != nil {
						prompt, compl, cached = chunk.Usage.PromptTokens, chunk.Usage.CompletionTokens, chunk.Usage.cached()
					}
					if ttft == 0 {
						for _, ch := range chunk.Choices {
							if ch.Delta.Content != "" || ch.Delta.ReasoningContent != "" ||
								ch.Delta.Reasoning != "" || ch.Delta.Text != "" {
								ttft = time.Since(start).Milliseconds()
								break
							}
						}
					}
				}
			}
		}
		sw.Line(line)
		// 第一条有效 data 到达即提交；缓冲过多（如大量注释帧）也强制提交，避免内存膨胀
		if !sw.Committed() && ((isData && payload != "") || sw.PendingLen() > 2000) {
			sw.Commit()
		}
		// 收到 [DONE] 即本次流已完整结束：主动收尾，不再继续读上游，
		// 避免客户端正常关闭连接后被误判成「客户端已断开」。
		if isData && payload == "[DONE]" {
			sw.Commit()
			sawDone = true
			break
		}
	}
	committed = sw.Committed()
	if sawDone {
		_ = resp.Body.Close()
		return prompt, compl, cached, ttft, true, nil
	}
	if serr := sc.Err(); serr != nil {
		if !committed {
			return 0, 0, 0, 0, false, serr
		}
		return prompt, compl, cached, ttft, true, serr
	}
	if !committed {
		return 0, 0, 0, 0, false, fmt.Errorf("上游未返回任何数据")
	}
	return prompt, compl, cached, ttft, true, nil
}

// sseWriter 统一管理 SSE 输出：未提交前先攒行（保证可重试），提交后逐行刷写；
// 另支持首字前心跳注释帧。多个 goroutine（扫描循环 / 心跳）写同一响应，需加锁。
type sseWriter struct {
	w         http.ResponseWriter
	rc        *http.ResponseController
	ct        string
	mu        sync.Mutex
	committed bool
	closed    bool
	pending   []string
	stop      chan struct{}
}

func newSSEWriter(w http.ResponseWriter, ct string) *sseWriter {
	return &sseWriter{w: w, rc: http.NewResponseController(w), ct: ct}
}

// commitLocked 写出响应头与缓存的待发行（调用方需持锁）。
func (sw *sseWriter) commitLocked() {
	if sw.committed {
		return
	}
	sw.committed = true
	h := sw.w.Header()
	h.Set("Content-Type", sw.ct)
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	h.Set("Connection", "keep-alive")
	sw.w.WriteHeader(http.StatusOK)
	for _, l := range sw.pending {
		_, _ = fmt.Fprintln(sw.w, l)
	}
	sw.pending = nil
	_ = sw.rc.Flush()
}

func (sw *sseWriter) Commit() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.commitLocked()
}

// Line 写一行上游原文；未提交时先攒起来。
func (sw *sseWriter) Line(line string) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if !sw.committed {
		sw.pending = append(sw.pending, line)
		return
	}
	_, _ = fmt.Fprintln(sw.w, line)
	_ = sw.rc.Flush()
}

// Raw 原样写一行并刷写（用于错误事件等非上游原文内容）。
func (sw *sseWriter) Raw(line string) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if !sw.committed {
		sw.commitLocked()
	}
	_, _ = fmt.Fprint(sw.w, line)
	_ = sw.rc.Flush()
}

// ErrorEvent 向已提交的流写入一条 OpenAI 风格的错误事件后收尾。
func (sw *sseWriter) ErrorEvent(msg string) {
	b, _ := json.Marshal(map[string]any{"error": map[string]any{"message": msg, "type": "upstream_error", "code": "upstream_error"}})
	sw.Raw("data: " + string(b) + "\n\ndata: [DONE]\n\n")
}

func (sw *sseWriter) Committed() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.committed
}

func (sw *sseWriter) PendingLen() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.pending)
}

// StartHeartbeat 每 d 发一个 SSE 注释帧（": keepalive"），客户端会忽略但连接保持活跃。
func (sw *sseWriter) StartHeartbeat(d time.Duration) {
	sw.mu.Lock()
	if sw.stop != nil {
		sw.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	sw.stop = stop
	sw.mu.Unlock()
	go func() {
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				sw.mu.Lock()
				if sw.closed {
					sw.mu.Unlock()
					return
				}
				sw.commitLocked()
				_, _ = fmt.Fprint(sw.w, ": keepalive\n\n")
				_ = sw.rc.Flush()
				sw.mu.Unlock()
			}
		}
	}()
}

func (sw *sseWriter) StopHeartbeat() {
	sw.mu.Lock()
	if sw.stop != nil {
		close(sw.stop)
		sw.stop = nil
	}
	sw.closed = true
	sw.mu.Unlock()
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
	key, ok := s.checkAPIKey(r)
	if !ok {
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
			if !key.AllowsModel(pm.Name, pm) {
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

// 日志状态：0=成功 1=上游失败 2=客户端主动断开（不计入错误率，也不惩罚密钥/供应商）
const statusCanceled = 2

func (s *Server) recordAttempt(prov Provider, platformName, upModel, keyNote, upKey string, stream bool, latency, ttft int64, prompt, compl, cached int64, isErr bool, errMsg, debug, reqID string, final bool) {
	status := 0
	if isErr {
		status = 1
	}
	s.recordAttemptStatus(prov, platformName, upModel, keyNote, upKey, stream, latency, ttft, prompt, compl, cached, status, errMsg, debug, reqID, final)
}

// recordAttemptStatus 按显式状态落日志；status=2（客户端断开）只写明细，不进每日统计与指标，
// 避免把「客户端自己不等了」算成上游故障。
func (s *Server) recordAttemptStatus(prov Provider, platformName, upModel, keyNote, upKey string, stream bool, latency, ttft int64, prompt, compl, cached int64, status int, errMsg, debug, reqID string, final bool) {
	rec := RequestLog{
		Time: time.Now().Unix(), ReqID: reqID, Final: final, KeyNote: keyNote, Model: platformName,
		Provider: prov.Name, ProviderID: prov.ID, UpModel: upModel, UpKey: upKey, Status: status,
		Latency: latency, TTFT: ttft, Stream: stream, PromptTok: prompt, ComplTok: compl, CachedTok: cached, Error: errMsg, Debug: debug,
	}
	s.db.InsertLog(rec)
	if status == statusCanceled {
		return
	}
	isErr := status == 1
	s.db.UpsertStat(time.Now().Format("2006-01-02"), prov.ID, platformName, isErr, prompt, compl, cached, latency, ttft, final)
	s.metrics.Observe(prov.Name, platformName, isErr, latency, ttft, prompt, compl, cached)
}

// newReqID 生成业务请求 ID（一次客户端请求内所有尝试共享）。
func newReqID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ---------- 管理端鉴权 ----------

func (s *Server) newPasswordToken() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

const (
	loginMaxFails   = 5                // 同 IP 连续失败次数上限
	loginLockWindow = 60 * time.Second // 超过上限后的锁定时间
)

// clientIP 取客户端 IP（LAN 直连场景取 RemoteAddr；部署在反代后需按其 X-Forwarded-For 配置）。
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	now := time.Now()

	// 防爆破：同 IP 连续失败超过上限后锁定一段时间
	s.loginMu.Lock()
	lf := s.loginFail[ip]
	if lf == nil {
		lf = &loginFails{}
		s.loginFail[ip] = lf
	}
	if now.Before(lf.until) {
		retry := int(lf.until.Sub(now).Seconds()) + 1
		s.loginMu.Unlock()
		w.Header().Set("Retry-After", strconv.Itoa(retry))
		writeErr(w, http.StatusTooManyRequests, "too_many_attempts", fmt.Sprintf("登录失败次数过多，请 %d 秒后重试", retry))
		return
	}
	if len(s.loginFail) > 4096 { // 粗粒度防膨胀
		for k, v := range s.loginFail {
			if now.After(v.until) && v.fails == 0 {
				delete(s.loginFail, k)
			}
		}
	}
	s.loginMu.Unlock()

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
		s.loginMu.Lock()
		lf = s.loginFail[ip]
		if lf == nil {
			lf = &loginFails{}
			s.loginFail[ip] = lf
		}
		lf.fails++
		if lf.fails >= loginMaxFails {
			lf.until = now.Add(loginLockWindow)
			lf.fails = 0
			s.loginMu.Unlock()
			writeErr(w, http.StatusTooManyRequests, "too_many_attempts", "登录失败次数过多，已锁定 60 秒")
			return
		}
		left := loginMaxFails - lf.fails
		s.loginMu.Unlock()
		writeErr(w, http.StatusUnauthorized, "bad_password", fmt.Sprintf("密码错误（还可尝试 %d 次）", left))
		return
	}
	// 登录成功：清除该 IP 的失败计数
	s.loginMu.Lock()
	delete(s.loginFail, ip)
	s.loginMu.Unlock()
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
			ModelCount    int `json:"model_count"`
			KeyCount      int `json:"key_count"`
			ExhaustedLeft int `json:"exhausted_left,omitempty"` // 额度耗尽剩余秒数（0=正常）
			BreakerLeft   int `json:"breaker_left,omitempty"`   // 熔断剩余秒数（0=正常）
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
				snap := s.router.ProviderSnapshot()
				st := snap[p.ID]
				out = append(out, pv{Provider: p, ModelCount: n, KeyCount: len(p.EnabledKeys()), ExhaustedLeft: st[0], BreakerLeft: st[1]})
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

// handleProviderStatus 启用/停用供应商（停用后不参与任何调用选择）。
func (s *Server) handleProviderStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "仅支持 POST")
		return
	}
	var body struct {
		Enabled bool `json:"enabled"`
	}
	if err := readJSON(r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不合法")
		return
	}
	id := r.PathValue("id")
	updated := false
	s.store.Update(func(c *Config) {
		for i := range c.Providers {
			if c.Providers[i].ID == id {
				c.Providers[i].Enabled = body.Enabled
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
				p.Keys[i].MaxInflight = body.MaxInflight
				p.Keys[i].RateLimit = body.RateLimit
				p.Keys[i].RateWindowSec = body.RateWindowSec
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
			Note   string   `json:"note"`
			Models []string `json:"models"`
		}
		_ = readJSON(r, &body)
		var raw [16]byte
		_, _ = rand.Read(raw[:])
		key := "sk-" + hex.EncodeToString(raw[:])
		nk := APIKey{Key: key, Note: body.Note, CreatedAt: time.Now().Unix(), Models: cleanModelList(body.Models)}
		s.store.Update(func(c *Config) { c.APIKeys = append(c.APIKeys, nk) })
		writeJSON(w, http.StatusOK, nk)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
	}
}

// cleanModelList 去空白、去重，保留声明顺序；返回 nil 表示「不限制」。
func cleanModelList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, m := range in {
		m = strings.TrimSpace(m)
		if m == "" || seen[m] {
			continue
		}
		seen[m] = true
		out = append(out, m)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (s *Server) handleKeyByID(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if r.Method == http.MethodPut || r.Method == http.MethodPatch {
		var body struct {
			Note   *string   `json:"note"`
			Models *[]string `json:"models"`
		}
		if err := readJSON(r, &body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_request", "请求体不是合法 JSON")
			return
		}
		var out APIKey
		ok := false
		s.store.Update(func(c *Config) {
			for i := range c.APIKeys {
				if c.APIKeys[i].Key != key {
					continue
				}
				if body.Note != nil {
					c.APIKeys[i].Note = *body.Note
				}
				if body.Models != nil {
					c.APIKeys[i].Models = cleanModelList(*body.Models)
				}
				out, ok = c.APIKeys[i], true
				return
			}
		})
		if !ok {
			writeErr(w, http.StatusNotFound, "key_not_found", "密钥不存在")
			return
		}
		writeJSON(w, http.StatusOK, out)
		return
	}
	if r.Method != http.MethodDelete {
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的方法")
		return
	}
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
	// 明细里的供应商展示为名称（库里存的是 ID）；旧日志无 ID 时按平台模型唯一成员反查
	nameByID := map[string]string{}
	modelToProv := map[string]string{}
	s.store.View(func(c *Config) {
		for _, p := range c.Providers {
			nameByID[p.ID] = p.Name
		}
		pmodOwner := map[string]string{}
		for _, pmod := range c.ProviderModels {
			if n := nameByID[pmod.ProviderID]; n != "" {
				pmodOwner[pmod.ID] = n
			}
		}
		for _, pm := range c.PlatformModels {
			set := map[string]bool{}
			for _, m := range pm.Members {
				if n := pmodOwner[m.ProviderModelID]; n != "" {
					set[n] = true
				}
			}
			if len(set) == 1 {
				for n := range set {
					modelToProv[pm.Name] = n
				}
			}
		}
	})
	for i := range rows {
		if n, ok := nameByID[rows[i].ProviderID]; ok && n != "" {
			rows[i].ProviderID = n
			continue
		}
		if rows[i].ProviderID == "" {
			if n, ok := modelToProv[rows[i].Model]; ok {
				rows[i].ProviderID = n
			}
		}
	}
	// 同 日+供应商+模型 合并（历史日志无 provider_id，会与迁移后的新日志拆成两行）
	merged := make([]DailyStat, 0, len(rows))
	rowIdx := map[string]int{}
	for _, r := range rows {
		k := r.Day + "|" + r.ProviderID + "|" + r.Model
		if i, ok := rowIdx[k]; ok {
			merged[i].Requests += r.Requests
			merged[i].Errors += r.Errors
			merged[i].CReq += r.CReq
			merged[i].CFail += r.CFail
			merged[i].PromptTok += r.PromptTok
			merged[i].ComplTok += r.ComplTok
			merged[i].CachedTok += r.CachedTok
			merged[i].LatencySum += r.LatencySum
			continue
		}
		rowIdx[k] = len(merged)
		merged = append(merged, r)
	}
	rows = merged

	// 汇总口径与明细一致：有业务口径用业务口径，历史行（creq=0）退回尝试数
	var creq, cfail, legacyRows int64
	finalDays := s.db.DaysWithFinal()
	for _, r := range rows {
		if r.CReq > 0 && finalDays[r.Day] {
			creq += r.CReq
			cfail += r.CFail
		} else {
			creq += r.Requests
			cfail += r.Errors
			if r.Requests > 0 {
				legacyRows++
			}
		}
	}

	fromTs := dayStartUnix(from)
	toTs := dayStartUnix(to) + 86400 - 1 // 当天 23:59:59
	logs := s.db.QueryLogs(fromTs, toTs, 20) // 最近请求只展示 20 条
	writeJSON(w, http.StatusOK, map[string]any{
		"from": from, "to": to, "rows": rows,
		"legacy_rows": legacyRows,
		"totals": map[string]any{"requests": reqs, "errors": errs, "client_requests": creq, "client_errors": cfail, "prompt_tokens": ptok, "completion_tokens": ctok, "cached_tokens": cktok, "total_tokens": ptok + ctok},
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

// handleLogs 请求日志，分页返回（新→旧）。参数：page（从 1 起）、page_size（默认 50，上限 200）。
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	page := 1
	if v, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && v > 0 {
		page = v
	}
	size := 50
	if v, err := strconv.Atoi(r.URL.Query().Get("page_size")); err == nil && v > 0 {
		size = v
	}
	if size > 200 {
		size = 200
	}
	total := s.db.CountLogs()
	pages := (total + int64(size) - 1) / int64(size)
	if pages < 1 {
		pages = 1
	}
	if int64(page) > pages {
		page = int(pages)
	}
	logs := s.db.QueryLogsPaged(size, (page-1)*size)
	writeJSON(w, http.StatusOK, map[string]any{"logs": logs, "total": total, "page": page, "page_size": size})
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
				v := body.ChatTimeoutSec
				if v < 10 {
					v = 10
				}
				if v > 1800 {
					v = 1800
				}
				c.Settings.ChatTimeoutSec = v
			}
			// 上游超时：允许 0（用默认）与 -1（关闭）；正数直接生效
			c.Settings.FirstByteTimeoutSec = body.FirstByteTimeoutSec
			c.Settings.StallTimeoutSec = body.StallTimeoutSec
			c.Settings.TimeoutRetries = body.TimeoutRetries
			hb := body.PreCommitHeartbeatSec
			if hb < -1 {
				hb = -1
			}
			if hb > 60 {
				hb = 60
			}
			c.Settings.PreCommitHeartbeatSec = hb
			if body.ExhaustedCooldownSec > 0 {
				v := body.ExhaustedCooldownSec
				if v < 60 {
					v = 60
				}
				if v > 86400 {
					v = 86400
				}
				c.Settings.ExhaustedCooldownSec = v
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
			// 布尔值直接赋值：面板每次都会带上完整设置
			c.Settings.DebugPayloads = body.DebugPayloads
			if body.LogRetentionDays > 0 {
				days := body.LogRetentionDays
				if days > 3650 {
					days = 3650
				}
				c.Settings.LogRetentionDays = days
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
	mux.HandleFunc("GET /metrics", s.handleMetrics) // Prometheus 文本指标（LAN 内开放）

	// 管理端鉴权
	mux.HandleFunc("POST /admin/api/login", s.handleLogin)
	mux.HandleFunc("POST /admin/api/logout", s.requireAuth(s.handleLogout))
	mux.HandleFunc("GET /admin/api/auth", s.requireAuth(s.handleAuthCheck))
	mux.HandleFunc("POST /admin/api/password", s.requireAuth(s.handlePassword))

	// 供应商
	mux.HandleFunc("/admin/api/providers", s.requireAuth(s.handleProviders))
	mux.HandleFunc("/admin/api/providers/{id}", s.requireAuth(s.handleProviderByID))
	mux.HandleFunc("/admin/api/providers/{id}/status", s.requireAuth(s.handleProviderStatus))
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
	mux.HandleFunc("GET /admin/api/logs", s.requireAuth(s.handleLogs))
	mux.HandleFunc("POST /admin/api/models/test", s.requireAuth(s.handleModelTest))
	mux.HandleFunc("/admin/api/settings", s.requireAuth(s.handleSettings))
	mux.HandleFunc("GET /admin/api/config/export", s.requireAuth(s.handleConfigExport))
	mux.HandleFunc("POST /admin/api/config/import", s.requireAuth(s.handleConfigImport))
	mux.HandleFunc("POST /admin/api/providers/{id}/recover", s.requireAuth(s.handleProviderRecover))

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

	return s.timingMiddleware(s.corsMiddleware(mux))
}

// clientGone 判断错误是否来自「客户端主动断开」（而非上游故障或本地超时看门狗）。
func clientGone(r *http.Request, err error) bool {
	if r.Context().Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled)
}

// cancelMsg 生成客户端断开的日志文案，区分是否已经输出过内容。
func cancelMsg(committed bool, err error) string {
	if committed {
		return "客户端已断开（已输出部分内容）"
	}
	return "客户端已断开（上游尚未返回任何数据）"
}

// statusWriter 记录状态码，供诊断日志使用。
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Flush 透传：不实现它会导致包装后的 ResponseWriter 丢失 http.Flusher，
// 流式响应（SSE）无法逐行刷写，客户端看到成块的「一顿一顿」输出。
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap 让 http.ResponseController 能穿透到原始 ResponseWriter。
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// timingMiddleware 记录慢请求（>800ms）与关键诊断路径（登录/鉴权/统计/面板），
// 用于区分「服务端慢」与「链路/客户端慢」。
func (s *Server) timingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 面板与管理接口禁用长连接：容器重建后旧连接会变成「半死」状态，
		// 浏览器复用它会一直等到 TCP 超时（十几秒）。每次请求新建连接可彻底避免。
		if r.URL.Path == "/" || r.URL.Path == "/admin" || strings.HasPrefix(r.URL.Path, "/admin/") {
			w.Header().Set("Connection", "close")
		}
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		d := time.Since(start)

		ua := r.UserAgent()
		if len(ua) > 60 {
			ua = ua[:60]
		}
		p := r.URL.Path
		keyPath := p == "/admin/api/login" || p == "/admin/api/auth" || p == "/admin/api/stats" || p == "/" || p == "/admin"
		if keyPath || d > 800*time.Millisecond {
			_, hasCookie := r.Cookie(sessionCookie)
			log.Printf("[req] %s %s %d %dms from=%s cookie=%v ua=%q", r.Method, p, sw.status, d.Milliseconds(), clientIP(r), hasCookie, ua)
		}
	})
}
