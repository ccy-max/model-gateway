package gateway

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Candidate 参与选路的成员快照。
type Candidate struct {
	Key       string // 平台模型内唯一：平台模型ID + "|" + 成员供应商模型ID
	Weight    int
	Priority  int  // 越小越优先
	Available bool // 供应商/供应商模型/成员均启用
}

// memberState 成员运行时状态（仅进程内，不落盘）。
type memberState struct {
	inflight      int
	currentWeight int         // 平滑加权轮询游标
	consecFails   int
	cooldownUntil time.Time
	hits          []time.Time // 时间窗限流：窗口内的请求时间戳
}

// stickyEntry 会话粘性记录。
type stickyEntry struct {
	key  string
	used int
	seen time.Time
}

// providerState 供应商级运行时状态（进程内）：额度耗尽（402）与连续失败熔断。
type providerState struct {
	exhaustedUntil time.Time // 额度耗尽：到期前整体跳过
	breakerUntil   time.Time // 熔断：到期前整体跳过（到期后放行一次作为半开探测）
	consecFails    int
}

// Router 成员选路器：加权轮询 / 优先级故障转移 / 失败冷却 / 会话粘性 / 最少并发。
//
// 熔断语义：连续失败进入指数冷却（base*2^(n-1)，封顶 max），冷却到期即恢复
// （相当于半开探测——下一请求放行，成功清零，失败翻倍再冷）。
type Router struct {
	mu      sync.Mutex
	states  map[string]*memberState
	provs   map[string]*providerState
	sticky  map[string]*stickyEntry
	nowFunc func() time.Time
}

// NewRouter 创建选路器。
func NewRouter() *Router {
	return &Router{
		states:  map[string]*memberState{},
		provs:   map[string]*providerState{},
		sticky:  map[string]*stickyEntry{},
		nowFunc: time.Now,
	}
}

// state 取成员状态（须持锁调用）。
func (r *Router) state(key string) *memberState {
	st, ok := r.states[key]
	if !ok {
		st = &memberState{}
		r.states[key] = st
	}
	return st
}

// Pick 返回按尝试顺序排列的候选：
//  1. 先过滤可用（Enabled 且不在冷却期）成员；若全被冷却则放行全部 Enabled（可用性优先）；
//  2. 按优先级升序分组，组内按策略排序（sticky 提升仅作用于最优组）；
//  3. 返回逐组拼接的完整尝试顺序，调用方按 MaxRetry 截断尝试。
func (r *Router) Pick(cands []Candidate, strategy, stickyKey string, stickyLimit int) []Candidate {
	pool := make([]Candidate, 0, len(cands))
	for _, c := range cands {
		if !c.Available {
			continue
		}
		r.mu.Lock()
		avail := r.nowFunc().After(r.state(c.Key).cooldownUntil)
		r.mu.Unlock()
		if avail {
			pool = append(pool, c)
		}
	}
	if len(pool) == 0 { // 全冷却：放行，避免整体不可用
		for _, c := range cands {
			if c.Available {
				pool = append(pool, c)
			}
		}
	}
	if len(pool) == 0 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	groups := map[int][]Candidate{}
	prios := make([]int, 0, 4)
	for _, c := range pool {
		if _, ok := groups[c.Priority]; !ok {
			prios = append(prios, c.Priority)
		}
		groups[c.Priority] = append(groups[c.Priority], c)
	}
	sort.Ints(prios)

	out := make([]Candidate, 0, len(pool))
	for i, p := range prios {
		g := groups[p]
		r.sortGroup(g, strategy, stickyKey, stickyLimit, i == 0)
		out = append(out, g...)
	}
	return out
}

// sortGroup 组内排序（须持锁调用）。allowSticky 时做粘性提升。
func (r *Router) sortGroup(g []Candidate, strategy, stickyKey string, stickyLimit int, allowSticky bool) {
	switch strategy {
	case "least-inflight":
		sort.SliceStable(g, func(i, j int) bool {
			ii := r.state(g[i].Key).inflight
			jj := r.state(g[j].Key).inflight
			if ii != jj {
				return ii < jj
			}
			return g[i].Weight > g[j].Weight
		})
	default: // sticky / round-robin：平滑加权轮询给出完整顺序
		ordered := r.wrrOrder(g)
		copy(g, ordered)
	}
	if allowSticky && strategy == "sticky" && stickyKey != "" {
		if e, ok := r.sticky[stickyKey]; ok {
			if stickyLimit <= 0 || e.used < stickyLimit {
				for i, c := range g {
					if c.Key == e.key {
						g[0], g[i] = g[i], g[0]
						break
					}
				}
			}
		}
	}
}

// wrrOrder 经典平滑加权轮询：对剩余集合反复 current+=weight、取最大者入选并扣减总量。
// wrrOrder 平滑加权轮询（标准 SWRR）：本次只做一次真实选择——全体候选累加自身权重，
// 取累计最大者，选中者扣减「全组总权重」；currentWeight 持久化在路由状态里跨请求连续轮转。
// 其余候选按 当前权重/权重 的比值降序紧随其后，仅作为失败转移顺序，不再改动持久状态。
// 旧实现在构建完整顺序时会反复累加剩余候选、并用不断缩小的「剩余总权重」扣减，
// 破坏 SWRR 不变量：3 把等权 key 会退化成前两把交替、第三把永远轮不到。
func (r *Router) wrrOrder(g []Candidate) []Candidate {
	if len(g) == 0 {
		return g
	}
	total := 0
	best := 0
	bestCur := math.MinInt
	for j := range g {
		w := g[j].Weight
		if w <= 0 {
			w = 1
		}
		st := r.state(g[j].Key)
		st.currentWeight += w
		total += w
		if st.currentWeight > bestCur {
			bestCur = st.currentWeight
			best = j
		}
	}
	r.state(g[best].Key).currentWeight -= total
	out := make([]Candidate, 0, len(g))
	out = append(out, g[best])
	if len(g) == 1 {
		return out
	}
	rest := make([]Candidate, 0, len(g)-1)
	for j := range g {
		if j != best {
			rest = append(rest, g[j])
		}
	}
	sort.SliceStable(rest, func(i, j int) bool {
		wi, wj := rest[i].Weight, rest[j].Weight
		if wi <= 0 {
			wi = 1
		}
		if wj <= 0 {
			wj = 1
		}
		li := r.state(rest[i].Key).currentWeight * wj
		lj := r.state(rest[j].Key).currentWeight * wi
		if li != lj {
			return li > lj
		}
		return wi > wj
	})
	return append(out, rest...)
}

// MarkSuccess 请求成功：清零失败计数并解除冷却。
func (r *Router) MarkSuccess(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.state(key)
	st.consecFails = 0
	st.cooldownUntil = time.Time{}
}

// MarkFailure 请求失败：连续失败计数 +1，并按指数退避设置冷却。
func (r *Router) MarkFailure(key string, baseSec, maxSec int) {
	if baseSec <= 0 {
		baseSec = 10
	}
	if maxSec < baseSec {
		maxSec = baseSec
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.state(key)
	st.consecFails++
	d := time.Duration(baseSec) * time.Second
	for i := 1; i < st.consecFails && d < time.Duration(maxSec)*time.Second; i++ {
		d *= 2
	}
	if d > time.Duration(maxSec)*time.Second {
		d = time.Duration(maxSec) * time.Second
	}
	st.cooldownUntil = r.nowFunc().Add(d)
}

// InflightOf 查询某成员/密钥当前的并发数。
func (r *Router) InflightOf(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state(key).inflight
}

// RateCheck 时间窗限流检查（滑动窗口）：窗口内已用次数 < limit 才放行。
// 只检查不记录；真正发起调用时用 RateRecord 记一笔。
func (r *Router) RateCheck(key string, limit, windowSec int) bool {
	if limit <= 0 || windowSec <= 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.nowFunc()
	st := r.state(key)
	cutoff := now.Add(-time.Duration(windowSec) * time.Second)
	kept := st.hits[:0]
	for _, t := range st.hits {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	st.hits = kept
	return len(st.hits) < limit
}

// RateRecord 记一次请求时间戳（配合 RateCheck 做滑动窗口限流）。
func (r *Router) RateRecord(key string, limit, windowSec int) {
	if limit <= 0 || windowSec <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.state(key)
	st.hits = append(st.hits, r.nowFunc())
}

// Inflight 调整成员并发计数（请求开始 +1，结束 -1）。
func (r *Router) Inflight(key string, delta int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.state(key)
	st.inflight += delta
	if st.inflight < 0 {
		st.inflight = 0
	}
}

// RecordSticky 记录会话粘性命中；换成员时重置计数。
func (r *Router) RecordSticky(stickyKey, memberKey string) {
	if stickyKey == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sticky) > 8192 { // 粗粒度防膨胀
		cut := r.nowFunc().Add(-time.Hour)
		for k, e := range r.sticky {
			if e.seen.Before(cut) {
				delete(r.sticky, k)
			}
		}
		if len(r.sticky) > 8192 {
			r.sticky = map[string]*stickyEntry{}
		}
	}
	e, ok := r.sticky[stickyKey]
	if !ok || e.key != memberKey {
		r.sticky[stickyKey] = &stickyEntry{key: memberKey, used: 1, seen: r.nowFunc()}
		return
	}
	e.used++
	e.seen = r.nowFunc()
}

// CooldownLeft 返回成员剩余冷却秒数（0 表示未冷却），供面板展示。
func (r *Router) CooldownLeft(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.states[key]
	if st == nil {
		return 0
	}
	d := time.Until(st.cooldownUntil)
	if d <= 0 {
		return 0
	}
	return int(d.Seconds())
}

// ---------- 供应商级：额度耗尽 / 熔断 ----------

// pstate 取供应商状态（须持锁调用）。
func (r *Router) pstate(id string) *providerState {
	st, ok := r.provs[id]
	if !ok {
		st = &providerState{}
		r.provs[id] = st
	}
	return st
}

// MarkExhausted 标记供应商额度耗尽（402/余额不足），sec 秒内整体跳过。
func (r *Router) MarkExhausted(providerID string, sec int) {
	if sec <= 0 {
		sec = 1800
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pstate(providerID).exhaustedUntil = r.nowFunc().Add(time.Duration(sec) * time.Second)
}

// ClearExhausted 手动恢复（面板「恢复」按钮）。
func (r *Router) ClearExhausted(providerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.provs[providerID]; ok {
		st.exhaustedUntil = time.Time{}
	}
}

// ClearBreaker 手动恢复熔断。
func (r *Router) ClearBreaker(providerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.provs[providerID]; ok {
		st.breakerUntil = time.Time{}
		st.consecFails = 0
	}
}

// ExhaustedLeft 剩余额度耗尽跳过秒数（0=未标记）。
func (r *Router) ExhaustedLeft(providerID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.provs[providerID]
	if st == nil {
		return 0
	}
	d := st.exhaustedUntil.Sub(r.nowFunc())
	if d <= 0 {
		return 0
	}
	return int(d.Seconds())
}

// BreakerLeft 剩余熔断秒数（0=未熔断）。
func (r *Router) BreakerLeft(providerID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.provs[providerID]
	if st == nil {
		return 0
	}
	d := st.breakerUntil.Sub(r.nowFunc())
	if d <= 0 {
		return 0
	}
	return int(d.Seconds())
}

// MarkProviderFailure 供应商级连续失败计数；达到阈值则熔断 openSec 秒。
func (r *Router) MarkProviderFailure(providerID string, threshold, openSec int) {
	if threshold <= 0 {
		threshold = 3
	}
	if openSec <= 0 {
		openSec = 60
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.pstate(providerID)
	st.consecFails++
	if st.consecFails >= threshold {
		st.breakerUntil = r.nowFunc().Add(time.Duration(openSec) * time.Second)
		st.consecFails = 0
	}
}

// MarkProviderSuccess 供应商级成功：清零连续失败并解除熔断。
func (r *Router) MarkProviderSuccess(providerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.pstate(providerID)
	st.consecFails = 0
	st.breakerUntil = time.Time{}
}

// ProviderSnapshot 返回供应商运行态：exhausted 剩余秒 / breaker 剩余秒 / 连续失败数。
func (r *Router) ProviderSnapshot() map[string][3]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][3]int, len(r.provs))
	now := r.nowFunc()
	for id, st := range r.provs {
		ex := int(st.exhaustedUntil.Sub(now).Seconds())
		if ex < 0 {
			ex = 0
		}
		br := int(st.breakerUntil.Sub(now).Seconds())
		if br < 0 {
			br = 0
		}
		out[id] = [3]int{ex, br, st.consecFails}
	}
	return out
}

// Snapshot 返回各成员运行态（并发/连续失败/剩余冷却秒）。
func (r *Router) Snapshot() map[string][3]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][3]int, len(r.states))
	for k, st := range r.states {
		cd := int(time.Until(st.cooldownUntil).Seconds())
		if cd < 0 {
			cd = 0
		}
		out[k] = [3]int{st.inflight, st.consecFails, cd}
	}
	return out
}
