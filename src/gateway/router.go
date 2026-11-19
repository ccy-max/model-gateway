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
	currentWeight int // 平滑加权轮询游标
	consecFails   int
	cooldownUntil time.Time
}

// stickyEntry 会话粘性记录。
type stickyEntry struct {
	key  string
	used int
	seen time.Time
}

// Router 成员选路器：加权轮询 / 优先级故障转移 / 失败冷却 / 会话粘性 / 最少并发。
//
// 熔断语义：连续失败进入指数冷却（base*2^(n-1)，封顶 max），冷却到期即恢复
// （相当于半开探测——下一请求放行，成功清零，失败翻倍再冷）。
type Router struct {
	mu      sync.Mutex
	states  map[string]*memberState
	sticky  map[string]*stickyEntry
	nowFunc func() time.Time
}

// NewRouter 创建选路器。
func NewRouter() *Router {
	return &Router{
		states:  map[string]*memberState{},
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
func (r *Router) wrrOrder(g []Candidate) []Candidate {
	rem := make([]Candidate, len(g))
	copy(rem, g)
	out := make([]Candidate, 0, len(g))
	for len(rem) > 0 {
		total := 0
		bestCur := math.MinInt
		bestIdx := 0
		for i := range rem {
			w := rem[i].Weight
			if w <= 0 {
				w = 1
			}
			st := r.state(rem[i].Key)
			st.currentWeight += w
			total += w
			if st.currentWeight > bestCur {
				bestCur = st.currentWeight
				bestIdx = i
			}
		}
		picked := rem[bestIdx]
		r.state(picked.Key).currentWeight -= total
		out = append(out, picked)
		rem = append(rem[:bestIdx], rem[bestIdx+1:]...)
	}
	return out
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
