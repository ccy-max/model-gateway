package gateway

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Metrics 进程内 Prometheus 指标（零依赖，手写文本暴露格式）。
// 记录点集中在 recordAttempt（每次上游尝试）。
type Metrics struct {
	mu       sync.Mutex
	counters map[string]uint64 // "<指标名>\x00<provider>\x00<model>" -> 累计值

	latBuckets []float64 // 请求总耗时（秒）
	latCounts  []uint64
	latSum     float64
	latCount   uint64

	ttftBuckets []float64 // 首字时间（秒）
	ttftCounts  []uint64
	ttftSum     float64
	ttftCount   uint64
}

// NewMetrics 创建指标收集器。
func NewMetrics() *Metrics {
	return &Metrics{
		counters:    map[string]uint64{},
		latBuckets:  []float64{1, 2, 5, 10, 20, 30, 60, 120, 300, 600, 1800},
		latCounts:   make([]uint64, 11),
		ttftBuckets: []float64{0.5, 1, 2, 3, 5, 10, 20, 30, 60, 120},
		ttftCounts:  make([]uint64, 10),
	}
}

func (m *Metrics) add(name, provider, model string, n uint64) {
	if n == 0 {
		return
	}
	m.counters[name+"\x00"+provider+"\x00"+model] += n
}

// Observe 记录一次上游尝试。
func (m *Metrics) Observe(provider, model string, isErr bool, latencyMs, ttftMs, prompt, compl, cached int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	status := "gw_requests_total_ok"
	if isErr {
		status = "gw_requests_total_error"
	}
	m.add(status, provider, model, 1)
	m.add("gw_tokens_total_prompt", provider, model, uint64(max64(prompt, 0)))
	m.add("gw_tokens_total_completion", provider, model, uint64(max64(compl, 0)))
	m.add("gw_tokens_total_cached", provider, model, uint64(max64(cached, 0)))

	// 每次尝试都计入耗时直方图（本地快速请求可能是 0ms，仍需计入 count）
	sec := float64(max64(latencyMs, 0)) / 1000
	m.latSum += sec
	m.latCount++
	for i, b := range m.latBuckets {
		if sec <= b {
			m.latCounts[i]++
		}
	}
	if ttftMs > 0 {
		sec := float64(ttftMs) / 1000
		m.ttftSum += sec
		m.ttftCount++
		for i, b := range m.ttftBuckets {
			if sec <= b {
				m.ttftCounts[i]++
			}
		}
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// escLabel 转义 Prometheus 标签值。
func escLabel(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return v
}

// Render 输出文本暴露格式（配置相关 gauge 由调用方追加）。
func (m *Metrics) Render() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder

	// 请求数（ok / error）
	type row struct{ provider, model, extra string; val uint64 }
	var reqs []row
	for k, v := range m.counters {
		p := strings.Split(k, "\x00")
		if len(p) != 3 {
			continue
		}
		switch p[0] {
		case "gw_requests_total_ok":
			reqs = append(reqs, row{p[1], p[2], "ok", v})
		case "gw_requests_total_error":
			reqs = append(reqs, row{p[1], p[2], "error", v})
		}
	}
	sort.Slice(reqs, func(i, j int) bool {
		if reqs[i].provider != reqs[j].provider {
			return reqs[i].provider < reqs[j].provider
		}
		if reqs[i].model != reqs[j].model {
			return reqs[i].model < reqs[j].model
		}
		return reqs[i].extra < reqs[j].extra
	})
	if len(reqs) > 0 {
		b.WriteString("# HELP gw_requests_total 上游尝试次数（按供应商/平台模型/结果）\n# TYPE gw_requests_total counter\n")
		for _, r := range reqs {
			fmt.Fprintf(&b, "gw_requests_total{provider=\"%s\",model=\"%s\",status=\"%s\"} %d\n",
				escLabel(r.provider), escLabel(r.model), r.extra, r.val)
		}
	}

	// tokens（prompt / completion / cached）
	var toks []row
	for k, v := range m.counters {
		p := strings.Split(k, "\x00")
		if len(p) != 3 || !strings.HasPrefix(p[0], "gw_tokens_total_") {
			continue
		}
		toks = append(toks, row{p[1], p[2], strings.TrimPrefix(p[0], "gw_tokens_total_"), v})
	}
	sort.Slice(toks, func(i, j int) bool {
		if toks[i].provider != toks[j].provider {
			return toks[i].provider < toks[j].provider
		}
		if toks[i].model != toks[j].model {
			return toks[i].model < toks[j].model
		}
		return toks[i].extra < toks[j].extra
	})
	if len(toks) > 0 {
		b.WriteString("# HELP gw_tokens_total 上游 tokens 用量（按供应商/平台模型/类型）\n# TYPE gw_tokens_total counter\n")
		for _, t := range toks {
			fmt.Fprintf(&b, "gw_tokens_total{provider=\"%s\",model=\"%s\",type=\"%s\"} %d\n",
				escLabel(t.provider), escLabel(t.model), t.extra, t.val)
		}
	}

	b.WriteString("# HELP gw_request_duration_seconds 上游请求总耗时（秒）\n# TYPE gw_request_duration_seconds histogram\n")
	for i, bb := range m.latBuckets {
		fmt.Fprintf(&b, "gw_request_duration_seconds_bucket{le=\"%g\"} %d\n", bb, m.latCounts[i])
	}
	fmt.Fprintf(&b, "gw_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", m.latCount)
	fmt.Fprintf(&b, "gw_request_duration_seconds_sum %g\n", m.latSum)
	fmt.Fprintf(&b, "gw_request_duration_seconds_count %d\n", m.latCount)

	b.WriteString("# HELP gw_ttft_seconds 流式首字时间（秒）\n# TYPE gw_ttft_seconds histogram\n")
	for i, bb := range m.ttftBuckets {
		fmt.Fprintf(&b, "gw_ttft_seconds_bucket{le=\"%g\"} %d\n", bb, m.ttftCounts[i])
	}
	fmt.Fprintf(&b, "gw_ttft_seconds_bucket{le=\"+Inf\"} %d\n", m.ttftCount)
	fmt.Fprintf(&b, "gw_ttft_seconds_sum %g\n", m.ttftSum)
	fmt.Fprintf(&b, "gw_ttft_seconds_count %d\n", m.ttftCount)

	return b.String()
}
