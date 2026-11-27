package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestStatsTotalsIncludeHistoricalRows 历史行（无业务口径）应退回尝试数参与汇总，
// 避免「选 7 天但卡片只显示今天」的问题。
func TestStatsTotalsIncludeHistoricalRows(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	s.store.Update(func(c *Config) { c.Password = "test-pass" })
	day := time.Now().Format("2006-01-02")
	// 历史行：final=false → creq=0，尝试 10 次、错误 3 次
	for i := 0; i < 10; i++ {
		isErr := i < 3
		s.db.UpsertStat(day, "p_old", "old-model", isErr, 10, 5, 0, 100, 0, false)
	}
	// 新行：final=true → 业务请求 2 次、失败 1 次（每次 2 个尝试）
	s.db.UpsertStat(day, "p_new", "new-model", true, 10, 5, 0, 100, 0, true)
	s.db.UpsertStat(day, "p_new", "new-model", false, 10, 5, 0, 100, 0, true)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/api/stats?from="+day+"&to="+day, nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: loginCookie(t, ts.URL)})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Totals map[string]any `json:"totals"`
		Rows   []map[string]any `json:"rows"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	get := func(k string) int {
		if v, ok := out.Totals[k].(float64); ok {
			return int(v)
		}
		return -1
	}
	// 业务口径：历史 10 + 新增 2 = 12；错误：历史 3 + 新增 1 = 4
	if got := get("client_requests"); got != 12 {
		t.Fatalf("client_requests 应为 12（含历史回退），实际 %d", got)
	}
	if got := get("client_errors"); got != 4 {
		t.Fatalf("client_errors 应为 4，实际 %d", got)
	}
	if got := get("requests"); got != 12 {
		t.Fatalf("尝试数应为 12，实际 %d", got)
	}
}
