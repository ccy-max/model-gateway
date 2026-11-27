package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestClientGoneDetection 客户端断开应可识别，且不被当成上游故障。
func TestClientGoneDetection(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if clientGone(r, context.Canceled) != true {
		t.Fatal("context.Canceled 应判定为客户端断开")
	}
	if clientGone(r, errors.New("首字超时(120s)")) != false {
		t.Fatal("普通上游错误不应判定为客户端断开")
	}
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	r2 := r.WithContext(ctx)
	if !clientGone(r2, errors.New("read tcp: context canceled")) {
		t.Fatal("请求上下文已取消时应判定为客户端断开")
	}
}

// TestRecordCanceledNotCountedAsError 客户端断开的尝试：写日志(status=2)但不进每日统计，
// 避免把「客户端自己不等了」算成上游错误、也不应触发密钥冷却。
func TestRecordCanceledNotCountedAsError(t *testing.T) {
	st := newTestStore(t)
	s := NewServer(st, "test")

	prov := Provider{ID: "p_test", Name: "测试供应商"}
	s.recordAttemptStatus(prov, "模型A", "up-a", "", "sk-xxx...yyyy", true, 9000, 0, 0, 0, 0, statusCanceled, "客户端已断开（上游尚未返回任何数据）", "", "req1", false)

	logs := s.db.QueryLogsRecent(10)
	if len(logs) != 1 {
		t.Fatalf("应写入 1 条日志，实际 %d", len(logs))
	}
	if logs[0].Status != statusCanceled {
		t.Fatalf("状态应为 %d（客户端断开），实际 %d", statusCanceled, logs[0].Status)
	}
	_, reqs, errs, _, _, _ := s.db.QueryStats("2000-01-01", "2999-12-31")
	if reqs != 0 || errs != 0 {
		t.Fatalf("客户端断开不应进入统计，实际 requests=%d errors=%d", reqs, errs)
	}
	if cancelMsg(false, context.Canceled) != "客户端已断开（上游尚未返回任何数据）" {
		t.Fatal("未输出内容的文案不符")
	}
	if cancelMsg(true, context.Canceled) != "客户端已断开（已输出部分内容）" {
		t.Fatal("已输出内容的文案不符")
	}
}
