package gateway

import "testing"

func TestPickPriorityFailover(t *testing.T) {
	r := NewRouter()
	cands := []Candidate{
		{Key: "pm|b", Weight: 1, Priority: 1, Available: true},
		{Key: "pm|a", Weight: 1, Priority: 0, Available: true},
		{Key: "pm|c", Weight: 1, Priority: 1, Available: true},
	}
	order := r.Pick(cands, "round-robin", "", 0)
	if len(order) != 3 {
		t.Fatalf("expect 3 candidates, got %d", len(order))
	}
	if order[0].Key != "pm|a" {
		t.Fatalf("priority 0 member should come first, got %s", order[0].Key)
	}
}

func TestPickCooldownExclusion(t *testing.T) {
	r := NewRouter()
	cands := []Candidate{
		{Key: "pm|a", Weight: 1, Priority: 0, Available: true},
		{Key: "pm|b", Weight: 1, Priority: 0, Available: true},
	}
	r.MarkFailure("pm|a", 60, 60)
	order := r.Pick(cands, "round-robin", "", 0)
	if len(order) != 1 || order[0].Key != "pm|b" {
		t.Fatalf("cooled member should be excluded, got %+v", order)
	}
}

func TestPickRelaxWhenAllCooling(t *testing.T) {
	r := NewRouter()
	cands := []Candidate{{Key: "pm|a", Weight: 1, Priority: 0, Available: true}}
	r.MarkFailure("pm|a", 60, 60)
	order := r.Pick(cands, "round-robin", "", 0)
	if len(order) != 1 {
		t.Fatalf("all-cooling should relax and still return candidates, got %d", len(order))
	}
}

func TestStickyPromotion(t *testing.T) {
	r := NewRouter()
	cands := []Candidate{
		{Key: "pm|a", Weight: 1, Priority: 0, Available: true},
		{Key: "pm|b", Weight: 1, Priority: 0, Available: true},
	}
	r.RecordSticky("sess1", "pm|b")
	order := r.Pick(cands, "sticky", "sess1", 0)
	if order[0].Key != "pm|b" {
		t.Fatalf("sticky member should be promoted, got %s", order[0].Key)
	}
	// 超过 sticky_limit 后不再强制提升
	for i := 0; i < 4; i++ {
		r.RecordSticky("sess1", "pm|b")
	}
	order = r.Pick(cands, "sticky", "sess1", 3)
	if len(order) != 2 {
		t.Fatalf("expect 2 candidates, got %d", len(order))
	}
}

func TestWRRDistribution(t *testing.T) {
	r := NewRouter()
	cands := []Candidate{
		{Key: "pm|a", Weight: 3, Priority: 0, Available: true},
		{Key: "pm|b", Weight: 1, Priority: 0, Available: true},
	}
	counts := map[string]int{}
	for i := 0; i < 40; i++ {
		order := r.Pick(cands, "round-robin", "", 0)
		if len(order) == 0 {
			t.Fatal("no candidate returned")
		}
		counts[order[0].Key]++
	}
	if counts["pm|a"] != 30 || counts["pm|b"] != 10 {
		t.Fatalf("WRR 3:1 over 40 picks expect 30/10, got %v", counts)
	}
}

func TestFailureBackoffAndRecovery(t *testing.T) {
	r := NewRouter()
	base, max := 10, 100
	r.MarkFailure("x", base, max)
	if got := r.CooldownLeft("x"); got < 8 || got > 10 {
		t.Fatalf("first failure cooldown ~10s, got %d", got)
	}
	r.MarkFailure("x", base, max)
	if got := r.CooldownLeft("x"); got < 18 || got > 20 {
		t.Fatalf("second failure cooldown ~20s, got %d", got)
	}
	for i := 0; i < 10; i++ {
		r.MarkFailure("x", base, max)
	}
	if got := r.CooldownLeft("x"); got > max {
		t.Fatalf("cooldown should cap at max %d, got %d", max, got)
	}
	r.MarkSuccess("x")
	if got := r.CooldownLeft("x"); got != 0 {
		t.Fatalf("success should clear cooldown, got %d", got)
	}
}

func TestInflightAccounting(t *testing.T) {
	r := NewRouter()
	r.Inflight("x", 1)
	r.Inflight("x", 1)
	snap := r.Snapshot()
	if snap["x"][0] != 2 {
		t.Fatalf("expect inflight 2, got %v", snap["x"])
	}
	r.Inflight("x", -1)
	r.Inflight("x", -3) // 不应为负
	snap = r.Snapshot()
	if snap["x"][0] != 0 {
		t.Fatalf("inflight should clamp at 0, got %v", snap["x"])
	}
}
