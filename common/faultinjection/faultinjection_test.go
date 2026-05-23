package faultinjection

import (
	"testing"
	"time"
)

func TestControllerFailNextAutoDisables(t *testing.T) {
	now := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	var controller Controller

	state := controller.Configure(Config{Enabled: true, RunLabel: "run-1", FailNext: 1, TTLSeconds: 60}, now)
	if !state.Enabled || state.RunLabel != "run-1" || state.Remaining != 1 || state.ExpiresAt == nil {
		t.Fatalf("configured state = %+v", state)
	}

	failed, state := controller.ShouldFail(now.Add(time.Second))
	if !failed || state.RunLabel != "run-1" || state.Remaining != 0 {
		t.Fatalf("first should fail = %v state=%+v", failed, state)
	}
	failed, state = controller.ShouldFail(now.Add(2 * time.Second))
	if failed || state.Enabled || state.Remaining != 0 {
		t.Fatalf("second should not fail = %v state=%+v", failed, state)
	}
}

func TestControllerEnabledDefaultsToOneFailure(t *testing.T) {
	now := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	var controller Controller
	state := controller.Configure(Config{Enabled: true}, now)
	if !state.Enabled || state.Remaining != 1 {
		t.Fatalf("configured state = %+v", state)
	}

	failed, _ := controller.ShouldFail(now)
	if !failed {
		t.Fatalf("first operation should fail")
	}
	failed, state = controller.ShouldFail(now.Add(time.Second))
	if failed || state.Enabled {
		t.Fatalf("second operation should not fail = %v state=%+v", failed, state)
	}
}

func TestControllerExpires(t *testing.T) {
	now := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	var controller Controller
	controller.Configure(Config{Enabled: true, FailNext: 10, TTLSeconds: 1}, now)

	failed, state := controller.ShouldFail(now.Add(2 * time.Second))
	if failed || state.Enabled {
		t.Fatalf("expired should not fail = %v state=%+v", failed, state)
	}
}
