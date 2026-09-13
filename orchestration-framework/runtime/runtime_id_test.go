package runtime

import (
	"sync"
	"testing"
)

func TestNewRuntimeIDIsUniqueUnderConcurrency(t *testing.T) {
	const goroutines = 32
	const idsPerGoroutine = 10000

	ids := make(chan string, goroutines*idsPerGoroutine)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range idsPerGoroutine {
				ids <- newRuntimeID()
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]struct{}, goroutines*idsPerGoroutine)
	for id := range ids {
		if id == "" {
			t.Fatal("generated empty runtime ID")
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate runtime ID: %s", id)
		}
		seen[id] = struct{}{}
	}
	if got, want := len(seen), goroutines*idsPerGoroutine; got != want {
		t.Fatalf("generated %d unique IDs, want %d", got, want)
	}
}
