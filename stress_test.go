package pgmcp_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pgmcp "github.com/rickchristie/postgres-mcp"
)

func TestStress_ConcurrentQueries(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p, _ := newTestInstance(t, config)

	const goroutines = 50
	const queriesPerGoroutine = 20

	var wg sync.WaitGroup
	var errCount atomic.Int64
	start := time.Now()

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < queriesPerGoroutine; j++ {
				output := p.Query(context.Background(), pgmcp.QueryInput{
					SQL: fmt.Sprintf("SELECT %d AS id, %d AS iter", id, j),
				})
				if output.Error != "" {
					errCount.Add(1)
					t.Errorf("goroutine %d iter %d: %s", id, j, output.Error)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	if errCount.Load() > 0 {
		t.Fatalf("%d errors in concurrent queries", errCount.Load())
	}

	// With pool size 5 and 1000 total queries, sequential would be much slower.
	// This is a sanity check, not a strict performance assertion.
	t.Logf("completed %d queries in %v (%d goroutines)", goroutines*queriesPerGoroutine, elapsed, goroutines)
}

func TestStress_SemaphoreLimit(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Pool.MaxConns = 3
	p, _ := newTestInstance(t, config)

	const goroutines = 20
	var concurrent atomic.Int64
	var maxConcurrent atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cur := concurrent.Add(1)
			// Track maximum concurrent.
			for {
				old := maxConcurrent.Load()
				if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
					break
				}
			}
			output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT pg_sleep(0.1)"})
			concurrent.Add(-1)
			if output.Error != "" {
				t.Errorf("query error: %s", output.Error)
			}
		}()
	}

	wg.Wait()

	// maxConcurrent tracks goroutines that called Query (not actual DB concurrency),
	// but the semaphore ensures only MaxConns execute at a time.
	// This test mainly validates no deadlocks or errors under contention.
	t.Logf("max concurrent goroutines entered Query: %d (pool max_conns: %d)", maxConcurrent.Load(), config.Pool.MaxConns)
}

func TestStress_LargeResultTruncation(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Query.MaxResultLength = 1000
	p, _ := newTestInstance(t, config)

	// Insert enough rows to exceed max_result_length.
	setupTable(t, p, "CREATE TABLE large_result (id serial PRIMARY KEY, data text)")
	for i := 0; i < 100; i++ {
		setupTable(t, p, fmt.Sprintf("INSERT INTO large_result (data) VALUES ('%s')", strings.Repeat("x", 50)))
	}

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT * FROM large_result"})
	if output.Error == "" {
		t.Fatal("expected truncation error for large result")
	}
	if !strings.Contains(output.Error, "[truncated] Result is too long! Add limits in your query!") {
		t.Fatalf("expected truncation message, got %q", output.Error)
	}
}

func TestStress_ConcurrentHooks(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "passthrough", Hook: &concurrentPassthroughBeforeHook{}},
	}
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "passthrough", Hook: &concurrentPassthroughAfterHook{}},
	}
	p, _ := newTestInstance(t, config)

	const goroutines = 20
	var wg sync.WaitGroup
	var errCount atomic.Int64

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				output := p.Query(context.Background(), pgmcp.QueryInput{
					SQL: fmt.Sprintf("SELECT %d AS id", id*10+j),
				})
				if output.Error != "" {
					errCount.Add(1)
					t.Errorf("goroutine %d iter %d: %s", id, j, output.Error)
				}
			}
		}(i)
	}

	wg.Wait()
	if errCount.Load() > 0 {
		t.Fatalf("%d errors in concurrent hook queries", errCount.Load())
	}
}

func TestStress_MixedOperations(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE mixed_ops (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "INSERT INTO mixed_ops (name) VALUES ('test1'), ('test2')")

	const goroutines = 30
	var wg sync.WaitGroup
	var errCount atomic.Int64

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			switch id % 3 {
			case 0:
				// Query
				output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT * FROM mixed_ops"})
				if output.Error != "" {
					errCount.Add(1)
					t.Errorf("query error: %s", output.Error)
				}
			case 1:
				// ListTables
				_, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
				if err != nil {
					errCount.Add(1)
					t.Errorf("list tables error: %v", err)
				}
			case 2:
				// DescribeTable
				_, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "mixed_ops"})
				if err != nil {
					errCount.Add(1)
					t.Errorf("describe table error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	if errCount.Load() > 0 {
		t.Fatalf("%d errors in mixed operations", errCount.Load())
	}
}

func TestStress_ConcurrentCommandHooks(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Pool.MaxConns = 3
	config.DefaultHookTimeoutSeconds = 5
	hooks := pgmcp.ServerHooksConfig{
		BeforeQuery: []pgmcp.HookEntry{
			{Pattern: ".*", Command: hookScript("accept.sh")},
		},
		AfterQuery: []pgmcp.HookEntry{
			{Pattern: ".*", Command: hookScript("accept.sh")},
		},
	}
	p := newTestInstanceWithHooks(t, config, hooks)

	const goroutines = 20
	var wg sync.WaitGroup
	var errCount atomic.Int64

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				output := p.Query(context.Background(), pgmcp.QueryInput{
					SQL: fmt.Sprintf("SELECT %d AS id", id*5+j),
				})
				if output.Error != "" {
					errCount.Add(1)
					t.Errorf("goroutine %d iter %d: %s", id, j, output.Error)
				}
			}
		}(i)
	}

	wg.Wait()
	if errCount.Load() > 0 {
		t.Fatalf("%d errors in concurrent command hook queries", errCount.Load())
	}
	t.Logf("completed %d queries with command hooks (pool max_conns: %d)", goroutines*5, config.Pool.MaxConns)
}

// concurrentPassthroughBeforeHook is a thread-safe passthrough for stress testing.
type concurrentPassthroughBeforeHook struct{}

func (h *concurrentPassthroughBeforeHook) Run(_ context.Context, sql string) (string, error) {
	return sql, nil
}

// concurrentPassthroughAfterHook is a thread-safe passthrough for stress testing.
type concurrentPassthroughAfterHook struct{}

func (h *concurrentPassthroughAfterHook) Run(_ context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	return result, nil
}
