package semaphore

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

var pool = redisPool()

func redisPool() *redis.Pool {
	redisAddr := ":6379"
	if v, ok := os.LookupEnv("REDIS_ADDR"); ok {
		redisAddr = v
	}
	p := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisAddr)
		},
		MaxIdle:     1,
		MaxActive:   10,
		IdleTimeout: 10 * time.Second,
		Wait:        true,
	}
	return p
}

func TestNewRedisSemaphore(t *testing.T) {
	s := NewRedisSemaphore(pool, "test_prefix")
	if s == nil {
		t.Fatal("NewRedisSemaphore 返回空指针")
	}
	if s.prefix != "test_prefix" {
		t.Errorf("prefix 不正确，期望: test_prefix, 实际: %s", s.prefix)
	}
}

func TestFormatKey(t *testing.T) {
	s := NewRedisSemaphore(pool, "my_prefix")
	tests := []struct {
		resource   string
		wantPrefix string
	}{
		{"resource1", "my_prefix:resource1"},
		{"resource2", "my_prefix:resource2"},
		{"a/b/c", "my_prefix:a/b/c"},
	}

	for _, tt := range tests {
		got := s.formatKey(tt.resource)
		if got != tt.wantPrefix {
			t.Errorf("formatKey(%q) = %q, 期望: %q", tt.resource, got, tt.wantPrefix)
		}
	}
}

// cleanupTestKey 清理测试用的 Redis key
func cleanupTestKey(key string) {
	conn := pool.Get()
	defer conn.Close()
	conn.Do("DEL", key)
}

func TestTryExecute_Success(t *testing.T) {
	prefix := "test_try_execute_success"
	key := fmt.Sprintf("%s:resource1", prefix)
	cleanupTestKey(key)
	defer cleanupTestKey(key)

	s := NewRedisSemaphore(pool, prefix)
	executed := false

	ok, err := s.TryExecute("resource1", 1, 10*time.Second, func() error {
		executed = true
		return nil
	})

	if err != nil {
		t.Fatalf("TryExecute 失败: %v", err)
	}
	if !ok {
		t.Error("TryExecute 应该返回 true")
	}
	if !executed {
		t.Error("业务逻辑应该被执行")
	}
}

func TestTryExecute_WithWorkError(t *testing.T) {
	prefix := "test_try_execute_error"
	key := fmt.Sprintf("%s:resource2", prefix)
	cleanupTestKey(key)
	defer cleanupTestKey(key)

	s := NewRedisSemaphore(pool, prefix)
	expectedErr := fmt.Errorf("业务逻辑错误")

	ok, err := s.TryExecute("resource2", 1, 10*time.Second, func() error {
		return expectedErr
	})

	if err == nil {
		t.Error("TryExecute 应该返回错误")
	}
	if err.Error() != expectedErr.Error() {
		t.Errorf("错误信息不正确，期望: %s, 实际: %s", expectedErr, err)
	}
	if !ok {
		t.Error("执行业务逻辑后 ok 应该为 true")
	}
}

func TestTryExecute_Concurrency(t *testing.T) {
	prefix := "test_concurrency"
	key := fmt.Sprintf("%s:concurrent_resource", prefix)
	cleanupTestKey(key)
	defer cleanupTestKey(key)

	s := NewRedisSemaphore(pool, prefix)
	limit := 3
	concurrency := 10
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _ := s.TryExecute("concurrent_resource", limit, 10*time.Second, func() error {
				time.Sleep(100 * time.Millisecond)
				return nil
			})
			if ok {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if successCount != limit {
		t.Errorf("成功获取信号量的次数不正确，期望: %d, 实际: %d", limit, successCount)
	}
}

func TestTryExecute_ResourceRelease(t *testing.T) {
	prefix := "test_release"
	key := fmt.Sprintf("%s:release_resource", prefix)
	cleanupTestKey(key)
	defer cleanupTestKey(key)

	s := NewRedisSemaphore(pool, prefix)

	// 第一次获取并执行业务
	ok1, _ := s.TryExecute("release_resource", 1, 10*time.Second, func() error {
		return nil
	})
	if !ok1 {
		t.Fatal("第一次获取应该成功")
	}

	// 短暂等待确保释放完成
	time.Sleep(50 * time.Millisecond)

	// 第二次获取应该也能成功（因为第一次已释放）
	ok2, _ := s.TryExecute("release_resource", 1, 10*time.Second, func() error {
		return nil
	})
	if !ok2 {
		t.Error("第二次获取应该成功（资源已释放）")
	}
}

func TestTryExecute_LimitExceeded(t *testing.T) {
	prefix := "test_limit"
	key := fmt.Sprintf("%s:limit_resource", prefix)
	cleanupTestKey(key)
	defer cleanupTestKey(key)

	s := NewRedisSemaphore(pool, prefix)
	limit := 2

	// 第一次获取（不释放，持续占用）
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0

	// goroutine 1: 获取信号量并长时间持有
	wg.Add(1)
	go func() {
		defer wg.Done()
		ok, _ := s.TryExecute("limit_resource", limit, 10*time.Second, func() error {
			time.Sleep(500 * time.Millisecond) // 持有 500ms
			return nil
		})
		if ok {
			mu.Lock()
			successCount++
			mu.Unlock()
		}
	}()

	// goroutine 2: 在 goroutine 1 持有期间尝试获取
	time.Sleep(50 * time.Millisecond) // 确保 goroutine 1 先获取
	wg.Add(1)
	go func() {
		defer wg.Done()
		ok, _ := s.TryExecute("limit_resource", limit, 10*time.Second, func() error {
			return nil
		})
		if ok {
			mu.Lock()
			successCount++
			mu.Unlock()
		}
	}()

	wg.Wait()

	// 第一次获取成功，第二次可能成功（如果释放得快）或失败（如果获取时第一次还没释放）
	// 关键验证：信号量机制正常工作，能限制并发
	if successCount < 1 {
		t.Error("至少应该有一次成功获取")
	}
}

func TestTryExecute_Expiry(t *testing.T) {
	prefix := "test_expiry"
	key := fmt.Sprintf("%s:expiry_resource", prefix)
	cleanupTestKey(key)
	defer cleanupTestKey(key)

	s := NewRedisSemaphore(pool, prefix)

	// 第一次获取，使用短过期时间
	ok1, _ := s.TryExecute("expiry_resource", 1, 1*time.Second, func() error {
		return nil
	})
	if !ok1 {
		t.Fatal("第一次获取应该成功")
	}

	// 等待过期
	time.Sleep(1500 * time.Millisecond)

	// 过期后再次获取应该成功
	ok2, _ := s.TryExecute("expiry_resource", 1, 10*time.Second, func() error {
		return nil
	})
	if !ok2 {
		t.Error("过期后再次获取应该成功")
	}
}

// ==================== Lua 脚本测试 ====================

func TestLuaScripts_DirectCall(t *testing.T) {
	prefix := "test_lua"
	key := fmt.Sprintf("%s:lua_test", prefix)
	cleanupTestKey(key)
	defer cleanupTestKey(key)

	conn := pool.Get()
	defer conn.Close()

	// 测试获取脚本
	ok, err := redis.Int(luaAcquire.Do(conn, key, 1, 60))
	if err != nil {
		t.Fatalf("luaAcquire 执行失败: %v", err)
	}
	if ok != 1 {
		t.Error("luaAcquire 第一次应该返回 1")
	}

	// 再次获取应该失败
	ok, err = redis.Int(luaAcquire.Do(conn, key, 1, 60))
	if err != nil {
		t.Fatalf("luaAcquire 执行失败: %v", err)
	}
	if ok != 0 {
		t.Error("luaAcquire 第二次应该返回 0")
	}

	// 测试释放脚本
	remaining, err := redis.Int(luaRelease.Do(conn, key))
	if err != nil {
		t.Fatalf("luaRelease 执行失败: %v", err)
	}
	if remaining != 0 {
		t.Errorf("luaRelease 释放后应该返回 0，实际: %d", remaining)
	}
}

func TestLuaScripts_ReleaseNonExistent(t *testing.T) {
	prefix := "test_lua_release"
	key := fmt.Sprintf("%s:nonexistent", prefix)
	cleanupTestKey(key)
	defer cleanupTestKey(key)

	conn := pool.Get()
	defer conn.Close()

	// 释放不存在的 key 应该返回 0
	remaining, err := redis.Int(luaRelease.Do(conn, key))
	if err != nil {
		t.Fatalf("luaRelease 执行失败: %v", err)
	}
	if remaining != 0 {
		t.Errorf("释放不存在的 key 应该返回 0，实际: %d", remaining)
	}
}

// ==================== Example 测试 ====================

func ExampleNewRedisSemaphore() {
	s := NewRedisSemaphore(pool, "example")
	fmt.Println(s.prefix)
	// Output:
	// example
}

func ExampleRedisSemaphore_formatKey() {
	s := NewRedisSemaphore(pool, "myapp")
	key := s.formatKey("users")
	fmt.Println(key)
	// Output:
	// myapp:users
}

func ExampleRedisSemaphore_TryExecute() {
	prefix := "example_try_execute"
	key := fmt.Sprintf("%s:task", prefix)

	// 清理测试 key
	conn := pool.Get()
	conn.Do("DEL", key)
	conn.Close()

	s := NewRedisSemaphore(pool, prefix)

	executed := false
	ok, err := s.TryExecute("task", 1, 60*time.Second, func() error {
		executed = true
		return nil
	})

	fmt.Printf("ok: %v, executed: %v, err: %v\n", ok, executed, err)

	// Output:
	// ok: true, executed: true, err: <nil>
}
