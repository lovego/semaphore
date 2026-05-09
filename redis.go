package semaphore

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	// 通用获取脚本
	// ARGV[1]: limit, ARGV[2]: expiry_seconds
	luaAcquire = redis.NewScript(1, `
local current = redis.call("get", KEYS[1])
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

if current and tonumber(current) >= limit then
    return 0 -- 达到并发上限
else
    -- 增加计数
    redis.call("incr", KEYS[1])
    -- 无论是不是第一个进入的，都强制续期
    -- 这样保证了“最后一次动作”之后起码还有 TTL 秒的存活时间
    redis.call("expire", KEYS[1], ttl)
    return 1
end
	`)

	luaRelease = redis.NewScript(1, `
local current = redis.call("get", KEYS[1])
if current and tonumber(current) > 0 then
    local next_val = redis.call("decr", KEYS[1])
    -- 如果减完之后变 0 了，可以直接删掉 Key 以节省内存
    -- 但如果为了极致安全，也可以保留 Key
    if next_val == 0 then
        redis.call("del", KEYS[1])
    end
end
return 0
	`)
)

type RedisSemaphore struct {
	pool   *redis.Pool
	prefix string // 用于区分不同业务模块的命名空间
}

func NewRedisSemaphore(pool *redis.Pool, prefix string) *RedisSemaphore {
	return &RedisSemaphore{pool: pool, prefix: prefix}
}

func (s *RedisSemaphore) formatKey(resource string) string {
	return fmt.Sprintf("%s:%s", s.prefix, resource)
}

// TryExecute 通用高阶函数：封装了获取、执行、释放的完整闭包
func (s *RedisSemaphore) TryExecute(resource string, limit int, expiry time.Duration, work func() error) (bool, error) {
	conn := s.pool.Get()
	defer conn.Close()

	fullKey := s.formatKey(resource)

	// 1. 获取权限
	ok, err := redis.Int(luaAcquire.Do(conn, fullKey, limit, int(expiry.Seconds())))
	if err != nil || ok == 0 {
		return false, err
	}

	// 2. 确保释放
	defer luaRelease.Do(conn, fullKey)

	// 3. 执行业务逻辑
	err = work()
	return true, err
}
