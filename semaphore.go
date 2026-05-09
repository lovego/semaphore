package semaphore

import (
	"context"
	"time"
)

// Semaphore 定义通用限流接口
type Semaphore interface {
	// TryExecute 执行业务闭包，处理了获取和释放的逻辑
	TryExecute(
		ctx context.Context, resource string, limit int, expiry time.Duration, work func() error,
	) (bool, error)
}
