package gateway

import (
	"context"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// 上游超时类型（用于区分日志与重试策略）
const (
	timeoutNone      int32 = 0
	timeoutFirstByte int32 = 1 // 首字节超时：还没收到任何响应数据
	timeoutStall     int32 = 2 // 流中停滞：已经开始返回数据，超过阈值没有新数据
)

// idleWatch 监听上游数据活跃度：
//   - 尚未收到任何数据时，超过 firstByteSec 判定「首字超时」（可重试）；
//   - 已收到数据后，超过 stallSec 没有新数据判定「停滞超时」（流已开始，只能中止）。
//
// 任一命中即调用 cancel 中断请求，并把类型写入 kind。
type idleWatch struct {
	stop chan struct{}
	once sync.Once
}

// activityBody 包装响应体，记录最后一次读到数据的时间。
type activityBody struct {
	rc   io.ReadCloser
	last *int64
	got  *int32
}

func (b *activityBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		atomic.StoreInt64(b.last, time.Now().UnixNano())
		atomic.StoreInt32(b.got, 1)
	}
	return n, err
}

func (b *activityBody) Close() error { return b.rc.Close() }

// startIdleWatch 启动活跃度监控；两个阈值都 <=0 时返回 nil（不监控）。
func startIdleWatch(ctx context.Context, cancel context.CancelFunc, firstByteSec, stallSec int, last *int64, got, kind *int32) *idleWatch {
	if firstByteSec <= 0 && stallSec <= 0 {
		return nil
	}
	w := &idleWatch{stop: make(chan struct{})}
	go func() {
		tk := time.NewTicker(200 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-ctx.Done():
				return
			case <-tk.C:
				idle := time.Duration(time.Now().UnixNano() - atomic.LoadInt64(last))
				threshold := firstByteSec
				if atomic.LoadInt32(got) == 1 {
					threshold = stallSec
				}
				if threshold > 0 && idle > time.Duration(threshold)*time.Second {
					if atomic.LoadInt32(got) == 1 {
						atomic.StoreInt32(kind, timeoutStall)
					} else {
						atomic.StoreInt32(kind, timeoutFirstByte)
					}
					cancel()
					return
				}
			}
		}
	}()
	return w
}

// Stop 结束监控（幂等）。
func (w *idleWatch) Stop() {
	if w == nil {
		return
	}
	w.once.Do(func() { close(w.stop) })
}

// timeoutLabel 把超时类型转成可读错误描述。
func timeoutLabel(kind int32, firstByteSec, stallSec int) string {
	switch kind {
	case timeoutFirstByte:
		return "首字超时（" + strconv.Itoa(firstByteSec) + " 秒内上游无响应）"
	case timeoutStall:
		return "流中停滞超时（" + strconv.Itoa(stallSec) + " 秒无新数据）"
	}
	return ""
}
