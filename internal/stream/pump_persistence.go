package stream

import (
	"context"
	"sync/atomic"
	"time"
)

const (
	pumpMarkRunningTimeout = 15 * time.Millisecond
	pumpHeartbeatTimeout   = 2 * time.Second
	pumpHeartbeatInterval  = 3 * time.Second
	pumpPersistenceStopMax = 250 * time.Millisecond
)

// pumpPersistence coalesces heartbeat state behind one bounded worker.
// Provider delivery never waits on the database, repeated media callbacks
// cannot create a goroutine queue, and canceling the worker also cancels the
// one in-flight SQL operation. The one-time MarkRunning transition is bounded
// separately while the validated prefix is still private.
type pumpPersistence struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wake      chan struct{}
	done      chan struct{}
	bytes     atomic.Int64
	heartbeat func(context.Context, int64)
	interval  time.Duration
}

func newPumpPersistence(heartbeat func(context.Context, int64)) *pumpPersistence {
	ctx, cancel := context.WithCancel(context.Background())
	p := &pumpPersistence{
		ctx: ctx, cancel: cancel,
		wake: make(chan struct{}, 1), done: make(chan struct{}),
		heartbeat: heartbeat,
		interval:  pumpHeartbeatInterval,
	}
	go p.run()
	return p
}

func (p *pumpPersistence) AddBytes(n int64) {
	if n <= 0 {
		return
	}
	p.bytes.Add(n)
	p.signal()
}

func (p *pumpPersistence) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *pumpPersistence) run() {
	defer close(p.done)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	var lastHeartbeat time.Time
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.wake:
		case <-ticker.C:
		}
		if p.bytes.Load() <= 0 ||
			(!lastHeartbeat.IsZero() && time.Since(lastHeartbeat) < p.interval) {
			continue
		}
		batch := p.bytes.Swap(0)
		if batch <= 0 {
			continue
		}
		lastHeartbeat = time.Now()
		p.call(pumpHeartbeatTimeout, func(ctx context.Context) {
			p.heartbeat(ctx, batch)
		})
	}
}

func (p *pumpPersistence) call(timeout time.Duration, operation func(context.Context)) {
	if operation == nil || p.ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()
	operation(ctx)
}

// Stop cancels any in-flight operation and bounds the join independently of
// driver behavior. Generation-fenced SQL makes a pathological late return a
// no-op against a replacement pump.
func (p *pumpPersistence) Stop(timeout time.Duration) bool {
	p.cancel()
	if timeout <= 0 {
		timeout = pumpPersistenceStopMax
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return true
	case <-timer.C:
		return false
	}
}
