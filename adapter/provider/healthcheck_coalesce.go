package provider

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

type automaticProbeKey struct {
	proxy        C.Proxy
	url, status  string
	timeout      time.Duration
	unifiedDelay bool
}

type automaticProbeCall struct {
	done     chan struct{}
	cancel   context.CancelFunc
	waiters  int
	finished bool
	delay    uint16
	err      error
}

type automaticProbeGroup struct {
	mu    sync.Mutex
	calls map[automaticProbeKey]*automaticProbeCall
}

var automaticProbes automaticProbeGroup

// Only scheduled/provider-update health checks share an in-flight test. The
// proxy object identity (not its name), URL, status contract and timeout must
// all match. Manual URL tests remain independent and completed results are not
// cached, so a later failure cannot be hidden behind a stale success.
func (g *automaticProbeGroup) test(ctx context.Context, proxy C.Proxy, url string, expected utils.IntRanges[uint16], timeout time.Duration) (uint16, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if proxy == nil {
		return 0, errors.New("nil health check proxy")
	}
	// A group's object can outlive a node selection change. Share individual
	// outbounds only; dynamic groups must resolve their own current path.
	switch proxy.Type() {
	case C.Relay, C.Selector, C.Fallback, C.URLTest, C.LoadBalance:
		return proxy.URLTest(ctx, url, expected)
	}
	if !reflect.TypeOf(proxy).Comparable() || timeout <= 0 {
		return proxy.URLTest(ctx, url, expected)
	}
	key := automaticProbeKey{proxy: proxy, url: url, status: expected.String(), timeout: timeout, unifiedDelay: adapter.UnifiedDelay.Load()}
	g.mu.Lock()
	call := g.calls[key]
	if call == nil {
		// Keep the optimization's own retained state bounded. Overflow retains
		// ordinary independent testing rather than suppressing health checks.
		if len(g.calls) >= 4096 {
			g.mu.Unlock()
			return proxy.URLTest(ctx, url, expected)
		}
		if g.calls == nil {
			g.calls = make(map[automaticProbeKey]*automaticProbeCall)
		}
		probeCtx, cancel := context.WithTimeout(context.Background(), timeout)
		call = &automaticProbeCall{done: make(chan struct{}), cancel: cancel}
		g.calls[key] = call
		go func() {
			delay, err := proxy.URLTest(probeCtx, url, expected)
			cancel()
			g.mu.Lock()
			call.delay, call.err, call.finished = delay, err, true
			if g.calls[key] == call {
				delete(g.calls, key)
			}
			close(call.done)
			g.mu.Unlock()
		}()
	}
	call.waiters++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		call.waiters--
		if call.waiters == 0 && !call.finished {
			if g.calls[key] == call {
				delete(g.calls, key)
			}
			call.cancel()
		}
		g.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-call.done:
		return call.delay, call.err
	}
}
