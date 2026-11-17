package provider

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

type probeTestProxy struct {
	C.Proxy
	run  func(context.Context) (uint16, error)
	kind C.AdapterType
}

func (p *probeTestProxy) Name() string        { return "same-name" }
func (p *probeTestProxy) Type() C.AdapterType { return p.kind }
func (p *probeTestProxy) URLTest(ctx context.Context, _ string, _ utils.IntRanges[uint16]) (uint16, error) {
	return p.run(ctx)
}

func waitForProbeWaiters(t *testing.T, g *automaticProbeGroup, want int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		g.mu.Lock()
		count := 0
		for _, call := range g.calls {
			count += call.waiters
		}
		g.mu.Unlock()
		if count == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("probe waiters=%d want=%d", count, want)
		case <-tick.C:
		}
	}
}

func TestAutomaticProbeCancellationIsPerWaiter(t *testing.T) {
	var g automaticProbeGroup
	release := make(chan struct{})
	var calls atomic.Int32
	p := &probeTestProxy{run: func(ctx context.Context) (uint16, error) {
		calls.Add(1)
		select {
		case <-release:
			return 47, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}}
	leader, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := g.test(leader, p, "https://test/", nil, time.Second); first <- err }()
	waitForProbeWaiters(t, &g, 1)
	second := make(chan error, 1)
	go func() {
		delay, err := g.test(context.Background(), p, "https://test/", nil, time.Second)
		if err == nil && delay != 47 {
			err = errors.New("shared delay changed")
		}
		second <- err
	}()
	waitForProbeWaiters(t, &g, 2)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal("canceled provider did not return promptly", err)
	}
	select {
	case err := <-second:
		t.Fatal("one provider cancellation terminated another provider's test", err)
	default:
	}
	close(release)
	if err := <-second; err != nil || calls.Load() != 1 {
		t.Fatalf("coalesced result failed: err=%v physical tests=%d", err, calls.Load())
	}
}

func TestAutomaticProbeIdentityAndContractsStaySeparate(t *testing.T) {
	var g automaticProbeGroup
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var calls atomic.Int32
	run := func(ctx context.Context) (uint16, error) {
		calls.Add(1)
		select {
		case <-release:
			return 37, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	p, other := &probeTestProxy{run: run}, &probeTestProxy{run: run}
	status := utils.IntRanges[uint16]{utils.NewRange[uint16](204, 204)}
	tests := []struct {
		proxy   C.Proxy
		url     string
		status  utils.IntRanges[uint16]
		timeout time.Duration
	}{
		{p, "https://one/", nil, time.Second},
		{p, "https://one/", nil, time.Second},
		{other, "https://one/", nil, time.Second},
		{p, "https://two/", nil, time.Second},
		{p, "https://one/", status, time.Second},
		{p, "https://one/", nil, 2 * time.Second},
	}
	var wg sync.WaitGroup
	for _, test := range tests {
		test := test
		wg.Add(1)
		go func() {
			defer wg.Done()
			if delay, err := g.test(context.Background(), test.proxy, test.url, test.status, test.timeout); err != nil || delay != 37 {
				t.Errorf("invalid probe result: %d %v", delay, err)
			}
		}()
	}
	waitForProbeWaiters(t, &g, len(tests))
	once.Do(func() { close(release) })
	wg.Wait()
	if calls.Load() != 5 {
		t.Fatalf("different identities/contracts incorrectly shared: calls=%d", calls.Load())
	}
}

func TestAutomaticProbeCancellationTimeoutAndFreshResults(t *testing.T) {
	var g automaticProbeGroup
	var calls atomic.Int32
	stopped := make(chan struct{}, 2)
	p := &probeTestProxy{run: func(ctx context.Context) (uint16, error) {
		calls.Add(1)
		<-ctx.Done()
		stopped <- struct{}{}
		return 0, ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := g.test(ctx, p, "https://test/", nil, time.Second); done <- err }()
	waitForProbeWaiters(t, &g, 1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("last waiter left an orphan probe")
	}
	if _, err := g.test(context.Background(), p, "https://test/", nil, 20*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 2 {
		t.Fatal("timeout was lost or canceled result reused", err, calls.Load())
	}
	var fresh atomic.Int32
	failure := errors.New("probe failed")
	p = &probeTestProxy{run: func(context.Context) (uint16, error) {
		if fresh.Add(1) == 1 {
			return 0, failure
		}
		return 23, nil
	}}
	if _, err := g.test(context.Background(), p, "https://test/", nil, time.Second); !errors.Is(err, failure) {
		t.Fatal("probe error was swallowed", err)
	}
	if delay, err := g.test(context.Background(), p, "https://test/", nil, time.Second); err != nil || delay != 23 || fresh.Load() != 2 {
		t.Fatal("completed result hid a fresh probe", delay, err)
	}
	waitForProbeWaiters(t, &g, 0)
}

func TestAutomaticProbeDynamicGroupsRemainIndependent(t *testing.T) {
	for _, kind := range []C.AdapterType{C.Relay, C.Selector, C.Fallback, C.URLTest, C.LoadBalance} {
		t.Run(kind.String(), func(t *testing.T) {
			var g automaticProbeGroup
			started := make(chan struct{}, 2)
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			p := &probeTestProxy{kind: kind, run: func(ctx context.Context) (uint16, error) {
				started <- struct{}{}
				select {
				case <-release:
					return 9, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}}
			var wg sync.WaitGroup
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _, _ = g.test(ctx, p, "https://test/", nil, time.Second) }()
			}
			for i := 0; i < 2; i++ {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("dynamic group incorrectly shared an old selected path")
				}
			}
			once.Do(func() { close(release) })
			wg.Wait()
		})
	}
}

func TestHealthChecksAcrossGroupsShareHTTPButManualChecksStayIndependent(t *testing.T) {
	var requests atomic.Int32
	var responseStatus atomic.Int32
	responseStatus.Store(http.StatusNoContent)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		select {
		case <-release:
			w.WriteHeader(int(responseStatus.Load()))
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	p := adapter.NewProxy(outbound.NewDirect())
	status := utils.IntRanges[uint16]{utils.NewRange[uint16](204, 204)}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		hc := NewHealthCheck([]C.Proxy{p}, server.URL, 5000, 300, true, status)
		defer hc.close()
		wg.Add(1)
		go func() { defer wg.Done(); hc.checkAutomatic() }()
	}
	waitForProbeWaiters(t, &automaticProbes, 8)
	once.Do(func() { close(release) })
	wg.Wait()
	if requests.Load() != 1 || !p.AliveForTestUrl(server.URL) || len(p.DelayHistory()) != 1 {
		t.Fatal("automatic groups did not share one real HTTP test and history", requests.Load(), p.DelayHistory())
	}
	for i := 0; i < 2; i++ {
		hc := NewHealthCheck([]C.Proxy{p}, server.URL, 5000, 300, true, status)
		hc.check()
		hc.close()
	}
	if requests.Load() != 3 || len(p.DelayHistory()) != 3 {
		t.Fatal("manual checks reused automatic or completed results", requests.Load(), p.DelayHistory())
	}
	responseStatus.Store(http.StatusServiceUnavailable)
	failure := NewHealthCheck([]C.Proxy{p}, server.URL, 5000, 300, true, status)
	failure.checkAutomatic()
	failure.close()
	if requests.Load() != 4 || p.AliveForTestUrl(server.URL) {
		t.Fatal("unexpected HTTP status was hidden by a completed success")
	}
	responseStatus.Store(http.StatusNoContent)
	recovery := NewHealthCheck([]C.Proxy{p}, server.URL, 5000, 300, true, status)
	recovery.checkAutomatic()
	recovery.close()
	if requests.Load() != 5 || !p.AliveForTestUrl(server.URL) {
		t.Fatal("automatic checks did not recover with a fresh HTTP result")
	}
}
