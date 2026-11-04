package redisotel

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newTestOpQueue(t *testing.T) (*opQueue, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	meter := mp.Meter("test")
	h, err := meter.Float64Histogram(MetricOperationDuration)
	if err != nil {
		t.Fatal(err)
	}
	q, err := newOpQueue(meter, h)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.stop)
	return q, reader
}

// collect returns the operation-duration sample count and every metric name.
func collect(t *testing.T, reader *metric.ManualReader) (uint64, []string) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var n uint64
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok && m.Name == MetricOperationDuration {
				for _, p := range h.DataPoints {
					n += p.Count
				}
			}
		}
	}
	return n, names
}

// TestOpQueueCollectSeesQueuedSamples: a Collect right after recording counts
// every sample, though the worker records them on its own goroutine.
func TestOpQueueCollectSeesQueuedSamples(t *testing.T) {
	q, reader := newTestOpQueue(t)
	set := attribute.NewSet(attribute.String(AttrDBOperationName, "get"))
	var want uint64
	for round := 0; round < 200; round++ {
		for i := 0; i < 500; i++ {
			q.record(context.Background(), 0.001, set)
		}
		want += 500
		got, names := collect(t, reader)
		if got != want {
			t.Fatalf("round %d: collected %d samples, want %d", round, got, want)
		}
		for _, name := range names {
			if name != MetricOperationDuration {
				t.Fatalf("exported %q: the queue's gauge must not be exported", name)
			}
		}
	}
}

// TestOpQueueConcurrentRecordAndCollect: producers and collects at once lose
// no sample, and the queue never exports its own gauge.
func TestOpQueueConcurrentRecordAndCollect(t *testing.T) {
	q, reader := newTestOpQueue(t)
	set := attribute.NewSet(attribute.String(AttrDBOperationName, "get"))
	const producers, each = 8, 20000
	var wg sync.WaitGroup
	stop := make(chan struct{})
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = collect(t, reader)
			}
		}
	}()
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				q.record(context.Background(), 0.001, set)
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-collected
	if got, _ := collect(t, reader); got != producers*each {
		t.Fatalf("collected %d samples, want %d", got, producers*each)
	}
}

// TestOpQueueStopRecordsQueued: stop records what is queued, and a sample
// after stop is recorded on the caller's goroutine.
func TestOpQueueStopRecordsQueued(t *testing.T) {
	q, reader := newTestOpQueue(t)
	set := attribute.NewSet(attribute.String(AttrDBOperationName, "get"))
	for i := 0; i < 1000; i++ {
		q.record(context.Background(), 0.001, set)
	}
	q.stop()
	q.record(context.Background(), 0.001, set)
	if got, _ := collect(t, reader); got != 1001 {
		t.Fatalf("collected %d samples, want 1001", got)
	}
}

// opQueueWorkers counts goroutines in the queue's worker loop.
func opQueueWorkers() int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	return strings.Count(string(buf), "(*opQueue).run(")
}

// waitOpQueueWorkers polls until want workers run: a new worker may not have
// entered run yet, and a stopped one may still be leaving it.
func waitOpQueueWorkers(t *testing.T, want int, when string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := opQueueWorkers()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d workers, want %d", when, got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestOpQueueWorkerEndsOnShutdown: Init starts one worker and Shutdown ends it.
func TestOpQueueWorkerEndsOnShutdown(t *testing.T) {
	mp := metric.NewMeterProvider(metric.WithReader(metric.NewManualReader()))
	defer func() { _ = mp.Shutdown(context.Background()) }()
	inst := GetObservabilityInstance()
	_ = inst.Shutdown()
	// Let workers of earlier tests finish leaving run before taking the baseline.
	before := opQueueWorkers()
	for i := 0; i < 50; i++ {
		time.Sleep(time.Millisecond)
		if n := opQueueWorkers(); n != before {
			before, i = n, 0
		}
	}
	for i := 0; i < 3; i++ {
		if err := inst.Init(NewConfig().WithEnabled(true).WithMeterProvider(mp)); err != nil {
			t.Fatal(err)
		}
		waitOpQueueWorkers(t, before+1, "after Init")
		if err := inst.Shutdown(); err != nil {
			t.Fatal(err)
		}
		waitOpQueueWorkers(t, before, "after Shutdown")
	}
}
