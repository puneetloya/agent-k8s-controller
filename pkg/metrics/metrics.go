package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"
)

const namespace = "semaphore_controller"

// Registry holds the controller's metrics. It is only served when METRICS_ADDR is set;
// otherwise the metrics below are updated in memory and never read.
var Registry = prometheus.NewRegistry()

var (
	JobsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "jobs_processed_total",
		Help:      "Kubernetes jobs the controller created or deleted.",
	}, []string{"action"})

	JobStartTimeouts = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "job_start_timeouts_total",
		Help:      "Kubernetes jobs deleted because they did not start within JOB_START_TIMEOUT.",
	})

	JobErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "job_errors_total",
		Help:      "Failed Kubernetes API requests to create or delete jobs.",
	}, []string{"action"})

	Polls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "polls_total",
		Help:      "Controller ticks, by whether the Semaphore job queue was polled or why it was skipped.",
	}, []string{"result"})

	PollErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "poll_errors_total",
		Help:      "Failed requests for the job queue of an agent type.",
	}, []string{"agent_type"})

	LastSuccessfulPoll = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "last_successful_poll_timestamp_seconds",
		Help:      "Unix time of the last poll in which at least one agent type's job queue was read.",
	})
)

// Label values, so callers cannot drift from them.
const (
	ActionCreated = "created"
	ActionDeleted = "deleted"
	ActionCreate  = "create"
	ActionDelete  = "delete"

	PollOK                  = "ok"
	PollError               = "error"
	PollSkippedNoSpace      = "skipped_no_space"
	PollSkippedPendingCap   = "skipped_pending_cap"
	PollSkippedNoAgentTypes = "skipped_no_agent_types"
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		JobsProcessed, JobStartTimeouts, JobErrors, Polls, PollErrors, LastSuccessfulPoll,
	)

	// Pre-create series so rates start at 0 instead of appearing on first event.
	for _, a := range []string{ActionCreated, ActionDeleted} {
		JobsProcessed.WithLabelValues(a)
	}
	for _, a := range []string{ActionCreate, ActionDelete} {
		JobErrors.WithLabelValues(a)
	}
	for _, r := range []string{PollOK, PollError, PollSkippedNoSpace, PollSkippedPendingCap, PollSkippedNoAgentTypes} {
		Polls.WithLabelValues(r)
	}
}

// Listen binds addr, so a bad address fails at startup rather than in the background.
func Listen(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

// Serve exposes /metrics on l until ctx is done.
func Serve(ctx context.Context, l net.Listener) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(Registry, promhttp.HandlerOpts{}))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	klog.InfoS("Serving metrics", "addr", l.Addr().String())
	if err := server.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
		klog.ErrorS(err, "Metrics server stopped")
	}
}
