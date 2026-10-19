package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog/v2"
)

const (
	stateAwaitingCreation = "awaiting_creation"
	stateStarting         = "starting"
	stateRunning          = "running"
	stateAwaitingCleanup  = "awaiting_cleanup"
)

var (
	jobsDesc = prometheus.NewDesc("semaphore_controller_jobs",
		"Jobs by state: awaiting_creation (queued, no Kubernetes job yet), starting (agent not ready), "+
			"running (agent ready), awaiting_cleanup (finished Kubernetes jobs that still exist).",
		[]string{"state"}, nil)

	pendingJobsDesc = prometheus.NewDesc("semaphore_controller_pending_jobs",
		"Jobs counted against MAX_PENDING_JOBS. Only reported when it is set.", nil, nil)

	maxParallelJobsDesc = prometheus.NewDesc("semaphore_controller_max_parallel_jobs",
		"The configured MAX_PARALLEL_JOBS.", nil, nil)

	maxPendingJobsDesc = prometheus.NewDesc("semaphore_controller_max_pending_jobs",
		"The configured MAX_PENDING_JOBS. Only reported when it is set.", nil, nil)
)

// Collector reports the controller's current state, read when metrics are scraped.
type Collector struct {
	c *Controller
}

func (c *Controller) Collector() *Collector {
	return &Collector{c: c}
}

func (col *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jobsDesc
	ch <- pendingJobsDesc
	ch <- maxParallelJobsDesc
	ch <- maxPendingJobsDesc
}

func (col *Collector) Collect(ch chan<- prometheus.Metric) {
	c := col.c
	starting, running := c.jobScheduler.Counts()
	gauge(ch, jobsDesc, c.awaitingCreation.Load(), stateAwaitingCreation)
	gauge(ch, jobsDesc, starting, stateStarting)
	gauge(ch, jobsDesc, running, stateRunning)

	// Omitted rather than reported as 0 when the cache cannot be read.
	if finished, err := c.jobScheduler.Finished(); err == nil {
		gauge(ch, jobsDesc, finished, stateAwaitingCleanup)
	} else {
		klog.ErrorS(err, "Error counting finished jobs for metrics")
	}

	gauge(ch, maxParallelJobsDesc, c.config.MaxParallelJobs)
	if c.pendingTracker != nil {
		gauge(ch, pendingJobsDesc, c.jobScheduler.Pending(c.pendingTracker.JobIDs()))
		gauge(ch, maxPendingJobsDesc, c.config.MaxPendingJobs)
	}
}

func gauge[T int | int64](ch chan<- prometheus.Metric, desc *prometheus.Desc, v T, labels ...string) {
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(v), labels...)
}
