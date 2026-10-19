package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/renderedtext/agent-k8s-stack/pkg/agenttypes"
	"github.com/renderedtext/agent-k8s-stack/pkg/metrics"
	"github.com/renderedtext/agent-k8s-stack/pkg/semaphore"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

func Test__CollectorReportsJobStates(t *testing.T) {
	t.Run("pending check off -> no pending metrics", func(t *testing.T) {
		c, jobs := newMetricsTestController(t, 10, 0)
		c.jobScheduler.track("starting", "s1-test", false)
		c.jobScheduler.track("running", "s1-test", true)
		c.awaitingCreation.Store(4)
		require.NoError(t, jobs.Add(finishedJob(c, "done", batchv1.JobComplete)))
		require.NoError(t, jobs.Add(finishedJob(c, "failed", batchv1.JobFailed)))

		require.NoError(t, testutil.CollectAndCompare(c.Collector(), strings.NewReader(`
# HELP semaphore_controller_jobs Jobs by state: awaiting_creation (queued, no Kubernetes job yet), starting (agent not ready), running (agent ready), awaiting_cleanup (finished Kubernetes jobs that still exist).
# TYPE semaphore_controller_jobs gauge
semaphore_controller_jobs{state="awaiting_cleanup"} 2
semaphore_controller_jobs{state="awaiting_creation"} 4
semaphore_controller_jobs{state="running"} 1
semaphore_controller_jobs{state="starting"} 1
# HELP semaphore_controller_max_parallel_jobs The configured MAX_PARALLEL_JOBS.
# TYPE semaphore_controller_max_parallel_jobs gauge
semaphore_controller_max_parallel_jobs 10
`)))
	})

	t.Run("pending check on -> reports what the cap compares against", func(t *testing.T) {
		c, _ := newMetricsTestController(t, 10, 3)
		c.jobScheduler.track("starting", "s1-test", false)
		c.jobScheduler.track("running", "s1-test", true)
		c.pendingTracker.OnAdd(jobPod("semaphore-job-running", "running", ""), false)

		require.NoError(t, testutil.CollectAndCompare(c.Collector(), strings.NewReader(`
# HELP semaphore_controller_max_pending_jobs The configured MAX_PENDING_JOBS. Only reported when it is set.
# TYPE semaphore_controller_max_pending_jobs gauge
semaphore_controller_max_pending_jobs 3
# HELP semaphore_controller_pending_jobs Jobs counted against MAX_PENDING_JOBS. Only reported when it is set.
# TYPE semaphore_controller_pending_jobs gauge
semaphore_controller_pending_jobs 2
`), "semaphore_controller_pending_jobs", "semaphore_controller_max_pending_jobs"))
	})

	t.Run("collector registers cleanly", func(t *testing.T) {
		c, _ := newMetricsTestController(t, 10, 3)
		require.NoError(t, prometheus.NewRegistry().Register(c.Collector()))
	})
}

func Test__JobCounters(t *testing.T) {
	scheduler, clientset := newTestScheduler(t)
	created := counter(metrics.JobsProcessed, metrics.ActionCreated)
	deleted := counter(metrics.JobsProcessed, metrics.ActionDeleted)
	deleteErrors := counter(metrics.JobErrors, metrics.ActionDelete)
	timeouts := testutil.ToFloat64(metrics.JobStartTimeouts)

	jobID := randJobID()
	req := semaphore.JobRequest{JobID: jobID, MachineType: "s1-test"}
	require.NoError(t, scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"}))
	require.Equal(t, created+1, counter(metrics.JobsProcessed, metrics.ActionCreated))

	// A start timeout deletes the job, and counts as both a timeout and a deletion.
	job := jobExists(t, scheduler, clientset, jobID)
	timedOut := job.DeepCopy()
	timedOut.CreationTimestamp = metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
	scheduler.OnUpdate(job, timedOut)
	require.Equal(t, timeouts+1, testutil.ToFloat64(metrics.JobStartTimeouts))
	require.Equal(t, deleted+1, counter(metrics.JobsProcessed, metrics.ActionDeleted))

	// Deleting a job that is already gone is not an API failure.
	require.Error(t, scheduler.delete(job))
	require.Equal(t, deleteErrors, counter(metrics.JobErrors, metrics.ActionDelete))
}

func Test__TickCountsPollResults(t *testing.T) {
	ctx := context.Background()

	t.Run("no space", func(t *testing.T) {
		c := newPendingTestController(t, 1, 0)
		c.jobScheduler.track("running", "s1-test", true)
		before := counter(metrics.Polls, metrics.PollSkippedNoSpace)
		c.tick(ctx)
		require.Equal(t, before+1, counter(metrics.Polls, metrics.PollSkippedNoSpace))
	})

	t.Run("pending cap reached", func(t *testing.T) {
		c := newPendingTestController(t, 10, 1)
		c.jobScheduler.track("starting", "s1-test", false)
		before := counter(metrics.Polls, metrics.PollSkippedPendingCap)
		c.tick(ctx)
		require.Equal(t, before+1, counter(metrics.Polls, metrics.PollSkippedPendingCap))
	})

	t.Run("no agent types", func(t *testing.T) {
		c := newPendingTestController(t, 10, 0)
		registry, err := agenttypes.NewRegistry()
		require.NoError(t, err)
		c.agentTypeRegistry = registry
		before := counter(metrics.Polls, metrics.PollSkippedNoAgentTypes)
		c.tick(ctx)
		require.Equal(t, before+1, counter(metrics.Polls, metrics.PollSkippedNoAgentTypes))
	})
}

func Test__AwaitingCreationExcludesCreatedJobs(t *testing.T) {
	c := newPendingTestController(t, 10, 5)
	jobs := queue("a", "b", "c", "d", "e")
	c.createJobs(context.Background(), jobs, 2)
	require.Equal(t, 3, c.countAwaitingCreation(jobs))
}

// Like newPendingTestController, with the job informer wired so finished jobs can be counted.
func newMetricsTestController(t *testing.T, maxParallel, maxPending int) (*Controller, cache.Store) {
	c := newPendingTestController(t, maxParallel, maxPending)
	factory := informers.NewSharedInformerFactory(c.jobScheduler.clientset, 0)
	require.NoError(t, c.jobScheduler.RegisterInformer(factory))
	return c, factory.Batch().V1().Jobs().Informer().GetStore()
}

func finishedJob(c *Controller, jobID string, condition batchv1.JobConditionType) *batchv1.Job {
	job := testJob(c.jobScheduler, jobID)
	job.Namespace = "default"
	job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue}}
	return job
}

func counter(vec *prometheus.CounterVec, label string) float64 {
	return testutil.ToFloat64(vec.WithLabelValues(label))
}
