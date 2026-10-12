package controller

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/renderedtext/agent-k8s-stack/pkg/agenttypes"
	"github.com/renderedtext/agent-k8s-stack/pkg/config"
	"github.com/renderedtext/agent-k8s-stack/pkg/semaphore"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

func jobPod(name, jobID, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
			Labels:    map[string]string{config.JobIDLabel: jobID, config.AgentTypeLabel: "s1-test"},
		},
		Spec:   corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
}

func sortedIDs(p *PendingTracker) []string {
	ids := p.JobIDs()
	sort.Strings(ids)
	return ids
}

func Test__PendingTracker(t *testing.T) {
	t.Run("unscheduled pod is tracked until it gets a node", func(t *testing.T) {
		p := NewPendingTracker()
		pod := jobPod("semaphore-job-a", "a", "")
		p.OnAdd(pod, false)
		require.Equal(t, []string{"a"}, sortedIDs(p))

		scheduled := pod.DeepCopy()
		scheduled.Spec.NodeName = "node-1"
		p.OnUpdate(pod, scheduled)
		require.Empty(t, p.JobIDs())
	})

	t.Run("terminating or finished pods are not tracked", func(t *testing.T) {
		p := NewPendingTracker()

		terminating := jobPod("semaphore-job-a", "a", "")
		now := metav1.Now()
		terminating.DeletionTimestamp = &now
		p.OnAdd(terminating, false)

		failed := jobPod("semaphore-job-b", "b", "")
		failed.Status.Phase = corev1.PodFailed
		p.OnAdd(failed, false)

		require.Empty(t, p.JobIDs())
	})

	t.Run("pod without job ID label is ignored", func(t *testing.T) {
		p := NewPendingTracker()
		pod := jobPod("other", "a", "")
		delete(pod.Labels, config.JobIDLabel)
		p.OnAdd(pod, false)
		require.Empty(t, p.JobIDs())
	})

	t.Run("deletes, including tombstones, stop tracking", func(t *testing.T) {
		p := NewPendingTracker()
		a := jobPod("semaphore-job-a", "a", "")
		b := jobPod("semaphore-job-b", "b", "")
		p.OnAdd(a, false)
		p.OnAdd(b, false)

		p.OnDelete(a)
		p.OnDelete(cache.DeletedFinalStateUnknown{Key: "default/semaphore-job-b", Obj: b})
		require.Empty(t, p.JobIDs())

		require.NotPanics(t, func() { p.OnDelete("not a pod") })
	})
}

func Test__JobPodSelector(t *testing.T) {
	raw, err := JobPodSelector()
	require.NoError(t, err)
	selector, err := labels.Parse(raw)
	require.NoError(t, err)

	jobPodLabels := labels.Set{config.JobIDLabel: "a", config.AgentTypeLabel: "s1-test"}
	require.True(t, selector.Matches(jobPodLabels))

	// The controller's own agent pods carry the resource type label.
	agentPodLabels := labels.Set{
		config.JobIDLabel:        "a",
		config.AgentTypeLabel:    "s1-test",
		config.ResourceTypeLabel: config.SemaphoreJobResourceType,
	}
	require.False(t, selector.Matches(agentPodLabels))

	// Job pods created before the label existed are not matched.
	require.False(t, selector.Matches(labels.Set{config.AgentTypeLabel: "s1-test"}))
}

func newPendingTestScheduler(t *testing.T, maxParallel, maxPending int) *JobScheduler {
	scheduler, err := NewJobScheduler(newFakeClientset(t, []runtime.Object{}), &config.Config{
		Namespace:              "default",
		AgentImage:             "semaphoreci/agent:latest",
		AgentStartupParameters: []string{},
		Labels:                 []string{},
		MaxParallelJobs:        maxParallel,
		MaxPendingJobs:         maxPending,
		JobStartTimeout:        time.Minute,
	})
	require.NoError(t, err)
	return scheduler
}

func Test__Pending(t *testing.T) {
	scheduler := newPendingTestScheduler(t, 10, 5)
	scheduler.track("starting", "s1-test", false)
	scheduler.track("running", "s1-test", true)
	scheduler.track("running-unscheduled", "s1-test", true)

	t.Run("counts starting jobs and running jobs whose pod has no node", func(t *testing.T) {
		require.Equal(t, 2, scheduler.Pending([]string{"running-unscheduled"}))
	})

	t.Run("a job that is both starting and unscheduled counts once", func(t *testing.T) {
		require.Equal(t, 2, scheduler.Pending([]string{"starting", "running-unscheduled"}))
	})

	t.Run("unscheduled pods of untracked jobs are ignored", func(t *testing.T) {
		require.Equal(t, 1, scheduler.Pending([]string{"orphan-1", "orphan-2"}))
	})

	t.Run("never exceeds the number of tracked jobs", func(t *testing.T) {
		all := []string{"starting", "running", "running-unscheduled", "orphan"}
		require.LessOrEqual(t, scheduler.Pending(all), len(scheduler.current))
	})

	t.Run("does not change tracked jobs", func(t *testing.T) {
		before := len(scheduler.current)
		_ = scheduler.Pending([]string{"orphan"})
		require.Equal(t, before, len(scheduler.current))
		require.False(t, scheduler.IsCurrentJob("orphan"))
	})
}

func Test__AgentLabels(t *testing.T) {
	agentType := &agenttypes.AgentType{AgentTypeName: "s1-test", AgentStartupParameters: []string{}}

	t.Run("feature off -> agent arguments are unchanged", func(t *testing.T) {
		params := newPendingTestScheduler(t, 10, 0).buildAgentStartupParameters(agentType, "job-1")
		require.Equal(t, []string{
			"--endpoint", "",
			"--job-id", "job-1",
			"--kubernetes-labels", config.AgentTypeLabel + "=s1-test",
			"--kubernetes-executor",
			"--disconnect-after-job",
		}, params)
	})

	t.Run("feature on -> job pods also get the job ID label", func(t *testing.T) {
		params := newPendingTestScheduler(t, 10, 5).buildAgentStartupParameters(agentType, "job-1")
		require.Contains(t, params, config.AgentTypeLabel+"=s1-test,"+config.JobIDLabel+"=job-1")
	})
}

func newPendingTestController(t *testing.T, maxParallel, maxPending int) *Controller {
	registry, err := agenttypes.NewRegistry()
	require.NoError(t, err)
	registry.OnAdd(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s1-test"},
		Data: map[string][]byte{
			"agentTypeName":     []byte("s1-test"),
			"registrationToken": []byte("token"),
		},
	}, false)

	scheduler := newPendingTestScheduler(t, maxParallel, maxPending)
	c := &Controller{
		agentTypeRegistry: registry,
		clientset:         scheduler.clientset,
		jobScheduler:      scheduler,
		config:            scheduler.config,
	}

	if maxPending > 0 {
		c.pendingTracker = NewPendingTracker()
	}

	return c
}

func queue(ids ...string) []semaphore.JobRequest {
	jobs := []semaphore.JobRequest{}
	for _, id := range ids {
		jobs = append(jobs, semaphore.JobRequest{JobID: id, MachineType: "s1-test"})
	}
	return jobs
}

func Test__PendingBudget(t *testing.T) {
	t.Run("feature off -> no budget and always polls", func(t *testing.T) {
		c := newPendingTestController(t, 10, 0)
		c.jobScheduler.track("starting", "s1-test", false)
		budget, ok := c.pendingBudget()
		require.True(t, ok)
		require.Equal(t, noBudget, budget)
	})

	t.Run("budget is the room left under the cap", func(t *testing.T) {
		c := newPendingTestController(t, 10, 3)
		c.jobScheduler.track("starting", "s1-test", false)
		budget, ok := c.pendingBudget()
		require.True(t, ok)
		require.Equal(t, 2, budget)
	})

	t.Run("cap reached -> does not poll", func(t *testing.T) {
		c := newPendingTestController(t, 10, 2)
		c.jobScheduler.track("starting", "s1-test", false)
		c.jobScheduler.track("running", "s1-test", true)
		c.pendingTracker.OnAdd(jobPod("semaphore-job-running", "running", ""), false)
		_, ok := c.pendingBudget()
		require.False(t, ok)
	})
}

func Test__CreateJobs(t *testing.T) {
	ctx := context.Background()

	t.Run("feature off -> creates up to the parallel limit, as before", func(t *testing.T) {
		c := newPendingTestController(t, 3, 0)
		c.createJobs(ctx, queue("a", "b", "c", "d", "e"), noBudget)
		require.Len(t, c.jobScheduler.current, 3)
	})

	t.Run("pending budget binds first", func(t *testing.T) {
		c := newPendingTestController(t, 10, 5)
		c.createJobs(ctx, queue("a", "b", "c", "d", "e"), 2)
		require.Len(t, c.jobScheduler.current, 2)
	})

	t.Run("parallel limit binds first", func(t *testing.T) {
		c := newPendingTestController(t, 2, 5)
		c.createJobs(ctx, queue("a", "b", "c", "d", "e"), 4)
		require.Len(t, c.jobScheduler.current, 2)
	})

	t.Run("already tracked jobs do not spend budget", func(t *testing.T) {
		c := newPendingTestController(t, 10, 5)
		c.jobScheduler.track("a", "s1-test", true)
		c.createJobs(ctx, queue("a", "b", "c"), 2)
		require.True(t, c.jobScheduler.IsCurrentJob("b"))
		require.True(t, c.jobScheduler.IsCurrentJob("c"))
		require.Len(t, c.jobScheduler.current, 3)
	})

	t.Run("zero budget creates nothing", func(t *testing.T) {
		c := newPendingTestController(t, 10, 5)
		c.createJobs(ctx, queue("a", "b"), 0)
		require.Empty(t, c.jobScheduler.current)
	})
}
