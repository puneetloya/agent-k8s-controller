package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

type orphanTest struct {
	cleaner *orphanCleaner
	jobs    cache.Indexer
	deleted []string
	err     error
}

func newOrphanTest(t *testing.T) *orphanTest {
	ot := &orphanTest{jobs: cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})}
	tracker := NewPendingTracker()
	tracker.synced = func() bool { return true }
	ot.cleaner = &orphanCleaner{
		tracker:    tracker,
		jobs:       batchlisters.NewJobLister(ot.jobs),
		jobsSynced: func() bool { return true },
		jobName:    func(id string) string { return "semaphore-agent-" + id },
		delete: func(_ context.Context, pod waitingPod) error {
			ot.deleted = append(ot.deleted, pod.name)
			return ot.err
		},
		since: map[string]time.Time{},
	}
	return ot
}

func (ot *orphanTest) agentJob(t *testing.T, jobID string, finished batchv1.JobConditionType) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "semaphore-agent-" + jobID}}
	if finished != "" {
		job.Status.Conditions = []batchv1.JobCondition{{Type: finished, Status: corev1.ConditionTrue}}
	}
	require.NoError(t, ot.jobs.Add(job))
}

func Test__OrphanCleanup(t *testing.T) {
	t0 := time.Now()
	ctx := context.Background()

	t.Run("no agent job -> deleted after the grace period, not before", func(t *testing.T) {
		ot := newOrphanTest(t)
		ot.cleaner.tracker.OnAdd(jobPod("job-pod-a", "a", ""), false)

		ot.cleaner.run(ctx, t0)
		ot.cleaner.run(ctx, t0.Add(orphanGrace-time.Second))
		require.Empty(t, ot.deleted)

		ot.cleaner.run(ctx, t0.Add(orphanGrace))
		require.Equal(t, []string{"job-pod-a"}, ot.deleted)
	})

	t.Run("agent job running -> kept", func(t *testing.T) {
		ot := newOrphanTest(t)
		ot.agentJob(t, "a", "")
		ot.cleaner.tracker.OnAdd(jobPod("job-pod-a", "a", ""), false)

		ot.cleaner.run(ctx, t0)
		ot.cleaner.run(ctx, t0.Add(time.Hour))
		require.Empty(t, ot.deleted)
	})

	for _, finished := range []batchv1.JobConditionType{batchv1.JobComplete, batchv1.JobFailed} {
		t.Run("agent job "+string(finished)+" -> deleted after the grace period", func(t *testing.T) {
			ot := newOrphanTest(t)
			ot.agentJob(t, "a", finished)
			ot.cleaner.tracker.OnAdd(jobPod("job-pod-a", "a", ""), false)

			ot.cleaner.run(ctx, t0)
			ot.cleaner.run(ctx, t0.Add(orphanGrace))
			require.Equal(t, []string{"job-pod-a"}, ot.deleted)
		})
	}

	t.Run("pod gets a node during the grace period -> kept", func(t *testing.T) {
		ot := newOrphanTest(t)
		ot.cleaner.tracker.OnAdd(jobPod("job-pod-a", "a", ""), false)
		ot.cleaner.run(ctx, t0)

		ot.cleaner.tracker.OnUpdate(nil, jobPod("job-pod-a", "a", "node-1"))
		ot.cleaner.run(ctx, t0.Add(orphanGrace))
		require.Empty(t, ot.deleted)
		require.Empty(t, ot.cleaner.since)
	})

	t.Run("agent job shows up during the grace period -> kept, timer reset", func(t *testing.T) {
		ot := newOrphanTest(t)
		ot.cleaner.tracker.OnAdd(jobPod("job-pod-a", "a", ""), false)
		ot.cleaner.run(ctx, t0)

		ot.agentJob(t, "a", "")
		ot.cleaner.run(ctx, t0.Add(orphanGrace))
		require.Empty(t, ot.deleted)
		require.Empty(t, ot.cleaner.since)
	})

	t.Run("caches not synced -> nothing happens", func(t *testing.T) {
		ot := newOrphanTest(t)
		ot.cleaner.jobsSynced = func() bool { return false }
		ot.cleaner.tracker.OnAdd(jobPod("job-pod-a", "a", ""), false)

		ot.cleaner.run(ctx, t0)
		ot.cleaner.run(ctx, t0.Add(time.Hour))
		require.Empty(t, ot.deleted)
	})

	t.Run("not allowed to delete pods -> stops trying", func(t *testing.T) {
		ot := newOrphanTest(t)
		ot.err = apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "job-pod-a", nil)
		ot.cleaner.tracker.OnAdd(jobPod("job-pod-a", "a", ""), false)
		ot.cleaner.tracker.OnAdd(jobPod("job-pod-b", "b", ""), false)

		ot.cleaner.run(ctx, t0)
		ot.cleaner.run(ctx, t0.Add(orphanGrace))
		require.Len(t, ot.deleted, 1)
		require.True(t, ot.cleaner.disabled)

		ot.cleaner.run(ctx, t0.Add(2*orphanGrace))
		require.Len(t, ot.deleted, 1)
	})
}

// Deletions carry the UID we saw, so a pod recreated under the same name is not removed.
func Test__OrphanDeleteIsPinned(t *testing.T) {
	c := newPendingTestController(t, 10, 5)
	clientset := c.clientset.(*fake.Clientset)

	var preconditions []*metav1.Preconditions
	clientset.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		preconditions = append(preconditions, action.(k8stesting.DeleteActionImpl).DeleteOptions.Preconditions)
		return true, nil, nil
	})

	require.NoError(t, deletePinned(c)(context.Background(), waitingPod{namespace: "default", name: "job-pod-a", uid: "the-pod-we-saw"}))
	require.Len(t, preconditions, 1)
	require.NotNil(t, preconditions[0])
	require.Equal(t, "the-pod-we-saw", string(*preconditions[0].UID))
}
