package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/renderedtext/agent-k8s-stack/pkg/config"
	"github.com/renderedtext/agent-k8s-stack/pkg/semaphore"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// A clientset whose pod list and watch calls fail with 403, as with no pod RBAC.
// Returns the number of pod list/watch calls made.
func clientsetWithoutPodAccess(t *testing.T) (*fake.Clientset, *atomic.Int32) {
	clientset := newFakeClientset(t, []runtime.Object{}).(*fake.Clientset)
	calls := &atomic.Int32{}
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", nil)

	clientset.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls.Add(1)
		return true, nil, forbidden
	})
	clientset.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
		calls.Add(1)
		return true, nil, forbidden
	})

	return clientset, calls
}

func newControllerForRBACTest(t *testing.T, clientset *fake.Clientset, maxPending int) (*Controller, informers.SharedInformerFactory) {
	factory := informers.NewSharedInformerFactory(clientset, time.Minute)
	c, err := New(context.Background(), factory, &config.Config{
		Namespace:              "default",
		AgentImage:             "semaphoreci/agent:latest",
		AgentStartupParameters: []string{},
		Labels:                 []string{},
		MaxParallelJobs:        5,
		MaxPendingJobs:         maxPending,
		JobStartTimeout:        time.Minute,
	}, semaphore.NewClient("example.invalid"), clientset)
	require.NoError(t, err)
	return c, factory
}

func Test__DisabledNeverTouchesPods(t *testing.T) {
	clientset, podCalls := clientsetWithoutPodAccess(t)
	c, factory := newControllerForRBACTest(t, clientset, 0)
	require.Nil(t, c.pendingTracker)
	require.Nil(t, c.podInformers)

	ctx, cancel := context.WithCancel(context.Background())
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	// Run with a cancelled context returns without polling; it must not start pod informers.
	cancel()
	require.NoError(t, c.Run(ctx))

	budget, ok := c.pendingBudget()
	require.True(t, ok)
	require.Equal(t, noBudget, budget)

	registerTestAgentType(t, c)
	c.createJobs(context.Background(), queue("a", "b", "c", "d", "e", "f"), budget)
	require.Len(t, c.jobScheduler.current, 5, "parallel limit applies as before")

	require.Zero(t, podCalls.Load(), "no pod list/watch calls when disabled")
}

func Test__EnabledWithoutPodAccessFallsBackToParallelLimit(t *testing.T) {
	clientset, podCalls := clientsetWithoutPodAccess(t)
	c, _ := newControllerForRBACTest(t, clientset, 3)
	require.NotNil(t, c.podInformers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.podInformers.Start(ctx.Done())

	require.Eventually(t, func() bool { return podCalls.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
	require.Empty(t, c.pendingTracker.JobIDs(), "no pod is counted while the watch fails")

	// Only starting jobs count, so the controller keeps scheduling.
	c.jobScheduler.track("starting", "s1-test", false)
	budget, ok := c.pendingBudget()
	require.True(t, ok)
	require.Equal(t, 2, budget)

	registerTestAgentType(t, c)
	c.createJobs(context.Background(), queue("a", "b", "c"), budget)
	require.Len(t, c.jobScheduler.current, 3)
}

func registerTestAgentType(t *testing.T, c *Controller) {
	tc := newPendingTestController(t, 1, 0)
	c.agentTypeRegistry = tc.agentTypeRegistry
}
