package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/renderedtext/agent-k8s-stack/pkg/semaphore"
	"github.com/stretchr/testify/require"
)

func ids(jobs []semaphore.JobRequest) []string {
	out := []string{}
	for _, j := range jobs {
		out = append(out, j.JobID)
	}
	return out
}

func typed(agentType string, ids ...string) []semaphore.JobRequest {
	jobs := []semaphore.JobRequest{}
	for _, id := range ids {
		jobs = append(jobs, semaphore.JobRequest{JobID: id, MachineType: agentType})
	}
	return jobs
}

func Test__QueueOrder(t *testing.T) {
	t0 := time.Now()

	t.Run("one poll -> API's newest-first order reversed", func(t *testing.T) {
		q := newQueueOrder()
		require.Equal(t, []string{"a", "b", "c"}, ids(q.sort(queue("c", "b", "a"), t0)))
	})

	t.Run("jobs seen earlier come before newer ones", func(t *testing.T) {
		q := newQueueOrder()
		q.sort(queue("b", "a"), t0)
		got := q.sort(queue("d", "c", "b", "a"), t0.Add(10*time.Second))
		require.Equal(t, []string{"a", "b", "c", "d"}, ids(got))
	})

	t.Run("across agent types, by first seen", func(t *testing.T) {
		q := newQueueOrder()
		q.sort(typed("s1-y", "y1"), t0)
		got := q.sort(append(typed("s1-x", "x2", "x1"), typed("s1-y", "y1")...), t0.Add(10*time.Second))
		require.Equal(t, []string{"y1", "x1", "x2"}, ids(got))
	})

	t.Run("jobs that left the queue are forgotten", func(t *testing.T) {
		q := newQueueOrder()
		q.sort(queue("b", "a"), t0)
		q.sort(queue("b"), t0.Add(10*time.Second))
		require.NotContains(t, q.firstSeen, "a")
		require.Contains(t, q.firstSeen, "b")
	})

	// A type that lists nothing may have failed to list, so its jobs keep their age.
	t.Run("a type listing nothing keeps its jobs", func(t *testing.T) {
		q := newQueueOrder()
		q.sort(append(typed("s1-x", "x1"), typed("s1-y", "y1")...), t0)
		q.sort(typed("s1-x", "x1"), t0.Add(10*time.Second))
		require.Contains(t, q.firstSeen, "y1")
		got := q.sort(append(typed("s1-x", "x2", "x1"), typed("s1-y", "y2", "y1")...), t0.Add(20*time.Second))
		require.ElementsMatch(t, []string{"x1", "y1"}, ids(got)[:2])
		require.ElementsMatch(t, []string{"x2", "y2"}, ids(got)[2:])
	})
}

// With newer jobs arriving on every poll and room for one job per poll,
// the oldest job must start first: the API alone would always offer the newest.
func Test__OldestJobIsNotStarved(t *testing.T) {
	c := newPendingTestController(t, 100, 1)
	c.queueOrder = newQueueOrder()

	waiting := []string{"old"}
	now := time.Now()
	for poll := 0; poll < 5; poll++ {
		waiting = append([]string{fmt.Sprintf("new-%d", poll)}, waiting...)
		c.createJobs(context.Background(), c.queueOrder.sort(queue(waiting...), now), 1)
		now = now.Add(10 * time.Second)

		if poll == 0 {
			require.True(t, c.jobScheduler.IsCurrentJob("old"), "the oldest job did not get the budget")
		}
	}

	require.Len(t, c.jobScheduler.current, 5)
	require.False(t, c.jobScheduler.IsCurrentJob("new-4"), "the newest job overtook older ones")
}
