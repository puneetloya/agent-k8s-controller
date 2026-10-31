package controller

import (
	"sort"
	"time"

	"github.com/renderedtext/agent-k8s-stack/pkg/semaphore"
)

// The jobs API lists each agent type's queue newest first, so a limited budget
// would always go to the newest jobs. queueOrder hands it out oldest first instead.
type queueOrder struct {
	firstSeen map[string]seenJob
}

type seenJob struct {
	at        time.Time
	agentType string
}

func newQueueOrder() *queueOrder {
	return &queueOrder{firstSeen: map[string]seenJob{}}
}

// Sorts jobs by when they were first seen, across agent types. Jobs first seen in
// the same poll are in the API's order reversed, so the oldest comes first.
func (q *queueOrder) sort(jobs []semaphore.JobRequest, now time.Time) []semaphore.JobRequest {
	listed := map[string]bool{}
	types := map[string]bool{}
	for _, j := range jobs {
		listed[j.JobID] = true
		types[j.MachineType] = true
		if _, ok := q.firstSeen[j.JobID]; !ok {
			q.firstSeen[j.JobID] = seenJob{at: now, agentType: j.MachineType}
		}
	}

	// A job its agent type no longer lists has left the queue. Types listing nothing
	// may have failed to list, so their jobs are kept; there are at most a page of them.
	for id, s := range q.firstSeen {
		if types[s.agentType] && !listed[id] {
			delete(q.firstSeen, id)
		}
	}

	sorted := make([]semaphore.JobRequest, len(jobs))
	for i, j := range jobs {
		sorted[len(jobs)-1-i] = j
	}

	sort.SliceStable(sorted, func(a, b int) bool {
		return q.firstSeen[sorted[a].JobID].at.Before(q.firstSeen[sorted[b].JobID].at)
	})

	return sorted
}
