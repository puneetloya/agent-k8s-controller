package semaphore

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/renderedtext/agent-k8s-stack/pkg/agenttypes"
	"github.com/renderedtext/agent-k8s-stack/pkg/metrics"
	"github.com/stretchr/testify/require"
)

// A failing agent type is counted under its name and does not count as a successful poll.
func Test__ListJobsCountsErrorsPerAgentType(t *testing.T) {
	lastPoll := testutil.ToFloat64(metrics.LastSuccessfulPoll)
	errors := testutil.ToFloat64(metrics.PollErrors.WithLabelValues("s1-unreachable"))

	client := NewClient("127.0.0.1:1")
	jobs, err := client.ListJobs([]*agenttypes.AgentType{{AgentTypeName: "s1-unreachable"}})
	require.NoError(t, err)
	require.Empty(t, jobs)

	require.Equal(t, errors+1, testutil.ToFloat64(metrics.PollErrors.WithLabelValues("s1-unreachable")))
	require.Equal(t, lastPoll, testutil.ToFloat64(metrics.LastSuccessfulPoll))
}
