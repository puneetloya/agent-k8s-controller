package semaphore

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/renderedtext/agent-k8s-stack/pkg/agenttypes"
	"github.com/stretchr/testify/require"
)

func serve(t *testing.T, handler http.HandlerFunc) *Client {
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)

	previous := httpClient
	httpClient = server.Client()
	httpClient.Timeout = 200 * time.Millisecond
	t.Cleanup(func() { httpClient = previous })

	return NewClient(strings.TrimPrefix(server.URL, "https://"))
}

func Test__ListJobsForAgentType(t *testing.T) {
	agentType := &agenttypes.AgentType{AgentTypeName: "s1-test", RegistrationToken: "token"}

	t.Run("ok -> job IDs", func(t *testing.T) {
		c := serve(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "Token token", r.Header.Get("Authorization"))
			_, _ = w.Write([]byte(`{"jobs":[{"id":"b"},{"id":"a"}]}`))
		})
		jobs, err := c.listJobsForAgentType(agentType)
		require.NoError(t, err)
		require.Equal(t, []string{"b", "a"}, jobs)
	})

	t.Run("not 200 -> error with the status, not an empty queue", func(t *testing.T) {
		c := serve(t, func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		})
		_, err := c.listJobsForAgentType(agentType)
		require.ErrorContains(t, err, "unexpected status 404")
	})

	t.Run("hangs -> times out", func(t *testing.T) {
		release := make(chan struct{})
		c := serve(t, func(w http.ResponseWriter, r *http.Request) { <-release })
		defer close(release)

		start := time.Now()
		_, err := c.listJobsForAgentType(agentType)
		require.Error(t, err)
		require.Less(t, time.Since(start), 5*time.Second)
	})
}
