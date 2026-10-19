package metrics

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test__ServeExposesMetrics(t *testing.T) {
	l, err := Listen("127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Serve(ctx, l)
		close(done)
	}()

	resp, err := http.Get("http://" + l.Addr().String() + "/metrics")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(body), `semaphore_controller_polls_total{result="skipped_pending_cap"} 0`)
	require.Contains(t, string(body), "go_goroutines")

	cancel()
	<-done
}

func Test__ListenRejectsBadAddress(t *testing.T) {
	_, err := Listen("not-an-address")
	require.Error(t, err)
}
