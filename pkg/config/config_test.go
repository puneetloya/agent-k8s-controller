package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test__MaxPendingJobs(t *testing.T) {
	cases := []struct {
		name     string
		env      string
		expected int
	}{
		{name: "unset -> disabled", env: "", expected: 0},
		{name: "zero -> disabled", env: "0", expected: 0},
		{name: "valid value", env: "20", expected: 20},
		{name: "not a number -> disabled", env: "abc", expected: 0},
		{name: "negative -> disabled", env: "-5", expected: 0},
		{name: "at or above parallel limit is kept", env: "500", expected: 500},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MAX_PARALLEL_JOBS", "100")
			t.Setenv("MAX_PENDING_JOBS", tc.env)

			cfg, err := NewConfigFromEnv("example.semaphoreci.com")
			require.NoError(t, err)
			require.Equal(t, 100, cfg.MaxParallelJobs)
			require.Equal(t, tc.expected, cfg.MaxPendingJobs)
		})
	}
}
