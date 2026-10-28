package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test__AgentPodSpec(t *testing.T) {
	t.Run("not set -> no spec", func(t *testing.T) {
		t.Setenv("SEMAPHORE_AGENT_POD_SPEC", "")
		cfg, err := NewConfigFromEnv("example.semaphoreci.com")
		require.NoError(t, err)
		require.Nil(t, cfg.AgentPodSpec)
	})

	t.Run("valid -> spec is loaded", func(t *testing.T) {
		t.Setenv("SEMAPHORE_AGENT_POD_SPEC", `{"nodeSelector":{"pool":"agents"}}`)
		cfg, err := NewConfigFromEnv("example.semaphoreci.com")
		require.NoError(t, err)
		require.NotNil(t, cfg.AgentPodSpec)
		require.Equal(t, map[string]string{"pool": "agents"}, cfg.AgentPodSpec.NodeSelector)
	})

	t.Run("invalid -> error", func(t *testing.T) {
		t.Setenv("SEMAPHORE_AGENT_POD_SPEC", "nodeSelectors:\n  pool: agents\n")
		_, err := NewConfigFromEnv("example.semaphoreci.com")
		require.ErrorContains(t, err, "invalid SEMAPHORE_AGENT_POD_SPEC")
		require.ErrorContains(t, err, `unknown field "nodeSelectors"`)
	})
}

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

func Test__JobTTLAfterFinished(t *testing.T) {
	cases := []struct {
		name     string
		env      string
		expected int32
	}{
		{name: "unset -> disabled", env: "", expected: 0},
		{name: "zero -> disabled", env: "0", expected: 0},
		{name: "valid value", env: "120", expected: 120},
		{name: "not a number -> disabled", env: "2m", expected: 0},
		{name: "negative -> disabled", env: "-5", expected: 0},
		{name: "out of range -> disabled", env: "4294967296", expected: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("JOB_TTL_SECONDS_AFTER_FINISHED", tc.env)
			cfg, err := NewConfigFromEnv("example.semaphoreci.com")
			require.NoError(t, err)
			require.Equal(t, tc.expected, cfg.JobTTLAfterFinished)
		})
	}

	for _, env := range []string{"KEEP_FAILED_JOBS_FOR", "KEEP_SUCCESSFUL_JOBS_FOR"} {
		t.Run("set with "+env+" -> error", func(t *testing.T) {
			t.Setenv("JOB_TTL_SECONDS_AFTER_FINISHED", "120")
			t.Setenv(env, "5m")
			_, err := NewConfigFromEnv("example.semaphoreci.com")
			require.ErrorContains(t, err, "cannot be combined")
		})

		t.Run("disabled with "+env+" -> retention applies", func(t *testing.T) {
			t.Setenv("JOB_TTL_SECONDS_AFTER_FINISHED", "0")
			t.Setenv(env, "5m")
			_, err := NewConfigFromEnv("example.semaphoreci.com")
			require.NoError(t, err)
		})
	}
}
