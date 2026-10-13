package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func Test__ApplyRateLimits(t *testing.T) {
	cases := []struct {
		name       string
		qps, burst string
		wantQPS    float32
		wantBurst  int
	}{
		{name: "unset keeps client-go defaults", qps: "", burst: "", wantQPS: 0, wantBurst: 0},
		{name: "valid values are applied", qps: "50", burst: "100", wantQPS: 50, wantBurst: 100},
		{name: "fractional QPS", qps: "12.5", burst: "", wantQPS: 12.5, wantBurst: 0},
		{name: "invalid values are ignored", qps: "abc", burst: "x", wantQPS: 0, wantBurst: 0},
		{name: "non-positive values are ignored", qps: "-1", burst: "0", wantQPS: 0, wantBurst: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KUBE_API_QPS", tc.qps)
			t.Setenv("KUBE_API_BURST", tc.burst)

			c := &rest.Config{}
			applyRateLimits(c)
			require.Equal(t, tc.wantQPS, c.QPS)
			require.Equal(t, tc.wantBurst, c.Burst)
		})
	}
}
