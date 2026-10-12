package config

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/renderedtext/agent-k8s-stack/pkg/podspec"
	"k8s.io/klog/v2"
)

const (
	AgentTypeLabel           = "semaphoreci.com/agent-type"
	JobIDLabel               = "semaphoreci.com/job-id"
	ResourceTypeLabel        = "semaphoreci.com/resource-type"
	AgentTypeResourceType    = "agent-type-configuration"
	SemaphoreJobResourceType = "semaphore-job"
)

var (
	defaultJobStartTimeout = 5 * time.Minute
)

type Config struct {
	Namespace              string
	ServiceAccountName     string
	AgentImage             string
	AgentStartupParameters []string
	AgentPodSpec           *podspec.Spec
	AgentLogLevel          string
	Labels                 []string
	MaxParallelJobs        int
	MaxPendingJobs         int
	SemaphoreEndpoint      string
	KeepFailedJobsFor      time.Duration
	KeepSuccessfulJobsFor  time.Duration
	JobStartTimeout        time.Duration
}

func NewConfigFromEnv(endpoint string) (*Config, error) {
	k8sNamespace := os.Getenv("KUBERNETES_NAMESPACE")
	if k8sNamespace == "" {
		k8sNamespace = "default"
		klog.Warningf("no KUBERNETES_NAMESPACE specified - using '%s'", k8sNamespace)
	}

	agentImage := os.Getenv("SEMAPHORE_AGENT_IMAGE")
	if agentImage == "" {
		agentImage = "semaphoreci/agent:latest"
		klog.Warningf("no SEMAPHORE_AGENT_IMAGE specified - using '%s'", agentImage)
	}

	maxParallelJobs := 10
	if os.Getenv("MAX_PARALLEL_JOBS") != "" {
		fromEnv := os.Getenv("MAX_PARALLEL_JOBS")
		v, err := strconv.Atoi(fromEnv)
		if err != nil {
			log.Printf("Error parsing MAX_PARALLEL_JOBS (%s): %v - using default", strconv.Quote(fromEnv), errors.Unwrap(err))
			maxParallelJobs = 10
		} else {
			maxParallelJobs = v
		}
	}

	agentStartupParameters := []string{}
	if os.Getenv("SEMAPHORE_AGENT_STARTUP_PARAMETERS") != "" {
		agentStartupParameters = strings.Split(os.Getenv("SEMAPHORE_AGENT_STARTUP_PARAMETERS"), " ")
	}

	labels, err := parseLabels()
	if err != nil {
		return nil, fmt.Errorf("unable to determine labels")
	}

	agentPodSpec, err := podspec.Parse(os.Getenv("SEMAPHORE_AGENT_POD_SPEC"))
	if err != nil {
		return nil, fmt.Errorf("invalid SEMAPHORE_AGENT_POD_SPEC: %w", err)
	}

	return &Config{
		SemaphoreEndpoint:      endpoint,
		Namespace:              k8sNamespace,
		ServiceAccountName:     os.Getenv("KUBERNETES_SERVICE_ACCOUNT"),
		AgentImage:             agentImage,
		AgentStartupParameters: agentStartupParameters,
		AgentPodSpec:           agentPodSpec,
		MaxParallelJobs:        maxParallelJobs,
		MaxPendingJobs:         maxPendingJobs(maxParallelJobs),
		Labels:                 labels,
		AgentLogLevel:          agentLogLevel(),
		KeepFailedJobsFor:      keepFailedJobsFor(),
		KeepSuccessfulJobsFor:  keepSuccessfulJobsFor(),
		JobStartTimeout:        jobStartTimeout(),
	}, nil
}

func parseLabels() ([]string, error) {
	labels := []string{}
	fromEnv := os.Getenv("SEMAPHORE_AGENT_LABELS")
	if fromEnv == "" {
		return labels, nil
	}

	for _, label := range strings.Split(fromEnv, ",") {
		parts := strings.Split(label, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("%s is not a valid label", label)
		}

		labels = append(labels, label)
	}

	return labels, nil
}

// 0, the default, disables the check. Invalid values disable it too.
func maxPendingJobs(maxParallelJobs int) int {
	fromEnv := os.Getenv("MAX_PENDING_JOBS")
	if fromEnv == "" {
		return 0
	}

	v, err := strconv.Atoi(fromEnv)
	if err != nil || v < 0 {
		klog.Warningf("Invalid MAX_PENDING_JOBS (%s) - pending jobs check disabled", fromEnv)
		return 0
	}

	if v >= maxParallelJobs {
		klog.Warningf("MAX_PENDING_JOBS (%d) >= MAX_PARALLEL_JOBS (%d) - it will have no effect", v, maxParallelJobs)
	}

	return v
}

func keepFailedJobsFor() time.Duration {
	keepFor, err := time.ParseDuration(os.Getenv("KEEP_FAILED_JOBS_FOR"))
	if err != nil {
		return 0
	}

	return keepFor
}

func keepSuccessfulJobsFor() time.Duration {
	keepFor, err := time.ParseDuration(os.Getenv("KEEP_SUCCESSFUL_JOBS_FOR"))
	if err != nil {
		return 0
	}

	return keepFor
}

func jobStartTimeout() time.Duration {
	timeout, err := time.ParseDuration(os.Getenv("JOB_START_TIMEOUT"))
	if err != nil {
		klog.Infof("No JOB_START_TIMEOUT set, using %v", defaultJobStartTimeout)
		return defaultJobStartTimeout
	}

	return timeout
}

func agentLogLevel() string {
	logLevel := os.Getenv("SEMAPHORE_AGENT_LOG_LEVEL")
	if logLevel == "" {
		klog.Info("No SEMAPHORE_AGENT_LOG_LEVEL set, using info")
		return "info"
	}

	klog.Infof("SEMAPHORE_AGENT_LOG_LEVEL=%s", logLevel)
	return logLevel
}
