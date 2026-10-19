package controller

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/renderedtext/agent-k8s-stack/pkg/agenttypes"
	"github.com/renderedtext/agent-k8s-stack/pkg/config"
	"github.com/renderedtext/agent-k8s-stack/pkg/metrics"
	"github.com/renderedtext/agent-k8s-stack/pkg/semaphore"
	"k8s.io/client-go/kubernetes"
)

type Controller struct {
	semaphoreClient   *semaphore.Client
	agentTypeRegistry *agenttypes.Registry
	clientset         kubernetes.Interface
	jobScheduler      *JobScheduler
	config            *config.Config

	// Both nil unless MAX_PENDING_JOBS is set.
	pendingTracker *PendingTracker
	podInformers   informers.SharedInformerFactory

	// Jobs from the last poll left without a Kubernetes job, for metrics.
	awaitingCreation atomic.Int64
}

func New(
	ctx context.Context,
	informerFactory informers.SharedInformerFactory,
	config *config.Config,
	semaphoreClient *semaphore.Client,
	clientset kubernetes.Interface) (*Controller, error) {

	agentTypeRegistry, err := agenttypes.NewRegistry()
	if err != nil {
		return nil, err
	}

	if err := agentTypeRegistry.RegisterInformer(informerFactory); err != nil {
		return nil, err
	}

	jobScheduler, err := NewJobScheduler(clientset, config)
	if err != nil {
		return nil, err
	}

	if err := jobScheduler.RegisterInformer(informerFactory); err != nil {
		return nil, err
	}

	c := &Controller{
		semaphoreClient:   semaphoreClient,
		clientset:         clientset,
		agentTypeRegistry: agentTypeRegistry,
		jobScheduler:      jobScheduler,
		config:            config,
	}

	if config.MaxPendingJobs > 0 {
		if err := c.setupPendingTracker(); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Job pods lack the resource type label the shared informer factory filters on,
// so they get their own factory, scoped to job pods only.
func (c *Controller) setupPendingTracker() error {
	selector, err := JobPodSelector()
	if err != nil {
		return err
	}

	factory := informers.NewSharedInformerFactoryWithOptions(
		c.clientset,
		time.Minute,
		informers.WithNamespace(c.config.Namespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = selector
		}),
	)

	tracker := NewPendingTracker()
	if err := tracker.RegisterInformer(factory); err != nil {
		return err
	}

	c.pendingTracker = tracker
	c.podInformers = factory
	klog.InfoS("Pending jobs check enabled", "maxPendingJobs", c.config.MaxPendingJobs)
	return nil
}

func (c *Controller) Run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.Info("Starting controller")

	// Not waiting for the cache to sync: until it does, no pod counts as unscheduled.
	if c.podInformers != nil {
		c.podInformers.Start(ctx.Done())
	}

	go wait.UntilWithContext(ctx, c.runWorker, time.Second)

	<-ctx.Done()
	logger.Info("Shutting down workers")

	return nil
}

func (c *Controller) runWorker(ctx context.Context) {
	for c.tick(ctx) {
		time.Sleep(10 * time.Second)
	}
}

func (c *Controller) tick(ctx context.Context) bool {
	agentTypes := c.agentTypeRegistry.All()
	if len(agentTypes) == 0 {
		klog.Info("Not polling Semaphore API - no agent types found")
		metrics.Polls.WithLabelValues(metrics.PollSkippedNoAgentTypes).Inc()
		return true
	}

	if !c.jobScheduler.HasSpace() {
		klog.Info("Not polling Semaphore API - no space")
		metrics.Polls.WithLabelValues(metrics.PollSkippedNoSpace).Inc()
		return true
	}

	budget, ok := c.pendingBudget()
	if !ok {
		metrics.Polls.WithLabelValues(metrics.PollSkippedPendingCap).Inc()
		return true
	}

	klog.InfoS("Polling Semaphore API", "types", agentTypeNames(agentTypes))
	jobs, err := c.semaphoreClient.ListJobs(agentTypes)
	if err != nil {
		klog.Error(err, "error polling job queue")
		metrics.Polls.WithLabelValues(metrics.PollError).Inc()
		return true
	}

	metrics.Polls.WithLabelValues(metrics.PollOK).Inc()
	klog.Infof("Found %d jobs in the queue", len(jobs))
	c.createJobs(ctx, jobs, budget)
	c.awaitingCreation.Store(int64(c.countAwaitingCreation(jobs)))
	return true
}

func (c *Controller) countAwaitingCreation(jobs []semaphore.JobRequest) int {
	n := 0
	for _, j := range jobs {
		if !c.jobScheduler.IsCurrentJob(j.JobID) {
			n++
		}
	}

	return n
}

// noBudget means the pending jobs check is off: only MAX_PARALLEL_JOBS applies.
const noBudget = -1

// Returns how many jobs this tick may create, and false if it should not poll at all.
func (c *Controller) pendingBudget() (int, bool) {
	if c.pendingTracker == nil {
		return noBudget, true
	}

	limit := c.config.MaxPendingJobs
	pending := c.jobScheduler.Pending(c.pendingTracker.JobIDs())
	if pending >= limit {
		klog.InfoS("Not polling Semaphore API - too many pending jobs", "pending", pending, "max", limit)
		return 0, false
	}

	return limit - pending, true
}

func (c *Controller) createJobs(ctx context.Context, jobs []semaphore.JobRequest, budget int) {
	for _, j := range jobs {
		if budget == 0 {
			klog.Info("Reached limit of pending jobs")
			break
		}

		agentType := c.agentTypeRegistry.Get(j.MachineType)
		if agentType == nil {
			klog.Warningf("agent type '%s' not registered", j.MachineType)
			continue
		}

		// Create() is a no-op for a tracked job; skip it so it does not spend budget.
		if budget != noBudget && c.jobScheduler.IsCurrentJob(j.JobID) {
			continue
		}

		err := c.jobScheduler.Create(ctx, j, agentType)
		if errors.Is(err, ErrParallelJobsLimitReached) {
			klog.Info("Reached limit of parallel jobs")
			break
		}

		if err != nil {
			klog.ErrorS(err, "Error creating job", "job", j.JobID, "type", j.MachineType)
		}

		if err == nil && budget != noBudget {
			budget--
		}
	}
}

func agentTypeNames(agentTypes []*agenttypes.AgentType) []string {
	names := []string{}
	for _, agentType := range agentTypes {
		names = append(names, agentType.AgentTypeName)
	}
	return names
}
