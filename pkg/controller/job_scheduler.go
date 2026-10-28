package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	versions "github.com/hashicorp/go-version"
	"github.com/renderedtext/agent-k8s-stack/pkg/agenttypes"
	checks "github.com/renderedtext/agent-k8s-stack/pkg/checks"
	"github.com/renderedtext/agent-k8s-stack/pkg/config"
	"github.com/renderedtext/agent-k8s-stack/pkg/podspec"
	"github.com/renderedtext/agent-k8s-stack/pkg/semaphore"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

var ErrParallelJobsLimitReached = errors.New("number of parallel jobs reached")

type JobState struct {
	ID        string
	AgentType string
	Running   bool

	//
	// False while we are only holding a slot for a job we are about to
	// create, true once the informer tells us the job actually exists.
	// Without this we cannot tell a reservation we should give back from
	// a job Kubernetes already persisted.
	//
	Confirmed bool
}

// Kubernetes requests made by the scheduler get a deadline, so that a slow
// or unreachable API server cannot block the caller forever. This is set per
// request on purpose: rest.Config.Timeout would also apply to the informer's
// watches, which are long lived by design, and cutting those every 30s adds
// API load and creates the very watch gaps we handle in jobFrom().
const apiRequestTimeout = 30 * time.Second

type JobScheduler struct {
	clientset         kubernetes.Interface
	config            *config.Config
	current           map[string]*JobState
	mu                sync.Mutex
	kubernetesVersion *versions.Version
}

func NewJobScheduler(clientset kubernetes.Interface, config *config.Config) (*JobScheduler, error) {
	version, err := clientset.Discovery().ServerVersion()
	if err != nil {
		return nil, err
	}

	klog.InfoS("Kubernetes version", "version", version)

	v, err := versions.NewVersion(version.String())
	if err != nil {
		return nil, err
	}

	return &JobScheduler{
		current:           map[string]*JobState{},
		clientset:         clientset,
		config:            config,
		kubernetesVersion: v,
	}, nil
}

func (s *JobScheduler) RegisterInformer(informerFactory informers.SharedInformerFactory) error {
	informer := informerFactory.Batch().V1().Jobs()
	_, err := informer.Informer().AddEventHandler(s)
	return err
}

func (s *JobScheduler) HasSpace() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.current) < s.config.MaxParallelJobs
}

func (s *JobScheduler) Create(ctx context.Context, req semaphore.JobRequest, agentType *agenttypes.AgentType) error {
	//
	// We take the slot before creating the job, and give it back if the
	// creation fails. That keeps the lock off the request: holding it here
	// would block the informer's goroutine, which needs the same lock, for
	// as long as the API server takes to answer.
	//
	// If the job was already created, we don't create it again.
	// This can happen if the time it takes for the agent to start
	// is bigger than the time it takes for the next controller tick to run.
	//
	reserved, err := s.reserve(req.JobID, req.MachineType)
	if err != nil {
		return err
	}

	if !reserved {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, apiRequestTimeout)
	defer cancel()

	_, err = s.clientset.BatchV1().
		Jobs(s.config.Namespace).
		Create(
			ctx,
			s.buildJob(req, agentType),
			metav1.CreateOptions{},
		)

	if err != nil {
		//
		// The request can fail after Kubernetes already persisted the job -
		// a timeout on the way back, for instance. The informer tells us when
		// that happened, so only give the slot back if it has not.
		//
		s.releaseReservation(req.JobID)
		return err
	}

	klog.InfoS("Job created", "job", req.JobID, "type", req.MachineType)
	return nil
}

func (s *JobScheduler) jobName(jobID string) string {
	return fmt.Sprintf("semaphore-agent-%s", jobID)
}

func (s *JobScheduler) buildJob(job semaphore.JobRequest, agentType *agenttypes.AgentType) *batchv1.Job {
	parallelism := int32(1)
	retries := int32(0)
	activeDeadlineSeconds := int64(60 * 60 * 24) // 1 day
	terminationGracePeriod := int64(300)

	podSpec := corev1.PodSpec{
		RestartPolicy:                 corev1.RestartPolicyNever,
		ServiceAccountName:            s.config.ServiceAccountName,
		TerminationGracePeriodSeconds: &terminationGracePeriod,
		Containers: []corev1.Container{
			{
				Name:  "semaphore-agent",
				Image: s.config.AgentImage,
				Command: []string{
					"/opt/semaphore/agent",
					"start",
				},
				Args: s.buildAgentStartupParameters(agentType, job.JobID),
				Env: []corev1.EnvVar{
					{
						Name: "KUBERNETES_NAMESPACE",
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
						},
					},
					{
						Name: "SEMAPHORE_AGENT_NAME",
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
						},
					},
					{
						Name:  "SEMAPHORE_AGENT_LOG_LEVEL",
						Value: s.config.AgentLogLevel,
					},
					{
						Name: "SEMAPHORE_AGENT_TOKEN",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								Key: "registrationToken",
								LocalObjectReference: corev1.LocalObjectReference{
									Name: agentType.SecretName,
								},
							},
						},
					},
				},
			},
		},
	}

	s.agentPodSpec(agentType).ApplyTo(&podSpec)

	var ttl *int32
	if s.config.JobTTLAfterFinished > 0 {
		ttl = &s.config.JobTTLAfterFinished
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.jobName(job.JobID),
			Namespace: s.config.Namespace,
			Labels:    s.buildLabels(job),
		},
		Spec: batchv1.JobSpec{
			Parallelism:             &parallelism,
			Completions:             &parallelism,
			BackoffLimit:            &retries,
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			TTLSecondsAfterFinished: ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: s.buildLabels(job)},
				Spec:       podSpec,
			},
		},
	}
}

// Like startup parameters, an agent type's pod spec
// replaces the controller's default as a whole.
func (s *JobScheduler) agentPodSpec(agentType *agenttypes.AgentType) *podspec.Spec {
	if agentType.AgentPodSpec != nil {
		return agentType.AgentPodSpec
	}

	return s.config.AgentPodSpec
}

func (s *JobScheduler) buildLabels(job semaphore.JobRequest) map[string]string {
	labels := map[string]string{
		config.ResourceTypeLabel: config.SemaphoreJobResourceType,
		config.AgentTypeLabel:    job.MachineType,
		config.JobIDLabel:        job.JobID,
	}

	for _, label := range s.config.Labels {
		parts := strings.Split(label, "=")
		labels[parts[0]] = parts[1]
	}

	return labels
}

func (s *JobScheduler) buildAgentStartupParameters(agentType *agenttypes.AgentType, jobID string) []string {
	labels := []string{
		fmt.Sprintf("%s=%s", config.AgentTypeLabel, agentType.AgentTypeName),
	}

	// Lets the pending jobs check find this job's pod. Off by default.
	if s.config.MaxPendingJobs > 0 {
		labels = append(labels, fmt.Sprintf("%s=%s", config.JobIDLabel, jobID))
	}

	if len(s.config.Labels) > 0 {
		labels = append(labels, s.config.Labels...)
	}

	parameters := []string{
		"--endpoint",
		s.config.SemaphoreEndpoint,
		"--job-id",
		jobID,
		"--kubernetes-labels",
		strings.Join(labels, ","),
		"--kubernetes-executor",
		"--disconnect-after-job",
	}

	// If agent type does not specify startup parameters, use the controller's defaults.
	if len(agentType.AgentStartupParameters) == 0 {
		return append(parameters, s.config.AgentStartupParameters...)
	}

	// Otherwise, use the agent type's startup parameters.
	return append(parameters, agentType.AgentStartupParameters...)
}

func (s *JobScheduler) delete(job *batchv1.Job) error {
	ctx, cancel := context.WithTimeout(context.Background(), apiRequestTimeout)
	defer cancel()

	propagationPolicy := metav1.DeletePropagationBackground
	options := metav1.DeleteOptions{PropagationPolicy: &propagationPolicy}

	//
	// A job's name comes from the Semaphore job ID, so a job that is re-queued
	// and created again carries the same name as the one we are looking at.
	// Pinning the UID keeps a deletion we decided for one job from removing
	// its replacement.
	//
	// Only pin when we have a UID: an empty one matches no object, so sending
	// it would turn every deletion into a failed precondition.
	//
	if job.UID != "" {
		options.Preconditions = &metav1.Preconditions{UID: &job.UID}
	}

	return s.clientset.BatchV1().
		Jobs(s.config.Namespace).
		Delete(ctx, job.Name, options)
}

// This method executes when a new job is added,
// but also executes for all jobs when the controller starts up.
// If the controller crashed for whatever reason, we reload the jobs.
func (s *JobScheduler) OnAdd(obj interface{}, _ bool) {
	job, ok := jobFrom(obj)
	if !ok {
		return
	}

	jobID, ok := job.Labels[config.JobIDLabel]
	if !ok {
		klog.Warningf("Job '%s' is missing '%s' label", job.Name, config.JobIDLabel)
		return
	}

	agentType, ok := job.Labels[config.AgentTypeLabel]
	if !ok {
		klog.Warningf("Job '%s' is missing '%s' label", job.Name, config.AgentTypeLabel)
		return
	}

	//
	// If we are already tracking the job, all we have to do is record that
	// the informer has now seen it, so Create() knows not to give the slot
	// back if its request fails.
	//
	if s.confirm(jobID) {
		return
	}

	//
	// isJobRunning() can talk to the Kubernetes API,
	// so it must not run while we hold the lock.
	//
	logger := klog.LoggerWithValues(klog.Background(), "job", jobID, "type", agentType)
	running := s.isJobRunning(logger, jobID, job)
	if s.track(jobID, agentType, running) {
		logger.Info("Job loaded")
	}
}

// Handles job state transitions
func (s *JobScheduler) OnUpdate(_, obj interface{}) {
	job, ok := jobFrom(obj)
	if !ok {
		return
	}

	jobID, ok := job.Labels[config.JobIDLabel]
	if !ok {
		klog.Warningf("Job '%s' is missing '%s' label", job.Name, config.JobIDLabel)
		return
	}

	agentType, ok := job.Labels[config.AgentTypeLabel]
	if !ok {
		klog.Warningf("Job '%s' is missing '%s' label", job.Name, config.AgentTypeLabel)
		return
	}

	logger := klog.LoggerWithValues(klog.Background(), "job", jobID, "type", agentType)
	conditions := jobConditions(job)

	//
	// If the "Complete" condition is set for the job,
	// we know it finished successfully.
	//
	if slices.Contains(conditions, batchv1.JobComplete) {
		s.handleSuccessfulJob(logger, jobID, job)
		return
	}

	//
	// If the "Failed" condition is set for the job,
	// we know it failed to complete successfully.
	//
	if slices.Contains(conditions, batchv1.JobFailed) {
		s.handleFailedJob(logger, jobID, job)
		return
	}

	//
	// If the job doesn't have any terminal condition set
	// it is still running.
	//
	s.handleInProgress(logger, jobID, job)
}

// This must not be called while holding s.mu,
// because checks.IsJobRunning() can talk to the Kubernetes API.
func (s *JobScheduler) isJobRunning(logger logr.Logger, jobID string, job *batchv1.Job) bool {
	//
	// Check if we have already marked this job as started.
	// The reason for this check is that there is a small period of time
	// between the pod finishing and the job being marked as complete,
	// where the status.ready counter goes back to 0.
	//
	if s.isTrackedAsRunning(jobID) {
		return true
	}

	return checks.IsJobRunning(s.clientset, logger, job, func() *versions.Version {
		return s.kubernetesVersion
	})
}

func (s *JobScheduler) handleInProgress(logger logr.Logger, jobID string, job *batchv1.Job) {
	if s.isJobRunning(logger, jobID, job) {
		//
		// A job can be running and not be tracked by us anymore.
		// That happens when we stop tracking a job that did not start in time
		// and delete it, but its pod becomes ready before the deletion is
		// observed by the informer. There is nothing to update in that case.
		//
		if !s.markRunning(jobID) {
			//
			// We already decided this job should not exist - it did not start
			// in time, so we stopped tracking it and deleted it. Seeing it run
			// means the deletion did not go through, so try again. Returning
			// here instead would leave a pod running in a slot that
			// HasSpace() does not count, until the job's deadline expires.
			//
			logger.Info("Job is running, but is not tracked anymore - deleting it again")

			//
			// Not finding it means somebody else already removed it, and a
			// conflict means what we are looking at is not the job that
			// carries this name anymore. Neither is worth an error.
			//
			err := s.delete(job)
			if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
				logger.Error(err, "Error deleting untracked job")
			}

			return
		}

		logger.Info("Job is running", "for", runningFor(job))
		return
	}

	waitingFor := time.Since(job.CreationTimestamp.Time)
	if waitingFor > s.config.JobStartTimeout {
		logger.Error(nil, "job did not start in time - canceling",
			"reason", s.notStartedReason(job), "status", job.Status, "for", waitingFor)
		s.untrack(jobID)
		if err := s.delete(job); err != nil {
			logger.Error(err, "Error deleting job")
		}

		return
	}

	logger.Info("Job is starting", "status", job.Status, "for", waitingFor)
}

// Tells a pod the scheduler could not place, or whose container cannot
// start, apart from one that was never created, e.g. rejected at admission.
// Pods are selected by the job's UID: an earlier job with the same name
// can still have a pod terminating.
func (s *JobScheduler) notStartedReason(job *batchv1.Job) string {
	ctx, cancel := context.WithTimeout(context.Background(), apiRequestTimeout)
	defer cancel()

	pods, err := s.clientset.CoreV1().
		Pods(s.config.Namespace).
		List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("controller-uid=%s", job.UID)})

	if err != nil {
		return fmt.Sprintf("unknown - error listing pods: %v", err)
	}

	if len(pods.Items) == 0 {
		return "no pod was created - see the job's events"
	}

	pod := pods.Items[0]
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status != corev1.ConditionTrue {
			return fmt.Sprintf("pod %s was not scheduled: %s", pod.Name, cond.Message)
		}
	}

	for _, container := range pod.Status.ContainerStatuses {
		if waiting := container.State.Waiting; waiting != nil {
			return fmt.Sprintf("pod %s is waiting: %s %s", pod.Name, waiting.Reason, waiting.Message)
		}
	}

	return fmt.Sprintf("pod %s is %s", pod.Name, pod.Status.Phase)
}

func (s *JobScheduler) handleSuccessfulJob(logger logr.Logger, jobID string, job *batchv1.Job) {
	logger.Info("Job finished successfully")

	// We remove it from the list of currently running jobs,
	// before we even check if the job should be deleted or not,
	// to make room for new jobs.
	s.untrack(jobID)
	if leftToKubernetes(job) {
		return
	}

	shouldDelete, err := s.ShouldDeleteJob(logger, s.config.KeepSuccessfulJobsFor, completedAt(job))
	if err != nil {
		logger.Error(err, "not able to determine if job is deletable - keeping job")
		return
	}

	if shouldDelete {
		logger.Info("Deleting job")
		if err := s.delete(job); err != nil {
			logger.Error(err, "Error deleting job")
			return
		}
	}
}

func (s *JobScheduler) handleFailedJob(logger logr.Logger, jobID string, job *batchv1.Job) {
	logger.Info("Job failed", "reason", getFailedReason(job), "message", getFailedMessage(job))

	// We remove it from the list of currently running jobs,
	// before we even check if the job should be deleted or not,
	// to make room for new jobs.
	s.untrack(jobID)
	if leftToKubernetes(job) {
		return
	}

	shouldDelete, err := s.ShouldDeleteJob(logger, s.config.KeepFailedJobsFor, job.CreationTimestamp.Time)
	if err != nil {
		logger.Error(err, "not able to determine current number of failed jobs - not deleting")
		return
	}

	if shouldDelete {
		logger.Info("Deleting job")
		if err := s.delete(job); err != nil {
			logger.Error(err, "Error deleting job")
			return
		}
	}
}

// Kubernetes deletes a finished job that carries a TTL, so the controller does not.
// Checking the job, not the config, keeps deleting jobs created before the TTL was set.
func leftToKubernetes(job *batchv1.Job) bool {
	return job.Spec.TTLSecondsAfterFinished != nil
}

func (s *JobScheduler) ShouldDeleteJob(l logr.Logger, keepFor time.Duration, t time.Time) (bool, error) {
	if keepFor == 0 {
		l.Info("No retention policy set - job should be deleted")
		return true, nil
	}

	since := time.Since(t)
	if since > keepFor {
		l.Info("Retention policy reached - job should be deleted", "policy", keepFor, "elapsed", since)
		return true, nil
	}

	l.Info("Retention policy not reached - job should be kept", "policy", keepFor, "elapsed", since)
	return false, nil
}

func (s *JobScheduler) OnDelete(obj interface{}) {
	job, ok := jobFrom(obj)
	if !ok {
		return
	}

	jobID, ok := job.Labels[config.JobIDLabel]
	if !ok {
		klog.Warningf("Job '%s' is missing '%s' label", job.Name, config.JobIDLabel)
		return
	}

	agentType, ok := job.Labels[config.AgentTypeLabel]
	if !ok {
		klog.Warningf("Job '%s' is missing '%s' label", job.Name, config.AgentTypeLabel)
		return
	}

	s.untrack(jobID)
	klog.InfoS("Job deleted", "job", jobID, "type", agentType)
}

// Pending counts tracked jobs that are not running yet: the agent pod is not ready,
// or its job pod has no node yet. Each job counts once, so this never exceeds len(current).
func (s *JobScheduler) Pending(unscheduled []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := map[string]struct{}{}
	for id, state := range s.current {
		if !state.Running {
			ids[id] = struct{}{}
		}
	}

	for _, id := range unscheduled {
		if _, tracked := s.current[id]; tracked {
			ids[id] = struct{}{}
		}
	}

	return len(ids)
}

func (s *JobScheduler) IsCurrentJob(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.current[jobID]
	return ok
}

//
// The informer handlers touch s.current only through the accessors below,
// which keep the critical section down to a single map operation. This is
// what keeps the lock off the paths that talk to the Kubernetes API -
// listing pods and deleting jobs. Holding it across an API call would
// stall every other caller that needs it, including HasSpace() and
// Create() on the controller's own goroutine.
//
// Create() takes its slot through reserve() before it creates the job, so
// no path holds the lock while waiting on the API server.
//

// Takes a slot for a job we are about to create.
// Returns false if the job is already being tracked, and
// ErrParallelJobsLimitReached if there is no room for it.
func (s *JobScheduler) reserve(jobID, agentType string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.current[jobID]; ok {
		return false, nil
	}

	if len(s.current) >= s.config.MaxParallelJobs {
		return false, ErrParallelJobsLimitReached
	}

	s.current[jobID] = &JobState{
		ID:        jobID,
		AgentType: agentType,
		Running:   false,
	}

	return true, nil
}

// Starts tracking a job the informer told us about.
// If we were already holding a slot for it, the slot is confirmed instead.
// Returns true if the job was added.
func (s *JobScheduler) track(jobID, agentType string, running bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if state, ok := s.current[jobID]; ok {
		state.Confirmed = true
		return false
	}

	s.current[jobID] = &JobState{
		ID:        jobID,
		AgentType: agentType,
		Running:   running,
		Confirmed: true,
	}

	return true
}

// Records that the informer has seen a job we are tracking.
// Returns false if we are not tracking it.
func (s *JobScheduler) confirm(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.current[jobID]
	if !ok {
		return false
	}

	state.Confirmed = true
	return true
}

// Gives back a slot taken by reserve(), unless the informer
// has meanwhile told us that the job exists.
func (s *JobScheduler) releaseReservation(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if state, ok := s.current[jobID]; ok && !state.Confirmed {
		delete(s.current, jobID)
	}
}

// Known limitation, for the two functions below.
//
// They find the job by its Semaphore job ID, which does not tell two
// incarnations of the same job apart. A job we delete goes back to the queue
// and is created again under the same ID, so an event left over from the old
// one can stop tracking its replacement, or mark the replacement as running
// and let it skip the start timeout. The accounting is wrong from then until
// the job ends, which ActiveDeadlineSeconds bounds to a day.
//
// Deleting the wrong job is already prevented - deletions are pinned to the
// UID we saw, see delete(). Closing the rest means carrying that UID in
// JobState and matching on it here, which also needs Create() to record the
// UID it gets back from the API server.
func (s *JobScheduler) untrack(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.current, jobID)
}

// Marks a tracked job as running.
// Returns false if the job is not being tracked anymore.
func (s *JobScheduler) markRunning(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.current[jobID]
	if !ok {
		return false
	}

	state.Running = true
	return true
}

func (s *JobScheduler) isTrackedAsRunning(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.current[jobID]
	return ok && state.Running
}

// The informer hands us the object for adds and updates, but for deletes it
// can hand us a tombstone instead: when its watch drops and the relist finds
// the job already gone, there is no final state to report, so it reports a
// cache.DeletedFinalStateUnknown wrapping the last known object. Asserting
// the type without checking kills the process.
func jobFrom(obj interface{}) (*batchv1.Job, bool) {
	if job, ok := obj.(*batchv1.Job); ok {
		return job, true
	}

	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		klog.Warningf("Expected a job, got %T", obj)
		return nil, false
	}

	job, ok := tombstone.Obj.(*batchv1.Job)
	if !ok {
		klog.Warningf("Expected a job in tombstone, got %T", tombstone.Obj)
		return nil, false
	}

	return job, true
}

// The job's completion time is set by the job controller when the job
// finishes, but it is a pointer and we should not count on it being there.
// Falling back to the creation timestamp would delete a job that ran for
// longer than the retention period as soon as it finished, so fall back to
// when it was marked complete, and treat it as just finished otherwise.
func completedAt(job *batchv1.Job) time.Time {
	if job.Status.CompletionTime != nil {
		return job.Status.CompletionTime.Time
	}

	for _, cond := range job.Status.Conditions {
		if cond.Type != batchv1.JobComplete || cond.Status != corev1.ConditionTrue {
			continue
		}

		if !cond.LastTransitionTime.IsZero() {
			return cond.LastTransitionTime.Time
		}
	}

	return time.Now()
}

// The job's start time is only set once the job controller
// starts creating pods for it, so it can be unset.
func runningFor(job *batchv1.Job) time.Duration {
	if job.Status.StartTime == nil {
		return 0
	}

	return time.Since(job.Status.StartTime.Time)
}

func getFailedMessage(job *batchv1.Job) string {
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed {
			return cond.Message
		}
	}

	return ""
}

func getFailedReason(job *batchv1.Job) string {
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed {
			return cond.Reason
		}
	}

	return ""
}

func jobConditions(job *batchv1.Job) []batchv1.JobConditionType {
	jobConditions := []batchv1.JobConditionType{}
	for _, cond := range job.Status.Conditions {
		if cond.Status != corev1.ConditionTrue {
			continue
		}

		jobConditions = append(jobConditions, cond.Type)
	}

	return jobConditions
}
