package controller

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/renderedtext/agent-k8s-stack/pkg/agenttypes"
	"github.com/renderedtext/agent-k8s-stack/pkg/config"
	"github.com/renderedtext/agent-k8s-stack/pkg/podspec"
	"github.com/renderedtext/agent-k8s-stack/pkg/semaphore"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/version"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

func Test__JobScheduler(t *testing.T) {
	agentType := agenttypes.AgentType{
		AgentTypeName:          "s1-test",
		RegistrationToken:      "very-sensitive-token",
		AgentStartupParameters: []string{},
	}

	maxParallelJobs := 5
	clientset := newFakeClientset(t, []runtime.Object{})
	scheduler, err := NewJobScheduler(clientset, &config.Config{
		Namespace:              "default",
		AgentImage:             "semaphoreci/agent:latest",
		AgentStartupParameters: []string{},
		Labels:                 []string{},
		MaxParallelJobs:        maxParallelJobs,
		JobStartTimeout:        time.Minute,
	})

	require.NoError(t, err)

	t.Run("non-running job is loaded on startup", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		jobID := randJobID()
		require.False(t, scheduler.IsCurrentJob(jobID))

		j := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					config.JobIDLabel:     jobID,
					config.AgentTypeLabel: agentType.AgentTypeName,
				},
			},
		}

		scheduler.OnAdd(j, false)
		require.True(t, scheduler.IsCurrentJob(jobID))
	})

	t.Run("running job is loaded on startup", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		jobID := randJobID()
		require.False(t, scheduler.IsCurrentJob(jobID))

		ready := int32(1)
		j := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					config.JobIDLabel:     jobID,
					config.AgentTypeLabel: agentType.AgentTypeName,
				},
			},
			Status: batchv1.JobStatus{
				Ready: &ready,
			},
		}

		scheduler.OnAdd(j, false)
		require.True(t, scheduler.IsCurrentJob(jobID))
		require.True(t, scheduler.current[jobID].Running)
	})

	t.Run("job is created", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		jobID := randJobID()
		jobDoesNotExist(t, scheduler, clientset, jobID)
		req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
		err := scheduler.Create(context.Background(), req, &agentType)
		require.NoError(t, err)
		jobExists(t, scheduler, clientset, jobID)
		require.True(t, scheduler.IsCurrentJob(jobID))

		// Job creation is idempotent
		require.NoError(t, scheduler.Create(context.Background(), req, &agentType))
	})

	t.Run("job is marked as started", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		// job is created
		jobID := randJobID()
		jobDoesNotExist(t, scheduler, clientset, jobID)
		req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
		err := scheduler.Create(context.Background(), req, &agentType)
		require.NoError(t, err)
		j := jobExists(t, scheduler, clientset, jobID)
		require.True(t, scheduler.IsCurrentJob(jobID))

		// job starts
		ready := int32(1)
		j2 := j.DeepCopy()
		j2.Status.Ready = &ready
		j2.Status.StartTime = &metav1.Time{Time: time.Now()}
		scheduler.OnUpdate(j, j2)
		require.True(t, scheduler.current[jobID].Running)
	})

	t.Run("job is deleted if it doesn't start in time", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		scheduler.config.KeepFailedJobsFor = time.Minute
		defer func() {
			scheduler.config.KeepFailedJobsFor = 0
		}()

		// job is created
		jobID := randJobID()
		req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
		require.NoError(t, scheduler.Create(context.Background(), req, &agentType))
		j := jobExists(t, scheduler, clientset, jobID)
		require.True(t, scheduler.IsCurrentJob(jobID))

		// job does not start in time and is deleted
		j2 := j.DeepCopy()
		twoMinutesAgo := time.Now().Add(-2 * time.Minute)
		j2.CreationTimestamp = metav1.Time{Time: twoMinutesAgo}
		scheduler.OnUpdate(j, j2)
		jobDoesNotExist(t, scheduler, clientset, jobID)
	})

	t.Run("job that starts after not starting in time is ignored", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		// job is created
		jobID := randJobID()
		req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
		require.NoError(t, scheduler.Create(context.Background(), req, &agentType))
		j := jobExists(t, scheduler, clientset, jobID)
		require.True(t, scheduler.IsCurrentJob(jobID))

		// job does not start in time, so we stop tracking it and delete it
		j2 := j.DeepCopy()
		j2.CreationTimestamp = metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
		scheduler.OnUpdate(j, j2)
		jobDoesNotExist(t, scheduler, clientset, jobID)

		// an update saying the job started arrives after that,
		// because the deletion wasn't observed by the informer yet
		ready := int32(1)
		j3 := j2.DeepCopy()
		j3.Status.Ready = &ready
		j3.Status.StartTime = &metav1.Time{Time: time.Now()}
		require.NotPanics(t, func() { scheduler.OnUpdate(j2, j3) })
		require.False(t, scheduler.IsCurrentJob(jobID))
	})

	t.Run("running job without a start time is marked as started", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		// job is created and is still tracked
		jobID := randJobID()
		req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
		require.NoError(t, scheduler.Create(context.Background(), req, &agentType))
		j := jobExists(t, scheduler, clientset, jobID)
		require.True(t, scheduler.IsCurrentJob(jobID))

		// the job reports ready pods, but no start time yet
		ready := int32(1)
		j2 := j.DeepCopy()
		j2.Status.Ready = &ready
		require.Nil(t, j2.Status.StartTime)

		require.NotPanics(t, func() { scheduler.OnUpdate(j, j2) })
		require.True(t, scheduler.current[jobID].Running)
	})

	t.Run("job is not created if limit was reached", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		// create jobs up to max
		require.True(t, scheduler.HasSpace())
		for i := 0; i < maxParallelJobs; i++ {
			jobID := randJobID()
			req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
			require.NoError(t, scheduler.Create(context.Background(), req, &agentType))
			_ = jobExists(t, scheduler, clientset, jobID)
			require.True(t, scheduler.IsCurrentJob(jobID))
		}

		// no more space available
		require.False(t, scheduler.HasSpace())

		// creating a job returns an error now
		jobID := randJobID()
		req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
		err := scheduler.Create(context.Background(), req, &agentType)
		require.ErrorIs(t, err, ErrParallelJobsLimitReached)
		jobDoesNotExist(t, scheduler, clientset, jobID)
	})

	t.Run("no retention used -> job is deleted", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		// job is created
		jobID := randJobID()
		req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
		require.NoError(t, scheduler.Create(context.Background(), req, &agentType))
		j := jobExists(t, scheduler, clientset, jobID)
		require.True(t, scheduler.IsCurrentJob(jobID))

		scheduler.OnDelete(j)
		require.False(t, scheduler.IsCurrentJob(jobID))
	})

	t.Run("retention for successful job is used", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		scheduler.config.KeepSuccessfulJobsFor = time.Minute
		defer func() {
			scheduler.config.KeepSuccessfulJobsFor = 0
		}()

		// job is created
		jobID := randJobID()
		req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
		require.NoError(t, scheduler.Create(context.Background(), req, &agentType))
		j := jobExists(t, scheduler, clientset, jobID)
		require.True(t, scheduler.IsCurrentJob(jobID))

		// job finishes successfully, but is not deleted
		j2 := j.DeepCopy()
		thirtySecondsAgo := time.Now().Add(-30 * time.Second)
		j2.Status.Conditions = append(j2.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobComplete, Status: v1.ConditionTrue})
		j2.Status.CompletionTime = &metav1.Time{Time: thirtySecondsAgo}
		scheduler.OnUpdate(j, j2)
		_ = jobExists(t, scheduler, clientset, jobID)

		// after some time, it is deleted
		j3 := j2.DeepCopy()
		twoMinutesAgo := time.Now().Add(-2 * time.Minute)
		j3.Status.CompletionTime = &metav1.Time{Time: twoMinutesAgo}
		scheduler.OnUpdate(j2, j3)
		jobDoesNotExist(t, scheduler, clientset, jobID)
	})

	t.Run("retention for failed job is used", func(t *testing.T) {
		clear(scheduler.current)
		defer clear(scheduler.current)

		scheduler.config.KeepFailedJobsFor = time.Minute
		defer func() {
			scheduler.config.KeepFailedJobsFor = 0
		}()

		// job is created
		jobID := randJobID()
		req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
		require.NoError(t, scheduler.Create(context.Background(), req, &agentType))
		j := jobExists(t, scheduler, clientset, jobID)
		require.True(t, scheduler.IsCurrentJob(jobID))

		// job finishes successfully, but is not deleted
		j2 := j.DeepCopy()
		thirtySecondsAgo := time.Now().Add(-30 * time.Second)
		j2.Status.Conditions = append(j2.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobFailed, Status: v1.ConditionTrue})
		j2.CreationTimestamp = metav1.Time{Time: thirtySecondsAgo}
		scheduler.OnUpdate(j, j2)
		_ = jobExists(t, scheduler, clientset, jobID)

		// after some time, it is deleted
		j3 := j2.DeepCopy()
		twoMinutesAgo := time.Now().Add(-2 * time.Minute)
		j3.CreationTimestamp = metav1.Time{Time: twoMinutesAgo}
		scheduler.OnUpdate(j2, j3)
		jobDoesNotExist(t, scheduler, clientset, jobID)
	})
}

func Test__JobSchedulerIsSafeForConcurrentUse(t *testing.T) {
	agentType := agenttypes.AgentType{
		AgentTypeName:          "s1-test",
		RegistrationToken:      "very-sensitive-token",
		AgentStartupParameters: []string{},
	}

	clientset := newFakeClientset(t, []runtime.Object{})
	scheduler, err := NewJobScheduler(clientset, &config.Config{
		Namespace:              "default",
		AgentImage:             "semaphoreci/agent:latest",
		AgentStartupParameters: []string{},
		Labels:                 []string{},
		MaxParallelJobs:        200,
		JobStartTimeout:        time.Minute,
	})

	require.NoError(t, err)

	//
	// The informer handlers and the controller's tick run on different
	// goroutines and both touch the scheduler's job map. Run them against
	// each other so that '-race' can catch an unguarded access.
	//
	jobIDs := make([]string, 50)
	for i := range jobIDs {
		jobIDs[i] = randJobID()
	}

	ready := int32(1)
	var wg sync.WaitGroup

	for _, jobID := range jobIDs {
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: scheduler.jobName(jobID),
				Labels: map[string]string{
					config.JobIDLabel:     jobID,
					config.AgentTypeLabel: agentType.AgentTypeName,
				},
				CreationTimestamp: metav1.Time{Time: time.Now()},
			},
			Status: batchv1.JobStatus{
				Ready:     &ready,
				StartTime: &metav1.Time{Time: time.Now()},
			},
		}

		wg.Add(4)

		// the controller's tick
		go func(jobID string) {
			defer wg.Done()
			req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
			_ = scheduler.Create(context.Background(), req, &agentType)
			scheduler.HasSpace()
			scheduler.IsCurrentJob(jobID)
		}(jobID)

		// the informer handlers
		go func(job *batchv1.Job) { defer wg.Done(); scheduler.OnAdd(job, false) }(job)
		go func(job *batchv1.Job) { defer wg.Done(); scheduler.OnUpdate(job, job) }(job)
		go func(job *batchv1.Job) { defer wg.Done(); scheduler.OnDelete(job) }(job)
	}

	wg.Wait()
}

func newTestScheduler(t *testing.T) (*JobScheduler, *fake.Clientset) {
	clientset := fake.NewSimpleClientset()
	fakeDiscovery, ok := clientset.Discovery().(*fakediscovery.FakeDiscovery)
	require.True(t, ok)
	fakeDiscovery.FakedServerVersion = &version.Info{GitVersion: "v1.27.1"}

	scheduler, err := NewJobScheduler(clientset, &config.Config{
		Namespace:              "default",
		AgentImage:             "semaphoreci/agent:latest",
		AgentStartupParameters: []string{},
		Labels:                 []string{},
		MaxParallelJobs:        5,
		JobStartTimeout:        time.Minute,
	})

	require.NoError(t, err)
	return scheduler, clientset
}

func testJob(scheduler *JobScheduler, jobID string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: scheduler.jobName(jobID),
			Labels: map[string]string{
				config.JobIDLabel:     jobID,
				config.AgentTypeLabel: "s1-test",
			},
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
	}
}

// The informer reports a tombstone instead of the object when its watch drops
// and the relist finds the job already gone.
func Test__OnDeleteHandlesTombstone(t *testing.T) {
	scheduler, _ := newTestScheduler(t)

	jobID := randJobID()
	job := testJob(scheduler, jobID)
	scheduler.OnAdd(job, false)
	require.True(t, scheduler.IsCurrentJob(jobID))

	tombstone := cache.DeletedFinalStateUnknown{
		Key: fmt.Sprintf("default/%s", job.Name),
		Obj: job,
	}

	require.NotPanics(t, func() { scheduler.OnDelete(tombstone) })
	require.False(t, scheduler.IsCurrentJob(jobID))

	// an unexpected type is ignored rather than fatal
	require.NotPanics(t, func() { scheduler.OnDelete(cache.DeletedFinalStateUnknown{Key: "x", Obj: "not a job"}) })
	require.NotPanics(t, func() { scheduler.OnDelete("not a job") })
}

func Test__CompletedJobWithoutCompletionTime(t *testing.T) {
	scheduler, clientset := newTestScheduler(t)
	scheduler.config.KeepSuccessfulJobsFor = time.Minute

	jobID := randJobID()
	job := testJob(scheduler, jobID)
	scheduler.OnAdd(job, false)

	_, err := clientset.BatchV1().Jobs("default").Create(context.Background(), job, metav1.CreateOptions{})
	require.NoError(t, err)

	// the job is complete, but carries no completion time
	finished := job.DeepCopy()
	finished.Status.Conditions = append(finished.Status.Conditions, batchv1.JobCondition{
		Type:   batchv1.JobComplete,
		Status: v1.ConditionTrue,
	})

	require.Nil(t, finished.Status.CompletionTime)
	require.NotPanics(t, func() { scheduler.OnUpdate(job, finished) })
	require.False(t, scheduler.IsCurrentJob(jobID))
}

// Create() must not hold the scheduler lock while the API server is answering.
func Test__CreateDoesNotBlockTheSchedulerOnSlowAPI(t *testing.T) {
	scheduler, clientset := newTestScheduler(t)

	release := make(chan struct{})
	inFlight := make(chan struct{})
	clientset.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		close(inFlight)
		<-release
		return false, nil, nil
	})

	req := semaphore.JobRequest{JobID: randJobID(), MachineType: "s1-test"}
	created := make(chan struct{})
	go func() {
		defer close(created)
		_ = scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"})
	}()

	// wait until the request is actually in flight, otherwise we could
	// check the scheduler before Create() even reaches the API call
	<-inFlight

	// the create is in flight; the scheduler must still answer
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		scheduler.HasSpace()
		scheduler.IsCurrentJob(req.JobID)
	}()

	select {
	case <-answered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("scheduler blocked while a job creation was in flight")
	}

	close(release)
	<-created
}

// A job we already decided to cancel must not keep a slot if its deletion failed.
func Test__UntrackedRunningJobIsDeletedAgain(t *testing.T) {
	scheduler, clientset := newTestScheduler(t)

	jobID := randJobID()
	req := semaphore.JobRequest{JobID: jobID, MachineType: "s1-test"}
	require.NoError(t, scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"}))
	job := jobExists(t, scheduler, clientset, jobID)

	// the first deletion fails
	var deletes int
	clientset.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		deletes++
		if deletes == 1 {
			return true, nil, fmt.Errorf("too many requests")
		}

		return false, nil, nil
	})

	// the job does not start in time, so we stop tracking it and try to delete it
	timedOut := job.DeepCopy()
	timedOut.CreationTimestamp = metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
	scheduler.OnUpdate(job, timedOut)
	require.Equal(t, 1, deletes)
	require.False(t, scheduler.IsCurrentJob(jobID))

	// the pod becomes ready anyway - the job is still there, so delete it again
	ready := int32(1)
	running := timedOut.DeepCopy()
	running.Status.Ready = &ready
	running.Status.StartTime = &metav1.Time{Time: time.Now()}
	scheduler.OnUpdate(timedOut, running)

	require.Equal(t, 2, deletes)
	jobDoesNotExist(t, scheduler, clientset, jobID)
}

// Kubernetes can persist the job and still fail the request on the way back.
// The slot must survive that, otherwise the job runs uncounted and the
// untracked-job path deletes it.
func Test__CreateKeepsSlotWhenTheJobWasPersisted(t *testing.T) {
	scheduler, clientset := newTestScheduler(t)

	jobID := randJobID()
	clientset.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job)

		// the job exists and the informer tells us about it
		scheduler.OnAdd(job, false)

		// but the response never makes it back to us
		return true, nil, context.DeadlineExceeded
	})

	req := semaphore.JobRequest{JobID: jobID, MachineType: "s1-test"}
	err := scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"})

	require.Error(t, err)
	require.True(t, scheduler.IsCurrentJob(jobID), "slot was released for a job that exists")

	// a create that genuinely failed still gives the slot back
	other := randJobID()
	clientset.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("nope")
	})

	require.Error(t, scheduler.Create(context.Background(),
		semaphore.JobRequest{JobID: other, MachineType: "s1-test"},
		&agenttypes.AgentType{AgentTypeName: "s1-test"}))
	require.False(t, scheduler.IsCurrentJob(other))
}

// A job that ran for longer than the retention period must still be kept
// for that period after it finishes, even without a completion time.
func Test__RetentionWithoutCompletionTime(t *testing.T) {
	scheduler, clientset := newTestScheduler(t)
	scheduler.config.KeepSuccessfulJobsFor = time.Hour

	jobID := randJobID()
	req := semaphore.JobRequest{JobID: jobID, MachineType: "s1-test"}
	require.NoError(t, scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"}))
	job := jobExists(t, scheduler, clientset, jobID)

	// the job was created two hours ago and has just finished,
	// but carries no completion time
	finished := job.DeepCopy()
	finished.CreationTimestamp = metav1.Time{Time: time.Now().Add(-2 * time.Hour)}
	finished.Status.Conditions = append(finished.Status.Conditions, batchv1.JobCondition{
		Type:   batchv1.JobComplete,
		Status: v1.ConditionTrue,
	})

	require.Nil(t, finished.Status.CompletionTime)
	scheduler.OnUpdate(job, finished)

	// retention has not been reached, so it is kept
	_ = jobExists(t, scheduler, clientset, jobID)
}

// Job names come from the Semaphore job ID, so a deletion decided for one job
// must not be able to remove a replacement carrying the same name.
func Test__DeleteIsPinnedToTheJobWeSaw(t *testing.T) {
	scheduler, clientset := newTestScheduler(t)

	jobID := randJobID()
	req := semaphore.JobRequest{JobID: jobID, MachineType: "s1-test"}
	require.NoError(t, scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"}))
	job := jobExists(t, scheduler, clientset, jobID)
	job.UID = "the-job-we-saw"

	var preconditions []*metav1.Preconditions
	clientset.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		opts := action.(k8stesting.DeleteActionImpl).DeleteOptions
		preconditions = append(preconditions, opts.Preconditions)
		return false, nil, nil
	})

	// the job does not start in time and is deleted
	timedOut := job.DeepCopy()
	timedOut.CreationTimestamp = metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
	scheduler.OnUpdate(job, timedOut)

	require.Len(t, preconditions, 1)
	require.NotNil(t, preconditions[0], "delete was not pinned to a job")
	require.NotNil(t, preconditions[0].UID)
	require.Equal(t, job.UID, *preconditions[0].UID)

	//
	// A job without a UID must not be pinned: an empty UID matches no
	// object, so it would stop the job from ever being deleted.
	//
	withoutUID := job.DeepCopy()
	withoutUID.UID = ""

	// the job is already gone by now, we only care about what was sent
	_ = scheduler.delete(withoutUID)

	require.Len(t, preconditions, 2)
	require.Nil(t, preconditions[1], "delete was pinned to an empty UID")
}

func Test__AgentPodScheduling(t *testing.T) {
	controllerDefault := &podspec.Spec{
		NodeSelector: map[string]string{"pool": "agents"},
		Tolerations:  []v1.Toleration{{Key: "dedicated", Operator: v1.TolerationOpEqual, Value: "agents"}},
	}

	agentTypeSpec := &podspec.Spec{
		NodeSelector:      map[string]string{"pool": "other"},
		PriorityClassName: "semaphore-agents",
	}

	createPod := func(t *testing.T, defaultSpec *podspec.Spec, agentType *agenttypes.AgentType) v1.PodSpec {
		scheduler, clientset := newTestScheduler(t)
		scheduler.config.ServiceAccountName = "semaphore-agent"
		scheduler.config.AgentPodSpec = defaultSpec

		jobID := randJobID()
		req := semaphore.JobRequest{JobID: jobID, MachineType: agentType.AgentTypeName}
		require.NoError(t, scheduler.Create(context.Background(), req, agentType))
		return jobExists(t, scheduler, clientset, jobID).Spec.Template.Spec
	}

	requireControllerOwnedFields := func(t *testing.T, pod v1.PodSpec) {
		require.Equal(t, v1.RestartPolicyNever, pod.RestartPolicy)
		require.Equal(t, "semaphore-agent", pod.ServiceAccountName)
		require.Equal(t, int64(300), *pod.TerminationGracePeriodSeconds)
		require.Len(t, pod.Containers, 1)
		require.Equal(t, "semaphore-agent", pod.Containers[0].Name)
		require.Equal(t, "semaphoreci/agent:latest", pod.Containers[0].Image)
		require.Equal(t, []string{"/opt/semaphore/agent", "start"}, pod.Containers[0].Command)
	}

	t.Run("nothing configured -> no scheduling fields", func(t *testing.T) {
		pod := createPod(t, nil, &agenttypes.AgentType{AgentTypeName: "s1-test"})
		requireControllerOwnedFields(t, pod)
		require.Nil(t, pod.NodeSelector)
		require.Nil(t, pod.Affinity)
		require.Nil(t, pod.Tolerations)
		require.Nil(t, pod.TopologySpreadConstraints)
		require.Empty(t, pod.PriorityClassName)
	})

	t.Run("controller default -> applied", func(t *testing.T) {
		pod := createPod(t, controllerDefault, &agenttypes.AgentType{AgentTypeName: "s1-test"})
		requireControllerOwnedFields(t, pod)
		require.Equal(t, controllerDefault.NodeSelector, pod.NodeSelector)
		require.Equal(t, controllerDefault.Tolerations, pod.Tolerations)
	})

	t.Run("agent type spec -> replaces the controller default", func(t *testing.T) {
		pod := createPod(t, controllerDefault, &agenttypes.AgentType{AgentTypeName: "s1-test", AgentPodSpec: agentTypeSpec})
		requireControllerOwnedFields(t, pod)
		require.Equal(t, agentTypeSpec.NodeSelector, pod.NodeSelector)
		require.Equal(t, "semaphore-agents", pod.PriorityClassName)

		// nothing is merged in from the default
		require.Nil(t, pod.Tolerations)
	})

	t.Run("agent type spec without default -> applied", func(t *testing.T) {
		pod := createPod(t, nil, &agenttypes.AgentType{AgentTypeName: "s1-test", AgentPodSpec: agentTypeSpec})
		requireControllerOwnedFields(t, pod)
		require.Equal(t, agentTypeSpec.NodeSelector, pod.NodeSelector)
	})

	t.Run("empty agent type spec -> opts the agent type out of the default", func(t *testing.T) {
		pod := createPod(t, controllerDefault, &agenttypes.AgentType{AgentTypeName: "s1-test", AgentPodSpec: &podspec.Spec{}})
		requireControllerOwnedFields(t, pod)
		require.Nil(t, pod.NodeSelector)
		require.Nil(t, pod.Tolerations)
	})
}

// A job that did not start in time is logged with what kept its agent pod from running.
func Test__NotStartedReason(t *testing.T) {
	podFor := func(job *batchv1.Job, uid string, status v1.PodStatus) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      job.Name + "-" + uid,
				Namespace: "default",
				Labels:    map[string]string{"job-name": job.Name, "controller-uid": uid},
			},
			Status: status,
		}
	}

	notScheduled := func(message string) v1.PodStatus {
		return v1.PodStatus{
			Phase:      v1.PodPending,
			Conditions: []v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse, Reason: "Unschedulable", Message: message}},
		}
	}

	t.Run("no pod", func(t *testing.T) {
		scheduler, _ := newTestScheduler(t)
		job := testJob(scheduler, randJobID())
		job.UID = "current"
		require.Equal(t, "no pod was created - see the job's events", scheduler.notStartedReason(job))
	})

	t.Run("pod of an earlier job with the same name is not reported", func(t *testing.T) {
		scheduler, clientset := newTestScheduler(t)
		job := testJob(scheduler, randJobID())
		job.UID = "current"

		stale := podFor(job, "earlier", notScheduled("stale pod"))
		stale.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		_, err := clientset.CoreV1().Pods("default").Create(context.Background(), stale, metav1.CreateOptions{})
		require.NoError(t, err)
		require.Equal(t, "no pod was created - see the job's events", scheduler.notStartedReason(job))

		current := podFor(job, "current", notScheduled("current pod"))
		_, err = clientset.CoreV1().Pods("default").Create(context.Background(), current, metav1.CreateOptions{})
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("pod %s was not scheduled: current pod", current.Name), scheduler.notStartedReason(job))
	})

	t.Run("pod not scheduled", func(t *testing.T) {
		scheduler, clientset := newTestScheduler(t)
		job := testJob(scheduler, randJobID())
		job.UID = "current"
		pod := podFor(job, "current", v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{{
				Type:    v1.PodScheduled,
				Status:  v1.ConditionFalse,
				Reason:  "Unschedulable",
				Message: "0/3 nodes are available: 3 node(s) didn't match Pod's node affinity/selector.",
			}},
		})

		_, err := clientset.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{})
		require.NoError(t, err)
		require.Equal(t,
			fmt.Sprintf("pod %s was not scheduled: 0/3 nodes are available: 3 node(s) didn't match Pod's node affinity/selector.", pod.Name),
			scheduler.notStartedReason(job))
	})

	t.Run("container waiting", func(t *testing.T) {
		scheduler, clientset := newTestScheduler(t)
		job := testJob(scheduler, randJobID())
		job.UID = "current"
		pod := podFor(job, "current", v1.PodStatus{
			Phase:      v1.PodPending,
			Conditions: []v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionTrue}},
			ContainerStatuses: []v1.ContainerStatus{{
				Name: "semaphore-agent",
				State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{
					Reason:  "ImagePullBackOff",
					Message: "Back-off pulling image",
				}},
			}},
		})

		_, err := clientset.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{})
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("pod %s is waiting: ImagePullBackOff Back-off pulling image", pod.Name), scheduler.notStartedReason(job))
	})
}

// The reason must reach the log line, not only be computed.
func Test__StartTimeoutLogsTheReason(t *testing.T) {
	var out bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(&out)
	defer func() {
		klog.SetOutput(os.Stderr)
		klog.LogToStderr(true)
	}()

	scheduler, clientset := newTestScheduler(t)
	jobID := randJobID()
	req := semaphore.JobRequest{JobID: jobID, MachineType: "s1-test"}
	require.NoError(t, scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"}))
	job := jobExists(t, scheduler, clientset, jobID)

	timedOut := job.DeepCopy()
	timedOut.CreationTimestamp = metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
	scheduler.OnUpdate(job, timedOut)
	klog.Flush()

	require.Contains(t, out.String(), "job did not start in time")
	require.Contains(t, out.String(), `reason="no pod was created - see the job's events"`)
}

func jobExists(t *testing.T, scheduler *JobScheduler, clientset kubernetes.Interface, jobID string) *batchv1.Job {
	j, err := clientset.BatchV1().Jobs("default").Get(context.Background(), scheduler.jobName(jobID), metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, j)
	return j
}

func jobDoesNotExist(t *testing.T, scheduler *JobScheduler, clientset kubernetes.Interface, jobID string) {
	_, err := clientset.BatchV1().Jobs("default").Get(context.Background(), scheduler.jobName(jobID), metav1.GetOptions{})
	require.Error(t, err)
	require.True(t, errors.IsNotFound(err))
	require.False(t, scheduler.IsCurrentJob(jobID))
}

func newFakeClientset(t *testing.T, objects []runtime.Object) kubernetes.Interface {
	fakeClientset := fake.NewSimpleClientset(objects...)
	fakeDiscovery, ok := fakeClientset.Discovery().(*fakediscovery.FakeDiscovery)
	require.True(t, ok)
	fakeDiscovery.FakedServerVersion = &version.Info{GitVersion: "v1.27.1"}
	return fakeClientset
}

func randJobID() string {
	return fmt.Sprintf("job-%d", rand.Int())
}

// With a TTL, Kubernetes deletes finished jobs: the controller frees the slot
// but sends no DELETE. Without one, it deletes them as before.
func Test__JobTTLAfterFinished(t *testing.T) {
	finish := func(job *batchv1.Job, condition batchv1.JobConditionType) *batchv1.Job {
		finished := job.DeepCopy()
		finished.Status.Conditions = append(finished.Status.Conditions, batchv1.JobCondition{
			Type:   condition,
			Status: v1.ConditionTrue,
		})
		return finished
	}

	countDeletes := func(clientset *fake.Clientset) int {
		n := 0
		for _, action := range clientset.Actions() {
			if action.Matches("delete", "jobs") {
				n++
			}
		}
		return n
	}

	for _, condition := range []batchv1.JobConditionType{batchv1.JobComplete, batchv1.JobFailed} {
		t.Run(fmt.Sprintf("set, %s -> slot freed, no delete", condition), func(t *testing.T) {
			scheduler, clientset := newTestScheduler(t)
			scheduler.config.JobTTLAfterFinished = 60

			jobID := randJobID()
			req := semaphore.JobRequest{JobID: jobID, MachineType: "s1-test"}
			require.NoError(t, scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"}))
			job := jobExists(t, scheduler, clientset, jobID)
			require.NotNil(t, job.Spec.TTLSecondsAfterFinished)
			require.Equal(t, int32(60), *job.Spec.TTLSecondsAfterFinished)

			scheduler.OnAdd(job, false)
			scheduler.OnUpdate(job, finish(job, condition))

			require.False(t, scheduler.IsCurrentJob(jobID))
			require.Equal(t, 0, countDeletes(clientset))
			_ = jobExists(t, scheduler, clientset, jobID)

			// Kubernetes deleting it later is just another delete event
			scheduler.OnDelete(job)
			require.False(t, scheduler.IsCurrentJob(jobID))
		})

		t.Run(fmt.Sprintf("unset, %s -> controller deletes", condition), func(t *testing.T) {
			scheduler, clientset := newTestScheduler(t)

			jobID := randJobID()
			req := semaphore.JobRequest{JobID: jobID, MachineType: "s1-test"}
			require.NoError(t, scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"}))
			job := jobExists(t, scheduler, clientset, jobID)
			require.Nil(t, job.Spec.TTLSecondsAfterFinished)

			scheduler.OnAdd(job, false)
			scheduler.OnUpdate(job, finish(job, condition))

			require.Equal(t, 1, countDeletes(clientset))
			jobDoesNotExist(t, scheduler, clientset, jobID)
		})
	}

	// Jobs created before the TTL was turned on carry none, so nothing else would delete them.
	t.Run("set, job without a TTL -> controller deletes", func(t *testing.T) {
		scheduler, clientset := newTestScheduler(t)

		jobID := randJobID()
		req := semaphore.JobRequest{JobID: jobID, MachineType: "s1-test"}
		require.NoError(t, scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"}))
		job := jobExists(t, scheduler, clientset, jobID)

		scheduler.config.JobTTLAfterFinished = 60
		scheduler.OnAdd(job, false)
		scheduler.OnUpdate(job, finish(job, batchv1.JobComplete))

		require.Equal(t, 1, countDeletes(clientset))
		jobDoesNotExist(t, scheduler, clientset, jobID)
	})

	// The TTL only applies to finished jobs, so a job that does not start is still ours to delete.
	t.Run("set, start timeout -> controller deletes", func(t *testing.T) {
		scheduler, clientset := newTestScheduler(t)
		scheduler.config.JobTTLAfterFinished = 60

		jobID := randJobID()
		req := semaphore.JobRequest{JobID: jobID, MachineType: "s1-test"}
		require.NoError(t, scheduler.Create(context.Background(), req, &agenttypes.AgentType{AgentTypeName: "s1-test"}))
		job := jobExists(t, scheduler, clientset, jobID)

		timedOut := job.DeepCopy()
		timedOut.CreationTimestamp = metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
		scheduler.OnUpdate(job, timedOut)

		require.Equal(t, 1, countDeletes(clientset))
		jobDoesNotExist(t, scheduler, clientset, jobID)
	})
}
