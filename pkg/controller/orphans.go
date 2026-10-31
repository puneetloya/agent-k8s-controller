package controller

import (
	"context"
	"slices"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	"k8s.io/klog/v2"
)

// How long a job pod may wait for a node after its agent's job is gone or finished.
// Nothing will run it, but it still counts as pending and would take a node.
const orphanGrace = 2 * time.Minute

type orphanCleaner struct {
	tracker    *PendingTracker
	jobs       batchlisters.JobLister
	jobsSynced func() bool
	jobName    func(jobID string) string
	delete     func(ctx context.Context, pod waitingPod) error

	since    map[string]time.Time
	disabled bool
}

func (o *orphanCleaner) run(ctx context.Context, now time.Time) {
	if o.disabled || !o.tracker.HasSynced() || !o.jobsSynced() {
		return
	}

	waiting := map[string]bool{}
	for _, pod := range o.tracker.Waiting() {
		key := pod.namespace + "/" + pod.name
		waiting[key] = true

		if o.hasAgent(pod) {
			delete(o.since, key)
			continue
		}

		first, ok := o.since[key]
		if !ok {
			o.since[key] = now
			continue
		}

		if now.Sub(first) < orphanGrace {
			continue
		}

		err := o.delete(ctx, pod)
		switch {
		case err == nil, apierrors.IsNotFound(err), apierrors.IsConflict(err):
			klog.InfoS("Deleted job pod with no agent", "pod", pod.name, "job", pod.jobID, "waited", now.Sub(first))
			delete(o.since, key)
		case apierrors.IsForbidden(err):
			klog.Warningf("Not allowed to delete pods - job pods left without an agent are kept: %v", err)
			o.disabled = true
			return
		default:
			klog.ErrorS(err, "Error deleting job pod with no agent", "pod", pod.name, "job", pod.jobID)
		}
	}

	for key := range o.since {
		if !waiting[key] {
			delete(o.since, key)
		}
	}
}

// An agent is there while its job exists and has not finished.
func (o *orphanCleaner) hasAgent(pod waitingPod) bool {
	job, err := o.jobs.Jobs(pod.namespace).Get(o.jobName(pod.jobID))
	if err != nil {
		return !apierrors.IsNotFound(err)
	}

	conditions := jobConditions(job)
	return !slices.Contains(conditions, batchv1.JobComplete) && !slices.Contains(conditions, batchv1.JobFailed)
}

func deletePinned(c *Controller) func(ctx context.Context, pod waitingPod) error {
	return func(ctx context.Context, pod waitingPod) error {
		ctx, cancel := context.WithTimeout(ctx, apiRequestTimeout)
		defer cancel()

		options := metav1.DeleteOptions{}
		if pod.uid != "" {
			options.Preconditions = &metav1.Preconditions{UID: &pod.uid}
		}

		return c.clientset.CoreV1().Pods(pod.namespace).Delete(ctx, pod.name, options)
	}
}
