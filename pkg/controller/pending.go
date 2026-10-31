package controller

import (
	"fmt"
	"sync"

	"github.com/renderedtext/agent-k8s-stack/pkg/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// PendingTracker records job pods that exist but have no node yet, by job ID.
// Only used when MAX_PENDING_JOBS is set.
type PendingTracker struct {
	mu          sync.Mutex
	unscheduled map[string]waitingPod
	synced      cache.InformerSynced
}

type waitingPod struct {
	namespace string
	name      string
	uid       types.UID
	jobID     string
}

func NewPendingTracker() *PendingTracker {
	return &PendingTracker{unscheduled: map[string]waitingPod{}}
}

// JobPodSelector matches the pods agents create for jobs, not the agents' own pods,
// which carry the resource type label.
func JobPodSelector() (string, error) {
	hasJobID, err := labels.NewRequirement(config.JobIDLabel, selection.Exists, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build job ID requirement: %w", err)
	}

	noResourceType, err := labels.NewRequirement(config.ResourceTypeLabel, selection.DoesNotExist, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build resource type requirement: %w", err)
	}

	return labels.NewSelector().Add(*hasJobID, *noResourceType).String(), nil
}

func (p *PendingTracker) RegisterInformer(informerFactory informers.SharedInformerFactory) error {
	informer := informerFactory.Core().V1().Pods().Informer()
	p.synced = informer.HasSynced
	_, err := informer.AddEventHandler(p)
	return err
}

func (p *PendingTracker) OnAdd(obj interface{}, _ bool) {
	p.observe(obj)
}

func (p *PendingTracker) OnUpdate(_, obj interface{}) {
	p.observe(obj)
}

func (p *PendingTracker) OnDelete(obj interface{}) {
	pod, ok := podFrom(obj)
	if !ok {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.unscheduled, podKey(pod))
}

// JobIDs returns the job IDs of job pods that are waiting for a node.
func (p *PendingTracker) JobIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	ids := make([]string, 0, len(p.unscheduled))
	for _, pod := range p.unscheduled {
		ids = append(ids, pod.jobID)
	}

	return ids
}

// Waiting returns the job pods that are waiting for a node.
func (p *PendingTracker) Waiting() []waitingPod {
	p.mu.Lock()
	defer p.mu.Unlock()

	pods := make([]waitingPod, 0, len(p.unscheduled))
	for _, pod := range p.unscheduled {
		pods = append(pods, pod)
	}

	return pods
}

// HasSynced tells whether the pod watch has listed the existing pods yet.
func (p *PendingTracker) HasSynced() bool {
	return p.synced != nil && p.synced()
}

func (p *PendingTracker) observe(obj interface{}) {
	pod, ok := podFrom(obj)
	if !ok {
		return
	}

	jobID, ok := pod.Labels[config.JobIDLabel]
	if !ok {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if isUnscheduled(pod) {
		p.unscheduled[podKey(pod)] = waitingPod{namespace: pod.Namespace, name: pod.Name, uid: pod.UID, jobID: jobID}
		return
	}

	delete(p.unscheduled, podKey(pod))
}

func isUnscheduled(pod *corev1.Pod) bool {
	if pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil {
		return false
	}

	return pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
}

func podKey(pod *corev1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}

// Deletes can arrive as tombstones when the watch drops; see jobFrom().
func podFrom(obj interface{}) (*corev1.Pod, bool) {
	if pod, ok := obj.(*corev1.Pod); ok {
		return pod, true
	}

	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}

	pod, ok := tombstone.Obj.(*corev1.Pod)
	return pod, ok
}
