// loadsim drives the agent controller under load in a kind cluster with KWOK nodes.
// It plays Semaphore (job queue API), the agents, and the cluster autoscaler, and
// checks that the controller obeys its limits while it runs.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersbatch "k8s.io/client-go/listers/batch/v1"
	listerscore "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	jobIDLabel     = "semaphoreci.com/job-id"
	agentTypeLabel = "semaphoreci.com/agent-type"
	resourceLabel  = "semaphoreci.com/resource-type"
	roleLabel      = "sim/role"
	nodeCPU        = 60
)

// ---------- scenario ----------

type AgentType struct {
	Name  string
	Token string
	CPU   int
}

type Region struct {
	Name        string
	NS          string
	MaxParallel int
	MaxPending  int
	PoolMax     int
	PoolStart   int
}

type Event struct {
	At     time.Duration
	Kind   string // remove-nodes, restart, fault-on, fault-off
	Region string
	N      int
	Fault  Fault
}

type Fault struct {
	ErrorRate float64
	Latency   time.Duration
	Hang      bool
}

type Scenario struct {
	Name        string
	Regions     []Region
	Types       []AgentType
	Arrivals    time.Duration
	Drain       time.Duration
	Rate        func(t time.Duration) float64 // jobs per second, all types
	Burst       map[time.Duration]int         // jobs enqueued at once at offset
	TypeWeights []float64
	MinDur      time.Duration
	MaxDur      time.Duration
	PodTimeout  time.Duration
	OrphanProb  float64
	Events      []Event
}

func scenarios(scale int) map[string]Scenario {
	types := []AgentType{{"s1-sim-8cpu", "tok-8", 8}, {"s1-sim-16cpu", "tok-16", 16}, {"s1-sim-2cpu", "tok-2", 2}}
	regions := func() []Region {
		west := Region{Name: "west", NS: "sim-west", MaxParallel: 500 * scale, MaxPending: 20 * scale, PoolMax: 15 * scale, PoolStart: 15 * scale}
		// WEST_PARALLEL overrides west's MAX_PARALLEL_JOBS.
		if v, err := strconv.Atoi(os.Getenv("WEST_PARALLEL")); err == nil && v > 0 {
			west.MaxParallel = v
		}
		return []Region{
			west,
			{Name: "east", NS: "sim-east", MaxParallel: 20 * scale, MaxPending: 1 * scale, PoolMax: 2 * scale, PoolStart: 2 * scale},
		}
	}
	// West holds about 100*scale job pods (avg ~9 CPU); with jobs of 30-90s it finishes ~100*scale per minute.
	capPerSec := float64(100*scale) / 60.0
	base := Scenario{Types: types, Arrivals: 8 * time.Minute, Drain: 10 * time.Minute, TypeWeights: []float64{0.5, 0.2, 0.3},
		MinDur: 30 * time.Second, MaxDur: 90 * time.Second, PodTimeout: 150 * time.Second, OrphanProb: 0.02}

	out := map[string]Scenario{}
	s := base
	s.Name, s.Regions, s.Rate = "step", regions(), func(time.Duration) float64 { return 0 }
	s.Burst = map[time.Duration]int{0: 20 * 100 * scale / 10}
	s.Arrivals = time.Second
	out[s.Name] = s

	s = base
	s.Name, s.Regions, s.Rate = "sustained", regions(), func(time.Duration) float64 { return 2 * capPerSec }
	out[s.Name] = s

	s = base
	s.Name, s.Regions = "lull-surge", regions()
	s.Arrivals = 12 * time.Minute
	s.Rate = func(t time.Duration) float64 {
		switch {
		case t < 4*time.Minute:
			return 0.8 * capPerSec
		case t < 6*time.Minute:
			return 0.1 * capPerSec
		default:
			return 1.8 * capPerSec
		}
	}
	s.Events = []Event{{At: 5 * time.Minute, Kind: "remove-nodes", Region: "west", N: 6 * scale}}
	out[s.Name] = s

	s = base
	s.Name, s.Regions, s.Rate = "overflow500", regions(), func(time.Duration) float64 { return 0 }
	s.Burst = map[time.Duration]int{0: 1200 * scale}
	s.TypeWeights = []float64{0, 0, 1}
	s.Arrivals = time.Second
	out[s.Name] = s

	s = base
	s.Name, s.Regions, s.Rate = "faults", regions(), func(time.Duration) float64 { return 1.5 * capPerSec }
	s.Events = []Event{
		{At: 1 * time.Minute, Kind: "fault-on", Fault: Fault{ErrorRate: 0.3, Latency: 2 * time.Second}},
		{At: 3 * time.Minute, Kind: "fault-on", Fault: Fault{Hang: true}},
		{At: 4 * time.Minute, Kind: "fault-off"},
	}
	out[s.Name] = s

	s = base
	s.Name, s.Regions, s.Rate = "restart", regions(), func(time.Duration) float64 { return 1.5 * capPerSec }
	s.Events = []Event{{At: 4 * time.Minute, Kind: "restart", Region: "west"}}
	out[s.Name] = s

	// Agents die without cleaning up while their job pods wait for a node: the path
	// that leaves waiting pods uncounted unless the controller watches them.
	s = base
	s.Name, s.Regions, s.Rate = "agent-loss", regions(), func(time.Duration) float64 { return 1.8 * capPerSec }
	s.Arrivals = 10 * time.Minute
	s.OrphanProb = 0.5
	out[s.Name] = s

	s = base
	s.Name, s.Regions, s.Rate = "uneven", regions(), func(time.Duration) float64 { return 1.5 * capPerSec }
	s.TypeWeights = []float64{0.9, 0.05, 0.05}
	out[s.Name] = s

	// Small, long jobs: CPU fits more than MAX_PARALLEL_JOBS at once, so the parallel limit binds.
	s = base
	s.Name, s.Regions, s.Rate = "parallel-bound", regions(), func(time.Duration) float64 { return 2 * float64(scale) }
	s.Arrivals = 5 * time.Minute
	s.TypeWeights = []float64{0, 0, 1}
	s.MinDur, s.MaxDur = 3*time.Minute, 5*time.Minute
	out[s.Name] = s
	return out
}

// ---------- fake Semaphore ----------

type simJob struct {
	ID         string
	Type       string
	Enqueued   time.Time
	Claimed    time.Time
	ClaimedBy  string
	Done       time.Time
	FailedPod  bool
	LostClaims int
}

type Semaphore struct {
	mu     sync.Mutex
	jobs   map[string]*simJob
	queues map[string][]string // per type, oldest first
	fault  Fault
	polls  int
	errors int
}

func newSemaphore() *Semaphore {
	return &Semaphore{jobs: map[string]*simJob{}, queues: map[string][]string{}}
}

func (s *Semaphore) enqueue(typ string, n int, prefix string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%06d", prefix, len(s.jobs))
		s.jobs[id] = &simJob{ID: id, Type: typ, Enqueued: now}
		s.queues[typ] = append(s.queues[typ], id)
	}
}

// First agent to claim a job gets it, like Semaphore's occupation requests.
func (s *Semaphore) claim(id, region string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return false
	}
	if !j.Claimed.IsZero() {
		j.LostClaims++
		return false
	}
	j.Claimed, j.ClaimedBy = time.Now(), region
	q := s.queues[j.Type]
	for i, x := range q {
		if x == id {
			s.queues[j.Type] = append(q[:i], q[i+1:]...)
			break
		}
	}
	return true
}

func (s *Semaphore) finish(id string, failedPod bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[id]; ok && j.Done.IsZero() {
		j.Done, j.FailedPod = time.Now(), failedPod
	}
}

func (s *Semaphore) queued() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, q := range s.queues {
		n += len(q)
	}
	return n
}

func (s *Semaphore) unfinished() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, j := range s.jobs {
		if j.Done.IsZero() {
			n++
		}
	}
	return n
}

func (s *Semaphore) serve(types []AgentType, port int) {
	byToken := map[string]string{}
	for _, t := range types {
		byToken[t.Token] = t.Name
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/self_hosted_agents/jobs", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.polls++
		f := s.fault
		typ := byToken[strings.TrimPrefix(r.Header.Get("Authorization"), "Token ")]
		q := s.queues[typ]
		ids := []map[string]string{}
		for i := len(q) - 1; i >= 0 && len(ids) < 500; i-- { // newest first, one page of 500
			ids = append(ids, map[string]string{"id": q[i]})
		}
		inject := f.ErrorRate > 0 && rand.Float64() < f.ErrorRate
		if inject || f.Hang {
			s.errors++
		}
		s.mu.Unlock()

		if f.Hang {
			time.Sleep(5 * time.Minute)
			return
		}
		if f.Latency > 0 {
			time.Sleep(f.Latency)
		}
		if inject {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jobs": ids})
	})
	go func() { log.Fatal(http.ListenAndServeTLS(fmt.Sprintf(":%d", port), "tls.crt", "tls.key", mux)) }()
}

// ---------- simulated agents and cluster ----------

type agentState struct {
	jobID    string
	region   string
	claimed  bool
	lost     bool
	jobPod   string
	killMe   bool
	killed   bool
	finished bool
}

type jobPodState struct {
	finishAt time.Time
	running  bool
}

type Sim struct {
	sc          Scenario
	cs          kubernetes.Interface
	sem         *Semaphore
	pods        map[string]listerscore.PodLister
	jobs        map[string]listersbatch.JobLister
	nodes       listerscore.NodeLister
	mu          sync.Mutex
	agents      map[types.UID]*agentState
	jpods       map[string]*jobPodState // ns/name
	pendingBoot map[string]int          // region -> nodes booting
	stats       *Stats
}

func (sim *Sim) region(ns string) Region {
	for _, r := range sim.sc.Regions {
		if r.NS == ns {
			return r
		}
	}
	return Region{}
}

func cpuOf(sc Scenario, typ string) int {
	for _, t := range sc.Types {
		if t.Name == typ {
			return t.CPU
		}
	}
	return 8
}

func jobIDFromArgs(pod *corev1.Pod) string {
	for _, c := range pod.Spec.Containers {
		for i, a := range c.Args {
			if a == "--job-id" && i+1 < len(c.Args) {
				return c.Args[i+1]
			}
		}
	}
	return ""
}

func terminal(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
}

func (sim *Sim) setPhase(ctx context.Context, p *corev1.Pod, phase corev1.PodPhase, exit int32) error {
	now := metav1.Now()
	ready := corev1.ConditionFalse
	var state corev1.ContainerState
	switch phase {
	case corev1.PodRunning:
		ready = corev1.ConditionTrue
		state.Running = &corev1.ContainerStateRunning{StartedAt: now}
	default:
		reason := "Completed"
		if exit != 0 {
			reason = "Error"
		}
		state.Terminated = &corev1.ContainerStateTerminated{ExitCode: exit, Reason: reason, StartedAt: now, FinishedAt: now}
	}
	statuses := []corev1.ContainerStatus{}
	for _, c := range p.Spec.Containers {
		statuses = append(statuses, corev1.ContainerStatus{Name: c.Name, Image: c.Image, Ready: ready == corev1.ConditionTrue, State: state})
	}
	cp := p.DeepCopy()
	cp.Status.Phase = phase
	if cp.Status.StartTime == nil {
		cp.Status.StartTime = &now
	}
	cp.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
		{Type: corev1.PodReady, Status: ready, LastTransitionTime: now},
		{Type: corev1.ContainersReady, Status: ready, LastTransitionTime: now},
	}
	cp.Status.ContainerStatuses = statuses
	_, err := sim.cs.CoreV1().Pods(p.Namespace).UpdateStatus(ctx, cp, metav1.UpdateOptions{})
	return err
}

func (sim *Sim) createJobPod(ctx context.Context, agent *corev1.Pod, jobID string) (string, error) {
	typ := agent.Labels[agentTypeLabel]
	name := "semaphore-job-" + jobID
	region := sim.region(agent.Namespace)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: agent.Namespace, Labels: map[string]string{
			jobIDLabel: jobID, agentTypeLabel: typ, roleLabel: "job"}},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"pool": "jobs-" + region.Name},
			Tolerations:  []corev1.Toleration{{Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
			Containers: []corev1.Container{{Name: "main", Image: "fake", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprint(cpuOf(sim.sc, typ)))}}}},
		},
	}
	_, err := sim.cs.CoreV1().Pods(agent.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		err = nil
	}
	return name, err
}

func (sim *Sim) deletePod(ctx context.Context, ns, name string, grace int64) {
	_ = sim.cs.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &grace})
}

// One pass over every agent and job pod, moving each along its lifecycle.
func (sim *Sim) reconcile(ctx context.Context) {
	now := time.Now()
	for _, r := range sim.sc.Regions {
		pods, _ := sim.pods[r.NS].List(everything())
		live := map[types.UID]bool{}
		byName := map[string]*corev1.Pod{}
		for _, p := range pods {
			byName[p.Name] = p
		}
		for _, p := range pods {
			if p.Labels[resourceLabel] != "semaphore-job" || p.DeletionTimestamp != nil {
				continue
			}
			live[p.UID] = true
			sim.mu.Lock()
			st, ok := sim.agents[p.UID]
			if !ok {
				st = &agentState{jobID: jobIDFromArgs(p), region: r.Name, killMe: rand.Float64() < sim.sc.OrphanProb}
				sim.agents[p.UID] = st
			}
			sim.mu.Unlock()
			if terminal(p) || st.finished || p.Spec.NodeName == "" {
				continue
			}
			if p.Status.Phase == corev1.PodPending {
				if now.Sub(p.CreationTimestamp.Time) >= 2*time.Second {
					_ = sim.setPhase(ctx, p, corev1.PodRunning, 0)
				}
				continue
			}
			if !st.claimed && !st.lost {
				if sim.sem.claim(st.jobID, r.Name) {
					st.claimed = true
					name, err := sim.createJobPod(ctx, p, st.jobID)
					if err != nil {
						log.Printf("create job pod: %v", err)
						continue
					}
					st.jobPod = name
				} else {
					st.lost = true
					st.finished = true
					_ = sim.setPhase(ctx, p, corev1.PodFailed, 1)
				}
				continue
			}
			jp := byName[st.jobPod]
			if jp == nil {
				continue
			}
			switch {
			case jp.Status.Phase == corev1.PodSucceeded:
				st.finished = true
				sim.sem.finish(st.jobID, false)
				_ = sim.setPhase(ctx, p, corev1.PodSucceeded, 0)
				sim.deletePod(ctx, r.NS, jp.Name, 0)
			case jp.Spec.NodeName == "" && now.Sub(jp.CreationTimestamp.Time) > sim.sc.PodTimeout:
				// The agent gives up on a job pod that never got a node, and fails the job.
				st.finished = true
				sim.sem.finish(st.jobID, true)
				sim.deletePod(ctx, r.NS, jp.Name, 0)
				_ = sim.setPhase(ctx, p, corev1.PodFailed, 1)
			case st.killMe && !st.killed && jp.Spec.NodeName == "" && now.Sub(jp.CreationTimestamp.Time) > 20*time.Second:
				// Agent dies without cleaning up, leaving its job pod waiting.
				st.killed, st.finished = true, true
				sim.stats.orphansMade(r.Name)
				sim.deletePod(ctx, r.NS, p.Name, 0)
			}
		}

		// Job pods: start once on a node, finish after their duration.
		for _, p := range pods {
			if p.Labels[roleLabel] != "job" || p.DeletionTimestamp != nil || terminal(p) {
				continue
			}
			key := r.NS + "/" + p.Name
			sim.mu.Lock()
			js, ok := sim.jpods[key]
			if !ok {
				js = &jobPodState{}
				sim.jpods[key] = js
			}
			sim.mu.Unlock()
			if p.Spec.NodeName == "" {
				continue
			}
			if !js.running {
				if sim.setPhase(ctx, p, corev1.PodRunning, 0) == nil {
					js.running = true
					d := sim.sc.MinDur + time.Duration(rand.Int63n(int64(sim.sc.MaxDur-sim.sc.MinDur)))
					js.finishAt = now.Add(d)
				}
				continue
			}
			if now.After(js.finishAt) {
				_ = sim.setPhase(ctx, p, corev1.PodSucceeded, 0)
			}
		}

		// Agents the controller deleted clean up their job pods, like the real agent.
		sim.mu.Lock()
		for uid, st := range sim.agents {
			if st.region != r.Name || live[uid] {
				continue
			}
			if !st.killed && !st.finished && st.jobPod != "" {
				sim.deletePod(ctx, r.NS, st.jobPod, 0)
				sim.sem.finish(st.jobID, true)
			}
			delete(sim.agents, uid)
		}
		for key := range sim.jpods {
			if strings.HasPrefix(key, r.NS+"/") {
				if _, ok := byName[strings.TrimPrefix(key, r.NS+"/")]; !ok {
					delete(sim.jpods, key)
				}
			}
		}
		sim.mu.Unlock()
	}
}

func everything() labels.Selector { return labels.Everything() }

// ---------- autoscaler ----------

func (sim *Sim) addNodes(ctx context.Context, pool string, n int, cpu int, pods int) {
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("kwok-%s-%d-%04d", pool, time.Now().UnixNano()%1e6, i)
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"type": "kwok", "pool": pool, "kubernetes.io/hostname": name},
				Annotations: map[string]string{"kwok.x-k8s.io/node": "fake", "node.alpha.kubernetes.io/ttl": "0"}},
			Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "kwok.x-k8s.io/node", Value: "fake", Effect: corev1.TaintEffectNoSchedule}}},
			Status: corev1.NodeStatus{
				Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprint(cpu)), corev1.ResourceMemory: resource.MustParse("512Gi"), corev1.ResourcePods: resource.MustParse(fmt.Sprint(pods))},
				Capacity:    corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprint(cpu)), corev1.ResourceMemory: resource.MustParse("512Gi"), corev1.ResourcePods: resource.MustParse(fmt.Sprint(pods))},
			},
		}
		if _, err := sim.cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
			log.Printf("add node: %v", err)
		}
	}
}

func (sim *Sim) poolNodes(pool string) []*corev1.Node {
	all, _ := sim.nodes.List(everything())
	out := []*corev1.Node{}
	for _, n := range all {
		if n.Labels["pool"] == pool && n.DeletionTimestamp == nil {
			out = append(out, n)
		}
	}
	return out
}

// Adds nodes 45s after job pods start waiting, up to the pool maximum, and removes
// nodes that have been empty for 2 minutes.
func (sim *Sim) autoscale(ctx context.Context, emptySince map[string]time.Time) {
	now := time.Now()
	for _, r := range sim.sc.Regions {
		pool := "jobs-" + r.Name
		nodes := sim.poolNodes(pool)
		pods, _ := sim.pods[r.NS].List(everything())
		demand := 0
		busy := map[string]bool{}
		for _, p := range pods {
			if p.Labels[roleLabel] != "job" || p.DeletionTimestamp != nil || terminal(p) {
				continue
			}
			if p.Spec.NodeName != "" {
				busy[p.Spec.NodeName] = true
			} else if now.Sub(p.CreationTimestamp.Time) > 5*time.Second {
				demand += cpuOf(sim.sc, p.Labels[agentTypeLabel])
			}
		}
		sim.mu.Lock()
		booting := sim.pendingBoot[r.Name]
		sim.mu.Unlock()
		if demand > 0 {
			want := int(math.Ceil(float64(demand) / nodeCPU))
			room := r.PoolMax - len(nodes) - booting
			n := min(want-booting, room, 10)
			if n > 0 {
				sim.mu.Lock()
				sim.pendingBoot[r.Name] += n
				sim.mu.Unlock()
				go func(region string, n int) {
					time.Sleep(45 * time.Second)
					sim.addNodes(ctx, pool, n, nodeCPU, 110)
					sim.mu.Lock()
					sim.pendingBoot[region] -= n
					sim.mu.Unlock()
				}(r.Name, n)
			}
			continue
		}
		for _, n := range nodes {
			if busy[n.Name] {
				delete(emptySince, n.Name)
				continue
			}
			first, ok := emptySince[n.Name]
			if !ok {
				emptySince[n.Name] = now
				continue
			}
			if now.Sub(first) > 2*time.Minute {
				_ = sim.cs.CoreV1().Nodes().Delete(ctx, n.Name, metav1.DeleteOptions{})
				delete(emptySince, n.Name)
			}
		}
	}
}

// ---------- invariants and stats ----------

type Sample struct {
	T       float64 `json:"t"`
	Region  string  `json:"region"`
	Active  int     `json:"activeJobs"`
	Pending int     `json:"pending"`
	Waiting int     `json:"waitingJobPods"`
	Nodes   int     `json:"nodes"`
	Queued  int     `json:"queued"`
}

type Stats struct {
	mu          sync.Mutex
	start       time.Time
	samples     []Sample
	created     map[string]int
	createdOver map[string]int
	lastPending map[string]int
	orphans     map[string]int
}

func (s *Stats) orphansMade(region string) {
	s.mu.Lock()
	s.orphans[region]++
	s.mu.Unlock()
}

func (sim *Sim) sample() {
	now := time.Now()
	for _, r := range sim.sc.Regions {
		jobs, _ := sim.jobs[r.NS].List(everything())
		active := 0
		for _, j := range jobs {
			if !jobFinished(j) {
				active++
			}
		}
		pods, _ := sim.pods[r.NS].List(everything())
		pendingIDs := map[string]bool{}
		waiting := 0
		for _, p := range pods {
			if p.DeletionTimestamp != nil || terminal(p) {
				continue
			}
			if p.Labels[roleLabel] == "job" && p.Spec.NodeName == "" {
				waiting++
				pendingIDs[p.Labels[jobIDLabel]] = true
			}
			if p.Labels[resourceLabel] == "semaphore-job" && !podReady(p) {
				pendingIDs[jobIDFromArgs(p)] = true
			}
		}
		sim.stats.mu.Lock()
		sim.stats.lastPending[r.Name] = len(pendingIDs)
		sim.stats.samples = append(sim.stats.samples, Sample{T: now.Sub(sim.stats.start).Seconds(), Region: r.Name, Active: active,
			Pending: len(pendingIDs), Waiting: waiting, Nodes: len(sim.poolNodes("jobs-" + r.Name)), Queued: sim.sem.queued()})
		sim.stats.mu.Unlock()
	}
}

func jobFinished(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// ---------- report ----------

type RegionReport struct {
	MaxParallel        int `json:"maxParallel"`
	MaxPending         int `json:"maxPending"`
	PeakActive         int `json:"peakActiveJobs"`
	ParallelViolations int `json:"samplesOverMaxParallel"`
	PeakPending        int `json:"peakPending"`
	PeakWaiting        int `json:"peakWaitingJobPods"`
	SamplesOverCap     int `json:"samplesPendingOverCap"`
	Samples            int `json:"samples"`
	Created            int `json:"agentJobsCreated"`
	CreatedAtOrOverCap int `json:"agentJobsCreatedWhilePendingAtOrOverCap"`
	OrphansMade        int `json:"orphanedJobPodsMade"`
	PeakNodes          int `json:"peakNodes"`
}

type Report struct {
	Scenario       string                   `json:"scenario"`
	Controller     string                   `json:"controller"`
	Scale          int                      `json:"scale"`
	Jobs           int                      `json:"jobs"`
	Ran            int                      `json:"jobsRan"`
	FailedPodStart int                      `json:"jobsFailedWaitingForNode"`
	NeverClaimed   int                      `json:"jobsNeverClaimed"`
	Unfinished     int                      `json:"jobsUnfinished"`
	LostClaims     int                      `json:"agentsThatLostTheClaim"`
	WaitP50        float64                  `json:"queueWaitP50s"`
	WaitP99        float64                  `json:"queueWaitP99s"`
	WaitMax        float64                  `json:"queueWaitMaxs"`
	WaitByType     map[string][3]float64    `json:"queueWaitByType_p50_p99_max"`
	Polls          int                      `json:"apiPolls"`
	PollErrors     int                      `json:"apiPollsFailedOrHung"`
	Regions        map[string]*RegionReport `json:"regions"`
	DurationS      float64                  `json:"durationS"`
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Float64s(xs)
	return xs[int(math.Min(float64(len(xs)-1), math.Floor(p*float64(len(xs)))))]
}

func (sim *Sim) report(controller string, scale int) Report {
	rep := Report{Scenario: sim.sc.Name, Controller: controller, Scale: scale, Regions: map[string]*RegionReport{}, WaitByType: map[string][3]float64{}}
	sim.sem.mu.Lock()
	waits := []float64{}
	byType := map[string][]float64{}
	now := time.Now()
	for _, j := range sim.sem.jobs {
		rep.Jobs++
		rep.LostClaims += j.LostClaims
		var w float64
		if j.Claimed.IsZero() {
			rep.NeverClaimed++
			w = now.Sub(j.Enqueued).Seconds()
		} else {
			w = j.Claimed.Sub(j.Enqueued).Seconds()
		}
		waits = append(waits, w)
		byType[j.Type] = append(byType[j.Type], w)
		switch {
		case j.Done.IsZero():
			rep.Unfinished++
		case j.FailedPod:
			rep.FailedPodStart++
		default:
			rep.Ran++
		}
	}
	rep.Polls, rep.PollErrors = sim.sem.polls, sim.sem.errors
	sim.sem.mu.Unlock()
	rep.WaitP50, rep.WaitP99, rep.WaitMax = pct(waits, 0.5), pct(waits, 0.99), pct(waits, 1)
	for t, ws := range byType {
		rep.WaitByType[t] = [3]float64{pct(ws, 0.5), pct(ws, 0.99), pct(ws, 1)}
	}
	sim.stats.mu.Lock()
	for _, r := range sim.sc.Regions {
		rr := &RegionReport{MaxParallel: r.MaxParallel, MaxPending: r.MaxPending, Created: sim.stats.created[r.Name],
			CreatedAtOrOverCap: sim.stats.createdOver[r.Name], OrphansMade: sim.stats.orphans[r.Name]}
		for _, s := range sim.stats.samples {
			if s.Region != r.Name {
				continue
			}
			rr.Samples++
			rr.PeakActive = max(rr.PeakActive, s.Active)
			rr.PeakPending = max(rr.PeakPending, s.Pending)
			rr.PeakWaiting = max(rr.PeakWaiting, s.Waiting)
			rr.PeakNodes = max(rr.PeakNodes, s.Nodes)
			if s.Active > r.MaxParallel {
				rr.ParallelViolations++
			}
			if s.Pending > r.MaxPending {
				rr.SamplesOverCap++
			}
		}
		rep.Regions[r.Name] = rr
	}
	rep.DurationS = time.Since(sim.stats.start).Seconds()
	sim.stats.mu.Unlock()
	return rep
}

// ---------- main ----------

func main() {
	name := flag.String("scenario", "sustained", "scenario name")
	controller := flag.String("controller", "", "label for the controller build under test")
	scale := flag.Int("scale", 1, "1 = small, 10 = ten times larger")
	out := flag.String("out", "results", "results directory")
	setupOnly := flag.Bool("setup", false, "only create nodes, then exit")
	kubeconfig := flag.String("kubeconfig", os.Getenv("HOME")+"/.kube/config", "kubeconfig of the cluster under test")
	port := flag.Int("port", 8443, "port of the fake Semaphore API")
	flag.Parse()

	sc, ok := scenarios(*scale)[*name]
	if !ok {
		log.Fatalf("unknown scenario %s", *name)
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	cfg.QPS, cfg.Burst = 500, 1000
	cs := kubernetes.NewForConfigOrDie(cfg)
	ctx := context.Background()

	sim := &Sim{sc: sc, cs: cs, sem: newSemaphore(), pods: map[string]listerscore.PodLister{}, jobs: map[string]listersbatch.JobLister{},
		agents: map[types.UID]*agentState{}, jpods: map[string]*jobPodState{}, pendingBoot: map[string]int{},
		stats: &Stats{created: map[string]int{}, createdOver: map[string]int{}, lastPending: map[string]int{}, orphans: map[string]int{}}}

	nodeFactory := informers.NewSharedInformerFactory(cs, time.Minute)
	sim.nodes = nodeFactory.Core().V1().Nodes().Lister()
	nodeFactory.Start(ctx.Done())
	nodeFactory.WaitForCacheSync(ctx.Done())

	// Start each region's job pool at its starting size, and give agents their own roomy pool.
	for _, r := range sc.Regions {
		if have := len(sim.poolNodes("jobs-" + r.Name)); have < r.PoolStart {
			sim.addNodes(ctx, "jobs-"+r.Name, r.PoolStart-have, nodeCPU, 110)
		}
		if len(sim.poolNodes("agents-"+r.Name)) == 0 {
			sim.addNodes(ctx, "agents-"+r.Name, 4, 4000, 3000)
		}
	}
	if *setupOnly {
		return
	}

	for _, r := range sc.Regions {
		f := informers.NewSharedInformerFactoryWithOptions(cs, time.Minute, informers.WithNamespace(r.NS))
		sim.pods[r.NS] = f.Core().V1().Pods().Lister()
		ji := f.Batch().V1().Jobs()
		sim.jobs[r.NS] = ji.Lister()
		region := r
		_, _ = ji.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(obj any) {
			if sim.stats.start.IsZero() {
				return
			}
			sim.stats.mu.Lock()
			sim.stats.created[region.Name]++
			if sim.stats.lastPending[region.Name] >= region.MaxPending {
				sim.stats.createdOver[region.Name]++
			}
			sim.stats.mu.Unlock()
		}})
		f.Core().V1().Pods().Informer()
		f.Start(ctx.Done())
		f.WaitForCacheSync(ctx.Done())
	}

	sim.sem.serve(sc.Types, *port)
	time.Sleep(2 * time.Second)
	sim.stats.start = time.Now()
	log.Printf("scenario %s (scale %d, controller %s) started", sc.Name, *scale, *controller)

	go func() {
		for range time.Tick(time.Second) {
			sim.reconcile(ctx)
		}
	}()
	go func() {
		empty := map[string]time.Time{}
		for range time.Tick(5 * time.Second) {
			sim.autoscale(ctx, empty)
		}
	}()
	go func() {
		for range time.Tick(2 * time.Second) {
			sim.sample()
		}
	}()
	for _, e := range sc.Events {
		e := e
		go func() {
			time.Sleep(e.At)
			log.Printf("event %s %s %d", e.Kind, e.Region, e.N)
			switch e.Kind {
			case "remove-nodes":
				nodes := sim.poolNodes("jobs-" + e.Region)
				for i := 0; i < e.N && i < len(nodes); i++ {
					_ = cs.CoreV1().Nodes().Delete(ctx, nodes[i].Name, metav1.DeleteOptions{})
				}
			case "restart":
				ns := ""
				for _, r := range sc.Regions {
					if r.Name == e.Region {
						ns = r.NS
					}
				}
				_ = cs.CoreV1().Pods(ns).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: "app=agent-k8s-controller"})
			case "fault-on":
				sim.sem.mu.Lock()
				sim.sem.fault = e.Fault
				sim.sem.mu.Unlock()
			case "fault-off":
				sim.sem.mu.Lock()
				sim.sem.fault = Fault{}
				sim.sem.mu.Unlock()
			}
		}()
	}

	// Arrivals.
	pick := func() AgentType {
		x, acc := rand.Float64(), 0.0
		for i, w := range sc.TypeWeights {
			acc += w
			if x < acc {
				return sc.Types[i]
			}
		}
		return sc.Types[0]
	}
	start := time.Now()
	carry := 0.0
	for t := time.Duration(0); t < sc.Arrivals; t = time.Since(start) {
		for at, n := range sc.Burst {
			if at <= t {
				for i := 0; i < n; i++ {
					sim.sem.enqueue(pick().Name, 1, sc.Name)
				}
				delete(sc.Burst, at)
			}
		}
		carry += sc.Rate(t)
		for ; carry >= 1; carry-- {
			sim.sem.enqueue(pick().Name, 1, sc.Name)
		}
		time.Sleep(time.Second)
	}
	log.Printf("arrivals done: %d jobs", sim.sem.unfinished())
	deadline := time.Now().Add(sc.Drain)
	for time.Now().Before(deadline) && sim.sem.unfinished() > 0 {
		time.Sleep(5 * time.Second)
	}

	rep := sim.report(*controller, *scale)
	_ = os.MkdirAll(*out, 0o755)
	b, _ := json.MarshalIndent(rep, "", "  ")
	file := fmt.Sprintf("%s/%s-%s-x%d.json", *out, sc.Name, *controller, *scale)
	_ = os.WriteFile(file, b, 0o644)
	sim.stats.mu.Lock()
	sb, _ := json.Marshal(sim.stats.samples)
	sim.stats.mu.Unlock()
	_ = os.WriteFile(strings.TrimSuffix(file, ".json")+"-samples.json", sb, 0o644)
	fmt.Println(string(b))
}
