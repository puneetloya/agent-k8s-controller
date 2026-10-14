# Semaphore agent controller for Kubernetes

A Kubernetes controller that runs Semaphore jobs in Kubernetes.

## Installation

### Requirements

- A Kubernetes cluster
- A Semaphore API token

### Configuration

| Environment variable                   | Description |
|----------------------------------------|-------------|
| SEMAPHORE_ENDPOINT                     | The Semaphore control plane endpoint, e.g. `<your-organization>.semaphoreci.com`. |
| KUBERNETES_NAMESPACE                   | The Kubernetes namespace where the resources for Semaphore jobs will be created. By default, the default namespace is used. |
| SEMAPHORE_AGENT_IMAGE                  | The [Semaphore agent](https://github.com/semaphoreci/agent) image to use when creating agents. By default, `semaphoreci/agent:latest`. |
| SEMAPHORE_AGENT_LOG_LEVEL              | The log level for the [Semaphore agent](https://github.com/semaphoreci/agent). By default, `info`. |
| MAX_PARALLEL_JOBS                      | The max number of Semaphore jobs to run in parallel. By default, 10. |
| KUBE_API_QPS                           | Client-side rate limit (requests per second) for the controller's Kubernetes API calls. By default, unset, which keeps the client-go default of 5. |
| KUBE_API_BURST                         | Client-side burst for the controller's Kubernetes API calls. By default, unset, which keeps the client-go default of 10. |
| KUBERNETES_SERVICE_ACCOUNT             | The Kubernetes service account to attach to the pods created for the [Semaphore agent](https://github.com/semaphoreci/agent). |
| SEMAPHORE_AGENT_LABELS                 | A comma-separated list of Kubernetes labels to apply on all resources created by the controller. |
| SEMAPHORE_AGENT_STARTUP_PARAMETERS     | Any additional [Semaphore agent configuration parameters](https://docs.semaphoreci.com/ci-cd-environment/configure-self-hosted-agent/) to pass to the agents being created. |
| SEMAPHORE_AGENT_POD_SPEC               | YAML or JSON with the scheduling fields to set on the agent pods, see [Scheduling agent pods](#scheduling-agent-pods). By default, none are set. |
| KEEP_FAILED_JOBS_FOR                   | A [duration string](https://pkg.go.dev/time#ParseDuration) indicating how long to keep failed Kubernetes jobs. For example, `5m`. Default is 0. |
| KEEP_SUCCESSFUL_JOBS_FOR               | A [duration string](https://pkg.go.dev/time#ParseDuration) indicating how long to successful failed Kubernetes jobs. For example, `5m`. Default is 0. |
| JOB_START_TIMEOUT                      | A [duration string](https://pkg.go.dev/time#ParseDuration) indicating how long to wait for a Kubernetes job created to start; after the timeout has passed, the Kubernetes job is deleted. Default is `5m`. |

### Scheduling agent pods

For each Semaphore job, the controller creates a pod for the [Semaphore agent](https://github.com/semaphoreci/agent), and the agent creates the pod that runs the job. Where the job pod runs is configured with the agent's `--kubernetes-pod-spec` parameter. Where the agent pod runs is configured here, with these pod spec fields:

- `nodeSelector`
- `affinity`
- `tolerations`
- `topologySpreadConstraints`
- `priorityClassName`

Any other field is rejected, as is a misspelled one or a value of the wrong type, so a mistake cannot leave agent pods free to run on any node. Quote label values that YAML reads as booleans or numbers, such as `"true"`, `"yes"` or `"10"`: unquoted, they are rejected. Only the first YAML document is read.

The controller checks field names and types only. A value Kubernetes does not accept, such as an unknown toleration `operator`, is loaded, and the controller logs the API server's error every time it tries to create a job for that agent type. Some settings only fail when Kubernetes creates the agent pod, such as a `priorityClassName` that does not exist, a resource quota or a Pod Security rejection: the job is created without a pod and holds its slot until `JOB_START_TIMEOUT`, the log says no pod was created, and the reason is in the job's events.

The fields can be set in two places:

- `SEMAPHORE_AGENT_POD_SPEC` sets them for every agent type. If it is not valid, the controller exits at startup. The Helm chart does not set this variable yet.
- The `agentPodSpec` key of an agent type secret sets them for that agent type, replacing `SEMAPHORE_AGENT_POD_SPEC` as a whole; nothing is merged. An empty object, `{}`, runs the agent type without the controller-wide fields. Changes apply to the next job created.

If an agent type secret is not valid, the controller logs the error and does not load it. A change that makes a loaded secret invalid is ignored, and the controller keeps using the secret's previous configuration until it is fixed or the controller restarts. Agent pods always read `registrationToken` from the secret itself, so a new token reaches new agent pods even when the rest of the change is ignored. If several secrets use the same `agentTypeName`, the oldest one is used, and deleting it hands the agent type to the next oldest.

Write access to agent type secrets is privileged: they control the agents' startup parameters, where agent pods run, and their priority class.

Agent pods carry the `semaphoreci.com/resource-type: semaphore-job` label, which job pods do not, and the `semaphoreci.com/agent-type: <agent type name>` label, which job pods also carry. Use them in label selectors for affinity and topology spread constraints.

For example, to run an agent type's agent pods on a dedicated, tainted node pool, spread across zones:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-agent-type
  labels:
    semaphoreci.com/resource-type: agent-type-configuration
stringData:
  agentTypeName: s1-my-agent-type
  registrationToken: <registration token>
  agentPodSpec: |
    nodeSelector:
      node-pool: semaphore-agents
    tolerations:
      - key: dedicated
        operator: Equal
        value: semaphore-agents
        effect: NoSchedule
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels:
            semaphoreci.com/resource-type: semaphore-job
            semaphoreci.com/agent-type: s1-my-agent-type
```

An agent pod that cannot be scheduled stays pending until `JOB_START_TIMEOUT`, when the controller logs why it did not start, deletes it, and creates it again on a later poll. While pending, it holds one of the `MAX_PARALLEL_JOBS` slots, which all agent types share, and the controller stops polling for jobs of any agent type while every slot is taken.

Avoid a toleration without a `key`: it tolerates every taint, including the ones Kubernetes uses to evict pods from failed nodes, so an agent pod on a failed node keeps its slot until its one-day deadline.
