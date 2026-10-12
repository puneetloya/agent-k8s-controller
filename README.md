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
| MAX_PENDING_JOBS                       | The max number of jobs waiting to start: agent pod not ready yet, or job pod not scheduled to a node yet. When reached, the controller stops polling for new jobs, so they stay queued. Requires `watch` on pods. By default, 0, which disables the check. Values at or above `MAX_PARALLEL_JOBS` have no effect. |
| KUBERNETES_SERVICE_ACCOUNT             | The Kubernetes service account to attach to the pods created for the [Semaphore agent](https://github.com/semaphoreci/agent). |
| SEMAPHORE_AGENT_LABELS                 | A comma-separated list of Kubernetes labels to apply on all resources created by the controller. |
| SEMAPHORE_AGENT_STARTUP_PARAMETERS     | Any additional [Semaphore agent configuration parameters](https://docs.semaphoreci.com/ci-cd-environment/configure-self-hosted-agent/) to pass to the agents being created. |
| KEEP_FAILED_JOBS_FOR                   | A [duration string](https://pkg.go.dev/time#ParseDuration) indicating how long to keep failed Kubernetes jobs. For example, `5m`. Default is 0. |
| KEEP_SUCCESSFUL_JOBS_FOR               | A [duration string](https://pkg.go.dev/time#ParseDuration) indicating how long to successful failed Kubernetes jobs. For example, `5m`. Default is 0. |
| JOB_START_TIMEOUT                      | A [duration string](https://pkg.go.dev/time#ParseDuration) indicating how long to wait for a Kubernetes job created to start; after the timeout has passed, the Kubernetes job is deleted. Default is `5m`. |
