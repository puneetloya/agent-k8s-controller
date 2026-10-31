#!/bin/bash
# deploy-controller.sh <namespace> <image> <max parallel> <max pending> <stub port>   (KUBECONFIG picks the cluster)
NS=$1; IMG=$2; PAR=$3; PEND=$4; PORT=$5
cat <<YAML | kubectl apply -f - >/dev/null
apiVersion: apps/v1
kind: Deployment
metadata: {name: agent-k8s-controller, namespace: $NS}
spec:
  replicas: 1
  strategy: {type: Recreate}
  selector: {matchLabels: {app: agent-k8s-controller}}
  template:
    metadata: {labels: {app: agent-k8s-controller}}
    spec:
      serviceAccountName: agent-k8s-controller
      volumes: [{name: ca, configMap: {name: stub-ca}}]
      containers:
        - name: controller
          image: ctrl:$IMG
          imagePullPolicy: Never
          volumeMounts: [{name: ca, mountPath: /certs}]
          env:
            - {name: SSL_CERT_FILE, value: /certs/ca.crt}
            - {name: SEMAPHORE_ENDPOINT, value: "172.30.0.1:$PORT"}
            - {name: KUBERNETES_NAMESPACE, value: $NS}
            - {name: SEMAPHORE_AGENT_IMAGE, value: "fake-agent:e2e"}
            - {name: MAX_PARALLEL_JOBS, value: "$PAR"}
            - {name: MAX_PENDING_JOBS, value: "$PEND"}
            - {name: KUBE_API_QPS, value: "50"}
            - {name: KUBE_API_BURST, value: "200"}
            - {name: JOB_START_TIMEOUT, value: "5m"}
YAML
kubectl -n $NS rollout restart deploy/agent-k8s-controller >/dev/null 2>&1
kubectl -n $NS rollout status deploy/agent-k8s-controller --timeout=120s >/dev/null
