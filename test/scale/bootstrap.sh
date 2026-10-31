#!/bin/bash
# bootstrap.sh: docker, kind, two clusters with KWOK nodes, both controller builds, the stub cert and the simulator.
# BEFORE_REV / NEW_REV pick the controller commits compared (cluster "before" and cluster "new").
set -e
cd "$(dirname "$0")"
BEFORE_REV=${BEFORE_REV:-5282f67} NEW_REV=${NEW_REV:-474593e}
export CGO_ENABLED=0 GOTOOLCHAIN=auto PATH=$PATH:/usr/local/bin
./ensure-docker.sh
docker network inspect kind >/dev/null 2>&1 || docker network create kind --subnet 172.30.0.0/16 >/dev/null
[ -x /usr/local/bin/kind ] || { curl -sSLo /usr/local/bin/kind https://kind.sigs.k8s.io/dl/v0.30.0/kind-linux-amd64; chmod +x /usr/local/bin/kind; }
[ -x /usr/local/bin/kubectl ] || { curl -sSLo /usr/local/bin/kubectl https://dl.k8s.io/release/v1.34.1/bin/linux/amd64/kubectl; chmod +x /usr/local/bin/kubectl; }

# Fake Semaphore API cert; the controller trusts it through the stub-ca ConfigMap.
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -keyout tls.key -out tls.crt -subj "/CN=stub" \
  -addext "subjectAltName=IP:172.30.0.1" -addext "basicConstraints=critical,CA:TRUE" 2>/dev/null

[ -d sem ] || git clone -q https://github.com/puneetloya/agent-k8s-controller sem
(cd sem && git fetch -q origin && for pair in before:$BEFORE_REV new:$NEW_REV; do
  git checkout -q ${pair##*:} && go build -o ../bin-${pair%%:*} .
done)
for img in before new; do
  printf 'FROM busybox:1.36\nCOPY bin-%s /controller\nENTRYPOINT ["/controller"]\n' $img > Dockerfile.$img
  docker build -q -f Dockerfile.$img -t ctrl:$img . >/dev/null
done
go build -o loadsim .

mkdir -p ~/.kube results logs
for cl in before new; do
  # kind's default controller-manager (20 req/s, 5 Job workers) lags Job status at thousands
  # of Jobs, which stalls any controller that waits on Job readiness. Give it more headroom.
  cat > kind-$cl.yaml <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - {hostPath: $PWD/audit, containerPath: /etc/kubernetes/policies, readOnly: true}
    kubeadmConfigPatches:
      - |
        kind: ClusterConfiguration
        apiServer:
          extraArgs:
            audit-policy-file: /etc/kubernetes/policies/policy.yaml
            audit-log-path: /var/log/kubernetes/audit.log
          extraVolumes:
            - {name: policies, hostPath: /etc/kubernetes/policies, mountPath: /etc/kubernetes/policies, readOnly: true, pathType: DirectoryOrCreate}
            - {name: audit-logs, hostPath: /var/log/kubernetes, mountPath: /var/log/kubernetes, readOnly: false, pathType: DirectoryOrCreate}
        controllerManager:
          extraArgs:
            kube-api-qps: "500"
            kube-api-burst: "1000"
            concurrent-job-syncs: "50"
YAML
  kind get clusters 2>/dev/null | grep -qx $cl || kind create cluster --name $cl --config kind-$cl.yaml >/dev/null 2>&1
  # One kubeconfig per cluster: a shared one sent both runs of a pair to the same cluster.
  kind get kubeconfig --name $cl > ~/.kube/$cl
  kind load docker-image ctrl:before ctrl:new --name $cl >/dev/null 2>&1
  export KUBECONFIG=~/.kube/$cl
  kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/download/v0.7.0/kwok.yaml >/dev/null 2>&1
  kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/download/v0.7.0/stage-fast.yaml >/dev/null 2>&1
  kubectl delete stages.kwok.x-k8s.io pod-ready pod-complete >/dev/null 2>&1 || true
  kubectl -n kube-system rollout status deploy/kwok-controller --timeout=180s >/dev/null
  ./setup-region.sh sim-west west && ./setup-region.sh sim-east east
  for ns in sim-west sim-east; do
    ./deploy-controller.sh $ns before 1 1 8443 >/dev/null 2>&1 || true
    kubectl -n $ns scale deploy/agent-k8s-controller --replicas=0 >/dev/null 2>&1
  done
done
echo BOOTSTRAP-DONE
