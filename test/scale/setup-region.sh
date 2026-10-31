#!/bin/bash
# setup-region.sh <namespace> <region>: permissions, agent types and stub CA for one simulated region.
NS=$1; REGION=$2
cat <<YAML | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Namespace
metadata: {name: $NS}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: agent-k8s-controller, namespace: $NS}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: agent-k8s-controller, namespace: $NS}
rules:
  - {apiGroups: ["batch"], resources: ["jobs"], verbs: ["get", "create", "delete", "list", "watch"]}
  - {apiGroups: [""], resources: ["pods"], verbs: ["list", "watch", "delete"]}
  - {apiGroups: [""], resources: ["secrets"], verbs: ["get", "watch", "list"]}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: agent-k8s-controller, namespace: $NS}
subjects: [{kind: ServiceAccount, name: agent-k8s-controller, namespace: $NS}]
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: agent-k8s-controller}
YAML
for t in "s1-sim-8cpu tok-8" "s1-sim-16cpu tok-16" "s1-sim-2cpu tok-2"; do
  set -- $t
  kubectl -n $NS create secret generic "type-$1" --from-literal=agentTypeName=$1 --from-literal=registrationToken=$2 \
    --from-literal=agentPodSpec="{\"nodeSelector\":{\"pool\":\"agents-$REGION\"},\"tolerations\":[{\"key\":\"kwok.x-k8s.io/node\",\"operator\":\"Exists\",\"effect\":\"NoSchedule\"}]}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n $NS label secret "type-$1" semaphoreci.com/resource-type=agent-type-configuration --overwrite >/dev/null
done
kubectl -n $NS create configmap stub-ca --from-file=ca.crt=tls.crt --dry-run=client -o yaml | kubectl apply -f - >/dev/null
