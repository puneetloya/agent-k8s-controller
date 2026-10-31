#!/bin/bash
# run.sh <scenario> <image> <cluster> <stub port> [scale]: one run on one kind cluster; skipped if done.
# scale 1 = small, 10 = ten times larger. WEST_PARALLEL overrides west MAX_PARALLEL_JOBS at scale 10.
SC=$1; IMG=$2; CL=$3; PORT=$4; SCALE=${5:-1}
cd "$(dirname "$0")"
export KUBECONFIG=$HOME/.kube/$CL
OUT=results/$SC-$IMG-x$SCALE.json
[ -f "$OUT" ] && { echo "skip $OUT"; exit 0; }
for ns in sim-west sim-east; do kubectl -n $ns scale deploy/agent-k8s-controller --replicas=0 >/dev/null 2>&1; done
for ns in sim-west sim-east; do
  kubectl -n $ns delete jobs --all --wait=false >/dev/null 2>&1
  kubectl -n $ns delete pods -l '!app' --grace-period=0 --force --wait=false >/dev/null 2>&1
done
kubectl delete nodes -l pool=jobs-west --wait=false >/dev/null 2>&1; kubectl delete nodes -l pool=jobs-east --wait=false >/dev/null 2>&1
for i in $(seq 1 60); do n=$(kubectl get pods -A -l '!app' --no-headers 2>/dev/null | grep -c '^sim-'); [ "$n" -eq 0 ] && break; sleep 3; done
./loadsim -scenario $SC -scale $SCALE -setup -kubeconfig $KUBECONFIG
case $SCALE in
  1) WP=500; WC=20; EP=20; EC=1;;
  10) WP=${WEST_PARALLEL:-5000}; WC=200; EP=200; EC=10;;
esac
export WEST_PARALLEL=$WP
./deploy-controller.sh sim-west $IMG $WP $WC $PORT && ./deploy-controller.sh sim-east $IMG $EP $EC $PORT
# Guard against comparing the wrong build.
got=$(kubectl -n sim-west get deploy agent-k8s-controller -o jsonpath='{.spec.template.spec.containers[0].image}')
[ "$got" = "ctrl:$IMG" ] || { echo "cluster $CL runs $got, expected ctrl:$IMG" >&2; exit 1; }
echo "cluster $CL runs $got"
docker exec $CL-control-plane sh -c ': > /var/log/kubernetes/audit.log'
./loadsim -scenario $SC -scale $SCALE -controller $IMG -out results -kubeconfig $KUBECONFIG -port $PORT > logs/$SC-$IMG-x$SCALE.log 2>&1
docker exec $CL-control-plane cat /var/log/kubernetes/audit.log > /tmp/audit-$SC-$IMG.log
python3 audit.py /tmp/audit-$SC-$IMG.log > results/$SC-$IMG-x$SCALE-api.json
for ns in sim-west sim-east; do
  kubectl -n $ns logs deploy/agent-k8s-controller --tail=-1 > logs/$SC-$IMG-x$SCALE-$ns-controller.log 2>&1
done
echo "done $OUT"
