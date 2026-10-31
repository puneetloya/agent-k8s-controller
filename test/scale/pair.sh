#!/bin/bash
# pair.sh [-s scale] <scenario>...: each scenario on both builds at once (current on "before", new on "new").
cd "$(dirname "$0")"
SCALE=1; [ "$1" = "-s" ] && { SCALE=$2; shift 2; }
./ensure-docker.sh || exit 1
for sc in "$@"; do
  ./run.sh $sc before before 8443 $SCALE &
  ./run.sh $sc new new 8444 $SCALE &
  wait
done
echo PAIR-DONE "$@"
