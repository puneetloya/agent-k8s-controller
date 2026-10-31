#!/bin/bash
# ensure-docker.sh: install and start dockerd if needed (it dies with the shell that started it in some sandboxes).
command -v dockerd >/dev/null || { apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker.io >/dev/null; }
docker info >/dev/null 2>&1 && exit 0
(setsid dockerd > /tmp/dockerd.log 2>&1 &)
for i in $(seq 1 60); do docker info >/dev/null 2>&1 && exit 0; sleep 1; done
echo "dockerd did not start; see /tmp/dockerd.log" >&2; exit 1
