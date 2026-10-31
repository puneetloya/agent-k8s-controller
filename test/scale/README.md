# Agent controller scale tests

Compares two builds of the Semaphore agent-k8s-controller under load, side by side, in kind clusters with KWOK nodes.
`loadsim` plays Semaphore (job queue API), the agents and the autoscaler, and checks that the controller respects
`MAX_PARALLEL_JOBS` and `MAX_PENDING_JOBS`.

## Run

```sh
sudo ./bootstrap.sh                                  # once per machine; BEFORE_REV/NEW_REV pick the builds
./pair.sh step agent-loss                            # 1x scale
WEST_PARALLEL=3000 ./pair.sh -s 10 parallel-bound    # 10x scale, custom west limit
./summarize.py -s 10 parallel-bound
```

A 10x pair takes 10–25 minutes and needs about 16 CPUs. To finish in under an hour, run one or two scenarios per machine.

## Scenarios

| Name | What it does |
|---|---|
| step | burst of jobs at once |
| parallel-bound | small long jobs; `MAX_PARALLEL_JOBS` binds before CPU |
| sustained | arrivals above capacity for 8 min |
| lull-surge | quiet, then a surge |
| agent-loss | half the agents die while their job pod waits for a node (orphans) |
| restart | controller restarted mid-run |
| faults | Semaphore API hangs mid-run |
| uneven | one agent type dominates |
| overflow500 | burst of small jobs on one agent type |

## Output

- `results/<scenario>-<build>-x<scale>.json`: jobs, queue waits, per-region peaks and limit violations.
- `results/...-api.json`: controller API requests per second, from the API server audit log.
- `logs/`: simulator and controller logs.

## Known artifacts

- In `agent-loss`, "unfinished" includes jobs whose agents were killed on purpose.
- Up to 3 connection-refused errors at start: the stub API starts after the controller.
- The stub cert lasts 30 days; rerun `bootstrap.sh` to renew it.

