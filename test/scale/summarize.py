#!/usr/bin/env python3
"""summarize.py [-s scale] <scenario>...: one comparison row per scenario and build from results/ and logs/."""
import json, os, sys

args = sys.argv[1:]
scale = 1
if args[:1] == ["-s"]:
    scale, args = int(args[1]), args[2:]
os.chdir(os.path.dirname(os.path.abspath(__file__)))

for sc in args:
    for img in ("before", "new"):
        base = f"{sc}-{img}-x{scale}"
        if not os.path.exists(f"results/{base}.json"):
            print(json.dumps(dict(sc=sc, img=img, missing=True)))
            continue
        r = json.load(open(f"results/{base}.json"))
        api = json.load(open(f"results/{base}-api.json")).get("sim-west", {})
        w, e = r["regions"]["west"], r["regions"]["east"]
        log = open(f"logs/{base}-sim-west-controller.log").read()
        print(json.dumps(dict(
            sc=sc, img=img, jobs=r["jobs"], ran=r["jobsRan"], never=r["jobsNeverClaimed"], unfinished=r["jobsUnfinished"],
            p50=round(r["queueWaitP50s"]), p99=round(r["queueWaitP99s"]), max=round(r["queueWaitMaxs"]),
            wMaxPar=w["maxParallel"], wPeakActive=w["peakActiveJobs"], wOverPar=w["samplesOverMaxParallel"],
            wCap=w["maxPending"], wPeakPend=w["peakPending"], wPeakWait=w["peakWaitingJobPods"], wOverCap=w["samplesPendingOverCap"],
            wCreatedAtCap=w["agentJobsCreatedWhilePendingAtOrOverCap"], wOrphansMade=w["orphanedJobPodsMade"],
            eCap=e["maxPending"], ePeakPend=e["peakPending"], eOverCap=e["samplesPendingOverCap"],
            apiPeak=api.get("peakRequestsPerSecond"),
            reachedPending=log.count("Reached limit of pending"), orphansDeleted=log.count("Deleted job pod with no agent"),
        )))
