#!/usr/bin/env python3
"""
003-ObjectSelectionII/scripts/condor/checkCondorStatus.py
==================================================
Check status of HTCondor selectionII jobs submitted via
submit_selectionII_condor.py, for a given work area.

Condor jobs write their output directly to the final flat layout, so there
is no CRAB-style "Jobs status: finished/running/..." summary to parse --
status here is a straightforward cross-check: for every job recorded in
{work_area}/jobs.txt, does its output ROOT file exist at
{out_dir}/{job_id}.root? Jobs without an output file are further classified
via condor_q (idle/running/held) vs missing entirely (never ran, or ran and
failed to produce output with no trace left in the queue).

Note this makes "missing" ambiguous with "already verified and deleted after
transfer" -- if you've cleaned up a work area's output after confirming it
landed safely elsewhere, every one of those jobs will show up as "missing"
here too, since there's no local trace of them left to tell the two cases
apart. --resubmitMissing refuses to act on a work area where a large
fraction of jobs are missing at once (see the check in main()) for exactly
this reason; pass --forceResubmitMissing if you're sure they genuinely never
completed.

Usage:
    python3 checkCondorStatus.py -d <work_area> [--resubmitHeld] [--resubmitMissing] [--forceResubmitMissing]
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

STATUS_MAP = {"1": "idle", "2": "running", "3": "removed", "4": "completed", "5": "held", "6": "running"}


def read_manifest(manifest_path):
    jobs = []
    with open(manifest_path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            job_id, era, files_json, golden_json, is_data, out_dir, dataset = line.split("\t")
            jobs.append({
                "job_id": job_id, "era": era, "files_json": files_json,
                "golden_json": golden_json, "is_data": is_data, "out_dir": out_dir,
                "dataset": dataset,
            })
    return jobs


def condor_q_states(cluster_ids):
    """Return {job_id: state} for every job currently in the queue across the given clusters.

    eossubmit spreads submissions across ~40 different bigbird*/ce*.cern.ch
    schedds (confirmed live: two submissions from the same shell landed on
    different schedds), and plain `condor_q <id>` only queries whichever
    schedd this shell's environment happens to default to -- a status check
    from a different session can silently see "0 jobs" for a real, active
    cluster. `-global` scans every schedd, which is the only way to reliably
    find jobs regardless of which schedd they landed on.

    Also note: the ClassAd attribute for a job's command-line arguments is
    `Arguments`, not `Args` (confirmed live -- `-af Args` prints `undefined`
    for every job).
    """
    if not cluster_ids:
        return {}
    result = subprocess.run(
        ["condor_q"] + cluster_ids + ["-global", "-af", "Arguments", "JobStatus"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=180,
    )
    states = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        job_id = parts[0]
        status_code = parts[-1]
        states[job_id] = STATUS_MAP.get(status_code, status_code)
    return states


def main():
    parser = argparse.ArgumentParser(description="Check status of condor selectionII jobs.")
    parser.add_argument("-d", "--work-area", required=True)
    parser.add_argument("--resubmitHeld", action="store_true",
                         help="condor_release all held jobs in this work area's clusters.")
    parser.add_argument("--resubmitMissing", action="store_true",
                         help="Re-submit jobs whose output is missing and not currently queued.")
    parser.add_argument("--forceResubmitMissing", action="store_true",
                         help="Bypass the large-batch safety check below and resubmit "
                              "regardless of how many jobs are missing.")
    args = parser.parse_args()

    work_area = Path(args.work_area)
    manifest_path = work_area / "jobs.txt"
    cluster_ids_path = work_area / "cluster_ids.txt"
    submit_path = work_area / "selectionII.sub"

    if not manifest_path.exists():
        print(f"ERROR: no jobs.txt manifest found at {manifest_path}", file=sys.stderr)
        sys.exit(1)

    jobs = read_manifest(manifest_path)
    cluster_ids = [l.strip() for l in open(cluster_ids_path)] if cluster_ids_path.exists() else []
    cluster_ids = [c for c in cluster_ids if c]

    queue_states = condor_q_states(cluster_ids)

    per_dataset = {}
    held_jobs, missing_jobs = [], []
    for job in jobs:
        out_path = Path(job["out_dir"]) / f"{job['job_id']}.root"
        label = job["out_dir"]
        counts = per_dataset.setdefault(label, {"total": 0, "done": 0, "idle": 0, "running": 0, "held": 0, "missing": 0})
        counts["total"] += 1
        if out_path.exists():
            counts["done"] += 1
            continue
        state = queue_states.get(job["job_id"])
        if state in ("idle", "running"):
            counts[state] += 1
        elif state == "held":
            counts["held"] += 1
            held_jobs.append(job)
        else:
            counts["missing"] += 1
            missing_jobs.append(job)

    total = {"total": 0, "done": 0, "idle": 0, "running": 0, "held": 0, "missing": 0}
    for label, counts in sorted(per_dataset.items()):
        print(f"{label}: {counts['done']}/{counts['total']} done  "
              f"(idle={counts['idle']} running={counts['running']} held={counts['held']} missing={counts['missing']})")
        for k in total:
            total[k] += counts[k]

    print(f"\nTOTAL: {total['done']}/{total['total']} done  "
          f"(idle={total['idle']} running={total['running']} held={total['held']} missing={total['missing']})")

    if args.resubmitHeld and held_jobs:
        print(f"\nReleasing {len(held_jobs)} held job(s) across cluster(s) {cluster_ids}...")
        subprocess.run(["condor_release"] + cluster_ids)

    if args.resubmitMissing and missing_jobs:
        # Safety check: "missing" here means "no output file at the expected
        # location" -- which is indistinguishable, from this script's point of
        # view, between "job never ran / died without a trace" and "job
        # completed fine, output was verified and transferred elsewhere, and
        # the local copy was deliberately deleted." A large fraction of a
        # work area going missing all at once is a strong signal of the
        # latter, not a real wall-time-kill wave (those, observed in
        # practice, top out around a few percent of a work area's jobs, not
        # a majority of them). Concretely: this check exists because
        # --resubmitMissing was once run right after a batch of verified,
        # already-transferred output was cleaned up, and it silently queued
        # ~3400 reprocessing jobs for data that already existed safely
        # elsewhere -- caught and killed within seconds, but only because
        # someone happened to be watching the queue right after submitting.
        missing_fraction = len(missing_jobs) / total["total"] if total["total"] else 0
        if not args.forceResubmitMissing and len(missing_jobs) > 25 and missing_fraction > 0.4:
            affected_labels = sorted({j["out_dir"] for j in missing_jobs})
            print(f"\nERROR: {len(missing_jobs)}/{total['total']} jobs "
                  f"({missing_fraction:.0%}) are missing -- refusing to auto-resubmit.")
            print("This usually means the output for these datasets was already "
                  "verified and deleted (not that the jobs actually failed). "
                  "Affected dataset(s):")
            for label in affected_labels:
                print(f"  {label}")
            print("If these genuinely never completed, rerun with --forceResubmitMissing "
                  "to proceed anyway.")
            return
        print(f"\nResubmitting {len(missing_jobs)} missing job(s) via a fresh queue-from-file cluster...")
        if not submit_path.exists():
            print(f"ERROR: original submit file not found at {submit_path}, cannot resubmit.", file=sys.stderr)
            return
        resub_manifest = work_area / "resubmit_jobs.txt"
        with open(resub_manifest, "w") as f:
            for j in missing_jobs:
                f.write("\t".join([j["job_id"], j["era"], j["files_json"], j["golden_json"], j["is_data"], j["out_dir"], j["dataset"]]) + "\n")
        submit_txt = open(submit_path).read()
        lines = [
            l if not l.startswith("queue ")
            else f"queue job_id,era,files_json,golden_json,is_data,out_dir,dataset from {resub_manifest}"
            for l in submit_txt.splitlines()
        ]
        resub_submit = work_area / "resubmit.sub"
        with open(resub_submit, "w") as f:
            f.write("\n".join(lines) + "\n")
        result = subprocess.run(["condor_submit", str(resub_submit)], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr, file=sys.stderr)
            return
        m = re.search(r"submitted to cluster (\d+)", result.stdout)
        if m:
            with open(cluster_ids_path, "a") as f:
                f.write(m.group(1) + "\n")
            print(f"Cluster ID: {m.group(1)}")


if __name__ == "__main__":
    main()
