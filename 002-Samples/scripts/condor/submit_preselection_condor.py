#!/usr/bin/env python3
"""
002-Samples/scripts/condor/submit_preselection_condor.py
==========================================================
Generate and (optionally) submit HTCondor jobs for the preselection stage,
as a parallel alternative to the CRAB path (submit_preselection_flexible.py).

Unlike CRAB, condor jobs write their output directly into the final flat
layout that --generatePreselectionDatasetJSON expects
({STORAGE}/preselection/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/
{filename}) -- there is no raw-tree/consolidation step for the condor path;
--generateCrabDatasetJSON / --consolidateCrabOutput are CRAB-only.

Everything the job touches lives on EOS and is referenced by absolute path
(nothing condor-transferred): CERN's eossubmit schedds (module load
lxbatch/eossubmit) reject /eos paths in transfer_input_files / working-dir
semantics, and per-job output/error/log filenames may only vary by
$(Cluster), not per-job -- both confirmed via live test jobs. Reading/writing
EOS directly by absolute path from inside the job sidesteps both
restrictions entirely (EOS is a normal mounted filesystem on lxbatch worker
nodes).

Usage
-----
    # Dry run — print what would be submitted, no actual submission
    python3 002-Samples/scripts/condor/submit_preselection_condor.py \\
        --era UL2016preVFP --das-json <path> --golden-json <path> \\
        --output-dir <STORAGE>/preselection/<tag>/<hash>/UL2016preVFP \\
        --work-area <STORAGE>/condor_work/<tag>/<hash>/UL2016preVFP

    # Actually submit
    ... --submit
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_YAML = SCRIPT_DIR.parent.parent / "config.yaml"
WRAPPER_SH = SCRIPT_DIR / "condor_preselection.sh"
WORKER_PY = SCRIPT_DIR / "condor_script_preselection.py"


def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", s)


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def main():
    parser = argparse.ArgumentParser(
        description="Generate/submit HTCondor preselection jobs for a user-specified era."
    )
    parser.add_argument("--submit", action="store_true",
                         help="Actually submit to condor (default: dry run).")
    parser.add_argument("--include", help='Regex matched against "DataMC/group/key".')
    parser.add_argument("--exclude", help='Regex matched against "DataMC/group/key".')
    parser.add_argument("--era", required=True)
    parser.add_argument("--das-json", required=True,
                         help="Path to DAS_{era}_dataset.json (from --generateDASDatasetJSON).")
    parser.add_argument("--golden-json", required=True)
    parser.add_argument("--output-dir", required=True,
                         help="Final flat preselection output base for this era, e.g. "
                              "{STORAGE}/preselection/{tag}/{config_hash}/{era}. Must be on EOS.")
    parser.add_argument("--work-area", required=True,
                         help="EOS directory to hold per-job files.json manifests, the condor "
                              "submit file, and logs. Must be on EOS (eossubmit requirement).")
    parser.add_argument("--files-per-job", type=int, default=1,
                         help="How many input files each condor job processes (default 1, "
                              "matching CRAB's Data.unitsPerJob=1).")
    parser.add_argument("--job-flavour", default="workday",
                         help='HTCondor +JobFlavour (default "workday", ~1 day walltime cap).')
    parser.add_argument("--sample", action="store_true",
                         help="Restrict each dataset to its first job only (for testing).")
    args = parser.parse_args()

    for path, label in [
        (WRAPPER_SH, "condor_preselection.sh"),
        (WORKER_PY, "condor_script_preselection.py"),
        (CONFIG_YAML, "config.yaml"),
        (args.golden_json, "golden JSON"),
    ]:
        if not Path(path).is_file():
            print(f"ERROR: required file not found: {path}  ({label})", file=sys.stderr)
            sys.exit(1)

    for path, label in [(args.output_dir, "--output-dir"), (args.work_area, "--work-area")]:
        if not str(path).startswith("/eos/"):
            print(f"ERROR: {label} must be an EOS path (eossubmit requirement). Got: {path}", file=sys.stderr)
            sys.exit(1)

    include_pat = re.compile(args.include) if args.include else None
    exclude_pat = re.compile(args.exclude) if args.exclude else None

    with open(args.das_json) as f:
        das_data = json.load(f)

    output_dir = Path(args.output_dir)
    work_area = Path(args.work_area)
    jobs_dir = work_area / "jobs"
    logs_dir = work_area / "logs"

    # Phase 1: collect job specs (no condor/CMSSW needed yet).
    job_specs = []  # (job_id, era, files, is_data, out_dir)
    for group, subgroups in das_data.items():
        is_data = "Data" in group
        for subgroup, keys in subgroups.items():
            for key, files in keys.items():
                label = f"{group}/{subgroup}/{key}"
                if include_pat and not include_pat.search(label):
                    continue
                if exclude_pat and exclude_pat.search(label):
                    continue
                if not files:
                    print(f"  [SKIP] {label} (no files)")
                    continue
                lfns = sorted(files.keys())
                dataset_out_dir = output_dir / group / subgroup / key
                chunks = list(chunked(lfns, args.files_per_job))
                if args.sample:
                    chunks = chunks[:1]
                for idx, file_chunk in enumerate(chunks):
                    job_id = f"{safe_name(group)}_{safe_name(subgroup)}_{safe_name(key)}_{idx}"
                    job_specs.append((job_id, label, file_chunk, is_data, dataset_out_dir))
                tag = "[Data]" if is_data else "[MC  ]"
                sample_tag = " [SAMPLE]" if args.sample else ""
                print(f"  {'[SUBMIT]' if args.submit else '[DRY-RUN]'} {tag}{sample_tag} {label}: "
                      f"{len(lfns)} files -> {len(chunks)} job(s)")

    print(f"\nTotal jobs: {len(job_specs)}")
    if not job_specs:
        print("Nothing to submit.")
        return

    if not args.submit:
        print("Dry run complete.  Pass --submit to submit to condor.")
        return

    # Phase 2: materialize per-job files.json manifests + the batch manifest,
    # then submit as a single `queue ... from` cluster.
    jobs_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = work_area / "jobs.txt"
    submit_path = work_area / "preselection.sub"

    manifest_lines = []
    for job_id, label, file_chunk, is_data, dataset_out_dir in job_specs:
        job_dir = jobs_dir / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        files_json_path = job_dir / "files.json"
        with open(files_json_path, "w") as f:
            json.dump(file_chunk, f)
        dataset_out_dir.mkdir(parents=True, exist_ok=True)
        golden = str(Path(args.golden_json).resolve()) if is_data else "NONE"
        manifest_lines.append("\t".join([
            job_id, args.era, str(files_json_path), golden,
            "1" if is_data else "0", str(dataset_out_dir),
        ]))

    with open(manifest_path, "w") as f:
        f.write("\n".join(manifest_lines) + "\n")

    # Input files are read via the xrootd global redirector (root://cms-xrd-global.cern.ch/),
    # which requires grid auth against the site that actually holds each file -- without a
    # proxy, TFile::Open loops through redirects and fails with "Redirect limit has been
    # reached" (confirmed via a live test job). `x509userproxy` has HTCondor transfer the
    # proxy to the job and set X509_USER_PROXY there; this is unrelated to eossubmit's EOS
    # path restrictions -- it's the standard CERN grid-job proxy mechanism.
    #
    # BUT: the proxy's default location (/tmp/x509up_u<uid>) is local to the lxplus *login*
    # node, and the eossubmit schedd's "access point" (a separate node, e.g. bigbird103) has
    # no access to it -- confirmed live: the job held immediately with "Transfer input files
    # failure ... reading from file /tmp/x509up_u<uid>: No such file or directory". Same class
    # of bug as local /tmp not being visible to the schedd for job files in general (see
    # README.md) -- copy the proxy onto EOS (under work_area) and point x509userproxy there.
    src_proxy_path = os.environ.get('X509_USER_PROXY') or f'/tmp/x509up_u{os.getuid()}'
    if not Path(src_proxy_path).is_file():
        print(f"ERROR: no grid proxy found at {src_proxy_path}. Run voms-proxy-init --voms cms first.", file=sys.stderr)
        sys.exit(1)
    work_area.mkdir(parents=True, exist_ok=True)
    proxy_path = work_area / "user_proxy"
    shutil.copy2(src_proxy_path, proxy_path)
    os.chmod(proxy_path, 0o600)

    # transfer_output_files = "" is load-bearing: with should_transfer_files=YES and no
    # explicit list, HTCondor's default is to auto-transfer *every new file* found in the
    # job's scratch sandbox back to wherever `condor_submit` was invoked from (there's no
    # initialdir here, and no reason to want one) -- condor_preselection.sh already copies
    # its *_Skim.root output to $OUT_DIR itself via `cp`, so that same file was ALSO getting
    # auto-transferred a second time into whatever directory this script's condor_submit call
    # happened to run from (in practice: 002-Samples/, since that's the run_all.py cwd),
    # silently doubling storage for every completed job. Confirmed live: 2714 stray files,
    # 234G, sitting in 002-Samples/ from the first production run before this was caught.
    submit_txt = f"""universe = vanilla
executable = {WRAPPER_SH}
arguments = "$(job_id) $(era) {CONFIG_YAML} $(files_json) $(golden_json) $(is_data) $(out_dir) {WORKER_PY}"
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
transfer_output_files = ""
x509userproxy = {proxy_path}
output = {logs_dir}/$(job_id).out
error = {logs_dir}/$(job_id).err
log = {logs_dir}/$(Cluster).log
+JobFlavour = "{args.job_flavour}"
request_cpus = 1
request_memory = 2000M
queue job_id,era,files_json,golden_json,is_data,out_dir from {manifest_path}
"""
    with open(submit_path, "w") as f:
        f.write(submit_txt)

    print(f"Wrote {len(job_specs)} job manifests under {jobs_dir}")
    print(f"Wrote submit file: {submit_path}")

    cmd = ["condor_submit", str(submit_path)]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(1)

    m = re.search(r"submitted to cluster (\d+)", result.stdout)
    if m:
        cluster_id = m.group(1)
        with open(work_area / "cluster_ids.txt", "a") as f:
            f.write(cluster_id + "\n")
        print(f"Cluster ID: {cluster_id}")
    else:
        print("WARNING: could not parse cluster ID from condor_submit output.", file=sys.stderr)


if __name__ == "__main__":
    print("Running submit_preselection_condor.py")
    main()
    print("Finished submit_preselection_condor.py")
