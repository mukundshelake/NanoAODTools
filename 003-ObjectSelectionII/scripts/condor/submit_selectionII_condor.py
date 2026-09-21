#!/usr/bin/env python3
"""
003-ObjectSelectionII/scripts/condor/submit_selectionII_condor.py
================================================================
Generate and (optionally) submit HTCondor jobs for the weight/SF
stage, as a parallel alternative to the CRAB path
(crab/submit_selectionII_flexible.py).

Both paths read the same selectionI skims and write the same
{STORAGE}/selectionII/{tag}/{hash}/{era}/{DataMC}/{group}/{dataset}/ layout,
so everything downstream works unchanged either way. Unlike the CRAB path
there is no LFN translation and no sandbox: everything the job touches lives
on EOS and is referenced by absolute path (nothing condor-transferred).
CERN's eossubmit schedds (module load lxbatch/eossubmit) reject /eos paths in
transfer_input_files, and per-job output/error/log filenames may only vary by
$(Cluster) -- reading and writing EOS directly from inside the job sidesteps
both, since EOS is a normal mounted filesystem on lxbatch worker nodes.

No grid proxy is needed here, unlike 002-Samples' condor path. That stage
reads DBS-registered inputs through the xrootd global redirector, which
requires grid auth against whichever site holds each file; this stage reads
private skims that are already on EOS.

Usage
-----
    # Dry run -- print what would be submitted, no actual submission
    python3 003-ObjectSelectionII/scripts/condor/submit_selectionII_condor.py \\
        --era UL2017 --dataset-json <path to preselection_UL2017_datasets.json> \\
        --golden-json <path> \\
        --output-dir <STORAGE>/selectionI/<tag>/<hash>/UL2017 \\
        --work-area <STORAGE>/condor_work_selectionI/<tag>/<hash>/UL2017

    # Actually submit
    ... --submit
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CHAPTER_DIR = SCRIPT_DIR.parent.parent          # condor/ -> scripts/ -> 003-ObjectSelectionII/
CONFIG_YAML = CHAPTER_DIR / "config.yaml"
MODULES_DIR = SCRIPT_DIR.parent / "modules"
WRAPPER_SH = SCRIPT_DIR / "condor_selectionII.sh"
WORKER_PY = SCRIPT_DIR / "condor_script_selectionII.py"

# Keyed by the names config.yaml's ModuleList uses. The wrapper copies the
# whole modules/ directory into the job's scratch, so this map is not what
# selects files -- it is the check that every ModuleList entry is actually
# known to the condor path, failing at submit time rather than letting jobs
# run and produce output that differs from the local path's.
MODULE_FILES = {
    "lheWeightSign": MODULES_DIR / "LHEWeightSign.py",
    "muonID":        MODULES_DIR / "MuonIDWeight.py",
    "muonIso":       MODULES_DIR / "MuonIsoWeight.py",
    "muonHLT":       MODULES_DIR / "MuonHLTWeight.py",
    "bTagging":      MODULES_DIR / "bTaggingWeight.py",
    "puWeight":      MODULES_DIR / "PUWeight.py",
    "topPtWeight":   MODULES_DIR / "TopPtWeight.py",
}


def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", s)


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def main():
    parser = argparse.ArgumentParser(
        description="Generate/submit HTCondor selectionII (weights/SF) jobs for one era."
    )
    parser.add_argument("--submit", action="store_true",
                        help="Actually submit to condor (default: dry run).")
    parser.add_argument("--include", help='Regex matched against "DataMC/group/dataset".')
    parser.add_argument("--exclude", help='Regex matched against "DataMC/group/dataset".')
    parser.add_argument("--era", required=True)
    parser.add_argument("--dataset-json", required=True,
                        help="preselection_{era}_datasets.json, or the "
                             "preSelectionCorrected_{era}_datasets.json produced by "
                             "--applyPreSelectionCorrections (preferred when it exists, "
                             "same as the local and CRAB paths).")
    parser.add_argument("--golden-json", required=True)
    parser.add_argument("--output-dir", required=True,
                        help="Selection output base for this era, e.g. "
                             "{STORAGE}/selectionII/{tag}/{hash}/{era}. Must be on EOS.")
    parser.add_argument("--work-area", required=True,
                        help="EOS directory holding per-job files.json manifests, the condor "
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
        (WRAPPER_SH, "condor_selectionII.sh"),
        (WORKER_PY, "condor_script_selectionII.py"),
        (CONFIG_YAML, "config.yaml"),
        (args.dataset_json, "dataset JSON"),
        (args.golden_json, "golden JSON"),
    ] + [(p, f"module: {name}") for name, p in MODULE_FILES.items()]:
        if not Path(path).is_file():
            print(f"ERROR: required file not found: {path}  ({label})", file=sys.stderr)
            sys.exit(1)

    for path, label in [(args.output_dir, "--output-dir"), (args.work_area, "--work-area")]:
        if not str(path).startswith("/eos/"):
            print(f"ERROR: {label} must be an EOS path (eossubmit requirement). Got: {path}",
                  file=sys.stderr)
            sys.exit(1)

    with open(CONFIG_YAML) as f:
        import yaml  # noqa: PLC0415 -- only needed here, keeps the import cost off dry runs
        config = yaml.safe_load(f)

    # Fail before queueing anything if ModuleList names something the condor
    # worker cannot build (see MODULE_FILES' comment).
    for data_mc_key in ("MC", "Data"):
        for mod_name in config["ModuleList"].get(data_mc_key, []):
            if mod_name not in MODULE_FILES:
                print(f"ERROR: ModuleList.{data_mc_key} names '{mod_name}', which the condor "
                      f"path does not know how to run. Add it to MODULE_FILES here and to "
                      f"condor_script_selectionII.py's build_modules().", file=sys.stderr)
                sys.exit(1)

    # metXYCorr and friends read correction files out of the chapter directory by
    # absolute path from inside the job; a missing one would fail every job
    # individually, so check once here instead.
    for data_mc_key in ("MC", "Data"):
        for mod_name in config["ModuleList"].get(data_mc_key, []):
            raw = config["Modules"].get(mod_name, {})
            mod_cfg = raw.get(args.era, raw)
            for file_key in ("IDSFFile", "HLTSFFile", "bTagSFFile", "StandardSFFile",
                             "TightIsoSFFile", "PUSFFile"):
                if file_key in mod_cfg:
                    sf_path = CHAPTER_DIR / mod_cfg[file_key]
                    if not sf_path.is_file():
                        print(f"ERROR: {mod_name}: correction file not found: {sf_path}\n"
                              f"       Run run_all.py --fetchSFFiles --filter {args.era} first.",
                              file=sys.stderr)
                        sys.exit(1)

    include_pat = re.compile(args.include) if args.include else None
    exclude_pat = re.compile(args.exclude) if args.exclude else None

    with open(args.dataset_json) as f:
        dataset_data = json.load(f)

    output_dir = Path(args.output_dir)
    work_area = Path(args.work_area)
    jobs_dir = work_area / "jobs"
    logs_dir = work_area / "logs"

    # Phase 1: collect job specs (no condor/CMSSW needed yet).
    job_specs = []  # (job_id, files, is_data, out_dir, dataset)
    for data_mc, groups in dataset_data.items():
        is_data = data_mc.lower().startswith("data")
        for group, datasets in groups.items():
            for dataset, files in datasets.items():
                label = f"{data_mc}/{group}/{dataset}"
                if include_pat and not include_pat.search(label):
                    continue
                if exclude_pat and exclude_pat.search(label):
                    continue
                filepaths = sorted(files.keys()) if isinstance(files, dict) else sorted(files)
                if not filepaths:
                    print(f"  [SKIP] {label} (no files)")
                    continue
                if not is_data and "bTagging" in config["ModuleList"].get("MC", []):
                    bt_raw = config["Modules"].get("bTagging", {})
                    bt_cfg = bt_raw.get(args.era, bt_raw)
                    eff = CHAPTER_DIR / bt_cfg["efficiencyFolder"] / args.era / f"{dataset}.root"
                    if not eff.is_file():
                        print(f"ERROR: bTagging efficiency map missing for {label}: {eff}\n"
                              f"       Run run_all.py --computeBTaggingEfficiency --filter "
                              f"{args.era} first.", file=sys.stderr)
                        sys.exit(1)
                dataset_out_dir = output_dir / data_mc / group / dataset
                chunks = list(chunked(filepaths, args.files_per_job))
                if args.sample:
                    chunks = chunks[:1]
                for idx, file_chunk in enumerate(chunks):
                    job_id = f"{safe_name(data_mc)}_{safe_name(group)}_{safe_name(dataset)}_{idx}"
                    job_specs.append((job_id, file_chunk, is_data, dataset_out_dir, dataset))
                tag = "[Data]" if is_data else "[MC  ]"
                sample_tag = " [SAMPLE]" if args.sample else ""
                print(f"  {'[SUBMIT]' if args.submit else '[DRY-RUN]'} {tag}{sample_tag} {label}: "
                      f"{len(filepaths)} files -> {len(chunks)} job(s)")

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
    submit_path = work_area / "selectionII.sub"

    golden_abs = str(Path(args.golden_json).resolve())
    manifest_lines = []
    for job_id, file_chunk, is_data, dataset_out_dir, dataset in job_specs:
        job_dir = jobs_dir / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        files_json_path = job_dir / "files.json"
        with open(files_json_path, "w") as f:
            json.dump(file_chunk, f)
        dataset_out_dir.mkdir(parents=True, exist_ok=True)
        manifest_lines.append("\t".join([
            job_id, args.era, str(files_json_path), golden_abs if is_data else "NONE",
            "1" if is_data else "0", str(dataset_out_dir), dataset,
        ]))

    with open(manifest_path, "w") as f:
        f.write("\n".join(manifest_lines) + "\n")

    # transfer_output_files = "" is load-bearing: with should_transfer_files=YES and no
    # explicit list, HTCondor auto-transfers every new file in the job's scratch back to
    # wherever condor_submit ran from. condor_selection.sh already copies its output to
    # $OUT_DIR itself, so without this every completed job's skim lands a second time in
    # the submitting directory (002-Samples hit exactly this: 2714 stray files, 234G).
    submit_txt = f"""universe = vanilla
executable = {WRAPPER_SH}
arguments = "$(job_id) $(era) {CONFIG_YAML} $(files_json) $(golden_json) $(is_data) $(out_dir) {WORKER_PY} {CHAPTER_DIR} {MODULES_DIR} $(dataset)"
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
transfer_output_files = ""
output = {logs_dir}/$(job_id).out
error = {logs_dir}/$(job_id).err
log = {logs_dir}/$(Cluster).log
+JobFlavour = "{args.job_flavour}"
request_cpus = 1
request_memory = 2000M
queue job_id,era,files_json,golden_json,is_data,out_dir,dataset from {manifest_path}
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
    print("Running submit_selectionII_condor.py")
    main()
    print("Finished submit_selectionII_condor.py")
