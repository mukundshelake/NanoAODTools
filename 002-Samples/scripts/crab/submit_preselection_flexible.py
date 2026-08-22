#!/usr/bin/env python3
"""
002-Samples/scripts/crab/submit_preselection_flexible.py
========================================================
Generate and (optionally) submit CRAB jobs for the preselection stage on
a user-specified era for DAS datasets.

Usage
-----
    # Dry run — print what would be submitted, no actual submission
    python3 002-Samples/scripts/crab/submit_preselection_flexible.py --era UL2016preVFP

    # Submit only Data jobs for a specific era
    python3 002-Samples/scripts/crab/submit_preselection_flexible.py --submit --include Data_mu --era UL2016preVFP

    # Submit all MC_mu jobs except QCD for a specific era
    python3 002-Samples/scripts/crab/submit_preselection_flexible.py --submit --include MC_mu --exclude QCD --era UL2016preVFP

    # Submit everything for a specific era
    python3 002-Samples/scripts/crab/submit_preselection_flexible.py --submit --era UL2016preVFP

Options
-------
    --submit       Actually submit to CRAB (default: dry run).
    --include      Regex matched against "group/key"; only matching pairs are processed.
    --exclude      Regex matched against "group/key"; matching pairs are skipped.
    --das-json     Path to the DAS input JSON. Default is constructed from era.
    --output-lfn   Base LFN for CRAB output on T2_CH_CERN. Default is constructed from era.
    --era          Data-taking era (e.g., UL2016preVFP, UL2017, UL2018). Required.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent.parent  # crab/ -> scripts/ -> 002-Samples/ -> repo root
CONFIG_YAML = SCRIPT_DIR.parent.parent / "config.yaml"  # 002-Samples/config.yaml
PSET        = SCRIPT_DIR / "PSet.py"
SCRIPT_SH   = SCRIPT_DIR / "crab_preselection.sh"
SCRIPT_PY   = SCRIPT_DIR / "crab_script_preselection.py"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def das_dataset_from_lfn(lfn: str) -> str:
    """
    Derive the DAS dataset path from a single file LFN.

    Data LFN:
        /store/data/{era}/{primary}/{tier}/{proc_tag}/...
        → /{primary}/{era}-{proc_tag}/{tier}

    MC LFN:
        /store/mc/{campaign}/{primary}/{tier}/{proc_tag}/...
        → /{primary}/{campaign}-{proc_tag}/{tier}
    """
    parts = lfn.lstrip("/").split("/")
    kind = parts[1]
    if kind == "data":
        era, primary, tier, proc_tag = parts[2], parts[3], parts[4], parts[5]
        return f"/{primary}/{era}-{proc_tag}/{tier}"
    else:  # mc
        campaign, primary, tier, proc_tag = parts[2], parts[3], parts[4], parts[5]
        return f"/{primary}/{campaign}-{proc_tag}/{tier}"

def safe_name(s: str) -> str:
    """Replace non-alphanumeric characters with underscores."""
    return re.sub(r"[^A-Za-z0-9_]", "_", s)

def make_crab_config(group, key, das_dataset, is_data, output_lfn, golden_json, era, work_area=None, sample=False):
    from WMCore.Configuration import Configuration  # noqa: PLC0415
    cfg = Configuration()
    request_name = f"presel_{safe_name(era)}_{safe_name(group)}_{safe_name(key)}"[:100]
    cfg.section_("General")
    cfg.General.requestName  = request_name
    cfg.General.transferLogs = True
    if work_area:
        cfg.General.workArea = work_area
    cfg.section_("JobType")
    cfg.JobType.pluginName       = "Analysis"
    cfg.JobType.psetName         = str(PSET)
    cfg.JobType.scriptExe        = str(SCRIPT_SH)
    cfg.JobType.inputFiles       = [str(SCRIPT_PY), str(CONFIG_YAML)]
    cfg.section_("Data")
    cfg.Data.inputDataset     = das_dataset
    cfg.Data.inputDBS         = "global"
    cfg.Data.publication      = False
    # era/group/key are already encoded in outLFNDirBase's directory nesting, so
    # repeating them here just eats into CRAB's 255-char LFN limit -- it pushed
    # some UL2016preVFP dataset paths over the limit and got them skipped by 003.
    cfg.Data.outputDatasetTag = "presel"
    cfg.Data.outLFNDirBase    = output_lfn
    if is_data:
        cfg.Data.splitting   = "FileBased"
        cfg.Data.unitsPerJob = 1
        cfg.Data.lumiMask    = str(golden_json)
    else:
        cfg.Data.splitting   = "FileBased"
        cfg.Data.unitsPerJob = 1
    if sample:
        # Unlike 003-I/II's Data.userInputFiles submissions (where --sample trims
        # the file list before submitting), this stage submits the whole
        # DBS-registered dataset via Data.inputDataset -- CRAB's own splitting
        # engine handles file enumeration, so there's no file list to trim here.
        # Data.totalUnits is CRAB's own knob for this: with FileBased splitting,
        # it caps how many files (units) get processed, same testing purpose.
        cfg.Data.totalUnits = 1
    cfg.section_("Site")
    cfg.Site.storageSite = "T3_CH_CERNBOX"

    cfg.JobType.scriptArgs = [f"era={era}"]

    return cfg

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate/submit CRAB preselection jobs for a user-specified era."
    )
    parser.add_argument(
        "--submit", action="store_true",
        help="Actually submit to CRAB (default: dry run).",
    )
    parser.add_argument(
        "--include",
        help='Regex applied to "group/key"; only matching pairs are processed.',
    )
    parser.add_argument(
        "--exclude",
        help='Regex applied to "group/key"; matching pairs are skipped.',
    )
    parser.add_argument(
        "--era", required=True,
        help="Data-taking era (e.g., UL2016preVFP, UL2017, UL2018). Required.",
    )
    parser.add_argument(
        "--das-json", required=True, 
        help="Path to the DAS input JSON. Default is constructed from era.",
    )
    parser.add_argument(
        "--output-lfn", required=True,
        help="Base LFN for CRAB output on T3_CH_CERNBOX.",
    )
    parser.add_argument(
        "--golden-json", required=True,
        help="Path to the golden JSON file. Required.",
    )
    parser.add_argument(
        "--work-area",
        help="Directory where CRAB project folders are created (default: current directory).",
    )
    parser.add_argument(
        "--sample", action="store_true",
        help="Restrict each submitted dataset to 1 file via Data.totalUnits (for testing purposes), "
             "same convention as --sample elsewhere in this chapter.",
    )
    args = parser.parse_args()

    das_json = args.das_json
    output_lfn = args.output_lfn.rstrip("/")
    golden_json = args.golden_json

    if not output_lfn.startswith("/store/"):
        print(
            f"ERROR: --output-lfn must be a /store/... LFN accepted by CRAB, "
            f"not an EOS or local path.\n       Got: {output_lfn}",
            file=sys.stderr,
        )
        sys.exit(1)

    include_pat = re.compile(args.include) if args.include else None
    exclude_pat = re.compile(args.exclude) if args.exclude else None

    # Sanity-check required files
    for path, label in [
        (SCRIPT_SH,   "crab_preselection.sh"),
        (SCRIPT_PY,   "crab_script_preselection.py"),
        (PSET,        "PSet.py"),
        (CONFIG_YAML, "config.yaml"),
        (golden_json, "golden JSON"),
    ]:
        if not Path(path).is_file():
            print(f"ERROR: required file not found: {path}  ({label})", file=sys.stderr)
            sys.exit(1)

    with open(das_json) as f:
        das_data = json.load(f)

    # Phase 1: collect job parameters (no WMCore needed)
    job_params = []
    for group, subgroups in das_data.items():
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
                first_lfn   = next(iter(files))
                das_dataset = das_dataset_from_lfn(first_lfn)
                is_data     = "Data" in group
                job_params.append((group, subgroup, key, das_dataset, is_data))
                tag = "[Data]" if is_data else "[MC  ]"
                sample_tag = " [SAMPLE]" if args.sample else ""
                print(f"  {'[SUBMIT]' if args.submit else '[DRY-RUN]'} {tag}{sample_tag} {label}")
                print(f"           DAS: {das_dataset}")

    print(f"\nTotal jobs: {len(job_params)}")

    if not args.submit:
        print("Dry run complete.  Pass --submit to submit to CRAB.")
        return

    # Phase 2: import CRAB client and submit
    try:
        from CRABAPI.RawCommand import crabCommand
    except ImportError:
        print(
            "ERROR: CRAB client not available.\n"
            "Run: source /cvmfs/cms.cern.ch/crab3/crab.sh",
            file=sys.stderr,
        )
        sys.exit(1)

    submitted, failed = 0, 0
    for group, subgroup, key, das_dataset, is_data in job_params:
        label = f"{group}/{subgroup}/{key}"
        try:
            cfg = make_crab_config(f"{group}_{subgroup}", key, das_dataset, is_data, output_lfn, golden_json, args.era, args.work_area, args.sample)
            crabCommand("submit", config=cfg)
            print(f"  Submitted: {label}")
            submitted += 1
        except Exception as e:
            print(f"  FAILED:    {label}  —  {e}")
            failed += 1

    print(f"\nDone: {submitted} submitted, {failed} failed.")
    if failed:
        # Non-zero exit so callers (run_all.py's subprocess.run check) don't report
        # "Successfully submitted" when some/all submissions actually failed.
        sys.exit(1)

if __name__ == "__main__":
    print("Running submit_preselection_flexible.py")
    main()
    print("Finished submit_preselection_flexible.py")
