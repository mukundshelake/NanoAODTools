#!/usr/bin/env python3
"""
003-ObjectSelectionI/scripts/crab/submit_selection_flexible.py
================================================================
Generate and (optionally) submit CRAB jobs for the object-selection stage,
processing the preselection-stage skims that already live on EOS.

Unlike 002-Samples' preselection CRAB job, the input here is not a
DBS-registered dataset (preselection output was published with
publication=False), so this uses Data.userInputFiles -- a plain LFN list --
instead of Data.inputDataset. The LFN list is built by translating the local
(EOS-mounted, on lxplus) file paths already present in
preselection_{era}_datasets.json into their /store/... equivalents.

Usage
-----
    # Dry run — print what would be submitted, no actual submission
    python3 submit_selection_flexible.py --era UL2018 \\
        --dataset-json inputs/preselection_UL2018_datasets.json \\
        --output-lfn /store/user/mshelake/DataFiles/selectionI/earlyApril/<hash>/UL2018 \\
        --work-area /tmp/crab_selection_UL2018

    # Submit only one dataset
    python3 submit_selection_flexible.py --submit --era UL2018 --include 'Data_mu/SingleMuon/Run2018A_mu' ...

Options
-------
    --submit         Actually submit to CRAB (default: dry run).
    --include        Regex matched against "DataMC/group/dataset"; only matching triples are processed.
    --exclude        Regex matched against "DataMC/group/dataset"; matching triples are skipped.
    --era            Data-taking era (e.g., UL2016preVFP, UL2017, UL2018). Required.
    --dataset-json   Path to preselection_{era}_datasets.json (from inputs/). Required.
    --golden-json    Path to the era's golden JSON (shipped to Data jobs only). Required if any Data job matches.
    --output-lfn     Base LFN for CRAB output on T3_CH_CERNBOX. Required.
    --work-area      Directory where CRAB project folders are created.
    --sample         Only submit the first file of each matching dataset (for testing).
"""

import argparse
import json
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))
import utils

REPO_ROOT   = SCRIPT_DIR.parent.parent.parent  # crab/ -> scripts/ -> 003-ObjectSelectionI/ -> repo root
CONFIG_YAML = SCRIPT_DIR.parent.parent / "config.yaml"  # 003-ObjectSelectionI/config.yaml
PSET        = SCRIPT_DIR / "PSet.py"
SCRIPT_SH   = SCRIPT_DIR / "crab_selection.sh"
SCRIPT_PY   = SCRIPT_DIR / "crab_script_selection.py"
MODULE_PY   = SCRIPT_DIR.parent / "modules" / "SelectedObjects.py"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_name(s: str) -> str:
    """Replace non-alphanumeric characters with underscores."""
    return re.sub(r"[^A-Za-z0-9_]", "_", s)


def make_crab_config(era, DataMC, group, key, lfn_files, is_data, output_lfn, golden_json, work_area=None):
    from WMCore.Configuration import Configuration  # noqa: PLC0415
    cfg = Configuration()
    request_name = f"sel_{safe_name(era)}_{safe_name(group)}_{safe_name(key)}"[:100]
    cfg.section_("General")
    cfg.General.requestName  = request_name
    cfg.General.transferLogs = True
    if work_area:
        cfg.General.workArea = work_area
    cfg.section_("JobType")
    cfg.JobType.pluginName = "Analysis"
    cfg.JobType.psetName   = str(PSET)
    cfg.JobType.scriptExe  = str(SCRIPT_SH)
    input_files = [str(SCRIPT_PY), str(MODULE_PY), str(CONFIG_YAML)]
    if is_data:
        input_files.append(str(golden_json))
    cfg.JobType.inputFiles  = input_files
    cfg.JobType.scriptArgs  = [f"era={era}", f"isData={is_data}"]
    cfg.section_("Data")
    cfg.Data.userInputFiles     = lfn_files
    cfg.Data.outputPrimaryDataset = safe_name(f"{group}_{key}")[:100]
    cfg.Data.splitting          = "FileBased"
    cfg.Data.unitsPerJob        = 1
    cfg.Data.publication        = False
    cfg.Data.outLFNDirBase      = output_lfn
    cfg.section_("Site")
    cfg.Site.storageSite = "T3_CH_CERNBOX"
    # Required for Data.userInputFiles: unlike a DBS-registered inputDataset, CRAB has
    # no location info for private files, so it refuses to submit without an explicit
    # whitelist. NOTE: Site.whitelist takes CMS *Processing Site* names (a different
    # registry from storage element names like T3_CH_CERNBOX above -- using the SE name
    # here gets a SUBMITREFUSED with "not in the list of known CMS Processing Site Names").
    # T2_CH_CERN is the processing site with access to CERN EOS (where our /store/user/...
    # input files physically live).
    cfg.Site.whitelist = ["T2_CH_CERN"]

    return cfg

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate/submit CRAB object-selection jobs for a user-specified era."
    )
    parser.add_argument("--submit", action="store_true",
                        help="Actually submit to CRAB (default: dry run).")
    parser.add_argument("--include", help='Regex applied to "DataMC/group/key"; only matching triples are processed.')
    parser.add_argument("--exclude", help='Regex applied to "DataMC/group/key"; matching triples are skipped.')
    parser.add_argument("--era", required=True,
                        help="Data-taking era (e.g., UL2016preVFP, UL2017, UL2018). Required.")
    parser.add_argument("--dataset-json", required=True,
                        help="Path to preselection_{era}_datasets.json listing the local/EOS-mounted "
                             "preselection skim file paths.")
    parser.add_argument("--golden-json", default=None,
                        help="Path to the era's golden JSON. Required if any matching job is Data.")
    parser.add_argument("--output-lfn", required=True,
                        help="Base LFN for CRAB output on T3_CH_CERNBOX.")
    parser.add_argument("--work-area",
                        help="Directory where CRAB project folders are created (default: current directory).")
    parser.add_argument("--sample", action="store_true",
                        help="Only submit the first file of each matching dataset (for testing purposes), "
                             "same convention as --sample elsewhere in this chapter.")
    args = parser.parse_args()

    output_lfn = args.output_lfn.rstrip("/")
    if not output_lfn.startswith("/store/"):
        print(
            f"ERROR: --output-lfn must be a /store/... LFN accepted by CRAB, "
            f"not an EOS or local path.\n       Got: {output_lfn}",
            file=sys.stderr,
        )
        sys.exit(1)

    include_pat = re.compile(args.include) if args.include else None
    exclude_pat = re.compile(args.exclude) if args.exclude else None

    for path, label in [
        (SCRIPT_SH,    "crab_selection.sh"),
        (SCRIPT_PY,    "crab_script_selection.py"),
        (MODULE_PY,    "SelectedObjects.py"),
        (PSET,         "PSet.py"),
        (CONFIG_YAML,  "config.yaml"),
        (args.dataset_json, "dataset JSON"),
    ]:
        if not Path(path).is_file():
            print(f"ERROR: required file not found: {path}  ({label})", file=sys.stderr)
            sys.exit(1)

    config = utils.load_config(CONFIG_YAML)
    storage_base = utils.resolve_storage_path(config)
    lfn_base = config["LFN_Base"].rstrip("/")

    with open(args.dataset_json) as f:
        dataset_data = json.load(f)

    # Phase 1: collect job parameters (no WMCore needed), translating local paths to LFNs
    job_params = []
    for DataMC, groups in dataset_data.items():
        for group, keys in groups.items():
            for key, files in keys.items():
                label = f"{DataMC}/{group}/{key}"
                if include_pat and not include_pat.search(label):
                    continue
                if exclude_pat and exclude_pat.search(label):
                    continue
                if not files:
                    print(f"  [SKIP] {label} (no files)")
                    continue
                is_data = DataMC.lower().startswith("data")
                file_list = list(files)[:1] if args.sample else list(files)
                try:
                    lfn_files = [utils.lfn_path_for_local_file(fp, storage_base, lfn_base) for fp in file_list]
                except ValueError as e:
                    print(f"  [SKIP] {label}: {e}")
                    continue

                # CRABServer hard-rejects (400 Bad Request) any Data.userInputFiles entry
                # over 255 chars. Filter those out here with a clear message instead of
                # letting the whole submission fail on an opaque server-side error.
                too_long = [lf for lf in lfn_files if len(lf) > 255]
                if too_long:
                    for lf in too_long:
                        print(f"  [SKIP FILE] {label}: LFN is {len(lf)} chars (>255 CRAB limit): {lf}")
                    lfn_files = [lf for lf in lfn_files if len(lf) <= 255]
                if not lfn_files:
                    print(f"  [SKIP] {label} (no files left under the 255-char LFN limit)")
                    continue
                job_params.append((DataMC, group, key, lfn_files, is_data))
                tag = "[Data]" if is_data else "[MC  ]"
                sample_tag = " [SAMPLE]" if args.sample else ""
                print(f"  {'[SUBMIT]' if args.submit else '[DRY-RUN]'} {tag}{sample_tag} {label} ({len(lfn_files)} files)")

    print(f"\nTotal jobs: {len(job_params)}")

    if any(is_data for *_, is_data in job_params) and not args.golden_json:
        print("ERROR: matching jobs include Data but --golden-json was not provided.", file=sys.stderr)
        sys.exit(1)
    if args.golden_json and not Path(args.golden_json).is_file():
        print(f"ERROR: --golden-json not found: {args.golden_json}", file=sys.stderr)
        sys.exit(1)

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
    for DataMC, group, key, lfn_files, is_data in job_params:
        label = f"{DataMC}/{group}/{key}"
        try:
            cfg = make_crab_config(args.era, DataMC, group, key, lfn_files, is_data,
                                    output_lfn, args.golden_json, args.work_area)
            crabCommand("submit", config=cfg)
            print(f"  Submitted: {label}")
            submitted += 1
        except Exception as e:
            print(f"  FAILED:    {label}  —  {e}")
            failed += 1

    print(f"\nDone: {submitted} submitted, {failed} failed.")

if __name__ == "__main__":
    print("Running submit_selection_flexible.py")
    main()
    print("Finished submit_selection_flexible.py")
