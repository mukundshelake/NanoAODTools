#!/usr/bin/env python3
"""
003-ObjectSelectionII/scripts/crab/submit_selectionII_flexible.py
====================================================================
Generate and (optionally) submit CRAB jobs for the scale-factor weight
stage, processing the selectionI skims that already live on EOS (produced
either locally or via 003-ObjectSelectionI's own CRAB support).

Same Data.userInputFiles approach as 003-ObjectSelectionI's
submit_selection_flexible.py, for the same reason: selectionI output is not
DBS-registered. See that script's docstring for the full rationale.

Usage
-----
    # Dry run — print what would be submitted, no actual submission
    python3 submit_selectionII_flexible.py --era UL2018 \\
        --dataset-json inputs/selectionI_UL2018_datasets.json \\
        --golden-json inputs/UL2018_goldenJSON.json \\
        --output-lfn /store/user/mshelake/DataFiles/selectionII/earlyApril/<hash>/UL2018 \\
        --work-area /tmp/crab_selectionII_UL2018

    # Submit only one dataset, one file each (testing)
    python3 submit_selectionII_flexible.py --submit --sample --era UL2018 \\
        --include 'MC_mu/SingleTop/Tchannel' ...

Options
-------
    --submit         Actually submit to CRAB (default: dry run).
    --sample         Only submit the first file of each matching dataset.
    --include        Regex matched against "DataMC/group/dataset"; only matching triples are processed.
    --exclude        Regex matched against "DataMC/group/dataset"; matching triples are skipped.
    --era            Data-taking era (e.g., UL2016preVFP, UL2017, UL2018). Required.
    --dataset-json   Path to selectionI_{era}_datasets.json (from inputs/). Required.
    --golden-json    Path to the era's golden JSON (shipped to Data jobs only). Required if any Data job matches.
    --output-lfn     Base LFN for CRAB output on T3_CH_CERNBOX. Required.
    --work-area      Directory where CRAB project folders are created.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))
import utils

CHAPTER_DIR = SCRIPT_DIR.parent.parent           # 003-ObjectSelectionII/
CONFIG_YAML = CHAPTER_DIR / "config.yaml"
PSET        = SCRIPT_DIR / "PSet.py"
SCRIPT_SH   = SCRIPT_DIR / "crab_selectionII.sh"
SCRIPT_PY   = SCRIPT_DIR / "crab_script_selectionII.py"
MODULES_DIR = CHAPTER_DIR / "scripts" / "modules"
MODULE_FILES = {
    "lheWeightSign": MODULES_DIR / "LHEWeightSign.py",
    "muonID":        MODULES_DIR / "MuonIDWeight.py",
    "muonIso":       MODULES_DIR / "MuonIsoWeight.py",
    "muonHLT":       MODULES_DIR / "MuonHLTWeight.py",
    "bTagging":      MODULES_DIR / "bTaggingWeight.py",
    "puWeight":      MODULES_DIR / "PUWeight.py",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_name(s: str) -> str:
    """Replace non-alphanumeric characters with underscores."""
    return re.sub(r"[^A-Za-z0-9_]", "_", s)


def make_crab_config(era, DataMC, group, key, lfn_files, is_data, output_lfn,
                      golden_json, config, work_area=None):
    from WMCore.Configuration import Configuration  # noqa: PLC0415
    cfg = Configuration()
    request_name = f"selII_{safe_name(era)}_{safe_name(group)}_{safe_name(key)}"[:100]
    cfg.section_("General")
    cfg.General.requestName  = request_name
    cfg.General.transferLogs = True
    if work_area:
        cfg.General.workArea = work_area
    cfg.section_("JobType")
    cfg.JobType.pluginName = "Analysis"
    cfg.JobType.psetName   = str(PSET)
    cfg.JobType.scriptExe  = str(SCRIPT_SH)

    input_files = [str(SCRIPT_PY), str(CONFIG_YAML)]
    module_names = config["ModuleList"]["Data" if is_data else "MC"]
    for mod_name in module_names:
        input_files.append(str(MODULE_FILES[mod_name]))
        mod_cfg_raw = config["Modules"].get(mod_name, {})
        mod_cfg = mod_cfg_raw.get(era, mod_cfg_raw)
        for file_key in ("IDSFFile", "HLTSFFile", "bTagSFFile", "StandardSFFile", "TightIsoSFFile", "PUSFFile"):
            if file_key in mod_cfg:
                # Chapter-relative ("inputs/SFs/...", fetched by run_all.py --fetchSFFiles).
                input_files.append(str(CHAPTER_DIR / mod_cfg[file_key]))
        if mod_name == "bTagging":
            # efficiencyFolder is also chapter-relative ("inputs/SFs/Efficiency"), same as
            # the file_key SF files above -- written by run_all.py --computeBTaggingEfficiency.
            eff_file = CHAPTER_DIR / mod_cfg["efficiencyFolder"] / era / f"{key}.root"
            if not eff_file.is_file():
                raise FileNotFoundError(f"b-tagging efficiency file not found: {eff_file}")
            input_files.append(str(eff_file))
    if is_data:
        input_files.append(str(golden_json))
    cfg.JobType.inputFiles = input_files

    script_args = [f"era={era}", f"isData={is_data}"]
    if not is_data:
        script_args.append(f"dataset={key}")
    cfg.JobType.scriptArgs = script_args

    cfg.section_("Data")
    cfg.Data.userInputFiles       = lfn_files
    cfg.Data.outputPrimaryDataset = safe_name(f"{group}_{key}")[:100]
    cfg.Data.splitting            = "FileBased"
    cfg.Data.unitsPerJob          = 1
    cfg.Data.publication          = False
    cfg.Data.outLFNDirBase        = output_lfn
    cfg.section_("Site")
    cfg.Site.storageSite = "T3_CH_CERNBOX"
    # See 003-ObjectSelectionI's submit_selection_flexible.py for why this is
    # needed and why it must be a processing site name, not a storage element.
    cfg.Site.whitelist = ["T2_CH_CERN"]

    return cfg

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate/submit CRAB scale-factor-weight jobs for a user-specified era."
    )
    parser.add_argument("--submit", action="store_true",
                        help="Actually submit to CRAB (default: dry run).")
    parser.add_argument("--sample", action="store_true",
                        help="Only submit the first file of each matching dataset (for testing).")
    parser.add_argument("--include", help='Regex applied to "DataMC/group/key"; only matching triples are processed.')
    parser.add_argument("--exclude", help='Regex applied to "DataMC/group/key"; matching triples are skipped.')
    parser.add_argument("--era", required=True,
                        help="Data-taking era (e.g., UL2016preVFP, UL2017, UL2018). Required.")
    parser.add_argument("--dataset-json", required=True,
                        help="Path to selectionI_{era}_datasets.json listing the local/EOS-mounted "
                             "selectionI skim file paths.")
    parser.add_argument("--golden-json", default=None,
                        help="Path to the era's golden JSON. Required if any matching job is Data.")
    parser.add_argument("--output-lfn", required=True,
                        help="Base LFN for CRAB output on T3_CH_CERNBOX.")
    parser.add_argument("--work-area",
                        help="Directory where CRAB project folders are created (default: current directory).")
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
        (SCRIPT_SH,   "crab_selectionII.sh"),
        (SCRIPT_PY,   "crab_script_selectionII.py"),
        (PSET,        "PSet.py"),
        (CONFIG_YAML, "config.yaml"),
        (args.dataset_json, "dataset JSON"),
    ] + [(p, f"module: {name}") for name, p in MODULE_FILES.items()]:
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

    # Explicit --proxy skips CRAB's per-submit myproxy re-delegation (crab submit
    # --help: "Use the given proxy. Skip Grid proxy creation and myproxy
    # delegation."). Without this, every single crabCommand("submit", ...) call --
    # even with an already-valid myproxy credential -- tries to re-delegate via
    # `myproxy-init`, which reads the grid cert private key and prompts for its
    # passphrase on a real terminal; that hangs/fails outright when this script
    # runs non-interactively (background/subprocess), which is how run_all.py's
    # --submitSelectionJobs always invokes it. The existing myproxy delegation
    # (created once, interactively, valid for weeks) is untouched either way --
    # this only skips redundantly refreshing it on every submission.
    proxy_path = os.environ.get('X509_USER_PROXY') or f'/tmp/x509up_u{os.getuid()}'

    submitted, failed = 0, 0
    for DataMC, group, key, lfn_files, is_data in job_params:
        label = f"{DataMC}/{group}/{key}"
        try:
            cfg = make_crab_config(args.era, DataMC, group, key, lfn_files, is_data,
                                    output_lfn, args.golden_json, config, args.work_area)
            crabCommand("submit", config=cfg, proxy=proxy_path)
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
    print("Running submit_selectionII_flexible.py")
    main()
    print("Finished submit_selectionII_flexible.py")
