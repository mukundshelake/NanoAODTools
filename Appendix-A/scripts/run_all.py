#!/usr/bin/env python3
"""
Master script for Appendix-A: gen-level ttbar A_FB study on selectionII events.

Steps (run in this order, any subset per invocation):
    [0] --generateSelectionIIDatasetJSON  scan the 003-ObjectSelectionII output on disk
    [1] --generateProcessListJSON         one task per ttbar_SemiLeptonic selectionII file
    [2] --runGenStudy                     run modules/GenAsymmetryModule.py via runGenStudy.py
        (or --writeBashScript to write the same commands to scripts/run_all_{tag}.sh)
    [3] --generateDatasetJSON             scan this chapter's own output skims
    [4] --computeAFB                      A_FB vs Mtt per production category (computeAFB.py)

Usage:
    python scripts/run_all.py --tag TAG --filter UL2016preVFP \\
        --generateSelectionIIDatasetJSON --selectionIITag <tag> --selectionIIHash <hash> \\
        --generateProcessListJSON --runGenStudy --generateDatasetJSON --computeAFB
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import utils


def matches_filter(filters, era, data_mc=None, group=None, dataset=None):
    """Check if era/DataMC/group/dataset matches any of the provided filters.

    Each filter is a slash-separated string, e.g. 'UL2017/MC_mu/SemiLeptonic'.
    Use '*' as a wildcard for any level.
    A shorter filter path matches all entries at deeper levels.
    """
    if not filters:
        return True
    for f in filters:
        parts = f.split('/')
        if parts[0] not in ('*', era):
            continue
        if data_mc is not None and len(parts) >= 2 and parts[1] not in ('*', data_mc):
            continue
        if group is not None and len(parts) >= 3 and parts[2] not in ('*', group):
            continue
        if dataset is not None and len(parts) >= 4 and parts[3] not in ('*', dataset):
            continue
        return True
    return False


def run_cmd(cmd, stream=False):
    print(f"    Running command: {' '.join(cmd)}")
    if stream:
        return subprocess.run(cmd).returncode == 0
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.rstrip())
    if result.returncode != 0:
        print(result.stderr.rstrip())
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description='Appendix-A: gen-level A_FB study')
    parser.add_argument('-t', '--tag', type=str, default='Dump',
                        help='Named tag for this run (e.g. earlySeptember_corrected)')
    parser.add_argument('--force', action='store_true',
                        help='Reprocess files whose output already exists')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                        help='Filter by era[/DataMC[/group[/dataset]]]. * is a wildcard. Filters are OR-ed.')
    parser.add_argument('--generateSelectionIIDatasetJSON', action='store_true',
                        help='[0] Scan {STORAGE}/selectionII/{selectionIITag}/{selectionIIHash}/{era} and build '
                             'inputs/selectionII_{era}_datasets.json (also copied into outputs/{tag}/{hash}/inputs/). '
                             'Requires --selectionIITag and --selectionIIHash.')
    parser.add_argument('--selectionIITag', '--inputTag', dest='selectionIITag', type=str, default=None,
                        help='[0] Tag of the input-stage run to scan')
    parser.add_argument('--selectionIIHash', '--inputHash', dest='selectionIIHash', type=str, default=None,
                        help='[0] Config hash of the input-stage run to scan')
    parser.add_argument('--inputStage', choices=['selectionII', 'preselection'], default='selectionII',
                        help='[0][1] Storage stage to read: selectionII (003-II, default) or preselection '
                             '(002-Samples CRAB skims). Use a distinct --tag per stage.')
    parser.add_argument('--generateProcessListJSON', action='store_true',
                        help='[1] Build the per-era task list from the selectionII dataset JSON, '
                             'keeping only config TargetDatasets')
    parser.add_argument('--runGenStudy', action='store_true',
                        help='[2] Run runGenStudy.py on the process list for each era')
    parser.add_argument('--writeBashScript', action='store_true',
                        help='[2alt] Write the runGenStudy.py commands to scripts/run_all_{tag}.sh instead')
    parser.add_argument('--generateDatasetJSON', action='store_true',
                        help='[3] Scan {STORAGE}/appendixA/{tag}/{hash}/{era} and build the output dataset JSON')
    parser.add_argument('--computeAFB', action='store_true',
                        help='[4] Compute A_FB vs Mtt (computeAFB.py) into outputs/{tag}/{hash}/{era}/AFB/')
    parser.add_argument('--printHash', action='store_true', help='Print the config hash and exit')
    parser.add_argument('--sample', action='store_true',
                        help='Only process the first file of each dataset (for testing)')
    parser.add_argument('--workers', type=int, default=15,
                        help='Number of parallel workers passed to runGenStudy.py (default: 15)')
    args = parser.parse_args()

    print("Arguments:")
    for k, v in vars(args).items():
        print(f"  --{k}: {v}")

    base_dir      = Path(__file__).parent.parent
    scripts_dir   = base_dir / 'scripts'
    config_path   = base_dir / 'config.yaml'
    outputs_base  = base_dir / 'outputs' / f'{args.tag}'
    inputs_folder = base_dir / 'inputs'

    print(f"Using config: {config_path}")
    config = utils.load_config(config_path)

    output_dir, config_hash, is_new_run = utils.create_output_directory(
        outputs_base, config_path, inputs_folder
    )
    if is_new_run:
        print(f"Config file has changed. Created new output directory: {output_dir}")
    else:
        print(f"No changes in config. Output directory already exists: {output_dir}")

    latest_link = outputs_base / 'latest'
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(output_dir.name)
    print(f"Updated symlink: {latest_link} -> {output_dir.name}")

    storageBase = utils.resolve_storage_path(config)
    print(f"Using storage base: {storageBase}")

    if args.printHash:
        print(f"Config hash: {config_hash}")
        return 0

    eras = [era for era in config['Eras'] if matches_filter(args.filter, era)]
    targets = [tuple(t.split('/')) for t in config['TargetDatasets']]

    def process_list_path(era):
        return output_dir / era / f"{args.tag}_{era}_processListJSON.json"

    def output_dataset_json(era):
        return output_dir / era / f"appendixA_{args.tag}_{era}_datasets.json"

    # [0] Same health-checked scan of the 003-ObjectSelectionII output as 004A-Reconstruction.
    if args.generateSelectionIIDatasetJSON:
        if not args.selectionIITag or not args.selectionIIHash:
            print("Error: --generateSelectionIIDatasetJSON requires --selectionIITag and --selectionIIHash.")
            return 1
        print(f"\nGenerating {args.inputStage} dataset JSON from disk (tag: {args.selectionIITag}, "
              f"hash: {args.selectionIIHash})...")
        for era in eras:
            print(f"  Era: {era}")
            output_json_name = f"{args.inputStage}_{era}_datasets.json"
            base_directory = os.path.join(storageBase, args.inputStage, args.selectionIITag,
                                          args.selectionIIHash, era)
            if not run_cmd([sys.executable, str(scripts_dir / 'generateDatasetJSON.py'),
                            '--outputDirectory', str(inputs_folder),
                            '--outputFileName', output_json_name,
                            '--baseDirectory', base_directory]):
                return 1
            # create_output_directory() snapshotted inputs/ before this step ran.
            output_path = output_dir / 'inputs' / output_json_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(inputs_folder / output_json_name, output_path)
            print(f"    Generated {inputs_folder / output_json_name} and copied to {output_path}")

    # [1]
    if args.generateProcessListJSON:
        print("\nGenerating process list JSON for runGenStudy.py...")
        module_configs = [{"name": name, "config": config.get("Modules", {}).get(name, {})}
                          for name in config["ModuleList"]["MC"]]
        total_tasks = 0
        for era in eras:
            selectionII_json = output_dir / 'inputs' / f'{args.inputStage}_{era}_datasets.json'
            if not selectionII_json.exists():
                print(f"  Warning: {selectionII_json} not found. Run --generateSelectionIIDatasetJSON first. "
                      f"Skipping era {era}.")
                continue
            with open(selectionII_json) as f:
                datasetJSON = json.load(f)

            era_process_list, era_skipped = [], 0
            for DataMC, group, dataset in targets:
                if not matches_filter(args.filter, era, DataMC, group, dataset):
                    continue
                files = datasetJSON.get(DataMC, {}).get(group, {}).get(dataset)
                if not files:
                    print(f"  Warning: {DataMC}/{group}/{dataset} not in {selectionII_json.name}. Skipping.")
                    continue
                outputDir = os.path.join(storageBase, "appendixA", args.tag, config_hash,
                                         era, DataMC, group, dataset)
                isSample = True
                for filePath in files:
                    skim_path = os.path.join(outputDir, os.path.basename(filePath).replace(".root", "_Skim.root"))
                    if not args.force and os.path.exists(skim_path):
                        era_skipped += 1
                        isSample = False
                        continue
                    era_process_list.append({
                        "era":             era,
                        "DataMC":          DataMC,
                        "group":           group,
                        "dataset":         dataset,
                        "outputDir":       outputDir,
                        "file":            filePath,
                        "cut_string":      None,   # events already selected in selectionII
                        "goldenJSON":      None,
                        "branchsel":       str(scripts_dir / 'keep_and_drop_input.txt'),
                        "outputbranchsel": str(scripts_dir / 'keep_and_drop_output.txt'),
                        "modules":         module_configs,
                        "isSample":        isSample,
                    })
                    isSample = False

            out_path = process_list_path(era)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, 'w') as f:
                json.dump(era_process_list, f, indent=2)
            print(f"  {era}: {len(era_process_list)} tasks ({era_skipped} already done) -> {out_path}")
            total_tasks += len(era_process_list)
        print(f"\nTotal tasks across all eras: {total_tasks}")

    # [2]
    def gen_study_cmd(era):
        cmd = [sys.executable, str(scripts_dir / 'runGenStudy.py'),
               '--processListJSON', str(process_list_path(era)),
               '--workers', str(args.workers)]
        if args.force:
            cmd.append('--force')
        if args.sample:
            cmd.append('--sample')
        return cmd

    if args.runGenStudy:
        print("\nRunning gen-level module...")
        for era in eras:
            if not process_list_path(era).exists():
                print(f"  Warning: {process_list_path(era)} not found. Run --generateProcessListJSON first. "
                      f"Skipping era {era}.")
                continue
            if not run_cmd(gen_study_cmd(era), stream=True):
                print(f"Error: runGenStudy.py failed for era {era}")
                return 1

    if args.writeBashScript:
        bash_script_path = scripts_dir / f"run_all_{args.tag}.sh"
        with open(bash_script_path, 'w') as f:
            f.write("#!/bin/bash\n\n")
            for era in eras:
                log = output_dir / era / f"{args.tag}_{era}_genStudy.log"
                f.write(f"mkdir -p {log.parent}\n")
                f.write(' '.join(gen_study_cmd(era)) + f" 2>&1 | tee -a {log}\n")
        os.chmod(bash_script_path, 0o755)
        print(f"\nBash script written to: {bash_script_path}")

    # [3]
    if args.generateDatasetJSON:
        print("\nGenerating dataset JSON by scanning the Appendix-A output directory...")
        for era in eras:
            out_json = output_dataset_json(era)
            out_json.parent.mkdir(parents=True, exist_ok=True)
            if not run_cmd([sys.executable, str(scripts_dir / 'generateDatasetJSON.py'),
                            '--outputDirectory', str(out_json.parent),
                            '--outputFileName', out_json.name,
                            '--baseDirectory', f'{storageBase}/appendixA/{args.tag}/{config_hash}/{era}']):
                return 1

    # [4]
    if args.computeAFB:
        print("\nComputing gen-level A_FB...")
        afb_cfg = config['AFB']
        for era in eras:
            dataset_json = output_dataset_json(era)
            if not dataset_json.exists():
                print(f"  Warning: {dataset_json} not found. Run --generateDatasetJSON first. Skipping era {era}.")
                continue
            cmd = [sys.executable, str(scripts_dir / 'computeAFB.py'),
                   '--json', str(dataset_json),
                   '--outDir', str(output_dir / era / 'AFB'),
                   '--label', era,
                   '--mttBins', *[str(b) for b in afb_cfg['MttBins']],
                   '--categories', *afb_cfg['Categories']]
            if not run_cmd(cmd):
                return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
