#!/usr/bin/env python3
"""
Master script to generate all outputs for 004A-Reconstruction.

Usage:
    python scripts/run_all.py [--force] [--tag TAG_NAME]

Options:
    --force: Regenerate outputs even if output files already exist
    --tag:   Create a named tag symlink to this run (e.g., "earlyApril")
"""

import argparse
import os
import sys
from pathlib import Path
import subprocess
import json

sys.path.insert(0, str(Path(__file__).parent))
import utils


def matches_filter(filters, era, data_mc=None, group=None, dataset=None):
    """Check if era/DataMC/group/dataset matches any of the provided filters.

    Each filter is a slash-separated string, e.g. 'UL2017/MC_mu/SingleTop/Tchannel'.
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


def main():
    parser = argparse.ArgumentParser(description='Generate all outputs for 004A-Reconstruction')
    parser.add_argument('-t', '--tag', type=str,
                       help='Create named tag for this run (e.g., earlyApril)', default='Dump')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate outputs even if output files already exist for this config hash')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--generateProcessListJSON', action='store_true',
                       help='[1] Generate process list JSON for runReco.py by reading per-era '
                            'selectionII dataset JSONs from the inputs folder')
    parser.add_argument('--writeBashScript', action='store_true',
                       help='[2] Write a bash script with all runReco.py commands instead of executing them directly')
    parser.add_argument('--generateDatasetJSON', action='store_true',
                       help='[3] Generate dataset JSON by scanning the reconstruction output directory')
    parser.add_argument('--printHash', action='store_true',
                       help='Print the config hash and exit')
    parser.add_argument('--sample', action='store_true',
                       help='Only add the first file of each dataset to the process list JSON (for testing)')
    parser.add_argument('--workers', type=int, default=15,
                       help='Number of parallel workers passed to runReco.py (default: 15)')
    args = parser.parse_args()

    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --generateProcessListJSON: {args.generateProcessListJSON}")
    print(f"  --writeBashScript: {args.writeBashScript}")
    print(f"  --generateDatasetJSON: {args.generateDatasetJSON}")
    print(f"  --sample: {args.sample}")
    print(f"  --workers: {args.workers}")
    print(f"  --force: {args.force}")
    print(f"  --filter: {args.filter}")
    print(f"  --printHash: {args.printHash}")

    base_dir      = Path(__file__).parent.parent
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

    storageBase     = config.get('STORAGE', '/path/to/storage')
    print(f"Using storage base: {storageBase}")

    if args.printHash:
        print(f"Config hash: {config_hash}")
        return 0

    # --generateProcessListJSON
    if args.generateProcessListJSON:
        print("\nGenerating process list JSON for runReco.py...")
        total_tasks = 0

        for era in config['NgenandXsec']:
            print(f"\nProcessing era: {era}")
            if not matches_filter(args.filter, era):
                continue

            selectionII_dataset_json = (
                output_dir / 'inputs' /
                f'selectionII_{args.tag}_{era}_datasets.json'
            )

            if not selectionII_dataset_json.exists():
                print(f"  Warning: Dataset JSON not found: {selectionII_dataset_json}. Skipping era {era}.")
                continue

            with open(selectionII_dataset_json) as f:
                datasetJSON = json.load(f)

            era_process_list = []
            era_skipped = 0
            for DataMC in datasetJSON:
                if not matches_filter(args.filter, era, DataMC):
                    continue
                print(f"  Processing {era} / {DataMC}...")
                is_data = DataMC.lower().startswith("data")
                modules_key  = "Data" if is_data else "MC"
                module_names = config.get("ModuleList", {}).get(modules_key, [])
                print(f"  Processing {era} / {DataMC} / with modules: {module_names}")

                for group in datasetJSON[DataMC]:
                    if not matches_filter(args.filter, era, DataMC, group):
                        continue
                    print(f"  Processing {era} / {DataMC} / {group}...")

                    for dataset in datasetJSON[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        print(f"  Processing {era} / {DataMC} / {group} / {dataset}...")

                        outputDir = os.path.join(
                            storageBase, "reconstruction", args.tag, config_hash, era, DataMC, group, dataset
                        )

                        # Build module configs; era is passed at runtime by runReco.py
                        module_configs = []
                        for mod_name in module_names:
                            mod_cfg = config.get("Modules", {}).get(mod_name, {})
                            module_configs.append({"name": mod_name, "config": mod_cfg})

                        isSample = True
                        for filePath in datasetJSON[DataMC][group][dataset]:
                            skim_name = os.path.basename(filePath).replace(".root", "_Skim.root")
                            skim_path = os.path.join(outputDir, skim_name)
                            if not args.force and os.path.exists(skim_path):
                                era_skipped += 1
                                print(f"    Skim output already exists, skipping: {skim_path}")
                                continue
                            task = {
                                "era":        era,
                                "DataMC":     DataMC,
                                "group":      group,
                                "dataset":    dataset,
                                "outputDir":  outputDir,
                                "file":       filePath,
                                "cut_string": None,   # events already selected in selectionII
                                "goldenJSON": None,   # already applied in selectionII
                                "branchsel":  None,
                                "modules":    module_configs,
                                "isSample":   isSample,
                            }
                            era_process_list.append(task)
                            isSample = False

            era_output_path = output_dir / era / f"{args.tag}_{era}_processListJSON.json"
            era_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(era_output_path, 'w') as f:
                json.dump(era_process_list, f, indent=2)
            total_tasks += len(era_process_list)

        print(f"\nTotal tasks across all eras: {total_tasks}")

    # --writeBashScript
    if args.writeBashScript:
        bash_script_path = base_dir / 'scripts' / f"run_all_{args.tag}.sh"
        with open(bash_script_path, 'w') as f:
            f.write("#!/bin/bash\n\n")
            for era in config['NgenandXsec']:
                if not matches_filter(args.filter, era):
                    continue
                process_list_json = output_dir / era / f"{args.tag}_{era}_processListJSON.json"
                if not process_list_json.exists():
                    print(f"  Warning: Process list JSON not found for era {era}: {process_list_json}. Skipping.")
                    continue
                for DataMC in config['NgenandXsec'][era]:
                    if not matches_filter(args.filter, era, DataMC):
                        continue
                    for group in config['NgenandXsec'][era][DataMC]:
                        if not matches_filter(args.filter, era, DataMC, group):
                            continue
                        log_dir = output_dir / era / DataMC / group
                        log_dir.mkdir(parents=True, exist_ok=True)
                        f.write(f"mkdir -p {log_dir}\n")
                        cmd = (
                            f"python {base_dir / 'scripts' / 'runReco.py'} "
                            f"--processListJSON {process_list_json} "
                            f"--workers {args.workers} "
                            f"{'--force ' if args.force else ''}"
                            f"{'--sample ' if args.sample else ''}"
                            f"{'--filter ' + era + '/' + DataMC + '/' + group}"
                            f"{' 2>&1 | tee -a ' + str(output_dir / era / DataMC / group / f'{args.tag}_{era}_{DataMC}_{group}.log')}"
                        )
                        f.write(cmd + "\n")
        os.chmod(bash_script_path, 0o755)
        print(f"\nBash script written to: {bash_script_path}")

    # --generateDatasetJSON
    if args.generateDatasetJSON:
        print("\nGenerating dataset JSON by scanning reconstruction output directory...")
        generate_dataset_json_script = base_dir / 'scripts' / 'generateDatasetJSON.py'
        if not generate_dataset_json_script.exists():
            print(f"Error: Script not found: {generate_dataset_json_script}")
            return 1
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            outputDirectory = output_dir / era
            outputDirectory.mkdir(parents=True, exist_ok=True)
            outputFileName  = f"reconstruction_{args.tag}_{era}_datasets.json"
            baseDirectory   = f'{storageBase}/reconstruction/{args.tag}/{config_hash}/{era}'
            cmd = [
                'python', str(generate_dataset_json_script),
                '--outputDirectory', str(outputDirectory),
                '--outputFileName',  outputFileName,
                '--baseDirectory',   baseDirectory,
            ]
            print(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Error running generateDatasetJSON.py for era {era}:\n{result.stderr}")
                return 1
            else:
                print(f"Successfully generated dataset JSON for era {era}: "
                      f"{outputDirectory / outputFileName}")


if __name__ == '__main__':
    sys.exit(main())
