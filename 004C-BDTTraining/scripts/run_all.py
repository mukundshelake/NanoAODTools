#!/usr/bin/env python3
"""
Master script to generate all outputs for 004C-BDTTraining.

This script orchestrates the BDT parquet-extraction workflow:
1. Fetches BDTVariables dataset JSONs from 004B outputs (optional)
2. Generates a process list JSON for extractParquet.py (one task per dataset)
3. Writes a bash script to execute the processing, or runs it directly
4. Generates dataset JSON from the parquet outputs (optional)

...and, separately, the BDT training workflow:
5. Trains the qqbar-vs-gg XGBoost classifier per era from a pinned parquet
   extraction run's outputs (--trainBDT --parquetHash <hash>)

Usage:
    python scripts/run_all.py [--force] [--tag TAG_NAME]

Options:
    --force: Regenerate outputs even if output files already exist
    --tag:   Create a named tag for this run (e.g., "earlyApril")
    --fetchFromPreviousChapter: Fetch BDTVariables JSONs from 004B-BDT outputs
    --generateProcessListJSON: Generate process list for extractParquet.py
    --writeBashScript: Create a bash script instead of running directly
    --generateDatasetJSON: Create dataset JSON from parquet outputs
    --trainBDT: Train the BDT per era (requires --parquetHash)
"""

import argparse
import os
import shutil
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
    parser = argparse.ArgumentParser(description='Generate all outputs for 004C-BDTTraining')
    parser.add_argument('-t', '--tag', type=str,
                       help='Create named tag for this run (e.g., earlyApril)', default='Dump')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate outputs even if output files already exist for this config hash')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--fetchFromPreviousChapter', action='store_true',
                       help='[0] Fetch BDTVariables_{tag}_{era}_datasets.json from 004B-BDT outputs '
                            'into inputs/ (and this run\'s outputs/inputs/ snapshot). Requires --previousHash.')
    parser.add_argument('--previousHash', type=str, default=None,
                       help='[0] Config hash of the 004B-BDT run to fetch from (its outputs/{tag}/{hash}/ '
                            'directory). Required by --fetchFromPreviousChapter.')
    parser.add_argument('--generateProcessListJSON', action='store_true',
                       help='[1] Generate process list JSON for extractParquet.py by reading per-era '
                            'BDTVariables dataset JSONs from the inputs folder. One task per dataset.')
    parser.add_argument('--writeBashScript', action='store_true',
                       help='[2] Write a bash script with all extractParquet.py commands instead of executing them directly')
    parser.add_argument('--generateDatasetJSON', action='store_true',
                       help='[3] Generate dataset JSON by scanning the parquet output directory')
    parser.add_argument('--printHash', action='store_true',
                       help='Print the config hash and exit')
    parser.add_argument('--sample', action='store_true',
                       help='Only add the first dataset of each era to the process list JSON, '
                            'and only its first file (for testing)')
    parser.add_argument('--workers', type=int, default=8,
                       help='Number of parallel workers passed to extractParquet.py (default: 8)')
    parser.add_argument('--trainBDT', action='store_true',
                       help='[4] Train the qqbar-vs-gg XGBoost classifier per era from the parquet '
                            'outputs of a (possibly different) extraction run. Requires --parquetHash.')
    parser.add_argument('--parquetHash', type=str, default=None,
                       help='[4] Config hash of the 004C-BDTTraining extraction run to train from '
                            '(its outputs/{tag}/{hash}/ directory, containing Parquet_{tag}_{era}_datasets.json '
                            'per era). Required by --trainBDT. Deliberately decoupled from config.yaml\'s own '
                            'hash so that tuning training_config.yaml never forces parquet re-extraction.')
    parser.add_argument('--trainWriteBashScript', action='store_true',
                       help='[4] Write a bash script with the per-era trainBDT.py commands instead of '
                            'running them directly (mirrors --writeBashScript for extraction).')
    args = parser.parse_args()

    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --fetchFromPreviousChapter: {args.fetchFromPreviousChapter}")
    print(f"  --previousHash: {args.previousHash}")
    print(f"  --generateProcessListJSON: {args.generateProcessListJSON}")
    print(f"  --writeBashScript: {args.writeBashScript}")
    print(f"  --generateDatasetJSON: {args.generateDatasetJSON}")
    print(f"  --sample: {args.sample}")
    print(f"  --workers: {args.workers}")
    print(f"  --force: {args.force}")
    print(f"  --filter: {args.filter}")
    print(f"  --printHash: {args.printHash}")
    print(f"  --trainBDT: {args.trainBDT}")
    print(f"  --parquetHash: {args.parquetHash}")
    print(f"  --trainWriteBashScript: {args.trainWriteBashScript}")

    base_dir      = Path(__file__).parent.parent
    config_path   = base_dir / 'config.yaml'
    outputs_base  = base_dir / 'outputs' / f'{args.tag}'
    inputs_folder = base_dir / 'inputs'

    print(f"Using config: {config_path}")

    config = utils.load_config(config_path)
    eras   = config.get('Eras', [])
    columns = list(config['BDTVariables']) + list(config.get('AdditionalFeatures', [])) + [config['TargetBranch']]
    max_events = config['MaxEventsPerParquet']

    output_dir, config_hash, is_new_run = utils.create_output_directory(
        outputs_base, config_path, inputs_folder
    )
    if is_new_run:
        print(f"Config file has changed. Created new output directory: {output_dir}")
    else:
        print(f"No changes in config. Output directory already exists: {output_dir}")

    # Create or update the 'latest' symlink to point to the current output directory
    latest_link = outputs_base / 'latest'
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(output_dir.name)
    print(f"Updated symlink: {latest_link} -> {output_dir.name}")

    storageBase = utils.resolve_storage_path(config)
    print(f"Using storage base: {storageBase}")

    if args.printHash:
        print(f"Config hash: {config_hash}")
        if args.trainBDT:
            training_config_path = base_dir / 'training_config.yaml'
            print(f"Training config hash: {utils.compute_config_hash(training_config_path)}")
        return 0

    # Fetch BDTVariables dataset JSON into inputs/
    if args.fetchFromPreviousChapter:
        if not args.previousHash:
            print("Error: --fetchFromPreviousChapter requires --previousHash to be specified.")
            return 1
        print(f"\nFetching inputs from 004B-BDT (hash: {args.previousHash})...")
        previous_chapter_outputs = base_dir.parent / '004B-BDT' / 'outputs' / args.tag / args.previousHash
        for era in eras:
            if not matches_filter(args.filter, era):
                continue
            print(f"  Era: {era}")
            filename = f'BDTVariables_{args.tag}_{era}_datasets.json'
            source_path = previous_chapter_outputs / era / filename
            if not source_path.exists():
                print(f"    Error: Source file not found: {source_path}. Skipping.")
                continue
            local_path, output_path = utils.fetch_and_snapshot(source_path, inputs_folder, output_dir, filename)
            print(f"    Fetched {filename} -> {local_path} and {output_path}")
        print("Finished fetching inputs from 004B-BDT.")

    # --generateProcessListJSON
    if args.generateProcessListJSON:
        print("\nGenerating process list JSON for extractParquet.py...")
        total_tasks = 0

        for era in eras:
            if not matches_filter(args.filter, era):
                continue
            print(f"\nProcessing era: {era}")

            bdtvariables_dataset_json = (
                output_dir / 'inputs' /
                f'BDTVariables_{args.tag}_{era}_datasets.json'
            )

            if not bdtvariables_dataset_json.exists():
                print(f"  Warning: Dataset JSON not found: {bdtvariables_dataset_json}. Skipping era {era}.")
                continue

            with open(bdtvariables_dataset_json) as f:
                datasetJSON = json.load(f)

            era_process_list = []
            era_skipped = 0
            isSample = True  # first dataset in this era is marked as the sample task
            for DataMC in datasetJSON:
                if not matches_filter(args.filter, era, DataMC):
                    continue
                print(f"  Processing {era} / {DataMC}...")

                for group in datasetJSON[DataMC]:
                    if not matches_filter(args.filter, era, DataMC, group):
                        continue
                    print(f"  Processing {era} / {DataMC} / {group}...")

                    for dataset in datasetJSON[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        print(f"  Processing {era} / {DataMC} / {group} / {dataset}...")

                        outputDir = os.path.join(
                            storageBase, "BDTParquet", args.tag, config_hash, era, DataMC, group, dataset
                        )

                        existing = (
                            os.path.isdir(outputDir)
                            and any(fn.endswith('.parquet') for fn in os.listdir(outputDir))
                        )
                        if not args.force and existing:
                            era_skipped += 1
                            print(f"    Parquet output already exists, skipping: {outputDir}")
                            continue

                        files = list(datasetJSON[DataMC][group][dataset].keys())
                        task = {
                            "era":        era,
                            "DataMC":     DataMC,
                            "group":      group,
                            "dataset":    dataset,
                            "outputDir":  outputDir,
                            "files":      files,
                            "columns":    columns,
                            "maxEvents":  max_events,
                            "isSample":   isSample,
                        }
                        era_process_list.append(task)
                        isSample = False

            era_output_path = output_dir / era / f"{args.tag}_{era}_processListJSON.json"
            era_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(era_output_path, 'w') as f:
                json.dump(era_process_list, f, indent=2)
            total_tasks += len(era_process_list)
            print(f"  Era {era}: {len(era_process_list)} dataset tasks added, {era_skipped} skipped (output already exists).")

        print(f"\nTotal dataset tasks across all eras: {total_tasks}")

    # --writeBashScript
    if args.writeBashScript:
        bash_script_path = base_dir / 'scripts' / f"run_all_{args.tag}.sh"
        with open(bash_script_path, 'w') as f:
            f.write("#!/bin/bash\n\n")
            for era in eras:
                if not matches_filter(args.filter, era):
                    continue
                process_list_json = output_dir / era / f"{args.tag}_{era}_processListJSON.json"
                if not process_list_json.exists():
                    print(f"  Warning: Process list JSON not found for era {era}: {process_list_json}. Skipping.")
                    continue
                log_dir = output_dir / era
                log_dir.mkdir(parents=True, exist_ok=True)
                f.write(f"mkdir -p {log_dir}\n")
                cmd = (
                    f"python3 {base_dir / 'scripts' / 'extractParquet.py'} "
                    f"--processListJSON {process_list_json} "
                    f"--workers {args.workers} "
                    f"{'--force ' if args.force else ''}"
                    f"{'--sample ' if args.sample else ''}"
                    f"--filter {era}"
                    f"{' 2>&1 | tee -a ' + str(log_dir / f'{args.tag}_{era}.log')}"
                )
                f.write(cmd + "\n")
        os.chmod(bash_script_path, 0o755)
        print(f"\nBash script written to: {bash_script_path}")

    # --generateDatasetJSON
    if args.generateDatasetJSON:
        print("\nGenerating dataset JSON by scanning parquet output directory...")
        generate_dataset_json_script = base_dir / 'scripts' / 'generateDatasetJSON.py'
        if not generate_dataset_json_script.exists():
            print(f"Error: Script not found: {generate_dataset_json_script}")
            return 1
        for era in eras:
            if not matches_filter(args.filter, era):
                continue
            outputDirectory = output_dir / era
            outputDirectory.mkdir(parents=True, exist_ok=True)
            outputFileName  = f"Parquet_{args.tag}_{era}_datasets.json"
            baseDirectory   = f'{storageBase}/BDTParquet/{args.tag}/{config_hash}/{era}'
            cmd = [
                sys.executable, str(generate_dataset_json_script),
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

    # --trainBDT
    if args.trainBDT:
        print("\nTraining BDT per era...")
        if not args.parquetHash:
            print("Error: --trainBDT requires --parquetHash to be specified.")
            return 1

        train_script = base_dir / 'scripts' / 'trainBDT.py'
        if not train_script.exists():
            print(f"Error: Script not found: {train_script}")
            return 1

        training_config_path = base_dir / 'training_config.yaml'
        training_hash = utils.compute_config_hash(training_config_path)
        print(f"Training config hash: {training_hash}")

        parquet_run_dir = outputs_base / args.parquetHash
        if not parquet_run_dir.exists():
            print(f"Error: --parquetHash {args.parquetHash} not found under {outputs_base}.")
            return 1

        bash_script_path = base_dir / 'scripts' / f"train_all_{args.tag}.sh"
        bash_lines = ["#!/bin/bash\n"]

        for era in eras:
            if not matches_filter(args.filter, era):
                continue

            dataset_json_path = parquet_run_dir / era / f"Parquet_{args.tag}_{era}_datasets.json"
            if not dataset_json_path.exists():
                print(f"  Warning: {dataset_json_path} not found, skipping era {era}. "
                      f"(Has --generateDatasetJSON been run for --parquetHash {args.parquetHash}?)")
                continue

            bdt_out_dir = parquet_run_dir / era / 'bdt' / training_hash
            if bdt_out_dir.exists() and (bdt_out_dir / 'best_params.json').exists() and not args.force:
                print(f"  Training output already exists for era {era}, skipping: {bdt_out_dir}")
                continue

            bdt_out_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(training_config_path, bdt_out_dir / 'training_config.yaml')

            cmd = [
                sys.executable, str(train_script),
                '--datasetJSON', str(dataset_json_path),
                '--trainingConfig', str(training_config_path),
                '--outputDir', str(bdt_out_dir),
                '--era', era,
                '--parquetHash', args.parquetHash,
            ]
            if args.sample:
                cmd.append('--sample')
            if args.force:
                cmd.append('--force')

            if args.trainWriteBashScript:
                bash_lines.append(' '.join(cmd) +
                                   f" 2>&1 | tee -a {bdt_out_dir / f'trainBDT_{era}_run_all.log'}\n")
                print(f"  Queued training command for era {era} in {bash_script_path}")
            else:
                print(f"  Running: {' '.join(cmd)}")
                result = subprocess.run(cmd)
                if result.returncode != 0:
                    print(f"Error training BDT for era {era} (see {bdt_out_dir / f'trainBDT_{era}.log'}).")
                    return 1
                print(f"  Successfully trained BDT for era {era}: {bdt_out_dir}")

        if args.trainWriteBashScript:
            with open(bash_script_path, 'w') as f:
                f.writelines(bash_lines)
            os.chmod(bash_script_path, 0o755)
            print(f"\nBash script written to: {bash_script_path}")

        print(f"\nTraining hash for this run: {training_hash}")

if __name__ == '__main__':
    sys.exit(main())
