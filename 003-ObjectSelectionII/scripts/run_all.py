#!/usr/bin/env python3
"""
Master script to generate all outputs for 002-Samples chapter.

Usage:
    python scripts/run_all.py [--force] [--tag TAG_NAME]
    
Options:
    --force: Regenerate outputs even if config hash already exists
    --tag: Create a named tag symlink to this run (e.g., "baseline", "paper_v1")
"""

import argparse
import os
import sys
from pathlib import Path
import subprocess
import json

# Add parent directory to path for imports
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
    parser = argparse.ArgumentParser(description='Generate all outputs for 002-Samples')
    parser.add_argument('-t', '--tag', type=str,
                       help='Create named tag for this run (e.g., baseline, paper_v1)', default='Dump')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate outputs even if output files already exist for this config hash')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--generateProcessListJSON', action='store_true',
                       help='[1] Generate process list JSON for runSelection.py by reading the per-era '
                            'dataset JSONs produced by --generateDatasetJSON')
    parser.add_argument('--writeBashScript', action='store_true',
                       help='[2] Write a bash script with all runSelection.py commands instead of executing them directly')
    parser.add_argument('--generateDatasetJSON', action='store_true',
                       help='[3] Generate dataset JSON file using the script generateDatasetJSON.py')
    parser.add_argument('--prepareFileset', action='store_true',
                       help='[4] Prepare the fileset for coffea processing.')
    parser.add_argument('--printHash', action='store_true',
                       help='Print config hash and exit (for testing purposes)')
    parser.add_argument('--sample', action='store_true',
                       help='Only add the first file of each dataset to the process list JSON (for testing purposes)')
    parser.add_argument('--workers', type=int, default=15,
                       help='Number of parallel workers passed to runSelection.py (default: 15)')
    args = parser.parse_args()

    # parsing arguments
    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --generateProcessListJSON: {args.generateProcessListJSON}")
    print(f"  --writeBashScript: {args.writeBashScript}")
    print(f"  --generateDatasetJSON: {args.generateDatasetJSON}")
    print(f"  --prepareFileset: {args.prepareFileset}")
    print(f"  --sample: {args.sample}")
    print(f"  --workers: {args.workers}")
    print(f"  --force: {args.force}")
    print(f"  --filter: {args.filter}")
    print(f"  --printHash: {args.printHash}")

    # Paths
    base_dir = Path(__file__).parent.parent
    config_path = base_dir / 'config.yaml'
    outputs_base = base_dir / 'outputs' / f'{args.tag}'
    inputs_folder = base_dir / 'inputs'
    sfs_folder = base_dir.parent / 'SFs'

    print(f"Using config: {config_path}")
    
    # Load config and compute hash
    config = utils.load_config(config_path)
    
    # Create output directory
    output_dir, config_hash, is_new_run = utils.create_output_directory(
        outputs_base, config_path, inputs_folder, sfs_folder
    )
    if is_new_run:
        print(f"Config file has changed. Created new output directory: {output_dir}")
    else:
        print(f"No changes in config. Output directory already exists: {output_dir}")
    
    storageBase = config.get('STORAGE', '/path/to/storage')
    print(f"Using storage base: {storageBase}")

    if args.printHash:
        print(f"Config hash: {config_hash}")
        return 0

    # Generate process list JSON for runSelection.py
    if args.generateProcessListJSON:
        print("\nGenerating process list JSON for runSelection.py...")
        # NanoAODTools root is one level above 003-ObjectSelection
        nanoaodtools_base = base_dir.parent
        total_tasks = 0

        for era in config['NgenandXsec']:
            print(f"\nProcessing era: {era}")
            if not matches_filter(args.filter, era):
                continue
            selectionI_dataset_json = output_dir / 'inputs' / f'selectionI_{args.tag}_{era}_datasets.json'
            golden_json_file = output_dir / 'inputs' / f'{era}_goldenJSON.json'

            if not selectionI_dataset_json.exists():
                print(f"  Warning: Dataset JSON not found: {selectionI_dataset_json}. Skipping era {era}.")
                continue
            if not golden_json_file.exists():
                print(f"  Warning: Golden JSON file not found: {golden_json_file}. Data tasks will run without golden JSON filtering for era {era}.")


            with open(selectionI_dataset_json) as f:
                datasetJSON = json.load(f)

            # Build combined cut string for this era
            era_cuts = config.get('SelectionCuts', {}).get(era, {})
            cut_string = " && ".join(v for v in era_cuts.values() if v and v.strip()) or None

            era_process_list = []
            era_skipped = 0
            for DataMC in datasetJSON:
                if not matches_filter(args.filter, era, DataMC):
                    continue
                print(f"  Processing {era} / {DataMC}...")
                is_data = DataMC.lower().startswith("data")
                modules_key = "Data" if is_data else "MC"
                module_names = config.get("ModuleList", {}).get(modules_key, [])
                print(f"  Processing {era} / {DataMC} / with modules: {module_names}")
                # continue
                # continue
                for group in datasetJSON[DataMC]:
                    print(f"  Processing {era} / {DataMC} / {group}...")
                    # continue
                    for dataset in datasetJSON[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, dataset):
                            continue
                        print (f"  Processing {era} / {DataMC} / {group} / {dataset}...")
                        # continue
                        # print(storageBase, args.tag, era, DataMC, group, dataset)
                        outputDir = os.path.join(
                            storageBase, "selectionII", args.tag, era, DataMC, group, dataset
                        )
                        isSample = True
                        for filePath in datasetJSON[DataMC][group][dataset]:
                            
                            # print(f"    Found file: {filePath}")
                            # continue

                            # Build module configs: era-resolved + absolute SF paths



                            module_configs = []
                            for mod_name in module_names:
                                mod_cfg_raw = config.get("Modules", {}).get(mod_name, {})
                                # Use era-specific sub-config if present, else use top-level config
                                mod_cfg = mod_cfg_raw.get(era, mod_cfg_raw)
                                if mod_name == "selectedObjects":
                                    mod_cfg = dict(mod_cfg, is_mc=not is_data)
                                module_configs.append({"name": mod_name, "config": mod_cfg})
                            task = {
                                "era":       era,
                                "DataMC":    DataMC,
                                "group":     group,
                                "dataset":   dataset,
                                "outputDir": outputDir,
                                "file":      filePath,
                                "cut_string": cut_string,
                                "goldenJSON": str(golden_json_file) if is_data else None,
                                "branchsel": None,
                                "modules":   module_configs,
                                "isSample": isSample
                            }
                            era_process_list.append(task)
                            isSample = False  # Only the first file of each dataset is added when --sample is used
            era_output_path = output_dir / era / f"{args.tag}_{era}_processListJSON.json"
            era_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(era_output_path, 'w') as f:
                json.dump(era_process_list, f, indent=2)
            # print(f"  Era {era}: {len(era_process_list)} tasks queued, {era_skipped} already done -> {era_output_path}")
            total_tasks += len(era_process_list)


        print(f"\nTotal tasks across all eras: {total_tasks}")
    
    # If --writeBashScript is set, write all runSelection.py commands to a bash script instead of executing them
    if args.writeBashScript:
        # place the script in the scripts folder itself (where this run_all.py is located)
        bash_script_path = base_dir / f"run_all_{args.tag}.sh"      
        with open(bash_script_path, 'w') as f:
            f.write("#!/bin/bash\n\n")
            for era in config['NgenandXsec']:
                if not matches_filter(args.filter, era):
                    continue
                process_list_json = output_dir / era / f"{args.tag}_{era}_processListJSON.json"
                if not process_list_json.exists():
                    print(f"  Warning: Process list JSON not found for era {era}: {process_list_json}. Skipping runSelection command for this era.")
                    continue
                for DataMC in config['NgenandXsec'][era]:
                    if not matches_filter(args.filter, era, DataMC):
                        continue
                    for group in config['NgenandXsec'][era][DataMC]:
                        if not matches_filter(args.filter, era, DataMC, group):
                            continue
                        cmd = (
                            f"python {base_dir / 'scripts' / 'runSelectionII.py'} "
                            f"--processListJSON {process_list_json} "
                            f"--workers {args.workers} "
                            f"{'--force' if args.force else ''}"
                            f"{'--sample' if args.sample else ''}"
                            f"{'--filter ' + era + '/' + DataMC + '/' + group}"
                            f"{' 2>&1 | tee -a ' + str(output_dir / era / DataMC / group / f'{args.tag}_{era}_{DataMC}_{group}.log')}"
                        )
                        f.write(cmd + "\n")
        os.chmod(bash_script_path, 0o755)
        print(f"\nBash script with runSelectionII.py commands written to: {bash_script_path}")

    if args.generateDatasetJSON:
        print("\nGenerating dataset JSON file using generateDatasetJSON.py...")
        generate_dataset_json_script = base_dir / 'scripts' / 'generateDatasetJSON.py'
        # Check if the script exists
        if not generate_dataset_json_script.exists():
            print(f"Error: Script not found: {generate_dataset_json_script}")
            return 1
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            outputDirectory = output_dir / era 
            outputDirectory.mkdir(parents=True, exist_ok=True)
            outputFileName = f"selectionII_{args.tag}_{era}_datasets.json"
            baseDirectory = f'/mnt/disk2/mukund/DataFiles/selectionII/{args.tag}/{era}'
            cmd = [
                'python', str(generate_dataset_json_script),
                '--outputDirectory', str(outputDirectory),
                '--outputFileName', outputFileName,
                '--baseDirectory', baseDirectory
            ]
            print(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Error running generateDatasetJSON.py for era {era}:\n{result.stderr}")
                return 1
            else:
                print(f"Successfully generated dataset JSON for era {era}: {outputDirectory / outputFileName}")
    # Prepare fileset for coffea processing
    if args.prepareFileset:
        print("\nPreparing fileset for coffea processing...")
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            selectionII_dataset_json = output_dir / era / f"selectionII_{args.tag}_{era}_datasets.json"
            if not selectionII_dataset_json.exists():
                print(f"  Warning: Dataset JSON not found for era {era}: {selectionII_dataset_json}. Skipping fileset preparation for this era.")
                continue
            with open(selectionII_dataset_json) as f:
                datasetJSON = json.load(f)
            for DataMC in datasetJSON:
                for group in datasetJSON[DataMC]:
                    for dataset in datasetJSON[DataMC][group]:
                        fileset = {}
                        datasetName = f'{era}_{DataMC}_{group}_{dataset}'
                        fileset[datasetName] = {"files": datasetJSON[DataMC][group][dataset]}
                        fileset[datasetName]['metadata'] = {}
                        if 'data' in DataMC.lower():
                            fileset[datasetName]['metadata']['isData'] = True
                        else:
                            fileset[datasetName]['metadata']['isData'] = False
                            fileset[datasetName]['metadata']['era'] = era
                            fileset[datasetName]['metadata']['sample'] = dataset
                        # Save the fileset as a JSON file for this era
                        fileset_output_path = output_dir / era / DataMC / group / dataset / f"{args.tag}_{DataMC}_{group}_{dataset}_{era}_fileset.json"
                        fileset_output_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(fileset_output_path, 'w') as f:
                            json.dump(fileset, f, indent=4)
                        print(f"Prepared fileset for era {era} and saved to: {fileset_output_path}")
    # exit(0)


if __name__ == '__main__':
    sys.exit(main())
