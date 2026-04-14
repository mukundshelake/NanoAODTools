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
import copy
import glob
import os
import sys
from pathlib import Path
import subprocess
import json

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
import utils


# ---------------------------------------------------------------------------
# SF path resolution for process list JSON generation
# ---------------------------------------------------------------------------
_SF_PATH_KEYS = {
    "muonID":   ["IDSFFile"],
    "muonHLT":  ["HLTSFFile"],
    "jetPUID":  ["efficiencyFile", "jetPUIdFile"],
    "bTagging": ["bTagSFFile", "efficiencyFolder"],
}


def _resolve_sf_paths(module_name, module_config, nanoaodtools_base):
    """Return a deep copy of module_config with relative SF paths made absolute."""
    if module_name not in _SF_PATH_KEYS:
        return module_config
    resolved = copy.deepcopy(module_config)
    for key in _SF_PATH_KEYS[module_name]:
        if key in resolved and not os.path.isabs(str(resolved[key])):
            resolved[key] = str(nanoaodtools_base / resolved[key])
    return resolved


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
    parser.add_argument('--generateDatasetJSON', action='store_true',
                       help='Generate dataset JSON file by running generateDatasetJSON.py')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--generateProcessListJSON', action='store_true',
                       help='Generate process list JSON for runSelection.py by reading the per-era '
                            'dataset JSONs produced by --generateDatasetJSON')
    parser.add_argument('--run', action='store_true',
                       help='After writing each era process list JSON, immediately submit it to '
                            'runSelection.py. Eras are processed sequentially; within each era '
                            'runSelection.py parallelises across files.')
    parser.add_argument('--workers', type=int, default=15,
                       help='Number of parallel workers passed to runSelection.py (default: 15)')
    parser.add_argument('--sample', action='store_true',
                       help='Queue only one file per dataset key (useful for quick testing)')
    parser.add_argument('--pass', type=int, default=1, choices=[1, 2],
                       dest='run_pass',
                       help='Processing pass: 1=SelectedObjects only (input: preselection), '
                            '2=SF weight modules (input: pass-1 output)')

    args = parser.parse_args()

    # parsing arguments
    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --generateDatasetJSON: {args.generateDatasetJSON}")
    print(f"  --generateProcessListJSON: {args.generateProcessListJSON}")
    print(f"  --run: {args.run}")
    print(f"  --workers: {args.workers}")
    print(f"  --sample: {args.sample}")
    print(f"  --force: {args.force}")
    print(f"  --filter: {args.filter}")

    # Paths
    base_dir = Path(__file__).parent.parent
    config_path = base_dir / 'config.yaml'
    outputs_base = base_dir / 'outputs' / f'{args.tag}'

    print(f"Using config: {config_path}")
    
    # Load config and compute hash
    config = utils.load_config(config_path)
    
    # Create output directory
    output_dir, config_hash, is_new_run = utils.create_output_directory(
        outputs_base, config_path
    )
    if is_new_run:
        print(f"Config file has changed. Created new output directory: {output_dir}")
    else:
        print(f"No changes in config. Output directory already exists: {output_dir}")
    
    storageBase = config.get('STORAGE', '/path/to/storage')
    print(f"Using storage base: {storageBase}")

    # Generate dataset JSON file by running generateDatasetJSON.py if requested
    if args.generateDatasetJSON:
        print("\nGenerating dataset JSON file by running generateDatasetJSON.py...")
        for era in config['SelectionCuts'].keys():
            print(f"\nGenerating dataset JSON for era: {era}")
            # check if script is there
            script_path = base_dir / 'scripts' / 'generateDatasetJSON.py'
            if not script_path.exists():
                print(f"Error: {script_path} not found. Skipping dataset JSON generation.")
                continue
            baseDirectory = os.path.join(storageBase, "preselection", args.tag, era)
            outputDir = os.path.join(output_dir, era)
            outputFileName = f"{args.tag}_preselection_{era}_dataset.json"
            cmd = [
                'python', str(script_path),
                '--baseDirectory', baseDirectory,
                '--outputDirectory', outputDir,
                '--outputFileName', outputFileName
            ]
            print(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Error running generateDatasetJSON.py for era {era}: {result.stderr}")
            else:
                print(f"Successfully generated dataset JSON for era {era}: {outputDir}/{outputFileName}")
    # Generate process list JSON for runSelection.py
    if args.generateProcessListJSON:
        print("\nGenerating process list JSON for runSelection.py...")
        # NanoAODTools root is one level above 003-ObjectSelection
        nanoaodtools_base = base_dir.parent
        total_tasks = 0

        for era in config['SelectionCuts'].keys():
            if not matches_filter(args.filter, era):
                continue

            dataset_json_path = output_dir / era / f"{args.tag}_preselection_{era}_dataset.json"
            if not dataset_json_path.exists():
                print(f"  Warning: Dataset JSON not found: {dataset_json_path}. Skipping era {era}.")
                continue

            with open(dataset_json_path) as f:
                dataset = json.load(f)

            # Build combined cut string for this era
            era_cuts = config['SelectionCuts'][era]
            cut_string = " && ".join(v for v in era_cuts.values() if v and v.strip())

            # Golden JSON absolute path for Data lumi-masking
            golden_json_rel = config.get('GoldenJSON', {}).get(era)
            _gjr = str(golden_json_rel).strip() if golden_json_rel is not None else ""
            golden_json_abs = str(nanoaodtools_base / golden_json_rel) if _gjr and _gjr.lower() != "none" else None

            era_process_list = []
            era_skipped = 0
            for data_mc, datasets_in_group in dataset.items():
                if not matches_filter(args.filter, era, data_mc):
                    continue

                is_data = data_mc.lower().startswith("data")
                modules_key = "Data" if is_data else "MC"
                pass_key = f"{modules_key}_pass{args.run_pass}"
                module_names = config.get("ModuleList", {}).get(pass_key, [])

                for key, files in datasets_in_group.items():
                    if not matches_filter(args.filter, era, data_mc, key):
                        continue

                    if args.run_pass == 1:
                        outputDir = os.path.join(
                            storageBase, "selection", args.tag, "pass1", era, data_mc, key
                        )
                        input_files = list(files.keys())
                        task_cut_string = cut_string
                    else:
                        outputDir = os.path.join(
                            storageBase, "selection", args.tag, era, data_mc, key
                        )
                        pass1_dir = os.path.join(
                            storageBase, "selection", args.tag, "pass1", era, data_mc, key
                        )
                        input_files = sorted(glob.glob(os.path.join(pass1_dir, "*_Skim.root")))
                        if not input_files:
                            print(f"    Warning: no pass-1 files in {pass1_dir}, skipping {key}.")
                            continue
                        task_cut_string = None  # pass-1 files are already selected

                    # Build module configs: era-resolved + absolute SF paths
                    module_configs = []
                    for mod_name in module_names:
                        mod_cfg_raw = config.get("Modules", {}).get(mod_name, {})
                        # Use era-specific sub-config if present, else use top-level config
                        mod_cfg = mod_cfg_raw.get(era, mod_cfg_raw)
                        mod_cfg = _resolve_sf_paths(mod_name, mod_cfg, nanoaodtools_base)
                        module_configs.append({"name": mod_name, "config": mod_cfg})

                    for file_path in input_files:
                        # Idempotency: skip if the Skim output already exists
                        skim_name = os.path.basename(file_path).replace(".root", "_Skim.root")
                        skim_path = os.path.join(outputDir, skim_name)
                        if not args.force and os.path.exists(skim_path):
                            era_skipped += 1
                            continue
                        task = {
                            "era":       era,
                            "DataMC":    data_mc,
                            "key":       key,
                            "outputDir": outputDir,
                            "file":      file_path,
                            "cut_string": task_cut_string,
                            "goldenJSON": golden_json_abs if is_data else None,
                            "branchsel": None,
                            "modules":   module_configs,
                        }
                        era_process_list.append(task)
                        if args.sample:
                            break  # one file per dataset key is enough

            era_output_path = output_dir / era / f"{args.tag}_{era}_pass{args.run_pass}_processListJSON.json"
            with open(era_output_path, 'w') as f:
                json.dump(era_process_list, f, indent=2)
            print(f"  Era {era}: {len(era_process_list)} tasks queued, {era_skipped} already done -> {era_output_path}")
            total_tasks += len(era_process_list)

            if args.run:
                if not era_process_list:
                    print(f"  Era {era}: nothing to run, skipping submission.")
                else:
                    run_script = base_dir / 'scripts' / 'runSelection.py'
                    cmd = [
                        sys.executable, str(run_script),
                        '--processListJSON', str(era_output_path),
                        '--workers', str(args.workers),
                    ]
                    print(f"  Submitting era {era}: {' '.join(cmd)}")
                    result = subprocess.run(cmd)
                    if result.returncode != 0:
                        print(f"  Error: runSelection.py exited with code {result.returncode} for era {era}.")

        print(f"\nTotal tasks across all eras: {total_tasks}")

    exit(0)


if __name__ == '__main__':
    sys.exit(main())
