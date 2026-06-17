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
from coffea.util import load, save

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
    parser.add_argument('--copyOutputsFromSelectionII', action='store_true',
                       help='Copy outputs from 003-ObjectSelectionII instead of running the processing steps')
    parser.add_argument('--selectionIIHash', type=str, default=None,
                       help='Hash of the 003-ObjectSelectionII run to copy outputs from (if --copyOutputsFromSelectionII is not used)')
    parser.add_argument('--buildSelectionHists', action='store_true',
                       help='Run buildSelectionHists.py to create histograms for selection optimization')
    parser.add_argument('--aggregrateGroupHists', action='store_true',
                       help='Stack up histograms from buildSelectionHists.py at the group level (e.g., "SingleTop") and save aggregated histograms to outputs/{tag}/{config_hash}/{era}/{DataMC}/{group}/{args.tag}_{era}_{DataMC}_{group}_selectionHists.coffea')
    parser.add_argument('--sample', action='store_true',
                       help='Only add the first file of each dataset to the process list JSON (for testing purposes)')
    parser.add_argument('--workers', type=int, default=15,
                       help='Number of parallel workers passed to runSelection.py (default: 15)')
    args = parser.parse_args()

    # parsing arguments
    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --sample: {args.sample}")
    print(f"  --copyOutputsFromSelectionII: {args.copyOutputsFromSelectionII}")
    print(f"  --selectionIIHash: {args.selectionIIHash}")
    print(f"  --buildSelectionHists: {args.buildSelectionHists}")
    print(f"  --aggregrateGroupHists: {args.aggregrateGroupHists}")
    print(f"  --workers: {args.workers}")
    print(f"  --force: {args.force}")
    print(f"  --filter: {args.filter}")

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

    # If  --copyOutputsFromSelectionII is set, copy outputs from 003-ObjectSelectionII. From 003-ObjectSelectionII's outputs/{tag}/{args.selectionIIHash}/*** to 003-ObjectSelectionIII's outputs/{tag}/{config_hash}/
    if args.copyOutputsFromSelectionII:
        if not args.selectionIIHash:
            print("Error: --selectionIIHash must be provided when --copyOutputsFromSelectionII is set.")
            sys.exit(1)
        source_dir = base_dir.parent / '003-ObjectSelectionII' / 'outputs' / args.tag / args.selectionIIHash
        if not source_dir.exists():
            print(f"Error: Source directory for copying does not exist: {source_dir}")
            sys.exit(1)
        print(f"Copying outputs from {source_dir} to {output_dir}...")
        subprocess.run(['cp', '-r', str(source_dir) + '/.', str(output_dir)], check=True)
        print("Copy completed.")
    
    if args.buildSelectionHists:
        print("Building selection histograms...")
        # Find the buildSelectionHists.py script in the current directory
        build_selection_hists_script = base_dir / 'scripts' / 'buildSelectionHists.py'
        # Check if the script exists        
        if not build_selection_hists_script.exists():
            print(f"Error: buildSelectionHists.py script not found at {build_selection_hists_script}")
            sys.exit(1)
        # Loop over all datasets in the config and run buildSelectionHists.py for each one
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"Processing era: {era}")
            for DataMC in config['NgenandXsec'][era]:
                if not matches_filter(args.filter, era, DataMC):
                    continue
                print(f"  Data/MC: {DataMC}")
                for group in config['NgenandXsec'][era][DataMC]:
                    if not matches_filter(args.filter, era, DataMC, group):
                        continue
                    print(f"    Group: {group}")
                    for dataset in config['NgenandXsec'][era][DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        print(f"      Dataset: {dataset}")
                        fileSetJSON = output_dir / era / DataMC / group / dataset / f'{args.tag}_{DataMC}_{group}_{dataset}_{era}_fileset.json'
                        if not fileSetJSON.exists():
                            print(f"Error: FileSet JSON not found for {era}/{DataMC}/{group}/{dataset} at {fileSetJSON}")
                            continue
                        outputDirectory = output_dir / era / DataMC / group / dataset
                        outputDirectory.mkdir(parents=True, exist_ok=True)
                        outputFileName = f'{args.tag}_{DataMC}_{group}_{dataset}_{era}_selectionHists.coffea'
                        # Skip if output file already exists and --force is not set
                        if (outputDirectory / outputFileName).exists() and not args.force:
                            print(f"Output file already exists for {era}/{DataMC}/{group}/{dataset} at {outputDirectory / outputFileName}. Skipping (use --force to overwrite).")
                            continue
                        command = [
                            'python', str(build_selection_hists_script),
                            '--fileSet', str(fileSetJSON),
                            '--configFile', str(config_path),
                            '--outputDir', str(outputDirectory),
                            '--outputFileName', outputFileName
                        ]
                        subprocess.run(command, check=True)
                        print(f"Finished building selection histograms for {era}/{DataMC}/{group}/{dataset}. Output saved to {outputDirectory / outputFileName}")
    # If --aggregrateGroupHists is set, aggregate histograms from buildSelectionHists.py at the group level (e.g., "SingleTop") and save aggregated histograms to outputs/{tag}/{config_hash}/{era}/{DataMC}/{group}/{args.tag}_{era}_{DataMC}_{group}_selectionHists.coffea
    if args.aggregrateGroupHists:
        print(f"Aggregating histograms at the group level for group: {args.aggregrateGroupHists}...")
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"Processing era: {era}")
            for DataMC in config['NgenandXsec'][era]:
                if not matches_filter(args.filter, era, DataMC):
                    continue
                print(f"  Data/MC: {DataMC}")
                for group in config['NgenandXsec'][era][DataMC]:
                    if not matches_filter(args.filter, era, DataMC, group):
                        continue
                    print(f"    Group: {group}")
                    # Loop over the histDetails elements
                    groupHists = {}
                    groupHists[f'{era}_{DataMC}_{group}'] = {}
                    for histInfo in config['histDetails']:
                        # create empty hist histogram for incrementing later
                        hist_ = None
                        for dataset in config['NgenandXsec'][era][DataMC][group]:
                            if not matches_filter(args.filter, era, DataMC, group, dataset):
                                continue
                            print(f"      Dataset: {dataset}")
                            histFile = output_dir / era / DataMC / group / dataset / f'{args.tag}_{DataMC}_{group}_{dataset}_{era}_selectionHists.coffea'
                            if not histFile.exists():
                                print(f"Error: Histogram file not found for {era}/{DataMC}/{group}/{dataset} at {histFile}")
                                continue
                            histData = load(histFile)
                            key = f'{era}_{DataMC}_{group}_{dataset}'
                            Lumi = config['DataLumiInfo'][era]['Lumi']
                            Ngen = config['NgenandXsec'][era][DataMC][group][dataset]['Ngen']
                            Xsec = config['NgenandXsec'][era][DataMC][group][dataset]['Xsec']
                            if 'MC' in DataMC:
                                weight = Lumi*Xsec/Ngen if Ngen > 0 else 0
                            else:
                                weight = 1
                            if hist_ is None:
                                hist_ = histData[key]['hists'][histInfo] * weight
                            else:
                                hist_ += histData[key]['hists'][histInfo] * weight
                        if hist_ is not None:
                            groupHists[f'{era}_{DataMC}_{group}'][histInfo] = hist_
                    # Save the aggregated histograms to outputs/{tag}/{config_hash}/{era}/{DataMC}/{group}/{args.tag}_{era}_{DataMC}_{group}_selectionHists.coffea
                    output_file = output_dir / era / DataMC / group / f'{args.tag}_{era}_{DataMC}_{group}_selectionHists.coffea'
                    # check if output file already exists and --force is not set
                    if output_file.exists() and not args.force:
                        print(f"Aggregated histogram file already exists for {era}/{DataMC}/{group} at {output_file}. Skipping (use --force to overwrite).")
                        continue
                    save(groupHists, output_file)
                    print(f"Finished aggregating histograms for {era}/{DataMC}/{group}. Output saved to {output_file}")
    print("All tasks completed.")


if __name__ == '__main__':
    sys.exit(main())
