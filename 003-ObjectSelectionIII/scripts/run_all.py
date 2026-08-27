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


# ABCD_region code -> label, for naming region-scoped histogram files.
# See 003-ObjectSelectionI's SelectedObjectsProducer for the full convention.
REGION_LABELS = {0: 'A', 1: 'B', 2: 'C', 3: 'D'}


def main():
    parser = argparse.ArgumentParser(description='Generate all outputs for 002-Samples')
    parser.add_argument('-t', '--tag', type=str,
                       help='Create named tag for this run (e.g., baseline, paper_v1)', default='Dump')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate outputs even if output files already exist for this config hash')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--fetchFromPreviousChapter', action='store_true',
                       help='[0] Fetch {tag}_{DataMC}_{group}_{dataset}_{era}_fileset.json for every era/DataMC/'
                            'group/dataset from 003-ObjectSelectionII outputs into inputs/ (and this run\'s '
                            'outputs/inputs/ snapshot). Requires --previousHash.')
    parser.add_argument('--previousHash', type=str, default=None,
                       help='[0] Config hash of the 003-ObjectSelectionII run to fetch from (its outputs/{tag}/{hash}/ '
                            'directory). Required by --fetchFromPreviousChapter.')
    parser.add_argument('--buildSelectionHists', action='store_true',
                       help='Run buildSelectionHists.py to create histograms for selection optimization')
    parser.add_argument('--regionFilter', type=int, default=0, choices=[0, 1, 2, 3],
                       help='ABCD_region code to scope --buildSelectionHists/--aggregrateGroupHists to '
                            '(0=A/signal region, 1=B, 2=C, 3=D/QCD control region). Default: 0 (region A) -- '
                            'the nominal Data/MC plots. Use 3 to build the region-D ingredients for '
                            '--buildQCDTemplate. Output filenames get a _region{A,B,C,D} suffix.')
    parser.add_argument('--aggregrateGroupHists', action='store_true',
                       help='Stack up histograms from buildSelectionHists.py at the group level (e.g., "SingleTop") and save aggregated histograms to outputs/{tag}/{config_hash}/{era}/{DataMC}/{group}[...]')
    parser.add_argument('--buildQCDTemplate', action='store_true',
                       help='Build the data-driven QCD template: max(regionD Data - sum of regionD non-QCD MC '
                            'groups, 0) per histDetails variable, per era. qcdABCDWeight (from '
                            '003-ObjectSelectionII\'s ABCDTransferWeight module, folded into the region-D '
                            'histograms by --regionFilter 3) makes this already the properly-normalized '
                            'region-A QCD prediction -- requires --aggregrateGroupHists --regionFilter 3 to '
                            'have been run first for Data_mu/SingleMuon and every non-QCD MC_mu group. Writes '
                            '{tag}_{era}_QCDTemplate_selectionHists.coffea.')
    parser.add_argument('--qcdGroup', type=str, default='QCD',
                       help='With --buildQCDTemplate: MC_mu group to exclude from the background sum and '
                            'whose stack entry the template replaces (default: QCD).')
    parser.add_argument('--makeplots', action='store_true',
                       help='Run rootHists.py to create publication-ready Data/MC comparison plots')
    parser.add_argument('--printHash', action='store_true',
                       help='Print the config hash and exit (useful for debugging)')
    parser.add_argument('--sample', action='store_true',
                       help='Only add the first file of each dataset to the process list JSON (for testing purposes)')
    parser.add_argument('--workers', type=int, default=15,
                       help='Number of parallel workers passed to runSelection.py (default: 15)')
    args = parser.parse_args()

    # parsing arguments
    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --sample: {args.sample}")
    print(f"  --fetchFromPreviousChapter: {args.fetchFromPreviousChapter}")
    print(f"  --previousHash: {args.previousHash}")
    print(f"  --buildSelectionHists: {args.buildSelectionHists}")
    print(f"  --regionFilter: {args.regionFilter} ({REGION_LABELS[args.regionFilter]})")
    print(f"  --aggregrateGroupHists: {args.aggregrateGroupHists}")
    print(f"  --buildQCDTemplate: {args.buildQCDTemplate}")
    print(f"  --qcdGroup: {args.qcdGroup}")
    print(f"  --makeplots: {args.makeplots}")
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
    
    # Create or update the 'latest' symlink to point to the current output directory
    latest_link = outputs_base / 'latest'
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(output_dir.name)
    print(f"Updated symlink: {latest_link} -> {output_dir.name}")
    
    if args.printHash:
        print(f"Config hash: {config_hash}")
        sys.exit(0)
    
    storageBase = utils.resolve_storage_path(config)
    print(f"Using storage base: {storageBase}")

    # Fetch the per-dataset coffea fileset JSON (built by 003-ObjectSelectionII's
    # --prepareFileset) into inputs/. This is the only thing this chapter actually
    # consumes from 003-ObjectSelectionII -- its filename already encodes
    # DataMC/group/dataset/era, so all fetched files live flat in inputs/, same as
    # every other chapter's fetch step.
    if args.fetchFromPreviousChapter:
        if not args.previousHash:
            print("Error: --fetchFromPreviousChapter requires --previousHash to be specified.")
            sys.exit(1)
        print(f"\nFetching inputs from 003-ObjectSelectionII (hash: {args.previousHash})...")
        previous_chapter_outputs = base_dir.parent / '003-ObjectSelectionII' / 'outputs' / args.tag / args.previousHash
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"  Era: {era}")
            for DataMC in config['NgenandXsec'][era]:
                if not matches_filter(args.filter, era, DataMC):
                    continue
                for group in config['NgenandXsec'][era][DataMC]:
                    if not matches_filter(args.filter, era, DataMC, group):
                        continue
                    for dataset in config['NgenandXsec'][era][DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        filename = f'{args.tag}_{DataMC}_{group}_{dataset}_{era}_fileset.json'
                        source_path = previous_chapter_outputs / era / DataMC / group / dataset / filename
                        if not source_path.exists():
                            print(f"    Error: Source file not found: {source_path}. Skipping.")
                            continue
                        local_path, output_path = utils.fetch_and_snapshot(source_path, inputs_folder, output_dir, filename)
                        print(f"    Fetched {filename} -> {local_path} and {output_path}")
        print("Finished fetching inputs from 003-ObjectSelectionII.")
    
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
                        fileSetJSON = output_dir / 'inputs' / f'{args.tag}_{DataMC}_{group}_{dataset}_{era}_fileset.json'
                        if not fileSetJSON.exists():
                            print(f"Error: FileSet JSON not found for {era}/{DataMC}/{group}/{dataset} at {fileSetJSON}. "
                                  f"Run --fetchFromPreviousChapter --previousHash <hash> first.")
                            continue
                        outputDirectory = output_dir / era / DataMC / group / dataset
                        outputDirectory.mkdir(parents=True, exist_ok=True)
                        region_label = REGION_LABELS[args.regionFilter]
                        outputFileName = f'{args.tag}_{DataMC}_{group}_{dataset}_{era}_region{region_label}_selectionHists.coffea'
                        # Skip if output file already exists and --force is not set
                        if (outputDirectory / outputFileName).exists() and not args.force:
                            print(f"Output file already exists for {era}/{DataMC}/{group}/{dataset} at {outputDirectory / outputFileName}. Skipping (use --force to overwrite).")
                            continue
                        command = [
                            sys.executable, str(build_selection_hists_script),
                            '--fileSet', str(fileSetJSON),
                            '--configFile', str(config_path),
                            '--outputDir', str(outputDirectory),
                            '--outputFileName', outputFileName,
                            '--regionFilter', str(args.regionFilter),
                        ]
                        subprocess.run(command, check=True)
                        print(f"Finished building selection histograms for {era}/{DataMC}/{group}/{dataset}. Output saved to {outputDirectory / outputFileName}")
    # If --aggregrateGroupHists is set, aggregate histograms from buildSelectionHists.py at the group level (e.g., "SingleTop") and save aggregated histograms to outputs/{tag}/{config_hash}/{era}[...]
    if args.aggregrateGroupHists:
        region_label = REGION_LABELS[args.regionFilter]
        print(f"Aggregating histograms at the group level for group: {args.aggregrateGroupHists} "
              f"(region {region_label})...")
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
                        print(f"        Working on histogram {histInfo}")
                        for dataset in config['NgenandXsec'][era][DataMC][group]:
                            if not matches_filter(args.filter, era, DataMC, group, dataset):
                                continue
                            print(f"      Dataset: {dataset}")
                            histFile = output_dir / era / DataMC / group / dataset / f'{args.tag}_{DataMC}_{group}_{dataset}_{era}_region{region_label}_selectionHists.coffea'
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
                            print(f"            Added histogram for {era}/{DataMC}/{group}/{dataset} with weight {weight}")
                        if hist_ is not None:
                            groupHists[f'{era}_{DataMC}_{group}'][histInfo] = hist_
                    # Save the aggregated histograms to outputs/{tag}/{config_hash}/{era}/{DataMC}/{group}/{args.tag}_{era}_{DataMC}_{group}_region{X}_selectionHists.coffea
                    output_file = output_dir / era / DataMC / group / f'{args.tag}_{era}_{DataMC}_{group}_region{region_label}_selectionHists.coffea'
                    # check if output file already exists and --force is not set
                    if output_file.exists() and not args.force:
                        print(f"Aggregated histogram file already exists for {era}/{DataMC}/{group} at {output_file}. Skipping (use --force to overwrite).")
                        continue
                    save(groupHists, output_file)
                    print(f"Finished aggregating histograms for {era}/{DataMC}/{group}. Output saved to {output_file}")

    if args.buildQCDTemplate:
        print(f"\nBuilding data-driven QCD template (region D, background-subtracted, qcdGroup={args.qcdGroup})...")
        region_d_label = REGION_LABELS[3]
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"Processing era: {era}")
            data_file = (output_dir / era / 'Data_mu' / 'SingleMuon' /
                         f'{args.tag}_{era}_Data_mu_SingleMuon_region{region_d_label}_selectionHists.coffea')
            if not data_file.exists():
                print(f"  Error: region-D Data histogram not found: {data_file}. "
                      f"Run --aggregrateGroupHists --regionFilter 3 first. Skipping era.")
                continue
            data_hists = load(data_file)[f'{era}_Data_mu_SingleMuon']

            bkg_groups = [g for g in config['NgenandXsec'][era].get('MC_mu', {}) if g != args.qcdGroup]
            print(f"  Background groups (region D): {bkg_groups}")
            template = {}
            floored_report = {}
            for histInfo in config['histDetails']:
                if histInfo not in data_hists:
                    print(f"  [WARN] '{histInfo}' missing from region-D Data histograms; skipping.")
                    continue
                h = data_hists[histInfo].copy()
                for group in bkg_groups:
                    bkg_file = (output_dir / era / 'MC_mu' / group /
                                f'{args.tag}_{era}_MC_mu_{group}_region{region_d_label}_selectionHists.coffea')
                    if not bkg_file.exists():
                        print(f"  [WARN] region-D histogram not found for background group '{group}': {bkg_file}. "
                              f"Treating its contribution as 0 for '{histInfo}'.")
                        continue
                    bkg_hists = load(bkg_file)[f'{era}_MC_mu_{group}']
                    if histInfo not in bkg_hists:
                        continue
                    h = h - bkg_hists[histInfo]
                # Floor negative bins at 0 -- a negative data-driven QCD count is
                # unphysical, usually background MC modestly overshooting data in a
                # low-stat bin (same practice as computeABCDScaleFactor.py's floor_at_zero()).
                view = h.view()
                n_negative = int((view < 0).sum())
                if n_negative:
                    floored_report[histInfo] = n_negative
                    view[view < 0] = 0
                template[histInfo] = h
            output_file = output_dir / era / f'{args.tag}_{era}_QCDTemplate_selectionHists.coffea'
            save({f'{era}_QCDTemplate': template}, output_file)
            if floored_report:
                print(f"  [WARN] Floored negative bins (per variable): {floored_report}")
            print(f"Finished building QCD template for {era}. Output saved to {output_file}")

    # If --makeplots is set, run rootHists.py to create publication-ready plots
    if args.makeplots:
        print("Creating plots from aggregated histograms...")
        root_hists_script = base_dir / 'scripts' / 'rootHists.py'
        if not root_hists_script.exists():
            print(f"Error: rootHists.py script not found at {root_hists_script}")
            sys.exit(1)
        
        plots_dir = output_dir / 'plots'
        plots_dir.mkdir(parents=True, exist_ok=True)
        
        command = [
            sys.executable, str(root_hists_script),
            '--tag', args.tag,
            '--hash', config_hash,
            '--outputDir', str(plots_dir),
            '--configFile', str(config_path),
            '--qcdGroup', args.qcdGroup,
        ]
        
        # Add filter argument if provided
        if args.filter:
            command.extend(['--filter'] + args.filter)
        
        subprocess.run(command, check=True)
        print(f"Finished creating plots. Output saved to {plots_dir}")
        
        # Create comprehensive PDF report with all plots and config details
        print("Creating comprehensive PDF report...")
        create_pdf_script = base_dir / 'scripts' / 'createPlotsPDF.py'
        if not create_pdf_script.exists():
            print(f"Warning: createPlotsPDF.py script not found at {create_pdf_script}. Skipping PDF creation.")
        else:
            try:
                command = [
                    sys.executable, str(create_pdf_script),
                    '--configFile', str(config_path),
                    '--hash', config_hash,
                    '--tag', args.tag,
                    '--outputDir', str(output_dir)
                ]
                subprocess.run(command, check=True)
                pdf_file = output_dir / f'{args.tag}_{config_hash}_report.pdf'
                print(f"PDF report successfully created: {pdf_file}")
            except subprocess.CalledProcessError as e:
                print(f"Error creating PDF report: {e}")
                print("Continuing without PDF creation...")
    
    print("All tasks completed.")


if __name__ == '__main__':
    sys.exit(main())
