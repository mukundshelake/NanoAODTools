#!/usr/bin/env python3
"""
Master script to generate all outputs for 003-ObjectSelectionIII chapter.

Usage:
    python scripts/run_all.py [--force] [--tag TAG_NAME]
    
Options:
    --force: Regenerate outputs even if config hash already exists
    --tag: Create a named tag symlink to this run (e.g., "baseline", "paper_v1")
"""

import argparse
import os
import shutil
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
    parser = argparse.ArgumentParser(description='Generate all outputs for 003-ObjectSelectionIII')
    parser.add_argument('-t', '--tag', type=str,
                       help='Create named tag for this run (e.g., baseline, paper_v1)', default='Dump')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate outputs even if output files already exist for this config hash')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--generateSelectionIIDatasetJSON', action='store_true',
                       help='[0] Scan {STORAGE}/selectionII/{selectionIITag}/{selectionIIHash}/{era} on disk '
                            '(the 003-ObjectSelectionII output) via scripts/generateDatasetJSON.py -- the same '
                            'health-checked scan 003-ObjectSelectionII itself uses, run fresh each time so the '
                            'recorded file paths always reflect where the files actually are right now -- and '
                            'build the per-dataset coffea fileset '
                            'inputs/{DataMC}_{group}_{dataset}_{era}_fileset.json for every era/DataMC/group/'
                            'dataset (also copied into this run\'s outputs/{tag}/{hash}/inputs/ snapshot). '
                            'Requires --selectionIITag and --selectionIIHash.')
    parser.add_argument('--selectionIITag', type=str, default=None,
                       help='[0] Tag of the 003-ObjectSelectionII run to scan (e.g. "midAugust"). '
                            'Required by --generateSelectionIIDatasetJSON.')
    parser.add_argument('--selectionIIHash', type=str, default=None,
                       help='[0] Config hash of the 003-ObjectSelectionII run to scan. '
                            'Required by --generateSelectionIIDatasetJSON.')
    parser.add_argument('--fetchABCDScaleFactor', action='store_true',
                       help='[0b] Fetch abcdScaleFactor_{era}.root (the ABCD_transferFactor_R TH2 map computed '
                            "by 003-ObjectSelectionII's computeABCDScaleFactor.py) from that chapter's own "
                            'output into inputs/SFs/{era}_abcdScaleFactor.root (and this run\'s '
                            "outputs/{tag}/{hash}/inputs/SFs/ snapshot). Reuses --selectionIITag/--selectionIIHash "
                            '-- the same 003-ObjectSelectionII run being read for selectionII skims is also where '
                            'R was computed. A direct, explicit copy, same as --generateSelectionIIDatasetJSON is '
                            'for the selectionII skims themselves.')
    parser.add_argument('--buildSelectionHists', action='store_true',
                       help='Run buildSelectionHists.py to create histograms for selection optimization')
    parser.add_argument('--regionFilter', type=int, default=0, choices=[0, 1, 2, 3],
                       help='ABCD_region code to scope --buildSelectionHists/--aggregrateGroupHists to '
                            '(0=A/signal region, 1=B/QCD control region, 2=C, 3=D). Default: 0 (region A) -- '
                            'the nominal Data/MC plots. Use 1 to build the region-B ingredients for '
                            '--buildQCDTemplate -- region-B histograms additionally get the ABCD transfer '
                            'factor R (from --fetchABCDScaleFactor, looked up live per event by SelMuon '
                            'pt/|eta|) folded into their weight. Output filenames get a _region{A,B,C,D} suffix.')
    parser.add_argument('--aggregrateGroupHists', action='store_true',
                       help='Stack up histograms from buildSelectionHists.py at the group level (e.g., "SingleTop") and save aggregated histograms to outputs/{tag}/{config_hash}/{era}/{DataMC}/{group}[...]')
    parser.add_argument('--buildQCDTemplate', action='store_true',
                       help='Build the data-driven QCD template: max(regionB Data - sum of regionB non-QCD MC '
                            'groups, 0) per histDetails variable, per era. Region-B histograms are already '
                            'weighted by the ABCD transfer factor R = N_C/N_D (folded in per event by '
                            '--regionFilter 1, looked up from --fetchABCDScaleFactor\'s output), so this '
                            'background-subtracted region-B shape is already the properly-normalized region-A '
                            'QCD prediction: N_A_pred = R * N_B -- requires --aggregrateGroupHists --regionFilter 1 '
                            'to have been run first for Data_mu/SingleMuon and every non-QCD MC_mu group. Writes '
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
    print(f"  --generateSelectionIIDatasetJSON: {args.generateSelectionIIDatasetJSON}")
    print(f"  --selectionIITag: {args.selectionIITag}")
    print(f"  --selectionIIHash: {args.selectionIIHash}")
    print(f"  --fetchABCDScaleFactor: {args.fetchABCDScaleFactor}")
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

    print(f"Using config: {config_path}")

    # Load config and compute hash
    config = utils.load_config(config_path)

    # Create output directory
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
    
    if args.printHash:
        print(f"Config hash: {config_hash}")
        sys.exit(0)
    
    storageBase = utils.resolve_storage_path(config)
    print(f"Using storage base: {storageBase}")

    # Scan 003-ObjectSelectionII's selectionII ROOT files on disk fresh via
    # generateDatasetJSON.py, then build the per-dataset coffea fileset directly from
    # that scan -- replaces the old --fetchFromPreviousChapter, which just copied a
    # fileset JSON 003-ObjectSelectionII had generated at some earlier point and could
    # go stale (its recorded paths pointing at files that had since moved or become
    # unreachable from this machine, with nothing to catch the drift). Same pattern as
    # 003-ObjectSelectionI's --generatePreselectionDatasetJSON and
    # 003-ObjectSelectionII's --generateSelectionIDatasetJSON.
    if args.generateSelectionIIDatasetJSON:
        if not args.selectionIITag or not args.selectionIIHash:
            print("Error: --generateSelectionIIDatasetJSON requires --selectionIITag and --selectionIIHash.")
            sys.exit(1)
        generateJSON_script = base_dir / 'scripts' / 'generateDatasetJSON.py'
        if not generateJSON_script.exists():
            print(f"Error: {generateJSON_script} not found!")
            sys.exit(1)
        print(f"\nGenerating selectionII dataset JSON / filesets from disk (tag: {args.selectionIITag}, "
              f"hash: {args.selectionIIHash})...")
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"  Era: {era}")
            dataset_json_name = f"selectionII_{era}_datasets.json"
            base_directory = os.path.join(storageBase, "selectionII", args.selectionIITag,
                                           args.selectionIIHash, era)
            cmd = [
                sys.executable, str(generateJSON_script),
                '--outputDirectory', str(inputs_folder),
                '--outputFileName', dataset_json_name,
                '--baseDirectory', base_directory,
            ]
            print(f"    Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"    Error running generateDatasetJSON.py for era {era}:\n{result.stderr}")
                sys.exit(1)
            dataset_json_path = inputs_folder / dataset_json_name
            output_dataset_json_path = output_dir / 'inputs' / dataset_json_name
            output_dataset_json_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(dataset_json_path, output_dataset_json_path)
            print(f"    Generated {dataset_json_path} and copied to {output_dataset_json_path}")

            # Build the per-dataset coffea fileset directly from the freshly-scanned
            # dataset JSON -- same wrapping 003-ObjectSelectionII's --prepareFileset does,
            # done here instead of fetching 003-ObjectSelectionII's own already-built copy.
            with open(dataset_json_path) as f:
                datasetJSON = json.load(f)
            for DataMC in datasetJSON:
                if not matches_filter(args.filter, era, DataMC):
                    continue
                for group in datasetJSON[DataMC]:
                    if not matches_filter(args.filter, era, DataMC, group):
                        continue
                    for dataset in datasetJSON[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        datasetName = f'{era}_{DataMC}_{group}_{dataset}'
                        fileset = {datasetName: {"files": datasetJSON[DataMC][group][dataset], "metadata": {}}}
                        if 'data' in DataMC.lower():
                            fileset[datasetName]['metadata']['isData'] = True
                        else:
                            fileset[datasetName]['metadata']['isData'] = False
                            fileset[datasetName]['metadata']['era'] = era
                            fileset[datasetName]['metadata']['sample'] = dataset
                        fileset_filename = f'{DataMC}_{group}_{dataset}_{era}_fileset.json'
                        local_path = inputs_folder / fileset_filename
                        with open(local_path, 'w') as f:
                            json.dump(fileset, f, indent=2)
                        output_path = output_dir / 'inputs' / fileset_filename
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(local_path, output_path)
                        print(f"    Built {fileset_filename}")
        print("Finished generating selectionII dataset JSON / filesets.")

    # Fetch abcdScaleFactor_{era}.root directly from 003-ObjectSelectionII's own output --
    # deliberately a direct, explicit copy (mirrors --generateSelectionIIDatasetJSON reusing
    # the same --selectionIITag/--selectionIIHash), not routed through the dataset-JSON scan
    # above, since this is a chapter-computed artifact, not a selectionII skim file list.
    if args.fetchABCDScaleFactor:
        print("\nFetching ABCD scale factor files from 003-ObjectSelectionII...")
        if not args.selectionIITag or not args.selectionIIHash:
            print("Error: --fetchABCDScaleFactor requires --selectionIITag and --selectionIIHash.")
            sys.exit(1)
        abcd_source_base = base_dir.parent / '003-ObjectSelectionII' / 'outputs' / args.selectionIITag / args.selectionIIHash
        any_fetched = False
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            source_path = abcd_source_base / era / f"abcdScaleFactor_{era}.root"
            if not source_path.exists():
                print(f"  Error: source not found: {source_path}. Skipping era {era}.")
                continue
            rel_path = Path('SFs') / f"{era}_abcdScaleFactor.root"
            local_path = inputs_folder / rel_path
            snapshot_path = output_dir / 'inputs' / rel_path
            if local_path.exists() and not args.force:
                print(f"  [skip, already fetched] {rel_path}")
                continue
            local_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, local_path)
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, snapshot_path)
            any_fetched = True
            print(f"  Fetched {source_path} -> {local_path}")
        if not any_fetched:
            print("  All ABCD scale factor files already present in inputs/SFs/ (use --force to refetch).")

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
                        fileSetJSON = output_dir / 'inputs' / f'{DataMC}_{group}_{dataset}_{era}_fileset.json'
                        if not fileSetJSON.exists():
                            print(f"Error: FileSet JSON not found for {era}/{DataMC}/{group}/{dataset} at {fileSetJSON}. "
                                  f"Run --generateSelectionIIDatasetJSON --selectionIITag <tag> --selectionIIHash <hash> first.")
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
                        if args.regionFilter == 1:
                            abcd_sf_file = inputs_folder / 'SFs' / f'{era}_abcdScaleFactor.root'
                            if not abcd_sf_file.exists():
                                print(f"Error: {abcd_sf_file} not found. Run --fetchABCDScaleFactor first "
                                      f"(needed for --regionFilter 1).")
                                sys.exit(1)
                            command += ['--abcdScaleFactorFile', str(abcd_sf_file)]
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
        print(f"\nBuilding data-driven QCD template (region B, background-subtracted, qcdGroup={args.qcdGroup})...")
        region_b_label = REGION_LABELS[1]
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"Processing era: {era}")
            data_file = (output_dir / era / 'Data_mu' / 'SingleMuon' /
                         f'{args.tag}_{era}_Data_mu_SingleMuon_region{region_b_label}_selectionHists.coffea')
            if not data_file.exists():
                print(f"  Error: region-B Data histogram not found: {data_file}. "
                      f"Run --aggregrateGroupHists --regionFilter 1 first. Skipping era.")
                continue
            data_hists = load(data_file)[f'{era}_Data_mu_SingleMuon']

            bkg_groups = [g for g in config['NgenandXsec'][era].get('MC_mu', {}) if g != args.qcdGroup]
            print(f"  Background groups (region B): {bkg_groups}")
            template = {}
            floored_report = {}
            for histInfo in config['histDetails']:
                if histInfo not in data_hists:
                    print(f"  [WARN] '{histInfo}' missing from region-B Data histograms; skipping.")
                    continue
                h = data_hists[histInfo].copy()
                for group in bkg_groups:
                    bkg_file = (output_dir / era / 'MC_mu' / group /
                                f'{args.tag}_{era}_MC_mu_{group}_region{region_b_label}_selectionHists.coffea')
                    if not bkg_file.exists():
                        print(f"  [WARN] region-B histogram not found for background group '{group}': {bkg_file}. "
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
