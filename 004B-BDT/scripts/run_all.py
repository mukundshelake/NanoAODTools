#!/usr/bin/env python3
"""
Master script to generate all outputs for 004B-BDT.

This script orchestrates the BDT variable computation workflow:
1. Fetches reconstruction dataset JSONs from 004A outputs (optional)
2. Generates a process list JSON for runBDTVariables.py
3. Writes a bash script to execute the processing, or runs it directly
4. Generates dataset JSON from the BDT outputs (optional)

Usage:
    python scripts/run_all.py [--force] [--tag TAG_NAME]

Options:
    --force: Regenerate outputs even if output files already exist
    --tag:   Create a named tag for this run (e.g., "earlyApril")
    --fetchFromPreviousChapter: Fetch reconstruction JSONs from 004A-Reconstruction outputs
    --generateProcessListJSON: Generate process list for runBDTVariables.py
    --writeBashScript: Create a bash script instead of running directly
    --generateDatasetJSON: Create dataset JSON from BDT outputs
"""

import argparse
import os
import sys
from pathlib import Path
import subprocess
import json
from coffea.util import load, save

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
    parser = argparse.ArgumentParser(description='Generate all outputs for 004B-BDT')
    parser.add_argument('-t', '--tag', type=str,
                       help='Create named tag for this run (e.g., earlyApril)', default='Dump')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate outputs even if output files already exist for this config hash')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--fetchFromPreviousChapter', action='store_true',
                       help='[0] Fetch reconstruction_{tag}_{era}_datasets.json from 004A-Reconstruction outputs '
                            'into inputs/ (and this run\'s outputs/inputs/ snapshot). Requires --previousHash.')
    parser.add_argument('--previousHash', type=str, default=None,
                       help='[0] Config hash of the 004A-Reconstruction run to fetch from (its outputs/{tag}/{hash}/ '
                            'directory). Required by --fetchFromPreviousChapter.')
    parser.add_argument('--generateProcessListJSON', action='store_true',
                       help='[1] Generate process list JSON for runBDTVariables.py by reading per-era '
                            'reconstruction dataset JSONs from the inputs folder')
    parser.add_argument('--writeBashScript', action='store_true',
                       help='[2] Write a bash script with all runBDTVariables.py commands instead of executing them directly')
    parser.add_argument('--generateDatasetJSON', action='store_true',
                       help='[3] Generate dataset JSON by scanning the reconstruction output directory')
    parser.add_argument('--printHash', action='store_true',
                       help='Print the config hash and exit')
    parser.add_argument('--sample', action='store_true',
                       help='Only add the first file of each dataset to the process list JSON (for testing)')
    parser.add_argument('--workers', type=int, default=15,
                       help='Number of parallel workers passed to runBDTVariables.py (default: 15)')
    parser.add_argument('--buildBDTVariableHists', action='store_true',
                       help='Run buildBDTVariableHists.py to create histograms for BDT variables')
    parser.add_argument('--aggregateBDTVariableHists', action='store_true',
                       help='Aggregate BDT variable histograms at the group level')
    parser.add_argument('--makeBDTVariablePlots', action='store_true',
                       help='Run BDTvariablesHistPlotter.py to create Data/MC comparison plots')
    args = parser.parse_args()

    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --fetchFromPreviousChapter: {args.fetchFromPreviousChapter}")
    print(f"  --previousHash: {args.previousHash}")
    print(f"  --generateProcessListJSON: {args.generateProcessListJSON}")
    print(f"  --writeBashScript: {args.writeBashScript}")
    print(f"  --generateDatasetJSON: {args.generateDatasetJSON}")
    print(f"  --buildBDTVariableHists: {args.buildBDTVariableHists}")
    print(f"  --aggregateBDTVariableHists: {args.aggregateBDTVariableHists}")
    print(f"  --makeBDTVariablePlots: {args.makeBDTVariablePlots}")
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

    # Create or update the 'latest' symlink to point to the current output directory
    latest_link = outputs_base / 'latest'
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(output_dir.name)
    print(f"Updated symlink: {latest_link} -> {output_dir.name}")

    storageBase     = utils.resolve_storage_path(config)
    print(f"Using storage base: {storageBase}")

    if args.printHash:
        print(f"Config hash: {config_hash}")
        return 0

    # Fetch reconstruction dataset JSON into inputs/
    if args.fetchFromPreviousChapter:
        if not args.previousHash:
            print("Error: --fetchFromPreviousChapter requires --previousHash to be specified.")
            return 1
        print(f"\nFetching inputs from 004A-Reconstruction (hash: {args.previousHash})...")
        previous_chapter_outputs = base_dir.parent / '004A-Reconstruction' / 'outputs' / args.tag / args.previousHash
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"  Era: {era}")
            filename = f'reconstruction_{args.tag}_{era}_datasets.json'
            source_path = previous_chapter_outputs / era / filename
            if not source_path.exists():
                print(f"    Error: Source file not found: {source_path}. Skipping.")
                continue
            local_path, output_path = utils.fetch_and_snapshot(source_path, inputs_folder, output_dir, filename)
            print(f"    Fetched {filename} -> {local_path} and {output_path}")
        print("Finished fetching inputs from 004A-Reconstruction.")

    # --generateProcessListJSON
    if args.generateProcessListJSON:
        print("\nGenerating process list JSON for runBDTVariables.py...")
        total_tasks = 0

        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"\nProcessing era: {era}")

            reconstruction_dataset_json = (
                output_dir / 'inputs' /
                f'reconstruction_{args.tag}_{era}_datasets.json'
            )

            if not reconstruction_dataset_json.exists():
                print(f"  Warning: Dataset JSON not found: {reconstruction_dataset_json}. Skipping era {era}.")
                continue

            with open(reconstruction_dataset_json) as f:
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
                            storageBase, "BDTVariables", args.tag, config_hash, era, DataMC, group, dataset
                        )

                        # Build module configs; era is passed at runtime by runBDTVariables.py
                        module_configs = []
                        for mod_name in module_names:
                            mod_cfg = config.get("Modules", {}).get(mod_name, {})
                            module_configs.append({"name": mod_name, "config": mod_cfg})

                        isSample = True
                        for filePath in datasetJSON[DataMC][group][dataset]:
                            bdt_name = os.path.basename(filePath).replace(".root", "_BDTVars.root")
                            bdt_path = os.path.join(outputDir, bdt_name)
                            if not args.force and os.path.exists(bdt_path):
                                era_skipped += 1
                                print(f"    BDT output already exists, skipping: {bdt_path}")
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
            print(f"  Era {era}: {len(era_process_list)} tasks added, {era_skipped} skipped (output already exists).")

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
                            f"python3 {base_dir / 'scripts' / 'runBDTVariables.py'} "
                            f"--processListJSON {process_list_json} "
                            f"--workers {args.workers} "
                            f"{'--force ' if args.force else ''}"
                            f"{'--sample ' if args.sample else ''}"
                            f"{'--filter ' + era + '/' + DataMC + '/' + group}"
                            f"{' 2>&1 | tee -a ' + str(output_dir / era / DataMC / group / f'{args.tag}_{era}_{DataMC}_{group}.log')}"
                        )
                        f.write(cmd + "\n")
            # (Placeholder: delta-plot generation commands can be appended here
            #  once a --generateDeltaPlots argument is wired up.)
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
            outputFileName  = f"BDTVariables_{args.tag}_{era}_datasets.json"
            baseDirectory   = f'{storageBase}/BDTVariables/{args.tag}/{config_hash}/{era}'
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
    
    # --buildBDTVariableHists
    if args.buildBDTVariableHists:
        print("\nBuilding BDT variable histograms...")
        build_bdt_hists_script = base_dir / 'scripts' / 'buildBDTVariableHists.py'
        if not build_bdt_hists_script.exists():
            print(f"Error: buildBDTVariableHists.py script not found at {build_bdt_hists_script}")
            return 1
        
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"Processing era: {era}")
            
            # Load the generated dataset JSON for this era
            dataset_json_file = output_dir / era / f"BDTVariables_{args.tag}_{era}_datasets.json"
            if not dataset_json_file.exists():
                print(f"  Error: Dataset JSON not found at {dataset_json_file}")
                print(f"  Please run --generateDatasetJSON first.")
                continue
            
            with open(dataset_json_file) as f:
                dataset_json = json.load(f)
            
            for DataMC in dataset_json:
                if not matches_filter(args.filter, era, DataMC):
                    continue
                print(f"  Data/MC: {DataMC}")
                
                for group in dataset_json[DataMC]:
                    if not matches_filter(args.filter, era, DataMC, group):
                        continue
                    print(f"    Group: {group}")
                    
                    for dataset in dataset_json[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        print(f"      Dataset: {dataset}")
                        
                        # Create fileset for this dataset
                        fileset = {
                            f"{era}_{DataMC}_{group}_{dataset}": {
                                "files": {file: "Events" for file in dataset_json[DataMC][group][dataset]}
                            }
                        }
                        
                        # Create temporary fileset JSON
                        fileset_json_path = output_dir / era / DataMC / group / dataset / f'{args.tag}_{DataMC}_{group}_{dataset}_{era}_bdtvariables_fileset.json'
                        fileset_json_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(fileset_json_path, 'w') as f:
                            json.dump(fileset, f, indent=2)
                        
                        # Output histogram file path
                        output_hists_dir = output_dir / era / DataMC / group / dataset
                        output_hists_dir.mkdir(parents=True, exist_ok=True)
                        output_hists_file = f'{args.tag}_{DataMC}_{group}_{dataset}_{era}_bdtvariableHists.coffea'
                        
                        # Skip if output file already exists and --force is not set
                        if (output_hists_dir / output_hists_file).exists() and not args.force:
                            print(f"        Output file already exists: {output_hists_dir / output_hists_file}. Skipping (use --force to overwrite).")
                            continue
                        
                        # Run buildBDTVariableHists.py
                        command = [
                            sys.executable, str(build_bdt_hists_script),
                            '--fileSet', str(fileset_json_path),
                            '--configFile', str(config_path),
                            '--outputDir', str(output_hists_dir),
                            '--outputFileName', output_hists_file
                        ]
                        print(f"        Running: {' '.join(command)}")
                        result = subprocess.run(command, capture_output=True, text=True)
                        if result.returncode != 0:
                            print(f"        Error: {result.stderr}")
                            return 1
                        print(f"        Successfully built histograms: {output_hists_dir / output_hists_file}")
    
    # --aggregateBDTVariableHists
    if args.aggregateBDTVariableHists:
        print("\nAggregating BDT variable histograms at group level...")
        from coffea.util import load, save
        
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
                    
                    # Dictionary to store aggregated histograms
                    group_hists = {}
                    group_hists[f'{era}_{DataMC}_{group}'] = {}
                    
                    # Loop over histDetails keys
                    for hist_name in config.get('histDetails', {}):
                        print(f"      Working on histogram: {hist_name}")
                        aggregated_hist = None
                        
                        for dataset in config['NgenandXsec'][era][DataMC][group]:
                            if not matches_filter(args.filter, era, DataMC, group, dataset):
                                continue
                            
                            hist_file = (output_dir / era / DataMC / group / dataset / 
                                        f'{args.tag}_{DataMC}_{group}_{dataset}_{era}_bdtvariableHists.coffea')
                            
                            if not hist_file.exists():
                                print(f"        Histogram file not found: {hist_file}. Skipping.")
                                continue
                            
                            hist_data = load(hist_file)
                            dataset_key = f"{era}_{DataMC}_{group}_{dataset}"
                            
                            # Get luminosity, Ngen, and Xsec for weighting
                            Lumi = config['DataLumiInfo'][era]['Lumi']
                            Ngen = config['NgenandXsec'][era][DataMC][group][dataset].get('Ngen', 1)
                            Xsec = config['NgenandXsec'][era][DataMC][group][dataset].get('Xsec', -1)
                            
                            # Calculate weight
                            if 'MC' in DataMC:
                                weight = (Lumi * Xsec / Ngen) if Ngen > 0 else 0
                            else:
                                weight = 1
                            
                            # Add to aggregated histogram
                            if hist_name in hist_data[dataset_key]['hists']:
                                if aggregated_hist is None:
                                    aggregated_hist = hist_data[dataset_key]['hists'][hist_name] * weight
                                else:
                                    aggregated_hist += hist_data[dataset_key]['hists'][hist_name] * weight
                                print(f"        Added {dataset} with weight {weight}")
                        
                        if aggregated_hist is not None:
                            group_hists[f'{era}_{DataMC}_{group}'][hist_name] = aggregated_hist
                    
                    # Save aggregated histograms
                    output_file = output_dir / era / DataMC / group / f'{args.tag}_{era}_{DataMC}_{group}_bdtvariableHists.coffea'
                    output_file.parent.mkdir(parents=True, exist_ok=True)
                    
                    if output_file.exists() and not args.force:
                        print(f"      Aggregated histogram file already exists: {output_file}. Skipping (use --force to overwrite).")
                        continue
                    
                    save(group_hists, output_file)
                    print(f"      Saved aggregated histograms: {output_file}")
    
    # --makeBDTVariablePlots
    if args.makeBDTVariablePlots:
        print("\nCreating BDT variable Data/MC comparison plots...")
        bdt_plotter_script = base_dir / 'scripts' / 'BDTvariablesHistPlotter.py'
        if not bdt_plotter_script.exists():
            print(f"Error: BDTvariablesHistPlotter.py script not found at {bdt_plotter_script}")
            return 1

        plots_dir = output_dir / 'plots'
        plots_dir.mkdir(parents=True, exist_ok=True)

        command = [
            sys.executable, str(bdt_plotter_script),
            '--tag', args.tag,
            '--hash', config_hash,
            '--outputDir', str(plots_dir),
            '--configFile', str(config_path)
        ]

        if args.filter:
            command.extend(['--filter'] + args.filter)

        subprocess.run(command, check=True)
        print(f"Finished creating BDT variable plots. Output saved to {plots_dir}")

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

if __name__ == '__main__':
    sys.exit(main())
