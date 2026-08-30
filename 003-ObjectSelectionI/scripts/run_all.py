#!/usr/bin/env python3
"""
Master script to generate all outputs for 003-ObjectSelectionI chapter.

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
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    parser = argparse.ArgumentParser(description='Generate all outputs for 003-ObjectSelectionI')
    parser.add_argument('-t', '--tag', type=str,
                       help='Create named tag for this run (e.g., baseline, paper_v1)', default='Dump')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate outputs even if output files already exist for this config hash')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--generatePreselectionDatasetJSON', action='store_true',
                       help='[0] Scan {STORAGE}/preselection/{preselectionTag}/{preselectionHash}/{era} on disk '
                            '(the 002-Samples preselection output) and build inputs/preselection_{era}_datasets.json '
                            'via scripts/generateDatasetJSON.py -- the same health-checked scan 002-Samples itself '
                            'uses, run fresh each time so the recorded file paths always reflect where the files '
                            'actually are right now (also copied into this run\'s outputs/{tag}/{hash}/inputs/ '
                            'snapshot). Requires --preselectionTag and --preselectionHash.')
    parser.add_argument('--preselectionTag', type=str, default=None,
                       help='[0] Tag of the 002-Samples preselection run to scan (e.g. "midAugust"). '
                            'Required by --generatePreselectionDatasetJSON.')
    parser.add_argument('--preselectionHash', type=str, default=None,
                       help='[0] Config hash of the 002-Samples preselection run to scan. '
                            'Required by --generatePreselectionDatasetJSON.')
    parser.add_argument('--downloadGoldenJSONs', action='store_true',
                       help='[0] Download {era}_goldenJSON.json for each era directly from the CMS URLs in '
                            'config.yaml\'s golden_json_urls, into inputs/ (and this run\'s outputs/{tag}/{hash}/'
                            'inputs/ snapshot). Independent of any particular 002-Samples run -- the golden JSON '
                            'only depends on era, not on a preselection tag/hash.')
    parser.add_argument('--generateProcessListJSON', action='store_true',
                       help='[1] Generate process list JSON for runSelection.py by reading the per-era '
                            'dataset JSONs produced by --generateDatasetJSON')
    parser.add_argument('--writeBashScript', action='store_true',
                       help='[2] Write a bash script with all runSelection.py commands instead of executing them directly')
    parser.add_argument('--submitSelectionJobs', action='store_true',
                       help='[2alt][lxplus][CRAB] Submit object-selection jobs to CRAB instead of running them '
                            'locally -- an alternative to --writeBashScript + local execution. Processes the same '
                            'preselection skims (via Data.userInputFiles, since they are not DBS-registered) and '
                            'writes output to the same {STORAGE}/selectionI/{tag}/{hash}/... layout, so downstream '
                            'steps work unchanged either way. Uses scripts/crab/submit_selection_flexible.py.')
    parser.add_argument('--checkCrabStatus', action='store_true',
                       help='[lxplus][CRAB] Check CRAB job status for jobs submitted with --submitSelectionJobs. '
                            'Uses scripts/crab/checkStatus.py.')
    parser.add_argument('--resubmitFailedCrabJobs', action='store_true',
                       help='[lxplus][CRAB] With --checkCrabStatus: resubmit failed CRAB jobs.')
    parser.add_argument('--removeSubmitFailedCrabJobs', action='store_true',
                       help='[lxplus][CRAB] With --checkCrabStatus: remove CRAB jobs that never submitted successfully.')
    parser.add_argument('--generateDatasetJSON', action='store_true',
                       help='[3] Generate dataset JSON file using the script generateDatasetJSON.py')
    parser.add_argument('--printHash', action='store_true',
                       help='Print the config hash and exit (useful for debugging)')
    parser.add_argument('--sample', action='store_true',
                       help='Only add the first file of each dataset to the process list JSON (for testing purposes). '
                            'For --submitSelectionJobs, only submits the first file of each dataset via CRAB.')
    parser.add_argument('--workers', type=int, default=15,
                       help='Number of parallel workers passed to runSelection.py (default: 15)')
    parser.add_argument('--verifyOutput', action='store_true',
                       help='[4] Run scripts/verifyOutput.py on selectionI_{tag}_{era}_datasets.json (from '
                            '--generateDatasetJSON): checks each skim opens cleanly, has an Events tree, and '
                            'has every branch SelectedObjectsProducer is supposed to write, plus per-dataset '
                            'min/max/mean/stddev and cross-branch invariant checks scoped to those branches '
                            '(not the pass-through NanoAOD branches -- see the script for why). '
                            'Writes a JSON report per era.')
    parser.add_argument('--plotABCDVariables', action='store_true',
                       help='[exploratory] Run scripts/plotABCDVariables.py on selectionI_{tag}_{era}_datasets.json '
                            '(from --generateDatasetJSON): overlays normalized shape distributions of the '
                            'ABCDVariables config block (muon isolation, MET) for --abcdGroups, straight from the '
                            'selectionI skims -- ahead of picking any ABCD region-boundary values. '
                            'Writes PNG/PDF/ROOT per era to outputs/{tag}/{hash}/{era}/abcdPlots/.')
    parser.add_argument('--abcdDataMC', type=str, default='MC_mu',
                       help='With --plotABCDVariables: DataMC key to plot from (default: MC_mu).')
    parser.add_argument('--abcdGroups', nargs='+', default=['SemiLeptonic', 'QCD'],
                       help='With --plotABCDVariables: groups to overlay (default: SemiLeptonic QCD).')
    parser.add_argument('--abcdClosureTest', action='store_true',
                       help='[exploratory] Run scripts/abcdClosureTest.py on selectionI_{tag}_{era}_datasets.json '
                            '(from --generateDatasetJSON): checks N_A ~= N_B*N_C/N_D on the ABCD_region branch '
                            'SelectedObjectsProducer already writes, for --closureGroups (default: QCD -- the '
                            'actual ABCD estimate target) under --abcdDataMC. The standard sanity check for '
                            'whether muon isolation and MET are independent enough within that process for the '
                            'ABCD estimate to be valid. Writes a JSON report per era.')
    parser.add_argument('--closureGroups', nargs='+', default=['QCD'],
                       help='With --abcdClosureTest: groups to test (default: QCD).')
    args = parser.parse_args()

    # parsing arguments
    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --generatePreselectionDatasetJSON: {args.generatePreselectionDatasetJSON}")
    print(f"  --preselectionTag: {args.preselectionTag}")
    print(f"  --preselectionHash: {args.preselectionHash}")
    print(f"  --downloadGoldenJSONs: {args.downloadGoldenJSONs}")
    print(f"  --generateProcessListJSON: {args.generateProcessListJSON}")
    print(f"  --writeBashScript: {args.writeBashScript}")
    print(f"  --submitSelectionJobs: {args.submitSelectionJobs}")
    print(f"  --checkCrabStatus: {args.checkCrabStatus}")
    print(f"  --resubmitFailedCrabJobs: {args.resubmitFailedCrabJobs}")
    print(f"  --removeSubmitFailedCrabJobs: {args.removeSubmitFailedCrabJobs}")
    print(f"  --generateDatasetJSON: {args.generateDatasetJSON}")
    print(f"  --sample: {args.sample}")
    print(f"  --workers: {args.workers}")
    print(f"  --force: {args.force}")
    print(f"  --filter: {args.filter}")
    print(f"  --printHash: {args.printHash}")
    print(f"  --verifyOutput: {args.verifyOutput}")
    print(f"  --plotABCDVariables: {args.plotABCDVariables}")
    print(f"  --abcdDataMC: {args.abcdDataMC}")
    print(f"  --abcdGroups: {args.abcdGroups}")
    print(f"  --abcdClosureTest: {args.abcdClosureTest}")
    print(f"  --closureGroups: {args.closureGroups}")

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

    storageBase = utils.resolve_storage_path(config)
    print(f"Using storage base: {storageBase}")

    # Print config hash if asked for
    if args.printHash:
        print(f"Config hash: {config_hash}")
        return 0

    # Scan 002-Samples' preselection ROOT files on disk and build
    # inputs/preselection_{era}_datasets.json fresh via generateDatasetJSON.py --
    # replaces the old --fetchFromPreviousChapter, which just copied a JSON that
    # 002-Samples had generated at some earlier point and could go stale (its
    # recorded paths pointing at files that had since moved or become
    # unreachable from this machine, with nothing to catch the drift).
    if args.generatePreselectionDatasetJSON:
        if not args.preselectionTag or not args.preselectionHash:
            print("Error: --generatePreselectionDatasetJSON requires --preselectionTag and --preselectionHash.")
            return 1
        print(f"\nGenerating preselection dataset JSON from disk (tag: {args.preselectionTag}, "
              f"hash: {args.preselectionHash})...")
        generateJSON_script = base_dir / 'scripts' / 'generateDatasetJSON.py'
        if not generateJSON_script.exists():
            print(f"Error: {generateJSON_script} not found!")
            return 1
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"  Era: {era}")
            output_json_name = f"preselection_{era}_datasets.json"
            base_directory = os.path.join(storageBase, "preselection", args.preselectionTag,
                                           args.preselectionHash, era)
            cmd = [
                sys.executable, str(generateJSON_script),
                '--outputDirectory', str(inputs_folder),
                '--outputFileName', output_json_name,
                '--baseDirectory', base_directory,
            ]
            print(f"    Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"    Error running generateDatasetJSON.py for era {era}:\n{result.stderr}")
                return 1
            # Also copy into this run's own outputs/{tag}/{hash}/inputs/ snapshot --
            # create_output_directory() only snapshotted inputs/ as it existed at the
            # start of this invocation, before this step ran.
            output_path = output_dir / 'inputs' / output_json_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(inputs_folder / output_json_name, output_path)
            print(f"    Generated {inputs_folder / output_json_name} and copied to {output_path}")
        print("Finished generating preselection dataset JSON files.")

    # Download {era}_goldenJSON.json directly from the CMS URLs in config.yaml's
    # golden_json_urls. Independent of any particular 002-Samples run -- the golden
    # JSON content only depends on era.
    if args.downloadGoldenJSONs:
        print("\nDownloading golden JSON files specified in config...")
        download_script = base_dir / 'scripts' / 'downloadGoldenJsons.py'
        if not download_script.exists():
            print(f"Error: {download_script} not found!")
            return 1
        golden_json_urls = config.get('golden_json_urls', {})
        if not golden_json_urls:
            print("Error: config.yaml has no golden_json_urls section.")
            return 1
        for era, url in golden_json_urls.items():
            if not matches_filter(args.filter, era):
                continue
            print(f"  Era: {era}")
            output_filename = f"{era}_goldenJSON.json"
            local_path = inputs_folder / output_filename
            if local_path.exists() and not args.force:
                print(f"    Already exists and --force not specified, skipping download: {local_path}")
            else:
                cmd = [sys.executable, str(download_script), '-u', url,
                       '-o', output_filename, '-outDir', str(inputs_folder)]
                print(f"    Running command: {' '.join(cmd)}")
                result = subprocess.run(cmd)
                if result.returncode != 0:
                    print(f"    Error downloading golden JSON for era {era}")
                    return 1
            output_path = output_dir / 'inputs' / output_filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, output_path)
            print(f"    {local_path} copied to {output_path}")
        print("Finished downloading golden JSON files.")

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
            preselection_dataset_json = output_dir / 'inputs' / f'preselection_{era}_datasets.json'
            golden_json_file = output_dir / 'inputs' / f'{era}_goldenJSON.json'

            if not preselection_dataset_json.exists():
                print(f"  Warning: Dataset JSON not found: {preselection_dataset_json}. Skipping era {era}.")
                continue
            if not golden_json_file.exists():
                print(f"  Warning: Golden JSON file not found: {golden_json_file}. Data tasks will run without golden JSON filtering for era {era}.")


            with open(preselection_dataset_json) as f:
                datasetJSON = json.load(f)

            # Build combined cut string for this era
            era_cuts = config['SelectionCuts'][era]
            try:
                utils.validate_selection_cuts_consistency(config, era)
            except ValueError as e:
                print(f"Error: {e}")
                return 1
            cut_string = " && ".join(v for v in era_cuts.values() if v and v.strip())

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
                    if not matches_filter(args.filter, era, DataMC, group):
                        continue
                    print(f"  Processing {era} / {DataMC} / {group}...")
                    # continue
                    for dataset in datasetJSON[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        print (f"  Processing {era} / {DataMC} / {group} / {dataset}...")
                        # continue
                        # print(storageBase, args.tag, era, DataMC, group, dataset)
                        outputDir = os.path.join(
                            storageBase, "selectionI", args.tag, config_hash, era, DataMC, group, dataset
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


                            skim_name = os.path.basename(filePath).replace(".root", "_Skim.root")
                            skim_path = os.path.join(outputDir, skim_name)
                            if not args.force and os.path.exists(skim_path):
                                era_skipped += 1
                                print(f"    Skim output already exists, skipping: {skim_path}")
                                continue
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
        bash_script_path = base_dir / 'scripts' / f"run_all_{args.tag}.sh"
        with open(bash_script_path, 'w') as f:
            f.write("#!/bin/bash\n\n")
            for era in config['NgenandXsec']:
                if not matches_filter(args.filter, era):
                    continue
                process_list_json = output_dir / era / f"{args.tag}_{era}_processListJSON.json"
                if not process_list_json.exists():
                    print(f"  Warning: Process list JSON not found for era {era}: {process_list_json}. Skipping runSelection command for this era.")
                    continue
                # Enumerate DataMC/group from the actual fetched preselection dataset JSON
                # (same source --generateProcessListJSON reads), not config['NgenandXsec'] --
                # NgenandXsec is a hand-maintained table that can drift from what preselection
                # actually produced, silently dropping coverage here with no warning.
                dataset_json_path = output_dir / 'inputs' / f'preselection_{era}_datasets.json'
                if not dataset_json_path.exists():
                    print(f"  Warning: preselection dataset JSON not found for era {era}: {dataset_json_path}. Skipping runSelection command for this era.")
                    continue
                with open(dataset_json_path) as jf:
                    era_dataset_json = json.load(jf)
                for DataMC in era_dataset_json:
                    if not matches_filter(args.filter, era, DataMC):
                        continue
                    for group in era_dataset_json[DataMC]:
                        if not matches_filter(args.filter, era, DataMC, group):
                            continue
                        # create directories for logs
                        log_dir = output_dir / era / DataMC / group
                        log_dir.mkdir(parents=True, exist_ok=True)
                        f.write(f"mkdir -p {log_dir}\n")
                        cmd = (
                            f"python3 {base_dir / 'scripts' / 'runSelection.py'} "
                            f"--processListJSON {process_list_json} "
                            f"--workers {args.workers} "
                            f"{'--force ' if args.force else ''}"
                            f"{'--sample ' if args.sample else ''}"
                            f"{'--filter ' + era + '/' + DataMC + '/' + group}"
                            f"{' 2>&1 | tee -a ' + str(output_dir / era / DataMC / group / f'{args.tag}_{era}_{DataMC}_{group}.log')}"
                        )
                        f.write(cmd + "\n")
        os.chmod(bash_script_path, 0o755)
        print(f"\nBash script with runSelection.py commands written to: {bash_script_path}")

    # Submit object-selection jobs to CRAB, as an alternative to --writeBashScript +
    # local execution. lxplus only (needs STORAGE to resolve to the EOS mount of LFN_Base).
    if args.submitSelectionJobs:
        print("\nSubmitting object-selection jobs to CRAB...")
        submit_selection_script = base_dir / 'scripts' / 'crab' / 'submit_selection_flexible.py'
        if not submit_selection_script.exists():
            print(f"Error: {submit_selection_script} not found!")
            return 1
        lfn_base = config.get('LFN_Base', '').rstrip('/')
        if not lfn_base:
            print("Error: LFN_Base not set in config.yaml; required for --submitSelectionJobs.")
            return 1
        submitted, failed, pre_skipped = 0, 0, 0
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"\nSubmitting object-selection jobs for era: {era}")
            dataset_json_path = output_dir / 'inputs' / f'preselection_{era}_datasets.json'
            if not dataset_json_path.exists():
                print(f"Error: Dataset JSON not found for era {era} at {dataset_json_path}. Run --generatePreselectionDatasetJSON first.")
                continue
            golden_json_path = output_dir / 'inputs' / f'{era}_goldenJSON.json'
            # Enumerate DataMC/group/dataset from the actual fetched preselection dataset
            # JSON, not config['NgenandXsec'] -- see the matching comment in --writeBashScript.
            with open(dataset_json_path) as jf:
                era_dataset_json = json.load(jf)
            for DataMC in era_dataset_json:
                print(f"  DataMC: {DataMC}")
                for group in era_dataset_json[DataMC]:
                    print(f"    Group: {group}")
                    for dataset in era_dataset_json[DataMC][group]:
                        print(f"      Dataset: {dataset}")
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        work_area = output_dir / era / DataMC / group / dataset / "crab_selection"
                        # Idempotency: CRAB refuses to submit into an existing requestName
                        # directory ("Working area already exists"). Skip datasets that were
                        # already submitted unless --force -- mirrors the fix made for
                        # 002-Samples' --submitPreSelectionJobs after hitting this for real.
                        if work_area.exists() and any(work_area.glob('crab_sel_*')) and not args.force:
                            print(f"      Already submitted (work area exists), skipping: {work_area}")
                            pre_skipped += 1
                            continue
                        lfn_output_path = f"{lfn_base}/selectionI/{args.tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}"
                        command = (
                            f"python3 {submit_selection_script} --submit --era {era} "
                            f"--dataset-json {dataset_json_path} --golden-json {golden_json_path} "
                            f"--output-lfn {lfn_output_path} --work-area {work_area} "
                            f"{'--sample ' if args.sample else ''}"
                            f"--include '{DataMC}/{group}/{dataset}'"
                        )
                        print(f"      Executing command: {command}")
                        result = subprocess.run(command, shell=True)
                        if result.returncode != 0:
                            print(f"Error submitting object-selection jobs for dataset: {dataset}")
                            failed += 1
                            continue
                        else:
                            print(f"      Successfully submitted object-selection jobs for dataset: {dataset} with CRAB and saved logs to: {work_area}")
                            submitted += 1
        print(f"\nsubmitSelectionJobs: {submitted} submitted, {failed} failed, {pre_skipped} pre-skipped "
              f"out of {submitted + failed + pre_skipped} total.")

    # Check CRAB job status for jobs submitted with --submitSelectionJobs
    if args.checkCrabStatus:
        print("\nChecking CRAB job status for object-selection jobs submitted with --submitSelectionJobs...")
        check_crab_status_script = base_dir / 'scripts' / 'crab' / 'checkStatus.py'
        if not check_crab_status_script.exists():
            print(f"Error: {check_crab_status_script} not found!")
            return 1
        # Phase 1: build the flat task list (one `crab status` check per dataset),
        # enumerating from the actual fetched preselection dataset JSON, not
        # config['NgenandXsec'] -- see the matching comment in --writeBashScript.
        tasks = []  # (label, command)
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            dataset_json_path = output_dir / 'inputs' / f'preselection_{era}_datasets.json'
            if not dataset_json_path.exists():
                print(f"  Warning: preselection dataset JSON not found for era {era}: {dataset_json_path}. Skipping.")
                continue
            with open(dataset_json_path) as jf:
                era_dataset_json = json.load(jf)
            for DataMC in era_dataset_json:
                for group in era_dataset_json[DataMC]:
                    for dataset in era_dataset_json[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        work_area = output_dir / era / DataMC / group / dataset / "crab_selection"
                        command = f"python3 {check_crab_status_script} -d {work_area}"
                        if args.resubmitFailedCrabJobs:
                            command += " --resubmit"
                        if args.removeSubmitFailedCrabJobs:
                            command += " --removeSubmitFailed"
                        tasks.append((f"{era}/{DataMC}/{group}/{dataset}", command))

        print(f"\n{len(tasks)} datasets to check. Checking with {args.workers} parallel workers...")

        # Phase 2: check in parallel -- each `crab status` call is dominated by
        # network round-trip time to cmsweb.cern.ch, same reasoning as
        # --submitSelectionJobs's parallelization potential. stdout/stderr are
        # captured (not inherited) so concurrent checks never interleave their
        # output; each dataset's full captured output is printed as one block
        # from this single main-thread loop only after its subprocess finishes,
        # keeping the terminal readable no matter how many run at once.
        succeeded, failed = 0, 0
        if tasks:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {
                    pool.submit(subprocess.run, command, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True): (label, command)
                    for label, command in tasks
                }
                for i, future in enumerate(as_completed(futures), start=1):
                    label, command = futures[future]
                    try:
                        result = future.result()
                        ok = (result.returncode == 0)
                        output = result.stdout
                    except Exception as e:
                        ok = False
                        output = str(e)
                    header = f"[{i}/{len(tasks)}] {label}"
                    print(f"\n{'=' * len(header)}\n{header}\n{'=' * len(header)}\n{output}")
                    if ok:
                        succeeded += 1
                    else:
                        failed += 1
                        print(f"Error checking CRAB status for dataset: {label}")

        print(f"\ncheckCrabStatus: {succeeded} succeeded, {failed} failed out of {len(tasks)} total.")

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
            outputFileName = f"selectionI_{args.tag}_{era}_datasets.json"
            baseDirectory = os.path.join(storageBase, "selectionI", args.tag, config_hash, era)
            cmd = [
                sys.executable, str(generate_dataset_json_script),
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

    if args.verifyOutput:
        print("\nVerifying object-selection skim output ROOT files (scripts/verifyOutput.py)...")
        verify_script = base_dir / 'scripts' / 'verifyOutput.py'
        if not verify_script.exists():
            print(f"Error: {verify_script} not found!")
            return 1
        verify_failed = False
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            dataset_json_path = output_dir / era / f"selectionI_{args.tag}_{era}_datasets.json"
            if not dataset_json_path.exists():
                print(f"Error: selectionI dataset JSON not found for era {era} at {dataset_json_path}. "
                      f"Run --generateDatasetJSON first.")
                verify_failed = True
                continue
            report_path = output_dir / era / f"verifyOutput_{era}_report.json"
            cmd = [
                sys.executable, str(verify_script),
                '--datasetJSON', str(dataset_json_path),
                '--config', str(config_path),
                '--era', era,
                '--outputReport', str(report_path),
            ]
            print(f"\nRunning command: {' '.join(cmd)}")
            result = subprocess.run(cmd)
            if result.returncode != 0:
                print(f"verifyOutput.py reported problems for era {era} (see {report_path}).")
                verify_failed = True
            else:
                print(f"verifyOutput.py: all files OK for era {era}. Report: {report_path}")
        if verify_failed:
            return 1

    if args.plotABCDVariables:
        print("\nPlotting ABCD-plane variable distributions (scripts/plotABCDVariables.py)...")
        plot_script = base_dir / 'scripts' / 'plotABCDVariables.py'
        if not plot_script.exists():
            print(f"Error: {plot_script} not found!")
            return 1
        plot_failed = False
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            dataset_json_path = output_dir / era / f"selectionI_{args.tag}_{era}_datasets.json"
            if not dataset_json_path.exists():
                print(f"Error: selectionI dataset JSON not found for era {era} at {dataset_json_path}. "
                      f"Run --generateDatasetJSON first.")
                plot_failed = True
                continue
            plots_dir = output_dir / era / 'abcdPlots'
            cmd = [
                sys.executable, str(plot_script),
                '--datasetJSON', str(dataset_json_path),
                '--config', str(config_path),
                '--era', era,
                '--outputDir', str(plots_dir),
                '--dataMC', args.abcdDataMC,
                '--groups', *args.abcdGroups,
            ]
            print(f"\nRunning command: {' '.join(cmd)}")
            result = subprocess.run(cmd)
            if result.returncode != 0:
                print(f"plotABCDVariables.py reported problems for era {era}.")
                plot_failed = True
            else:
                print(f"plotABCDVariables.py: plots written for era {era} to {plots_dir}")
        if plot_failed:
            return 1

    if args.abcdClosureTest:
        print("\nRunning ABCD closure test (scripts/abcdClosureTest.py)...")
        closure_script = base_dir / 'scripts' / 'abcdClosureTest.py'
        if not closure_script.exists():
            print(f"Error: {closure_script} not found!")
            return 1
        closure_failed = False
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            dataset_json_path = output_dir / era / f"selectionI_{args.tag}_{era}_datasets.json"
            if not dataset_json_path.exists():
                print(f"Error: selectionI dataset JSON not found for era {era} at {dataset_json_path}. "
                      f"Run --generateDatasetJSON first.")
                closure_failed = True
                continue
            report_path = output_dir / era / f"abcdClosureTest_{era}_report.json"
            cmd = [
                sys.executable, str(closure_script),
                '--datasetJSON', str(dataset_json_path),
                '--config', str(config_path),
                '--era', era,
                '--outputReport', str(report_path),
                '--dataMC', args.abcdDataMC,
                '--groups', *args.closureGroups,
            ]
            print(f"\nRunning command: {' '.join(cmd)}")
            result = subprocess.run(cmd)
            if result.returncode != 0:
                print(f"abcdClosureTest.py reported problems for era {era}.")
                closure_failed = True
            else:
                print(f"abcdClosureTest.py: report written for era {era} to {report_path}")
        if closure_failed:
            return 1

    # exit(0)


if __name__ == '__main__':
    sys.exit(main())
