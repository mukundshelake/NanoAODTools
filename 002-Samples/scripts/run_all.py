#!/usr/bin/env python3
"""
Master script to generate all outputs for 002-Samples chapter.

This chapter spans two environments:
  - lxplus: DAS querying, golden JSON download, luminosity info, and CRAB
    submission/monitoring for the preselection stage. Output ROOT files land
    on EOS under LFN_Base, nested by CRAB itself under
    {primaryDataset}/{outputDatasetTag}/{timestamp}/{counter}/ -- none of
    which is meaningful once a winning submission wave is picked, and left
    in place it pushes output LFNs toward CRAB's 255-char limit.
  - Wherever the analysis actually runs (e.g. cms2): three steps turn that
    raw CRAB output into what 003-ObjectSelectionI expects:
      1. --generateCrabDatasetJSON scans the raw CRAB output tree
         ({STORAGE}/{config_hash}/{era}), dedupes multiple submission waves
         (lxplus only), and writes crabOutput_{era}_datasets.json.
      2. --consolidateCrabOutput moves every file that JSON lists into the
         clean flat layout {STORAGE}/preselection/{tag}/{config_hash}/{era}/
         {DataMC}/{group}/{dataset}/{filename}, then deletes the old raw
         per-dataset subtree once every file for it is confirmed moved.
      3. --generatePreselectionDatasetJSON scans that clean layout and
         writes preselection_{era}_datasets.json, the actual input
         003-ObjectSelectionI expects.

Usage:
    python scripts/run_all.py --tag TAG_NAME [--force] [--filter ...] [step flags]

Options:
    --force: Regenerate outputs even if config hash already exists
    --tag: Create a named tag for this run (e.g., "earlyApril")
"""

import argparse
import sys
from pathlib import Path
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
import utils
import getFileInfo
import generateRunLumiFiles


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
    parser.add_argument('-t', '--tag', type=str, default='Dump',
                       help='Create named tag for this run (e.g., baseline, paper_v1)')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate outputs even if output files already exist for this config hash')
    parser.add_argument('--printHash', action='store_true',
                       help='Print the config hash and exit (useful for debugging)')
    parser.add_argument('--workers', type=int, default=15,
                       help='Number of parallel operations for --getFileInfo (DAS queries) and '
                            '--submitPreSelectionJobs (CRAB submissions). Default: 15.')

    # --- lxplus: DAS querying / golden JSON / lumi info -----------------------------
    parser.add_argument('--getFileList', action='store_true',
                       help='[1][lxplus] Run getFileList.py to fetch file lists from DAS')
    parser.add_argument('--getDatasetInfo', action='store_true',
                       help='[1][lxplus] Run getDatasetInfo.py to fetch dataset info from DAS')
    parser.add_argument('--downloadGoldenJSONs', action='store_true',
                       help='[1] Download golden JSON files specified in config')
    parser.add_argument('--getLumiInformation', action='store_true',
                       help='[1][lxplus] Run getLumiInformation.py to fetch luminosity information using brilcalc')
    parser.add_argument('--aggregrateDatasetInfo', action='store_true',
                       help='[2] Aggregate dataset info from individual dataset info JSON files generated from '
                            '--getDatasetInfo into a single JSON file for each era: era_aggregated_dataset_info.json')
    parser.add_argument('--getFileInfo', action='store_true',
                       help='[2][lxplus] Run getFileInfo.py to fetch per-file run-lumi information from DAS')
    parser.add_argument('--generateRunLumiFiles', action='store_true',
                       help='[2] Run generateRunLumiFiles.py to generate brilcalc-friendly run-lumi JSON files '
                            'from the output of --getFileInfo')
    parser.add_argument('--generateDASDatasetJSON', action='store_true',
                       help='[2] Build DAS_{era}_dataset.json (remote LFN -> "Events" map) from the file lists '
                            'obtained via --getFileList. This is the DAS-side dataset JSON consumed by '
                            '--submitPreSelectionJobs -- not to be confused with --generatePreselectionDatasetJSON.')

    # --- lxplus: CRAB submission / monitoring ----------------------------------------
    parser.add_argument('--submitPreSelectionJobs', action='store_true',
                       help='[3][lxplus][CRAB] Submit pre-selection jobs to CRAB based on the DAS dataset JSON '
                            'generated by --generateDASDatasetJSON. Uses scripts/crab/submit_preselection_flexible.py.')
    parser.add_argument('--checkCrabStatus', action='store_true',
                       help='[4][lxplus][CRAB] Check CRAB job status for jobs submitted with --submitPreSelectionJobs. '
                            'Uses scripts/crab/checkStatus.py.')
    parser.add_argument('--resubmitFailedCrabJobs', action='store_true',
                       help='[4][lxplus][CRAB] With --checkCrabStatus: resubmit failed CRAB jobs.')
    parser.add_argument('--removeSubmitFailedCrabJobs', action='store_true',
                       help='[4][lxplus][CRAB] With --checkCrabStatus: remove CRAB jobs that never submitted successfully.')

    # --- local disk: raw CRAB output -> clean layout -> final dataset JSON -----------
    parser.add_argument('--generateCrabDatasetJSON', action='store_true',
                       help='[5] Scan the raw CRAB output tree {STORAGE}/{config_hash}/{era} on local disk (after '
                            'copying it down from EOS) for output ROOT files, dedupe multiple submission waves '
                            '(lxplus only -- see generateDatasetJSON.py), and write crabOutput_{era}_datasets.json. '
                            'Not the input 003-ObjectSelectionI expects -- run --consolidateCrabOutput and '
                            '--generatePreselectionDatasetJSON after this.')
    parser.add_argument('--consolidateCrabOutput', action='store_true',
                       help='[6] Move every file listed in crabOutput_{era}_datasets.json (from '
                            '--generateCrabDatasetJSON) out of CRAB\'s auto-nested layout into the clean flat '
                            'layout {STORAGE}/preselection/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/'
                            '{filename}, then delete the old raw per-dataset subtree once every one of its files '
                            'is confirmed at the new location. Safe to interrupt and re-run: a file already at '
                            'its destination is treated as already done rather than re-moved, and a dataset\'s '
                            'old subtree is only deleted once all of its files are verified moved.')
    parser.add_argument('--generatePreselectionDatasetJSON', action='store_true',
                       help='[7] Scan the clean {STORAGE}/preselection/{tag}/{config_hash}/{era} layout (after '
                            '--consolidateCrabOutput) and generate preselection_{era}_datasets.json, the input '
                            'format expected by 003-ObjectSelectionI.')

    parser.add_argument('--getStatus', action='store_true',
                       help='[8] Get overall status of the preselection processing by comparing expected number of '
                            'files from DAS with the number of files obtained from getFileInfo.py, the number of '
                            'files for which dataset JSON file is generated, the number of CRAB jobs submitted for '
                            'pre-selection, and the number of pre-selection output root files found on EOS.')
    parser.add_argument('--verifyOutput', action='store_true',
                       help='[9] Run scripts/verifyOutput.py on preselection_{era}_datasets.json (from '
                            '--generatePreselectionDatasetJSON): checks each output ROOT file opens cleanly, has '
                            'an Events tree, and its branch list matches config.yaml branch_selection (missing '
                            'expected / unexpected extra branches). Writes a JSON report per era.')

    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--sample', action='store_true',
                       help='For --submitPreSelectionJobs: restrict each submitted dataset to 1 file '
                            '(via Data.totalUnits) for testing purposes, same convention as --sample '
                            'elsewhere in this chapter.')

    args = parser.parse_args()

    # parsing arguments
    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --force: {args.force}")
    print(f"  --getFileList: {args.getFileList}")
    print(f"  --getDatasetInfo: {args.getDatasetInfo}")
    print(f"  --downloadGoldenJSONs: {args.downloadGoldenJSONs}")
    print(f"  --getLumiInformation: {args.getLumiInformation}")
    print(f"  --aggregrateDatasetInfo: {args.aggregrateDatasetInfo}")
    print(f"  --getFileInfo: {args.getFileInfo}")
    print(f"  --generateRunLumiFiles: {args.generateRunLumiFiles}")
    print(f"  --generateDASDatasetJSON: {args.generateDASDatasetJSON}")
    print(f"  --submitPreSelectionJobs: {args.submitPreSelectionJobs}")
    print(f"  --checkCrabStatus: {args.checkCrabStatus}")
    print(f"  --resubmitFailedCrabJobs: {args.resubmitFailedCrabJobs}")
    print(f"  --removeSubmitFailedCrabJobs: {args.removeSubmitFailedCrabJobs}")
    print(f"  --generateCrabDatasetJSON: {args.generateCrabDatasetJSON}")
    print(f"  --consolidateCrabOutput: {args.consolidateCrabOutput}")
    print(f"  --generatePreselectionDatasetJSON: {args.generatePreselectionDatasetJSON}")
    print(f"  --getStatus: {args.getStatus}")
    print(f"  --verifyOutput: {args.verifyOutput}")
    print(f"  --filter: {args.filter}")
    print(f"  --printHash: {args.printHash}")
    print(f"  --workers: {args.workers}")
    print(f"  --sample: {args.sample}")

    # Paths
    base_dir = Path(__file__).parent.parent
    config_path = base_dir / 'config.yaml'
    outputs_base = base_dir / 'outputs' / f'{args.tag}'
    history_file = base_dir / 'run_history.txt'

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

    # Create or update the 'latest' symlink to point to the current output directory
    latest_link = outputs_base / 'latest'
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(output_dir.name)
    print(f"Updated symlink: {latest_link} -> {output_dir.name}")

    if args.printHash:
        print(f"Config hash: {config_hash}")
        return 0

    # Run getFileList.py if requested
    if args.getFileList:
        print("\nRunning getFileList.py to fetch file lists from DAS...")
        get_file_list_script = base_dir / 'scripts' / 'getFileList.py'
        if not get_file_list_script.exists():
            print(f"Error: {get_file_list_script} not found!")
            return 1

        for era in config['DASQueries']:
            print(f"\nFetching file list for era: {era}")
            for DataMC in config['DASQueries'][era]:
                print(f"  DataMC: {DataMC}")
                for group in config['DASQueries'][era][DataMC]:
                    print(f"    Group: {group}")
                    for dataset_name, das_query in config['DASQueries'][era][DataMC][group].items():
                        print(f"    Dataset: {dataset_name}")
                        if not matches_filter(args.filter, era, DataMC, group, dataset_name):
                            continue
                        output_filename = f"{era}_{DataMC}_{group}_{dataset_name}_file_list.json"
                        queryResults_dir = output_dir / era / DataMC / group / dataset_name
                        queryResults_dir.mkdir(parents=True, exist_ok=True)
                        output_file_path = queryResults_dir / output_filename
                        if output_file_path.exists() and not args.force:
                            print(f"    Output file already exists: {output_file_path} and --force not specified. Skipping DAS query.")
                            continue
                        query = "file dataset={dataset}".format(dataset=das_query)
                        cmd = f"python3 {get_file_list_script} -q \"{query}\" -o {output_filename} -outDir {queryResults_dir}"
                        print(f"    Executing command: {cmd}")
                        result = subprocess.run(cmd, shell=True)
                        if result.returncode != 0:
                            print(f"Error running getFileList.py for dataset: {dataset_name}")
                            return 1
                        else:
                            print(f"    Successfully fetched file list and saved to: {queryResults_dir / output_filename}")

    # Run getDatasetInfo.py if requested
    if args.getDatasetInfo:
        print("\nRunning getDatasetInfo.py to fetch dataset info from DAS...")
        get_dataset_info_script = base_dir / 'scripts' / 'getDatasetInfo.py'
        if not get_dataset_info_script.exists():
            print(f"Error: {get_dataset_info_script} not found!")
            return 1

        for era in config['DASQueries']:
            print(f"\nFetching dataset info for era: {era}")
            for DataMC in config['DASQueries'][era]:
                print(f"  DataMC: {DataMC}")
                for group in config['DASQueries'][era][DataMC]:
                    print(f"    Group: {group}")
                    for dataset_name, das_query in config['DASQueries'][era][DataMC][group].items():
                        print(f"    Dataset: {dataset_name}")
                        if not matches_filter(args.filter, era, DataMC, group, dataset_name):
                            continue
                        output_filename = f"{era}_{DataMC}_{group}_{dataset_name}_dataset_info.json"
                        queryResults_dir = output_dir / era / DataMC / group / dataset_name
                        queryResults_dir.mkdir(parents=True, exist_ok=True)
                        output_file_path = queryResults_dir / output_filename
                        if output_file_path.exists() and not args.force:
                            print(f"    Output file already exists: {output_file_path} and --force not specified. Skipping DAS query.")
                            continue
                        query = "dataset={dataset} | grep dataset.nevents | grep dataset.num_file".format(dataset=das_query)
                        cmd = f"python3 {get_dataset_info_script} -q \"{query}\" -o {output_filename} -outDir {queryResults_dir}"
                        print(f"    Executing command: {cmd}")
                        result = subprocess.run(cmd, shell=True)
                        if result.returncode != 0:
                            print(f"Error running getDatasetInfo.py for dataset: {dataset_name}")
                            return 1
                        else:
                            print(f"    Successfully fetched dataset info and saved to: {queryResults_dir / output_filename}")

    # Download golden JSONs if requested
    if args.downloadGoldenJSONs:
        print("\nDownloading golden JSON files specified in config...")
        download_golden_json_script = base_dir / 'scripts' / 'downloadGoldenJsons.py'
        if not download_golden_json_script.exists():
            print(f"Error: {download_golden_json_script} not found!")
            return 1
        for era, url in config['golden_json_urls'].items():
            if not matches_filter(args.filter, era):
                continue
            output_filename = f"{era}_goldenJSON.json"
            output_directory = output_dir / era
            output_directory.mkdir(parents=True, exist_ok=True)
            output_file_path = output_directory / output_filename
            if output_file_path.exists() and not args.force:
                print(f"  Golden JSON for era {era} already exists at: {output_file_path} and --force not specified. Skipping download.")
                continue
            cmd = f"python3 {download_golden_json_script} -u {url} -o {output_filename} -outDir {output_directory}"
            print(f"Executing command: {cmd}")
            result = subprocess.run(cmd, shell=True)
            if result.returncode != 0:
                print(f"Error downloading golden JSON for era: {era}")
                return 1
            else:
                print(f"Successfully downloaded golden JSON for era: {era} and saved to: {output_directory / output_filename}")

    # Aggregrate dataset info from individual dataset info JSON files generated from --getDatasetInfo
    if args.aggregrateDatasetInfo:
        print("\nAggregating dataset info from individual dataset info JSON files generated from --getDatasetInfo into a single JSON file for each era...")
        for era in config['DASQueries']:
            if not matches_filter(args.filter, era):
                continue
            aggregated_info = {}
            eraEvents = 0
            eraFiles = 0
            for DataMC in config['DASQueries'][era]:
                print(f"  DataMC: {DataMC}")
                aggregated_info[DataMC] = {}
                DataMCTotalEvents = 0
                DataMCTotalFiles = 0
                for group in config['DASQueries'][era][DataMC]:
                    print(f"    Group: {group}")
                    aggregated_info[DataMC][group] = {}
                    groupEvents = 0
                    groupFiles = 0
                    for dataset_name in config['DASQueries'][era][DataMC][group]:
                        print(f"      Dataset: {dataset_name}")
                        if not matches_filter(args.filter, era, DataMC, group, dataset_name):
                            continue
                        dataset_info_json = output_dir / era / DataMC / group / dataset_name / f"{era}_{DataMC}_{group}_{dataset_name}_dataset_info.json"
                        if not dataset_info_json.exists():
                            print(f"Error: Dataset info JSON file not found for dataset {dataset_name} at expected location: {dataset_info_json}. Please ensure the dataset info is fetched before running this aggregation step.")
                            continue
                        with open(dataset_info_json) as f:
                            dataset_info = json.load(f)
                        aggregated_info[DataMC][group][dataset_name] = dataset_info
                        num_events = dataset_info.get('num_events', 0)
                        num_files = dataset_info.get('num_files', 0)
                        groupEvents += num_events
                        groupFiles += num_files
                    aggregated_info[DataMC][group]['group_total'] = {
                        'num_events': groupEvents,
                        'num_files': groupFiles
                    }
                    DataMCTotalEvents += groupEvents
                    DataMCTotalFiles += groupFiles
                aggregated_info[DataMC]['DataMC_total'] = {
                    'num_events': DataMCTotalEvents,
                    'num_files': DataMCTotalFiles
                }
                eraEvents += DataMCTotalEvents
                eraFiles += DataMCTotalFiles
            aggregated_info['era_total'] = {
                'num_events': eraEvents,
                'num_files': eraFiles
            }
            output_directory = output_dir / era
            output_directory.mkdir(parents=True, exist_ok=True)
            output_filename = f"{era}_aggregated_dataset_info.json"
            output_file_path = output_directory / output_filename
            if output_file_path.exists() and not args.force:
                print(f"  Aggregated dataset info JSON for era {era} already exists at: {output_file_path} and --force not specified. Skipping aggregation.")
                continue
            with open(output_file_path, 'w') as f:
                json.dump(aggregated_info, f, indent=4)
            print(f"  Successfully aggregated dataset info for era: {era} and saved to: {output_file_path}")

    # Get luminosity information using brilcalc if requested
    if args.getLumiInformation:
        print("\nRunning getLumiInformation.py to fetch luminosity information using brilcalc...")
        get_lumi_info_script = base_dir / 'scripts' / 'getLumiInformation.py'
        if not get_lumi_info_script.exists():
            print(f"Error: {get_lumi_info_script} not found!")
            return 1
        for era in config['DASQueries']:
            if not matches_filter(args.filter, era):
                continue
            print(f"\nFetching luminosity information for era: {era}")
            golden_json_path = output_dir / era / f"{era}_goldenJSON.json"
            if not golden_json_path.exists():
                print(f"Error: Golden JSON file not found for era {era} at expected location: {golden_json_path}. Please ensure the golden JSON is downloaded before fetching luminosity information.")
                continue
            output_filename = f"{era}_lumi_info.csv"
            lumi_info_dir = output_dir / era
            lumi_info_dir.mkdir(parents=True, exist_ok=True)
            output_file_path = lumi_info_dir / output_filename
            if output_file_path.exists() and not args.force:
                print(f"  Luminosity information for era {era} already exists at: {output_file_path} and --force not specified. Skipping luminosity information retrieval.")
                continue
            cmd = f"python3 {get_lumi_info_script} --json {golden_json_path} -o {output_filename} --outDir {lumi_info_dir}"
            print(f"Executing command: {cmd}")
            result = subprocess.run(cmd, shell=True)
            if result.returncode != 0:
                print(f"Error fetching luminosity information for era: {era}")
                return 1
            else:
                print(f"Successfully fetched luminosity information for era: {era} and saved to: {lumi_info_dir / output_filename}")

    # Get file run-lumi information from DAS if requested
    if args.getFileInfo:
        print("\nRunning getFileInfo.py to fetch file run-lumi information from DAS...")

        # Phase 1: build the flat task list (one dasgoclient query per file),
        # honoring --force/existing-file skip logic, same as every Pool-based
        # worker script elsewhere in this pipeline.
        tasks = []
        pre_skipped = 0
        for era in config['DASQueries']:
            print(f"\nFetching file run-lumi information for era: {era}")
            for DataMC in config['DASQueries'][era]:
                print(f"  DataMC: {DataMC}")
                for group in config['DASQueries'][era][DataMC]:
                    print(f"    Group: {group}")
                    for dataset_name, das_query in config['DASQueries'][era][DataMC][group].items():
                        print(f"      Dataset: {dataset_name}")
                        if not matches_filter(args.filter, era, DataMC, group, dataset_name):
                            continue
                        dataset_files_json = output_dir / era / DataMC / group / dataset_name / f"{era}_{DataMC}_{group}_{dataset_name}_file_list.json"
                        if not dataset_files_json.exists():
                            print(f"Error: File list JSON not found for dataset {dataset_name} at expected location: {dataset_files_json}. Please ensure the file list is fetched before fetching file run-lumi information.")
                            continue
                        fileList = json.load(open(dataset_files_json))
                        print(f"    Found {len(fileList)} files in file list JSON: {dataset_files_json}")
                        for file in fileList:
                            file_name = file["file"][0]["name"]
                            file_id = file["file"][0]["file_id"]
                            output_directory = output_dir / era / DataMC / group / dataset_name / f"file_{file_id}"
                            output_filename = f"{era}_{DataMC}_{group}_{dataset_name}_file{file_id}_run_lumi_info.json"
                            output_directory.mkdir(parents=True, exist_ok=True)
                            output_file_path = output_directory / output_filename
                            if output_file_path.exists() and not args.force:
                                pre_skipped += 1
                                continue
                            tasks.append((file_name, str(output_directory / output_filename)))

        print(f"\n{len(tasks)} files to query, {pre_skipped} already done / filtered out. "
              f"Running with {args.workers} parallel workers...")

        # Phase 2: run the DAS queries in parallel. dasgoclient calls are I/O-bound
        # (waiting on the network), so a thread pool is enough -- no need for
        # separate processes. Calling getFileInfo.get_file_info() directly here
        # (instead of spawning `python3 getFileInfo.py` per file, as before) also
        # drops ~13000 redundant Python-interpreter-startup subprocess launches.
        succeeded, failed = 0, 0
        if tasks:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {
                    pool.submit(getFileInfo.get_file_info, f"lumi file={file_name}", out_path): (file_name, out_path)
                    for file_name, out_path in tasks
                }
                for i, future in enumerate(as_completed(futures), start=1):
                    file_name, out_path = futures[future]
                    try:
                        ok = future.result()
                    except Exception as e:
                        print(f"      [{i}/{len(tasks)}] Exception fetching run-lumi information for {file_name}: {e}")
                        ok = False
                    if ok:
                        succeeded += 1
                    else:
                        failed += 1
                        print(f"      [{i}/{len(tasks)}] FAILED: {file_name}")
                    if i % 200 == 0 or i == len(tasks):
                        print(f"      Progress: {i}/{len(tasks)} ({succeeded} succeeded, {failed} failed)")

        print(f"\ngetFileInfo: {succeeded} succeeded, {failed} failed, {pre_skipped} pre-skipped "
              f"out of {len(tasks) + pre_skipped} total.")
        if tasks and succeeded == 0:
            # Every single query failed -- almost certainly something systemic
            # (expired proxy, DAS outage), not per-file flakiness. Don't let
            # the run silently look like it did something.
            print("Error: all getFileInfo queries failed.")
            return 1

    # Generate brilcalc friendly run-lumi JSON files for each data file if requested
    if args.generateRunLumiFiles:
        print("\nRunning generateRunLumiFiles.py to generate brilcalc friendly run-lumi JSON files from the output of getFileInfo.py...")
        # NOTE: this loop used to have a bug where the per-file existence-check/
        # generation logic sat outside the `for file in fileList:` loop (wrong
        # indentation), so only the *last* file of each dataset was ever
        # processed. Fixed here -- also switched from spawning a `python3
        # generateRunLumiFiles.py` subprocess per file (this is a pure local
        # JSON transform, no network call, so the interpreter-startup overhead
        # for ~13k files was pure waste) to calling generate_run_lumi_files()
        # directly in-process.
        succeeded, failed, pre_skipped = 0, 0, 0
        for era in config['DASQueries']:
            print(f"\nGenerating run-lumi JSON files for era: {era}")
            for DataMC in config['DASQueries'][era]:
                print(f"  DataMC: {DataMC}")
                for group in config['DASQueries'][era][DataMC]:
                    print(f"    Group: {group}")
                    for dataset_name, das_query in config['DASQueries'][era][DataMC][group].items():
                        print(f"      Dataset: {dataset_name}")
                        if not matches_filter(args.filter, era, DataMC, group, dataset_name):
                            continue
                        dataset_files_json = output_dir / era / DataMC / group / dataset_name / f"{era}_{DataMC}_{group}_{dataset_name}_file_list.json"
                        if not dataset_files_json.exists():
                            print(f"Error: File list JSON not found for dataset {dataset_name} at expected location: {dataset_files_json}. Please ensure the file list is fetched before generating run-lumi JSON files.")
                            continue
                        fileList = json.load(open(dataset_files_json))
                        print(f"    Found {len(fileList)} files in file list JSON: {dataset_files_json}")
                        for file in fileList:
                            file_id = file["file"][0]["file_id"]
                            file_info_json = output_dir / era / DataMC / group / dataset_name / f"file_{file_id}" / f"{era}_{DataMC}_{group}_{dataset_name}_file{file_id}_run_lumi_info.json"
                            if not file_info_json.exists():
                                print(f"Error: File run-lumi information JSON not found for file ID {file_id} at expected location: {file_info_json}. Please ensure the file run-lumi information is fetched before generating run-lumi JSON files for this file.")
                                failed += 1
                                continue
                            output_directory = output_dir / era / DataMC / group / dataset_name / f"file_{file_id}"
                            output_filename = f"{era}_{DataMC}_{group}_{dataset_name}_file{file_id}_run_lumi.json"
                            output_file_path = output_directory / output_filename
                            if output_file_path.exists() and not args.force:
                                pre_skipped += 1
                                continue
                            if generateRunLumiFiles.generate_run_lumi_files(str(file_info_json), str(output_file_path)):
                                succeeded += 1
                            else:
                                print(f"Error generating run-lumi JSON file for file ID: {file_id}")
                                failed += 1

        print(f"\ngenerateRunLumiFiles: {succeeded} succeeded, {failed} failed, {pre_skipped} pre-skipped.")
        if failed and succeeded == 0:
            print("Error: all generateRunLumiFiles conversions failed.")
            return 1

    # Build DAS_{era}_dataset.json (remote LFN dataset map, feeds CRAB submission)
    if args.generateDASDatasetJSON:
        print("\nGenerating DAS dataset JSON files from the file lists obtained from --getFileList...")
        for era in config['DASQueries']:
            if not matches_filter(args.filter, era):
                continue
            print(f"\nGenerating DAS dataset JSON for era: {era}")
            datasetJSON = {}
            for DataMC in config['DASQueries'][era]:
                print(f"  DataMC: {DataMC}")
                datasetJSON[DataMC] = {}
                for group in config['DASQueries'][era][DataMC]:
                    print(f"    Group: {group}")
                    datasetJSON[DataMC][group] = {}
                    for dataset_name, das_query in config['DASQueries'][era][DataMC][group].items():
                        if not matches_filter(args.filter, era, DataMC, group, dataset_name):
                            continue
                        datasetJSON[DataMC][group][dataset_name] = {}
                        print(f"      Dataset: {dataset_name}")
                        dataset_files_json = output_dir / era / DataMC / group / dataset_name / f"{era}_{DataMC}_{group}_{dataset_name}_file_list.json"
                        if not dataset_files_json.exists():
                            print(f"Error: File list JSON not found for dataset {dataset_name} at expected location: {dataset_files_json}. Please ensure the file list is fetched before generating dataset JSON file.")
                            continue
                        fileList = json.load(open(dataset_files_json))
                        for entry in fileList:
                            file_name = entry["file"][0]["name"]
                            datasetJSON[DataMC][group][dataset_name][file_name] = "Events"
            output_directory = output_dir / era
            output_directory.mkdir(parents=True, exist_ok=True)
            output_filename = f"DAS_{era}_dataset.json"
            output_file_path = output_directory / output_filename
            if output_file_path.exists() and not args.force:
                print(f"      DAS dataset JSON file for era {era} already exists at: {output_file_path} and --force not specified. Skipping generation.")
                continue
            with open(output_file_path, 'w') as f:
                json.dump(datasetJSON, f, indent=4)
            print(f"      Successfully generated DAS dataset JSON file for era {era} and saved to: {output_file_path}")

    # Submit pre-selection jobs to CRAB
    if args.submitPreSelectionJobs:
        print("\nSubmitting pre-selection jobs to CRAB based on the DAS dataset JSON generated from --generateDASDatasetJSON...")
        submit_preselection_script = base_dir / 'scripts' / 'crab' / 'submit_preselection_flexible.py'
        if not submit_preselection_script.exists():
            print(f"Error: {submit_preselection_script} not found!")
            return 1

        # Phase 1: build the flat task list (one CRAB submission per dataset),
        # honoring the same filter/skip logic as before.
        tasks = []  # (label, command, work_area)
        pre_skipped = 0
        for era in config['DASQueries']:
            print(f"\nPreparing pre-selection submissions for era: {era}")
            dataset_json_path = output_dir / era / f"DAS_{era}_dataset.json"
            if not dataset_json_path.exists():
                print(f"Error: DAS dataset JSON file not found for era {era} at expected location: {dataset_json_path}. Please ensure --generateDASDatasetJSON has been run before submitting pre-selection jobs.")
                continue
            golden_json_path = output_dir / era / f"{era}_goldenJSON.json"
            if not golden_json_path.exists():
                print(f"Error: Golden JSON file not found for era {era} at expected location: {golden_json_path}. Please ensure the golden JSON is downloaded before submitting pre-selection jobs.")
                continue
            for DataMC in config['DASQueries'][era]:
                for group in config['DASQueries'][era][DataMC]:
                    for dataset_name in config['DASQueries'][era][DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset_name):
                            continue
                        lfn_base = config['LFN_Base'].rstrip('/')
                        # NOTE: kept deliberately short (just the hash, no "preselection/{tag}/"
                        # prefix) -- CRABServer enforces a hard 255-char limit on each
                        # Data.userInputFiles entry (see 003-ObjectSelectionI's submit_selection_flexible.py),
                        # and CRAB's own nested output naming (dataset name/request name/timestamp/
                        # counter/filename) already eats most of that budget for MC datasets with
                        # long official CMS dataset names. Every extra prefix character here comes
                        # straight out of that budget downstream.
                        lfn_path = f"{lfn_base}/{config_hash}/{era}/{DataMC}/{group}/{dataset_name}"
                        work_area = output_dir / era / DataMC / group / dataset_name / "crab_preselection"
                        # Skip datasets already submitted (a crab_presel_* project dir already
                        # exists under work_area). Without this, re-running after an interrupted
                        # submission pass -- or just re-running --submitPreSelectionJobs at all --
                        # would resubmit every dataset again with a fresh CRAB task each time.
                        if work_area.exists() and any(work_area.glob('crab_presel_*')) and not args.force:
                            pre_skipped += 1
                            continue
                        command = (
                            f"python3 {submit_preselection_script} --submit --era {era} "
                            f"--das-json {dataset_json_path} --golden-json {golden_json_path} "
                            f"--output-lfn {lfn_path} --work-area {work_area} "
                            f"{'--sample ' if args.sample else ''}"
                            f"--include '{DataMC}/{group}/{dataset_name}'"
                        )
                        tasks.append((f"{era}/{DataMC}/{group}/{dataset_name}", command, work_area))

        print(f"\n{len(tasks)} datasets to submit, {pre_skipped} already submitted / filtered out. "
              f"Submitting with {args.workers} parallel workers...")

        # Phase 2: submit in parallel. Each task is a separate `python3
        # submit_preselection_flexible.py` OS process (as before, just no
        # longer run one-at-a-time) -- CRAB submission is dominated by network
        # round-trip time to cmsweb.cern.ch, so running several concurrently
        # cuts wall-clock time roughly by the worker count without any
        # concern about the CRAB client's thread-safety (there isn't any
        # shared state to worry about -- each submission is its own process).
        submitted, failed = 0, 0
        if tasks:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {
                    pool.submit(subprocess.run, command, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True): (label, command, work_area)
                    for label, command, work_area in tasks
                }
                for i, future in enumerate(as_completed(futures), start=1):
                    label, command, work_area = futures[future]
                    try:
                        result = future.result()
                        ok = (result.returncode == 0)
                        output = result.stdout
                    except Exception as e:
                        ok = False
                        output = str(e)
                    if ok:
                        submitted += 1
                        print(f"  [{i}/{len(tasks)}] Submitted: {label} (logs: {work_area})")
                    else:
                        failed += 1
                        print(f"  [{i}/{len(tasks)}] FAILED: {label}\n{output}")

        print(f"\nsubmitPreSelectionJobs: {submitted} submitted, {failed} failed, {pre_skipped} pre-skipped "
              f"out of {len(tasks) + pre_skipped} total.")
        if tasks and submitted == 0:
            print("Error: all pre-selection submissions failed.")
            return 1

    # Check CRAB job status
    if args.checkCrabStatus:
        print("\nChecking CRAB job status for pre-selection jobs submitted with --submitPreSelectionJobs...")
        check_crab_status_script = base_dir / 'scripts' / 'crab' / 'checkStatus.py'
        if not check_crab_status_script.exists():
            print(f"Error: {check_crab_status_script} not found!")
            return 1

        # Phase 1: build the flat task list (one `crab status` check per dataset).
        tasks = []  # (label, command)
        for era in config['DASQueries']:
            for DataMC in config['DASQueries'][era]:
                for group in config['DASQueries'][era][DataMC]:
                    for dataset_name in config['DASQueries'][era][DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset_name):
                            continue
                        work_area = output_dir / era / DataMC / group / dataset_name / "crab_preselection"
                        command = f"python3 {check_crab_status_script} -d {work_area}"
                        if args.resubmitFailedCrabJobs:
                            command += " --resubmit"
                        if args.removeSubmitFailedCrabJobs:
                            command += " --removeSubmitFailed"
                        tasks.append((f"{era}/{DataMC}/{group}/{dataset_name}", command))

        print(f"\n{len(tasks)} datasets to check. Checking with {args.workers} parallel workers...")

        # Phase 2: check in parallel -- each `crab status` call is dominated by
        # network round-trip time to cmsweb.cern.ch, same reasoning as
        # --submitPreSelectionJobs's parallelization. stdout/stderr are
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

    # Scan the raw CRAB output tree on local disk (once copied down from EOS) and
    # build crabOutput_{era}_datasets.json -- an intermediate JSON, still pointing
    # at CRAB's auto-nested paths. Not the input 003-ObjectSelectionI expects; see
    # --consolidateCrabOutput and --generatePreselectionDatasetJSON below.
    if args.generateCrabDatasetJSON:
        print("\nGenerating CRAB output dataset JSON files from local disk (scripts/generateDatasetJSON.py)...")
        storageBase = utils.resolve_storage_path(config)
        print(f"Using storage base: {storageBase}")
        for era in config['DASQueries']:
            if not matches_filter(args.filter, era):
                print(f"Skipping era {era} due to filter")
                continue
            generateJSON_script = base_dir / 'scripts' / 'generateDatasetJSON.py'
            if not generateJSON_script.exists():
                print(f"Error: {generateJSON_script} does not exist. Please make sure it is in the correct location.")
                return 1
            output_json_name = f"crabOutput_{era}_datasets.json"
            output_json_path = output_dir / era / output_json_name
            if output_json_path.exists() and not args.force:
                print(f"Output JSON file already exists for {era} and --force not set. Skipping: {output_json_path}")
                continue
            base_directory = str(Path(storageBase) / config_hash / era)
            cmd = [
                sys.executable, str(generateJSON_script),
                '--outputDirectory', str(output_dir / era),
                '--outputFileName', output_json_name,
                '--baseDirectory', base_directory
            ]
            print(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Error running generateDatasetJSON.py for {era}:\n{result.stderr}")
                return 1
            else:
                print(f"Successfully generated CRAB output dataset JSON for {era}: {output_json_path}")

    # Move every file crabOutput_{era}_datasets.json lists out of CRAB's auto-nested
    # layout into the clean flat layout this pipeline actually wants, deleting the
    # old raw per-dataset subtree once each dataset's files are confirmed moved.
    if args.consolidateCrabOutput:
        print("\nConsolidating CRAB output into the clean layout (scripts/consolidateCrabOutput.py)...")
        storageBase = utils.resolve_storage_path(config)
        print(f"Using storage base: {storageBase}")
        consolidate_script = base_dir / 'scripts' / 'consolidateCrabOutput.py'
        if not consolidate_script.exists():
            print(f"Error: {consolidate_script} does not exist. Please make sure it is in the correct location.")
            return 1
        consolidate_failed = False
        for era in config['DASQueries']:
            if not matches_filter(args.filter, era):
                print(f"Skipping era {era} due to filter")
                continue
            crab_json_path = output_dir / era / f"crabOutput_{era}_datasets.json"
            if not crab_json_path.exists():
                print(f"Error: {crab_json_path} not found for era {era}. Run --generateCrabDatasetJSON first.")
                consolidate_failed = True
                continue
            source_base = str(Path(storageBase) / config_hash / era)
            destination_base = str(Path(storageBase) / 'preselection' / args.tag / config_hash / era)
            cmd = [
                sys.executable, str(consolidate_script),
                '--datasetJSON', str(crab_json_path),
                '--sourceBase', source_base,
                '--destinationBase', destination_base,
            ]
            print(f"\nRunning command: {' '.join(cmd)}")
            result = subprocess.run(cmd)
            if result.returncode != 0:
                print(f"consolidateCrabOutput.py reported problems for era {era} (see output above).")
                consolidate_failed = True
            else:
                print(f"Successfully consolidated CRAB output for era {era} into: {destination_base}")
        if consolidate_failed:
            return 1

    # Scan the clean, consolidated layout and build preselection_{era}_datasets.json,
    # the actual input 003-ObjectSelectionI expects.
    if args.generatePreselectionDatasetJSON:
        print("\nGenerating preselection dataset JSON files from the consolidated layout (scripts/generateDatasetJSON.py)...")
        storageBase = utils.resolve_storage_path(config)
        print(f"Using storage base: {storageBase}")
        for era in config['DASQueries']:
            if not matches_filter(args.filter, era):
                print(f"Skipping era {era} due to filter")
                continue
            generateJSON_script = base_dir / 'scripts' / 'generateDatasetJSON.py'
            if not generateJSON_script.exists():
                print(f"Error: {generateJSON_script} does not exist. Please make sure it is in the correct location.")
                return 1
            output_json_name = f"preselection_{era}_datasets.json"
            output_json_path = output_dir / era / output_json_name
            if output_json_path.exists() and not args.force:
                print(f"Output JSON file already exists for {era} and --force not set. Skipping: {output_json_path}")
                continue
            base_directory = str(Path(storageBase) / 'preselection' / args.tag / config_hash / era)
            cmd = [
                sys.executable, str(generateJSON_script),
                '--outputDirectory', str(output_dir / era),
                '--outputFileName', output_json_name,
                '--baseDirectory', base_directory
            ]
            print(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Error running generateDatasetJSON.py for {era}:\n{result.stderr}")
                return 1
            else:
                print(f"Successfully generated preselection dataset JSON for {era}: {output_json_path}")

    # Overall status across DAS expectations / getFileInfo / dataset JSON / CRAB / EOS output
    if args.getStatus:
        print("\nGetting overall status of the preselection processing...")
        eos_base = f"{utils.eos_path_from_lfn_base(config['LFN_Base'])}/{config_hash}"
        for era in config['DASQueries']:
            if not matches_filter(args.filter, era):
                continue
            print(f"\nStatus for era: {era}")
            aggregated_info_json = output_dir / era / f"{era}_aggregated_dataset_info.json"
            if not aggregated_info_json.exists():
                print(f"Error: Aggregated dataset info JSON file not found for era {era} at expected location: {aggregated_info_json}. Please ensure the aggregated dataset info JSON is generated before checking status.")
                continue
            with open(aggregated_info_json) as f:
                aggregated_info = json.load(f)
            DAS_expected_files = aggregated_info['era_total']['num_files']
            print(f"  Total files according to DAS: {DAS_expected_files}")

            getFileInfo_files = list(output_dir.glob(f"{era}/**/*_file*_run_lumi_info.json"))
            num_getFileInfo_files = len(getFileInfo_files)
            print(f"  Number of files with run-lumi information obtained from getFileInfo.py: {num_getFileInfo_files}")

            dataset_json_file = output_dir / era / f"DAS_{era}_dataset.json"
            num_dataset_json_files = 0
            with open(dataset_json_file) as f:
                dataset_json = json.load(f)
            for DataMC in dataset_json:
                for group in dataset_json[DataMC]:
                    for dataset_name in dataset_json[DataMC][group]:
                        num_dataset_json_files += len(dataset_json[DataMC][group][dataset_name])
            print(f"  Number of files for which dataset JSON file is generated: {num_dataset_json_files}")

            crab_preselection_dirs = list(output_dir.glob(f"{era}/**/crab_preselection"))
            num_crab_preselection_jobs = len(crab_preselection_dirs)
            print(f"  Number of CRAB pre-selection jobs submitted: {num_crab_preselection_jobs}")

            crab_job_expected_output_dir = f"{eos_base}/{era}/"
            num_preselection_output_files = len(list(Path(crab_job_expected_output_dir).glob(f"**/*.root")))
            print(f"  Number of pre-selection output root files found at expected location {crab_job_expected_output_dir}: {num_preselection_output_files}")
            for DataMC in config['DASQueries'][era]:
                print(f"      Getting status for era/DataMC: {era}/{DataMC}")
                DataMC_expected_files = aggregated_info[DataMC]['DataMC_total']['num_files']
                print(f"        Total files according to DAS for {era}/{DataMC}: {DataMC_expected_files}")
                getFileInfo_files = list(output_dir.glob(f"{era}/{DataMC}/**/*_file*_run_lumi_info.json"))
                num_getFileInfo_files = len(getFileInfo_files)
                print(f"        Number of files with run-lumi information obtained from getFileInfo.py for {era}/{DataMC}: {num_getFileInfo_files}")
                dataset_json_file = output_dir / era / f"DAS_{era}_dataset.json"
                num_dataset_json_files = 0
                with open(dataset_json_file) as f:
                    dataset_json = json.load(f)
                for group in dataset_json[DataMC]:
                    for dataset_name in dataset_json[DataMC][group]:
                        num_dataset_json_files += len(dataset_json[DataMC][group][dataset_name])
                print(f"        Number of files for which dataset JSON file is generated for {era}/{DataMC}: {num_dataset_json_files}")
                crab_preselection_dirs = list(output_dir.glob(f"{era}/{DataMC}/**/crab_preselection"))
                num_crab_preselection_jobs = len(crab_preselection_dirs)
                print(f"        Number of CRAB pre-selection jobs submitted for {era}/{DataMC}: {num_crab_preselection_jobs}")
                crab_job_expected_output_dir = f"{eos_base}/{era}/{DataMC}/"
                num_preselection_output_files = len(list(Path(crab_job_expected_output_dir).glob(f"**/*.root")))
                print(f"        Number of pre-selection output root files found at expected location {crab_job_expected_output_dir}: {num_preselection_output_files}")
                for group in config['DASQueries'][era][DataMC]:
                    print(f"            Getting status for era/DataMC/Group: {era}/{DataMC}/{group}")
                    group_expected_files = aggregated_info[DataMC][group]['group_total']['num_files']
                    print(f"              Total files according to DAS for {era}/{DataMC}/{group}: {group_expected_files}")
                    getFileInfo_files = list(output_dir.glob(f"{era}/{DataMC}/{group}/**/*_file*_run_lumi_info.json"))
                    num_getFileInfo_files = len(getFileInfo_files)
                    print(f"              Number of files with run-lumi information obtained from getFileInfo.py for {era}/{DataMC}/{group}: {num_getFileInfo_files}")
                    dataset_json_file = output_dir / era / f"DAS_{era}_dataset.json"
                    num_dataset_json_files = 0
                    with open(dataset_json_file) as f:
                        dataset_json = json.load(f)
                    for dataset_name in dataset_json[DataMC][group]:
                        num_dataset_json_files += len(dataset_json[DataMC][group][dataset_name])
                    print(f"              Number of files for which dataset JSON file is generated for {era}/{DataMC}/{group}: {num_dataset_json_files}")
                    crab_preselection_dirs = list(output_dir.glob(f"{era}/{DataMC}/{group}/**/crab_preselection"))
                    num_crab_preselection_jobs = len(crab_preselection_dirs)
                    print(f"              Number of CRAB pre-selection jobs submitted for {era}/{DataMC}/{group}: {num_crab_preselection_jobs}")
                    crab_job_expected_output_dir = f"{eos_base}/{era}/{DataMC}/{group}/"
                    num_preselection_output_files = len(list(Path(crab_job_expected_output_dir).glob(f"**/*.root")))
                    print(f"              Number of pre-selection output root files found at expected location {crab_job_expected_output_dir}: {num_preselection_output_files}")
                    for dataset_name in config['DASQueries'][era][DataMC][group]:
                        print(f"                  Getting status for era/DataMC/Group/Dataset: {era}/{DataMC}/{group}/{dataset_name}")
                        dataset_expected_files = aggregated_info[DataMC][group][dataset_name]['num_files']
                        print(f"                    Total files according to DAS for {era}/{DataMC}/{group}/{dataset_name}: {dataset_expected_files}")
                        getFileInfo_files = list(output_dir.glob(f"{era}/{DataMC}/{group}/{dataset_name}/**/*_file*_run_lumi_info.json"))
                        num_getFileInfo_files = len(getFileInfo_files)
                        print(f"                    Number of files with run-lumi information obtained from getFileInfo.py for {era}/{DataMC}/{group}/{dataset_name}: {num_getFileInfo_files}")
                        dataset_json_file = output_dir / era / f"DAS_{era}_dataset.json"
                        num_dataset_json_files = 0
                        with open(dataset_json_file) as f:
                            dataset_json = json.load(f)
                        num_dataset_json_files += len(dataset_json[DataMC][group][dataset_name])
                        print(f"                    Number of files for which dataset JSON file is generated for {era}/{DataMC}/{group}/{dataset_name}: {num_dataset_json_files}")
                        crab_preselection_dirs = list(output_dir.glob(f"{era}/{DataMC}/{group}/{dataset_name}/**/crab_preselection"))
                        num_crab_preselection_jobs = len(crab_preselection_dirs)
                        print(f"                    Number of CRAB pre-selection jobs submitted for {era}/{DataMC}/{group}/{dataset_name}: {num_crab_preselection_jobs}")
                        crab_job_expected_output_dir = f"{eos_base}/{era}/{DataMC}/{group}/{dataset_name}/"
                        num_preselection_output_files = len(list(Path(crab_job_expected_output_dir).glob(f"**/*.root")))
                        print(f"                    Number of pre-selection output root files found at expected location {crab_job_expected_output_dir}: {num_preselection_output_files}")

    # Verify preselection output ROOT files (branch completeness only)
    if args.verifyOutput:
        print("\nVerifying preselection output ROOT files (scripts/verifyOutput.py)...")
        verify_script = base_dir / 'scripts' / 'verifyOutput.py'
        if not verify_script.exists():
            print(f"Error: {verify_script} not found!")
            return 1
        verify_failed = False
        for era in config['DASQueries']:
            if not matches_filter(args.filter, era):
                continue
            dataset_json_path = output_dir / era / f"preselection_{era}_datasets.json"
            if not dataset_json_path.exists():
                print(f"Error: preselection dataset JSON not found for era {era} at {dataset_json_path}. "
                      f"Run --generatePreselectionDatasetJSON first.")
                verify_failed = True
                continue
            report_path = output_dir / era / f"verifyOutput_{era}_report.json"
            cmd = [
                sys.executable, str(verify_script),
                '--datasetJSON', str(dataset_json_path),
                '--config', str(config_path),
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

    exit(0)


if __name__ == '__main__':
    sys.exit(main())
