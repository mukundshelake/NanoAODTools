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
    parser.add_argument('-t', '--tag', type=str, required=True,
                       help='Create named tag for this run (e.g., baseline, paper_v1)')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate outputs even if output files already exist for this config hash')
    parser.add_argument('--generateDatasetJSON', action='store_true',
                       help='[2] Generate dataset JSON file using the script generateDatasetJSON.py')
    parser.add_argument('--getStatus', action='store_true',
                       help='[3] Get overall status of the preselection processing by comparing expected number of files from DAS with the number of files obtained from getFileInfo.py, the number of files for which dataset JSON file is generated, the number of CRAB jobs submitted for pre-selection, and the number of pre-selection output root files found at the expected output location.')
    
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    
    args = parser.parse_args()

    # parsing arguments
    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --force: {args.force}")
    print(f"  --generateDatasetJSON: {args.generateDatasetJSON}")
    print(f"  --getStatus: {args.getStatus}")
    print(f"  --filter: {args.filter}")

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

    # Generate dataset JSON file from the file list obtained from --getFileList if requested
    if args.generateDatasetJSON:
        print("\nGenerating dataset JSON files from the file lists obtained from --getFileList...")
        for era in config['DASQueries']:
            if not matches_filter(args.filter, era):
                print(f"Skipping era {era} due to filter")
                continue
            generateJSON_script = base_dir / 'scripts' / 'generateDatasetJSON.py'
            # Return error if generateDatasetJSON.py does not exist
            if not generateJSON_script.exists():
                print(f"Error: {generateJSON_script} does not exist. Please make sure it is in the correct location.")
                return 1
            output_json_name = f"preselection_{era}_datasets.json"
            output_json_path = output_dir / era / output_json_name
            if output_json_path.exists() and not args.force:
                print(f"Output JSON file already exists for {era} and --force not set. Skipping: {output_json_path}")
                continue
            base_directory = f'/mnt/disk2/mukund/DataFiles/preselection/{args.tag}/{era}'
            cmd = [
                'python', str(generateJSON_script),
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
                print(f"Successfully generated dataset JSON for {era}: {output_json_path}")
    if args.getStatus:
        print("\nGetting overall status of the preselection processing...")
        for era in config['DASQueries']:
            if not matches_filter(args.filter, era):
                continue
            print(f"\nStatus for era: {era}")
            # Open aggregrate file info JSON to get expected number of files in that era
            aggregated_info_json = output_dir / era / f"{era}_aggregated_dataset_info.json"
            if not aggregated_info_json.exists():
                print(f"Error: Aggregated dataset info JSON file not found for era {era} at expected location: {aggregated_info_json}. Please ensure the aggregated dataset info JSON is generated before checking status.")
                continue
            with open(aggregated_info_json) as f:
                aggregated_info = json.load(f)
            DAS_expected_files = aggregated_info['era_total']['num_files']
            print(f"  Total files according to DAS: {DAS_expected_files}")

            # Count number of files that are obtained from getFileInfo.py (i.e. files for which we have run-lumi information)
            getFileInfo_files = list(output_dir.glob(f"{era}/**/*_file*_run_lumi_info.json"))
            num_getFileInfo_files = len(getFileInfo_files)
            print(f"  Number of files with run-lumi information obtained from getFileInfo.py: {num_getFileInfo_files}")

            # Count the number of files in the output of generateDatasetJSON (i.e. files for which we have generated dataset JSON file)
            dataset_json_file = output_dir / era / f"DAS_{era}_dataset.json"
            num_dataset_json_files = 0
            with open(dataset_json_file) as f:
                dataset_json = json.load(f)
            for DataMC in dataset_json:
                for group in dataset_json[DataMC]:
                    for dataset_name in dataset_json[DataMC][group]:
                        num_dataset_json_files += len(dataset_json[DataMC][group][dataset_name])
            print(f"  Number of files for which dataset JSON file is generated: {num_dataset_json_files}")

            # Count the number of CRAB jobs submitted for pre-selection (i.e. number of crab_preselection directories)
            crab_preselection_dirs = list(output_dir.glob(f"{era}/**/crab_preselection"))
            num_crab_preselection_jobs = len(crab_preselection_dirs)
            print(f"  Number of CRAB pre-selection jobs submitted: {num_crab_preselection_jobs}")

            # Count the number of root files produced from the pre-selection jobs at the expected output location (i.e. outputs/{tag}/{era}/**/preselection_output/*.root)
            crab_job_expected_output_dir = f"/eos/user/m/mshelake/DataFiles/{era}/"
            num_preselection_output_files = len(list(Path(crab_job_expected_output_dir).glob(f"**/*.root")))
            print(f"  Number of pre-selection output root files found at expected location {crab_job_expected_output_dir}: {num_preselection_output_files}")
            for DataMC in config['DASQueries'][era]:
                print(f"      Getting status for era/DataMC: {era}/{DataMC}")
                # Open aggregrate file info JSON to get expected number of files for that era/DataMC
                DataMC_expected_files = aggregated_info[DataMC]['DataMC_total']['num_files']
                print(f"        Total files according to DAS for {era}/{DataMC}: {DataMC_expected_files}")
                # Count number of files that are obtained from getFileInfo.py for that era/DataMC (i.e. files for which we have run-lumi information)
                getFileInfo_files = list(output_dir.glob(f"{era}/{DataMC}/**/*_file*_run_lumi_info.json"))
                num_getFileInfo_files = len(getFileInfo_files)
                print(f"        Number of files with run-lumi information obtained from getFileInfo.py for {era}/{DataMC}: {num_getFileInfo_files}")
                # Count the number of files in the output of generateDatasetJSON for that era/DataMC (i.e. files for which we have generated dataset JSON file)
                dataset_json_file = output_dir / era / f"DAS_{era}_dataset.json"
                num_dataset_json_files = 0
                with open(dataset_json_file) as f:
                    dataset_json = json.load(f)
                for group in dataset_json[DataMC]:
                    for dataset_name in dataset_json[DataMC][group]:
                        num_dataset_json_files += len(dataset_json[DataMC][group][dataset_name])
                print(f"        Number of files for which dataset JSON file is generated for {era}/{DataMC}: {num_dataset_json_files}")
                # Count the number of CRAB jobs submitted for pre-selection for that era/DataMC (i.e. number of crab_preselection directories)
                crab_preselection_dirs = list(output_dir.glob(f"{era}/{DataMC}/**/crab_preselection"))
                num_crab_preselection_jobs = len(crab_preselection_dirs)
                print(f"        Number of CRAB pre-selection jobs submitted for {era}/{DataMC}: {num_crab_preselection_jobs}")
                # Count the number of root files produced from the pre-selection jobs for that era/DataMC at the expected output location (i.e. outputs/{tag}/{era}/{DataMC}/**/preselection_output/*.root)
                crab_job_expected_output_dir = f"/eos/user/m/mshelake/DataFiles/{era}/{DataMC}/"
                num_preselection_output_files = len(list(Path(crab_job_expected_output_dir).glob(f"**/*.root")))
                print(f"        Number of pre-selection output root files found at expected location {crab_job_expected_output_dir}: {num_preselection_output_files}")
                for group in config['DASQueries'][era][DataMC]:
                    print(f"            Getting status for era/DataMC/Group: {era}/{DataMC}/{group}")
                    # Open aggregrate file info JSON to get expected number of files for that era/DataMC/Group
                    group_expected_files = aggregated_info[DataMC][group]['group_total']['num_files']
                    print(f"              Total files according to DAS for {era}/{DataMC}/{group}: {group_expected_files}")
                    # Count number of files that are obtained from getFileInfo.py for that era/DataMC/Group (i.e. files for which we have run-lumi information)
                    getFileInfo_files = list(output_dir.glob(f"{era}/{DataMC}/{group}/**/*_file*_run_lumi_info.json"))
                    num_getFileInfo_files = len(getFileInfo_files)
                    print(f"              Number of files with run-lumi information obtained from getFileInfo.py for {era}/{DataMC}/{group}: {num_getFileInfo_files}")
                    # Count the number of files in the output of generateDatasetJSON for that era/DataMC/Group (i.e. files for which we have generated dataset JSON file)
                    dataset_json_file = output_dir / era / f"DAS_{era}_dataset.json"
                    num_dataset_json_files = 0
                    with open(dataset_json_file) as f:
                        dataset_json = json.load(f)
                    for dataset_name in dataset_json[DataMC][group]:
                        num_dataset_json_files += len(dataset_json[DataMC][group][dataset_name])
                    print(f"              Number of files for which dataset JSON file is generated for {era}/{DataMC}/{group}: {num_dataset_json_files}")
                    # Count the number of CRAB jobs submitted for pre-selection for that era/DataMC/Group (i.e. number of crab_preselection directories)
                    crab_preselection_dirs = list(output_dir.glob(f"{era}/{DataMC}/{group}/**/crab_preselection"))
                    num_crab_preselection_jobs = len(crab_preselection_dirs)
                    print(f"              Number of CRAB pre-selection jobs submitted for {era}/{DataMC}/{group}: {num_crab_preselection_jobs}")
                    # Count the number of root files produced from the pre-selection jobs for that era/DataMC/Group at the expected output location (i.e. outputs/{tag}/{era}/{DataMC}/{group}/**/preselection_output/*.root)
                    crab_job_expected_output_dir = f"/eos/user/m/mshelake/DataFiles/{era}/{DataMC}/{group}/"
                    num_preselection_output_files = len(list(Path(crab_job_expected_output_dir).glob(f"**/*.root")))
                    print(f"              Number of pre-selection output root files found at expected location {crab_job_expected_output_dir}: {num_preselection_output_files}")
                    for dataset_name in config['DASQueries'][era][DataMC][group]:
                        print(f"                  Getting status for era/DataMC/Group/Dataset: {era}/{DataMC}/{group}/{dataset_name}")
                        # Open aggregrate file info JSON to get expected number of files for that era/DataMC/Group/Dataset
                        dataset_expected_files = aggregated_info[DataMC][group][dataset_name]['num_files']
                        print(f"                    Total files according to DAS for {era}/{DataMC}/{group}/{dataset_name}: {dataset_expected_files}")
                        # Count number of files that are obtained from getFileInfo.py for that era/DataMC/Group/Dataset (i.e. files for which we have run-lumi information)
                        getFileInfo_files = list(output_dir.glob(f"{era}/{DataMC}/{group}/{dataset_name}/**/*_file*_run_lumi_info.json"))
                        num_getFileInfo_files = len(getFileInfo_files)
                        print(f"                    Number of files with run-lumi information obtained from getFileInfo.py for {era}/{DataMC}/{group}/{dataset_name}: {num_getFileInfo_files}")
                        # Count the number of files in the output of generateDatasetJSON for that era/DataMC/Group/Dataset (i.e. files for which we have generated dataset JSON file)
                        dataset_json_file = output_dir / era / f"DAS_{era}_dataset.json"
                        num_dataset_json_files = 0
                        with open(dataset_json_file) as f:
                            dataset_json = json.load(f)
                        num_dataset_json_files += len(dataset_json[DataMC][group][dataset_name])
                        print(f"                    Number of files for which dataset JSON file is generated for {era}/{DataMC}/{group}/{dataset_name}: {num_dataset_json_files}")
                        # Count the number of CRAB jobs submitted for pre-selection for that era/DataMC/Group/Dataset (i.e. number of crab_preselection directories)
                        crab_preselection_dirs = list(output_dir.glob(f"{era}/{DataMC}/{group}/{dataset_name}/**/crab_preselection"))
                        num_crab_preselection_jobs = len(crab_preselection_dirs)
                        print(f"                    Number of CRAB pre-selection jobs submitted for {era}/{DataMC}/{group}/{dataset_name}: {num_crab_preselection_jobs}")
                        # Count the number of root files produced from the pre-selection jobs for that era/DataMC/Group/Dataset at the expected output location (i.e. outputs/{tag}/{era}/{DataMC}/{group}/{dataset_name}/**/preselection_output/*.root)
                        crab_job_expected_output_dir = f"/eos/user/m/mshelake/DataFiles/{era}/{DataMC}/{group}/{dataset_name}/"
                        num_preselection_output_files = len(list(Path(crab_job_expected_output_dir).glob(f"**/*.root")))
                        print(f"                    Number of pre-selection output root files found at expected location {crab_job_expected_output_dir}: {num_preselection_output_files}")
    exit(0)


if __name__ == '__main__':
    sys.exit(main())
