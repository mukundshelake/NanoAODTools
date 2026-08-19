# This script is used to fetch dataset info from DAS and save it into a json file.
"""Get dataset info from DAS and save to JSON file.
Usage:
    python3 scripts/getDatasetInfo.py -q "dataset=/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM | grep dataset.nevents | grep dataset.num_file" -o UL2016preVFP_ttbarSemiLeptonic.json -outDir outputs/queryResults/UL2016preVFP/
"""
import argparse
import subprocess   
import os
import json

def get_dataset_info(das_query, output_file):
    """Get dataset info from DAS query and save to JSON file."""
    cmd = f"dasgoclient -query='{das_query}'"
    print(f"Executing command: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"Command output: \n{result.stdout}")

    # result will look like this: `132178000   117` a single line; where the first number is the number of events and the second number is the number of files. We want to parse this output to extract the dataset info.

    # Make sure the command executed successfully
    if result.returncode != 0:
        print("Error running dasgoclient:")
        print(f"Command output: \n{result.stdout}")
        print(f"Command error (if any): \n{result.stderr}")  
        return
    
    # Parsing the result to extract dataset info
    output_lines = result.stdout.strip().splitlines()
    if len(output_lines) == 0:
        print("No output from DAS query. Please check the query and try again.")
        return
    
    # Assuming the output is in the format: `132178000  117`
    dataset_info = {}
    for line in output_lines:
        parts = line.split()
        if len(parts) >= 2:
            dataset_info['num_events'] = int(parts[0])
            dataset_info['num_files'] = int(parts[1])
        else:
            print(f"Unexpected output format: {line}")
            return
    
    # make sure the output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save the dataset info to a JSON file
    with open(output_file, "w") as f:
        json.dump(dataset_info, f, indent=4)
        print(f"Dataset info saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get dataset info from DAS query and save to JSON file.")
    parser.add_argument("-q", "--das_query", help="DAS query string")
    parser.add_argument("-o", "--output_filename", help="Output file name (e.g., UL2016preVFP_ttbarSemiLeptonic.json)")
    parser.add_argument("-outDir", "--output_directory", help="Output directory path", default="./")
    print("Parsing arguments...")
    print("DAS Query:", parser.parse_args().das_query)
    print("Output Directory:", parser.parse_args().output_directory)
    print("Output File:", parser.parse_args().output_filename)

    args = parser.parse_args()
    output_file_path = os.path.join(args.output_directory, args.output_filename)
    get_dataset_info(args.das_query, output_file_path)