# This script generates the brilcalc friendly run-lumi json file for each root file. It takes in the output of the getfileInfo as input and saves the run-lumi json file as output.

"""
Usage: python3 generateRunLumiFiles.py -i <input_json_file> -outDir <output_directory> -o <output_file_name?
"""

import argparse
import json
import os


def generate_run_lumi_files(input_json_file, output_file_path):
    # Read the input JSON file
    with open(input_json_file, 'r') as f:
        data = json.load(f)
    runLumi_info = {}
    for entry in data:
        lumi = entry["lumi"][0]["lumi_section_num"]
        run = entry["lumi"][0]["run_number"]
        if run not in runLumi_info:
            runLumi_info[run] = []
        runLumi_info[run].append(lumi)

    # Save the run-lumi information to a new JSON file
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    with open(output_file_path, 'w') as f:
        json.dump(runLumi_info, f, indent=4)

    print(f"Run-lumi information saved to {output_file_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate run-lumi JSON files for brilcalc.')
    parser.add_argument('-i', '--input', required=True, help='The input JSON file containing the lumi information.')
    parser.add_argument('--outDir', default='.', help='The output directory to save the run-lumi JSON file. Defaults to the current directory.')
    parser.add_argument('-o', '--output', default='run_lumi_info.json', help='The name of the output run-lumi JSON file. Defaults to run_lumi_info.json.')

    args = parser.parse_args()

    output_file_path = os.path.join(args.outDir, args.output)

    generate_run_lumi_files(args.input, output_file_path)
