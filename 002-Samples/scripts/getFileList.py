# THe objective of the script is to get the list of files from a DAS query and save it to a json file.
"""
Get list of files from DAS query and save to json file.
Usage:
    python3 scripts/getFileList.py -q "file dataset=/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM" -o UL2016preVFP_ttbarSemiLeptonic.json -outDir outputs/queryResults/UL2016preVFP/
"""
import argparse
import subprocess
import os

def get_file_list(das_query, output_file):
    """Get list of files from DAS query and save to json file."""
    cmd = f"dasgoclient -query='{das_query}' -json"
    print(f"Executing command: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
 
    # Make sure the command executed successfully
    if result.returncode != 0:
        print("Error running dasgoclient:")
        print(f"Command output: \n{result.stdout}")
        print(f"Command error (if any): \n{result.stderr}")  
        return
    # make sure the output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        f.write(result.stdout)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get list of files from DAS query and save to json file.")
    parser.add_argument("-q", "--das_query", help="DAS query string")
    parser.add_argument("-o", "--output_filename", help="Output file name (e.g., UL2016preVFP_ttbarSemiLeptonic.json)")
    parser.add_argument("-outDir", "--output_directory", help="Output directory path", default="./")
    print("Parsing arguments...")
    print("DAS Query:", parser.parse_args().das_query)
    print("Output Directory:", parser.parse_args().output_directory)
    print("Output File:", parser.parse_args().output_filename)

    args = parser.parse_args()
    output_file_path = os.path.join(args.output_directory, args.output_filename)
    get_file_list(args.das_query, output_file_path)
