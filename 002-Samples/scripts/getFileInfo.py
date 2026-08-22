# This script gets the file run-lumi info provided the DAS query and saves it as a json file.

"""
Usage: python3 getFileInfo.py -q "lumi file=/store/data/Run2016E/SingleMuon/NANOAOD/HIPM_UL2016_MiniAODv2_NanoAODv9-v2/2520000/A15F073D-B3DA-294F-8138-7B816FA7970E.root" -o UL2016preVFP_ttbarSemiLeptonic_file1.json -outDir outputs/queryResults/UL2016preVFP/
"""

import argparse
import subprocess
import os
import sys
import threading

# run_all.py's --getFileInfo calls get_file_info() from multiple threads at
# once (ThreadPoolExecutor). Each thread's own print() calls are individually
# atomic, but a multi-line message built from several print() calls can still
# interleave with another thread's -- this lock keeps each message's lines
# together in the shared log.
_print_lock = threading.Lock()


def get_file_info(das_query, output_file):
    """Get file run-lumi info from DAS query and save to json file.

    Returns True on success, False if dasgoclient failed (caller decides how
    to handle/report -- this used to swallow failures by returning None in
    both cases, which let callers report success even when nothing was
    written).
    """
    cmd = f"dasgoclient -query='{das_query}' -json"
    with _print_lock:
        print(f"Executing command: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    # Make sure the command executed successfully
    if result.returncode != 0:
        with _print_lock:
            print("Error running dasgoclient:")
            print(f"Command output: \n{result.stdout}")
            print(f"Command error (if any): \n{result.stderr}")
        return False
    # make sure the output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        f.write(result.stdout)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get file run-lumi info from DAS query and save to json file.")
    parser.add_argument("-q", "--das_query", help="DAS query string")
    parser.add_argument("-o", "--output_filename", help="Output file name (e.g., UL2016preVFP_ttbarSemiLeptonic_file1.json)")
    parser.add_argument("-outDir", "--output_directory", help="Output directory path", default="./")
    print("Parsing arguments...")
    print("DAS Query:", parser.parse_args().das_query)
    print("Output Directory:", parser.parse_args().output_directory)
    print("Output File:", parser.parse_args().output_filename)

    args = parser.parse_args()
    output_file_path = os.path.join(args.output_directory, args.output_filename)
    ok = get_file_info(args.das_query, output_file_path)
    sys.exit(0 if ok else 1)