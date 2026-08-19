# It just runs the brilcalc command with provided normtag, unit and json file and saves the output to a csv file.
"""
You have to first source the brilcalc environment by `source /cvmfs/cms-bril.cern.ch/cms-lumi-pog/brilws-docker/brilws-env`.


Usage: python getLumiInfo.py --normtag <normtag> --unit <unit> --json <json_file> --outDir <output_directory> -o <output_file_name>
"""

import argparse
import subprocess
import os

def getLumiInformation(normtag, unit, json_file, output_file):
    # brilcalc is a shell alias wrapping singularity; use the real command directly
    # so subprocess doesn't need a shell and alias expansion is not required.
    singularity_image = '/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-cloud/brilws-docker:latest'
    command = [
        'singularity', '-s', 'exec',
        '--env', 'PYTHONPATH=/home/bril/.local/lib/python3.10/site-packages',
        singularity_image,
        'brilcalc',
        'lumi',
        '-i', json_file,
        '--normtag', normtag,
        '-u', unit,
        '-o', output_file
    ]
    # Run the command and capture the output
    print(f"Running command: {' '.join(command)}")
    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running brilcalc: {e.stderr}")
        return

    print(f"Luminosity information saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Get luminosity information using brilcalc.')
    parser.add_argument('--normtag', default='/cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json', help='The normtag to use for brilcalc. Defaults to /cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json.')
    parser.add_argument('--unit', default='/fb', help='The unit to use for brilcalc. Defaults to /fb.')
    parser.add_argument('--json', required=True, help='The JSON file containing the luminosity sections.')
    parser.add_argument('--outDir', default='.', help='The output directory to save the CSV file. Defaults to the current directory.')
    parser.add_argument('-o', '--output', default='lumi_info.csv', help='The name of the output CSV file. Defaults to lumi_info.csv.')

    args = parser.parse_args()

    # make sure output directory exists
    os.makedirs(args.outDir, exist_ok=True)

    output_path = os.path.join(args.outDir, args.output)

    # Call the getLumiInformation function with the parsed arguments
    getLumiInformation(args.normtag, args.unit, args.json, output_path)

if __name__ == "__main__":
    main()