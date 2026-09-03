# This script is used to download the golden JSON files provided their URLs and save as json file in the specified output directory with the specified output file name. The golden JSON files contain the list of good luminosity sections for each dataset, which is essential for data analysis in high energy physics.
"""Download golden JSON files from provided URLs.
Usage:
    python3 scripts/downloadGoldenJsons.py -u https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions16/13TeV/Legacy_2016/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt -outDir outputs/goldenJsons/UL2016/ -o UL2016_goldenJSON.json
"""

import argparse
import requests
import os
import json

def download_golden_json(url, output_file):
    """Download golden JSON file from the provided URL and save it to the specified output file."""
    try:
        print(f"Downloading golden JSON from: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        os.makedirs(os.path.dirname(output_file), exist_ok=True)  # Ensure output directory exists
        with open(output_file, "w") as f:
            json.dump(response.json(), f, indent=4)
        print(f"Golden JSON saved to: {output_file}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading golden JSON: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download golden JSON files from provided URLs.")
    parser.add_argument("-u", "--url", help="URL of the golden JSON file to download")
    parser.add_argument("-o", "--output_filename", help="Output file name (e.g., UL2016_goldenJSON.json)")
    parser.add_argument("-outDir", "--output_directory", help="Output directory path", default="./")
    print("Parsing arguments...")
    print("URL:", parser.parse_args().url)
    print("Output Directory:", parser.parse_args().output_directory)
    print("Output File:", parser.parse_args().output_filename)

    args = parser.parse_args()
    output_file_path = os.path.join(args.output_directory, args.output_filename)
    download_golden_json(args.url, output_file_path)
