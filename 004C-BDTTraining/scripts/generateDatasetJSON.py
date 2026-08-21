# This script generates a dataset JSON file given a base directory of parquet
# outputs and saves it in the given output directory with the given name.
#
# Pure-python (pyarrow) equivalent of the other chapters' generateDatasetJSON.py,
# which uses PyROOT to health-check ROOT files -- there's no ROOT/CMSSW
# dependency at this stage since the outputs here are parquet, not ROOT.

import logging
import os
import json
import argparse
import pyarrow.parquet as pq


def is_parquet_file_healthy(filepath: str) -> int:
    """Check if a parquet file is readable and non-empty.

    Returns the row count if healthy, or -1 if the file should be rejected.
    """
    if not os.path.exists(filepath):
        logging.error(f"File does not exist: {filepath}")
        return -1
    if not os.path.isfile(filepath):
        logging.error(f"Path is not a file: {filepath}")
        return -1
    if os.path.getsize(filepath) == 0:
        logging.error(f"File is empty: {filepath}")
        return -1
    try:
        num_rows = pq.ParquetFile(filepath).metadata.num_rows
    except Exception as e:
        logging.error(f"Failed to open parquet file {filepath}: {e}")
        return -1

    if num_rows == 0:
        logging.error(f"Parquet file has no rows: {filepath}")
        return -1

    return num_rows


def generate_dataset_json(base_dir, output_dir, output_name):
    dataset_dict = {}
    totalEraFiles = 0
    rejected_totalEraFiles = 0
    for DataMC in os.listdir(base_dir):
        DataMCDir = os.path.join(base_dir, DataMC)
        if not os.path.isdir(DataMCDir):
            logging.warning(f"Skipping non-directory: {DataMCDir}")
            continue
        dataset_dict[DataMC] = {}
        totalDataMCFiles = 0
        rejected_totalDataMCFiles = 0
        for group in os.listdir(DataMCDir):
            sampleDir = os.path.join(DataMCDir, group)
            if not os.path.isdir(sampleDir):
                logging.warning(f"Skipping non-directory: {sampleDir}")
                continue
            dataset_dict[DataMC][group] = {}
            totalGroupFiles = 0
            rejected_totalGroupFiles = 0
            for dataset in os.listdir(sampleDir):
                datasetDir = os.path.join(sampleDir, dataset)
                if not os.path.isdir(datasetDir):
                    logging.warning(f"Skipping non-directory: {datasetDir}")
                    continue
                dataset_dict[DataMC][group][dataset] = {}
                # loop over all parquet files in datasetDir and its subdirectories
                totalDatasetFiles = 0
                rejected_totalDatasetFiles = 0
                for dirpath, _, filenames in os.walk(datasetDir):
                    for file in filenames:
                        if file.endswith('.parquet'):
                            filePath = os.path.join(dirpath, file)
                            num_rows = is_parquet_file_healthy(filePath)
                            if num_rows >= 0:
                                # Value is the row count -- useful bookkeeping for
                                # the training step, unlike ROOT's tree-name value.
                                dataset_dict[DataMC][group][dataset][filePath] = num_rows
                                totalDatasetFiles += 1
                                totalGroupFiles += 1
                                totalDataMCFiles += 1
                                totalEraFiles += 1
                            else:
                                logging.warning(f"Skipping unhealthy parquet file: {filePath}")
                                rejected_totalDatasetFiles += 1
                                rejected_totalGroupFiles += 1
                                rejected_totalDataMCFiles += 1
                                rejected_totalEraFiles += 1
                logging.info(f"Total healthy (unhealthy) parquet files in dataset {dataset}: {totalDatasetFiles} ({rejected_totalDatasetFiles})")
            logging.info(f"Total healthy (unhealthy) parquet files in group {group}: {totalGroupFiles} ({rejected_totalGroupFiles})")
        logging.info(f"Total healthy (unhealthy) parquet files in Data/MC {DataMC}: {totalDataMCFiles} ({rejected_totalDataMCFiles})")
    logging.info(f"Total healthy (unhealthy) parquet files in all eras: {totalEraFiles} ({rejected_totalEraFiles})")

    output_path = os.path.join(output_dir, output_name)
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, 'w') as json_file:
        json.dump(dataset_dict, json_file, indent=4)
    print(f"Dataset JSON file generated at: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate dataset JSON file from a base directory of parquet outputs.")
    parser.add_argument("--outputDirectory", required=True, help="Output directory for JSON files")
    parser.add_argument("--outputFileName", required=True, help="Output file name for the JSON file")
    parser.add_argument("--baseDirectory", required=True, help="Base directory for the datasets")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    generate_dataset_json(args.baseDirectory, args.outputDirectory, args.outputFileName)
