import os
import json
import sys
import argparse
from multiprocessing import Pool
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor

def process_dataset(data):
    """Function to process a single dataset."""
    DataMC, key, files, outDir, cut_string = data

    print(f"Processing {key} in {DataMC}")

    if not os.path.exists(outDir):
        os.makedirs(outDir)

    # Set up the PostProcessor
    post_processor = PostProcessor(
        outDir,
        files,
        cut=cut_string,
        branchsel="python/postprocessing/examples/keep_and_drop.txt",  # Adjust this if needed
        modules=[],  # Add your custom modules if needed
        noOut=False,
        justcount=True,
    )

    # Run the PostProcessor
    post_processor.run()
    print(f"Finished processing {key} in {DataMC}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run post-processing on datasets.")
    parser.add_argument('--cut_string', type=str, required=True, help='Cut string to be applied.')
    parser.add_argument('--json_file', type=str, required=True, help='Path to the JSON file with dataset configuration.')

    args = parser.parse_args()

    # Load the dataset configuration
    with open(args.json_file, 'r') as json_file:
        dicti = json.load(json_file)

    cut_string = args.cut_string

    print("Cut string being applied:", cut_string)

    # Prepare datasets for parallel processing
    dataset_list = []
    for DataMC in dicti:
        if 'mu' in DataMC:
            for key in dicti[DataMC]:
                outDir = f'outputs/UL2016preVFP_count/{DataMC}/{key}'
                input_files = dicti[DataMC][key]
                dataset_list.append((DataMC, key, input_files, outDir, cut_string))

    # Use multiprocessing to process datasets in parallel
    num_cores = 20
    with Pool(num_cores) as pool:
        pool.map(process_dataset, dataset_list)