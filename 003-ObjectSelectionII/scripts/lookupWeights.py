# This script uses coffea to compute the histograms of weights for given root files.
# Inputs: 
# - fileList: A coffea compatible file list (something similar to `/home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/selectionII_earlyApril_UL2018_datasets.json`)
# - configFile: Config path that contains all the relevant info to build the hists
# - outputDir: Directory to save the output .coffea file
# - outputFileName: Name of the output .coffea file

import os
import sys
import json
from pathlib import Path
import yaml
import argparse
import logging
import numpy as np
import dask
import awkward as ak
import hist.dask as hda
import dask_awkward as dak
from coffea.nanoevents import NanoAODSchema
from coffea import processor
import hist
from coffea.dataset_tools import (
    apply_to_fileset,
    max_chunks,
    preprocess,
)
from coffea.util import save


# Configure logger
logging.basicConfig(
    level=logging.INFO,  # Use INFO or WARNING in production
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


class WeightLookupProcessor(processor.ProcessorABC):
    def __init__(self, config):
        self.config = config

    def process(self, events):
        dataset = events.metadata['dataset']
        era = events.metadata['era']    
        logger.info(f"Processing dataset: {dataset} for era: {era}")
        # Initialize histograms for this dataset
        hists = {}
        for module in self.config['Modules']:
            for sftype, branchName in self.config['Modules'][module][era]['branchNames'].items():
                hist_name = f"{module}_{branchName}"
                hists[hist_name] = hist.Hist(
                    hist.axis.Regular(10, 0.5, 1.5, name=branchName ),
                    storage=hist.storage.Weight()
                )
        # Loop over modules and branches to fill the histograms and collect running stats
        # Store count/sum/sum_sq so they accumulate correctly via + across chunks.
        # mean/std/min/max are derived in postprocess() after full accumulation.
        stats = {}
        for module in self.config['Modules']:
            for sftype, branchName in self.config['Modules'][module][era]['branchNames'].items():
                hist_name = f"{module}_{branchName}"
                if branchName in events.fields:
                    logger.info(f"Filling histogram: {hist_name} with branch: {branchName}")
                    values = events[branchName].compute()
                    values_np = ak.to_numpy(values)
                    hists[hist_name].fill(values_np, weight=values_np)
                    stats[hist_name] = {
                        "count":  int(len(values_np)),
                        "sum":    float(np.sum(values_np)),
                        "sum_sq": float(np.sum(values_np ** 2)),
                    }
                else:
                    logger.warning(f"Branch {branchName} not found in events for dataset {dataset}. Skipping histogram {hist_name}.")
        return {
            "nEvents": ak.num(events, axis=0).compute(),
            "hists": hists,
            "stats": stats,
        }

    def postprocess(self, accumulator):
        # Derive mean/std from accumulated count/sum/sum_sq.
        # Derive min/max from the filled histogram bin edges (resolution = bin width).
        for dataset in accumulator:
            for hist_name, s in accumulator[dataset]['stats'].items():
                count = s['count']
                if count > 0:
                    mean = s['sum'] / count
                    variance = max(s['sum_sq'] / count - mean ** 2, 0.0)
                    std = float(np.sqrt(variance))
                else:
                    mean = std = float('nan')
                h = accumulator[dataset]['hists'][hist_name]
                nonzero = np.where(h.values() > 0)[0]
                lo = float(h.axes[0].edges[nonzero[0]])       if len(nonzero) else float('nan')
                hi = float(h.axes[0].edges[nonzero[-1] + 1])  if len(nonzero) else float('nan')
                accumulator[dataset]['stats'][hist_name] = {
                    'mean': mean,
                    'std':  std,
                    'min':  lo,
                    'max':  hi,
                }
        return accumulator

def main():
    parser = argparse.ArgumentParser(description="Lookup weights using coffea")
    parser.add_argument('--fileList', type=str, required=True, help='Path to the coffea compatible file list JSON')
    parser.add_argument('--configFile', type=str, required=True, help='Path to the YAML config file')
    parser.add_argument('--outputDir', type=str, required=True, help='Directory to save the output .coffea file')
    parser.add_argument('--outputFileName', type=str, required=True, help='Name of the output .coffea file')
    args = parser.parse_args()

    # Load config
    with open(args.configFile, 'r') as f:
        config = yaml.safe_load(f)

    # Load fileset
    with open(args.fileList, 'r') as f:
        fileset = json.load(f)

    # Create processor instance
    processor_instance = WeightLookupProcessor(config)

    # Preprocess and run with dask
    logger.info("Preprocessing fileset...")
    dataset_runnable, dataset_updated = preprocess(
        fileset,
        align_clusters=False,
        files_per_batch=1,
        skip_bad_files=True,
        save_form=False,
    )

    to_compute = apply_to_fileset(
        processor_instance,
        max_chunks(dataset_runnable, 300),
        schemaclass=NanoAODSchema,
    )

    (output,) = dask.compute(to_compute, scheduler='threads')

    # Save output
    output_path = os.path.join(args.outputDir, args.outputFileName)
    save(output, output_path)
    logger.info(f"Saved output to {output_path}")   


if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()