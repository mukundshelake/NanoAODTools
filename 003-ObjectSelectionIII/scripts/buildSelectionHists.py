# This script takes the fileset and config file. Builds the Hist histograms using coffea and saves the output as .coffea file in the said output directory with the said name.
# Usage: python buildSelectionHists.py --fileset <path to fileset> --config <path to config file> --outputDir <path to output directory> --outputName <name of output file>

import os
import json
from pathlib import Path
import yaml
import argparse
import logging
import dask
import dask_awkward as dak
import dask_histogram as dh
import boost_histogram as bh
import awkward as ak
from coffea.nanoevents import NanoAODSchema, BaseSchema
from coffea import processor
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
        isData = events.metadata['isData']
        logger.info(f"Processing dataset: {dataset}")
        # Build total weights by multiplying the individual weights together
        total_weights = None
        weightList = self.config['weightList']['Data'] if isData else self.config['weightList']['MC']
        for weight in weightList:
            if weight in events.fields:
                w = dak.to_dask_array(events[weight])
                total_weights = w if total_weights is None else total_weights * w
            else:
                logger.warning(f"Weight '{weight}' not found in events. Skipping this weight.")
        # Fill histograms lazily using dask_histogram
        hists = {}
        for hist_ in self.config['histDetails']:
            cfg = self.config['histDetails'][hist_]
            var_name = cfg['variable']
            if var_name not in events.fields:
                logger.warning(f"Variable '{var_name}' not found in events. Skipping histogram '{hist_}'.")
                continue
            data = dak.to_dask_array(events[var_name])
            hists[hist_] = dh.factory(
                data,
                axes=[bh.axis.Regular(cfg['bins'], cfg['range'][0], cfg['range'][1],
                                      metadata={'name': cfg['name'], 'label': cfg['label']})],
                storage=bh.storage.Double(),
                weights=total_weights,
            )
        return {
            "nEvents": ak.num(events, axis=0),
            "hists": hists
        }
    def postprocess(self, accumulator):
        return accumulator

def main():
    parser = argparse.ArgumentParser(description="Lookup weights using coffea")
    parser.add_argument('--fileSet', type=str, required=True, help='Path to the coffea compatible file list JSON')
    parser.add_argument('--configFile', type=str, required=True, help='Path to the YAML config file')
    parser.add_argument('--outputDir', type=str, required=True, help='Directory to save the output .coffea file')
    parser.add_argument('--outputFileName', type=str, required=True, help='Name of the output .coffea file')
    args = parser.parse_args()

    # Load config
    with open(args.configFile, 'r') as f:
        config = yaml.safe_load(f)

    # Load fileset
    with open(args.fileSet, 'r') as f:
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
        schemaclass=BaseSchema,
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