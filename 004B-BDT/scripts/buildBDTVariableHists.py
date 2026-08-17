#!/usr/bin/env python3
"""
Build Data/MC histograms for BDT variables from ROOT files.

This script takes BDT variable ROOT files (output from 004B-BDT processing)
and builds histograms for the 17 BDT variables using the coffea framework.

The histogram specifications come from the config file's histDetails section.
Weights are applied according to the weightList in the config.

Usage:
    python buildBDTVariableHists.py --fileSet <path to fileset> \
                                      --config <path to config file> \
                                      --outputDir <path to output directory> \
                                      --outputName <name of output file>
"""

import os
import json
from pathlib import Path
import yaml
import argparse
import logging
import numpy as np
import dask
import dask_awkward as dak
import dask_histogram as dh
import boost_histogram as bh
import awkward as ak
from coffea.nanoevents import BaseSchema
from coffea import processor
from coffea.dataset_tools import (
    apply_to_fileset,
    max_chunks,
    preprocess,
)
from coffea.util import save


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


class BDTVariableHistProcessor(processor.ProcessorABC):
    """
    Processor to build histograms for BDT variables.
    
    Reads 17 BDT variable branches and creates histograms according to config.
    Applies weights from the weightList in config (Data vs MC).
    """
    def __init__(self, config):
        self.config = config

    def process(self, events):
        dataset = events.metadata['dataset']
        # Infer isData from dataset name (contains 'Data_' if data, 'MC_' if MC)
        isData = 'Data_' in dataset if 'dataset' in events.metadata else False
        logger.info(f"Processing dataset: {dataset} (isData={isData})")
        
        # Build total weights by multiplying individual weights together
        total_weights = None
        weightList = self.config['weightList']['Data'] if isData else self.config['weightList']['MC']
        
        for weight in weightList:
            if weight in events.fields:
                w = dak.to_dask_array(events[weight])
                total_weights = w if total_weights is None else total_weights * w
            else:
                logger.warning(f"Weight '{weight}' not found in events. Skipping this weight.")
        
        # If no weights were found, use ones
        if total_weights is None:
            logger.warning("No weights found in events. Using unit weights.")
            # Get any field to determine array length
            if len(events.fields) > 0:
                first_field = events.fields[0]
                total_weights = ak.ones_like(events[first_field], dtype=np.float32)
            else:
                logger.error("No fields found in events!")
                return {"nEvents": 0, "hists": {}}
        
        # Fill histograms lazily using dask_histogram
        hists = {}
        for hist_name in self.config['histDetails']:
            cfg = self.config['histDetails'][hist_name]
            var_name = cfg['variable']
            
            if var_name not in events.fields:
                logger.warning(f"Variable '{var_name}' not found in events. Skipping histogram '{hist_name}'.")
                continue
            
            data = dak.to_dask_array(events[var_name])
            hists[hist_name] = dh.factory(
                data,
                axes=[bh.axis.Regular(
                    cfg['bins'], 
                    cfg['range'][0], 
                    cfg['range'][1],
                    metadata={'name': cfg['name'], 'label': cfg['label']}
                )],
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
    parser = argparse.ArgumentParser(
        description="Build BDT variable histograms using coffea"
    )
    parser.add_argument(
        '--fileSet', 
        type=str, 
        required=True, 
        help='Path to the coffea compatible file list JSON'
    )
    parser.add_argument(
        '--configFile', 
        type=str, 
        required=True, 
        help='Path to the YAML config file'
    )
    parser.add_argument(
        '--outputDir', 
        type=str, 
        required=True, 
        help='Directory to save the output .coffea file'
    )
    parser.add_argument(
        '--outputFileName', 
        type=str, 
        required=True, 
        help='Name of the output .coffea file'
    )
    args = parser.parse_args()

    # Load config
    with open(args.configFile, 'r') as f:
        config = yaml.safe_load(f)
    
    logger.info(f"Loaded configuration from {args.configFile}")

    # Load fileset
    with open(args.fileSet, 'r') as f:
        fileset = json.load(f)
    
    logger.info(f"Loaded fileset from {args.fileSet}")

    # Create processor instance
    processor_instance = BDTVariableHistProcessor(config)

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

    logger.info("Computing histograms with dask...")
    (output,) = dask.compute(to_compute, scheduler='threads')

    # Save output
    output_path = os.path.join(args.outputDir, args.outputFileName)
    os.makedirs(args.outputDir, exist_ok=True)
    save(output, output_path)
    logger.info(f"Saved output to {output_path}")


if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()
