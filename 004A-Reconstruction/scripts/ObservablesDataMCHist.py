#!/usr/bin/env python3
"""
ObservablesDataMCHist.py - Process ttbar observables ROOT files to create Data/MC histograms

This script reads post-reconstruction ROOT files with observable branches (from observables.py)
and creates histograms for the 7 physics observables using Coffea framework.

Usage:
    python scripts/ObservablesDataMCHist.py -e UL2017 -t midNov [--sample]
"""

import json, os, argparse
import hist
import numpy as np
import dask
import awkward as ak
import hist.dask as hda
import dask_awkward as dak
from coffea.nanoevents import BaseSchema
from coffea import processor

from coffea.dataset_tools import (
    apply_to_fileset,
    max_chunks,
    preprocess,
)
from coffea.util import save
import uproot
import logging
import yaml
from pathlib import Path

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

# Suppress lower-level logs from noisy modules
for noisy_module in ["uproot", "dask", "fsspec", "urllib3"]:
    logging.getLogger(noisy_module).setLevel(logging.WARNING)


def remove_empty_files(fileset):
    """Remove files with zero entries or errors from fileset"""
    cleaned_fileset = {}
    for dataset, content in fileset.items():
        files = content.get("files", {})
        valid_files = {}

        for filepath, treename in files.items():
            try:
                with uproot.open(f"{filepath}:{treename}") as tree:
                    if tree.num_entries > 0:
                        valid_files[filepath] = treename
                    else:
                        logger.warning(f"[Zero Entries] Skipping {filepath}")
            except Exception as e:
                logger.error(f"[Error] Skipping {filepath} due to: {e}")

        if valid_files:
            cleaned_fileset[dataset] = {"files": valid_files}
        else:
            logger.warning(f"[Warning] Skipping dataset '{dataset}' — all files empty or bad.")

    return cleaned_fileset


class ObservablesProcessor(processor.ProcessorABC):
    def __init__(self):
        pass

    def process(self, events):
        dataset = events.metadata['dataset']
        logger.info(f"Processing dataset: {dataset}")

        # Initialize histograms for 7 observable branches
        logger.debug("Initializing observable histograms...")
        histograms = {
            # Polar angles in ttbar rest frame
            "cosTheta": hist.Hist.new.Reg(50, -1.0, 1.0, name="cosTheta", label="cos#theta").Double(),
            "anticosTheta": hist.Hist.new.Reg(50, -1.0, 1.0, name="anticosTheta", label="cos#bar{#theta}").Double(),
            "LabcosTheta": hist.Hist.new.Reg(50, -1.0, 1.0, name="LabcosTheta", label="cos#theta_{lab}").Double(),
            
            # Rapidities
            "yt": hist.Hist.new.Reg(50, -2.5, 2.5, name="yt", label="y_{t}").Double(),
            "ytbar": hist.Hist.new.Reg(50, -2.5, 2.5, name="ytbar", label="y_{#bar{t}}").Double(),
            
            # ttbar system kinematics
            "ttbar_pz": hist.Hist.new.Reg(50, -500.0, 500.0, name="ttbar_pz", label="p_{z}^{t#bar{t}} [GeV]").Double(),
            "ttbar_mass": hist.Hist.new.Reg(50, 300.0, 2000.0, name="ttbar_mass", label="m_{t#bar{t}} [GeV]").Double(),
        }

        # Access observable branches directly (flat branches from ROOT file)
        # BaseSchema should provide direct attribute access
        try:
            cosTheta = events.cosTheta
            anticosTheta = events.anticosTheta
            LabcosTheta = events.LabcosTheta
            yt = events.yt
            ytbar = events.ytbar
            ttbar_pz = events.ttbar_pz
            ttbar_mass = events.ttbar_mass
        except AttributeError as e:
            logger.error(f"Error accessing observable branches: {e}")
            logger.info("Attempting dictionary-style access...")
            # Fallback to dictionary-style access if attribute access fails
            cosTheta = events["cosTheta"]
            anticosTheta = events["anticosTheta"]
            LabcosTheta = events["LabcosTheta"]
            yt = events["yt"]
            ytbar = events["ytbar"]
            ttbar_pz = events["ttbar_pz"]
            ttbar_mass = events["ttbar_mass"]

        # No chi2_status filter needed for observables (already computed from good reconstructions)
        
        # --- Calculate Total Weights ---
        # Initialize with ones
        total_weight = ak.ones_like(cosTheta, dtype=np.float32)
        
        # Multiply by available weights (check fields to avoid dask lazy evaluation issues)
        available_fields = events.fields
        if "MuonHLTWeight" in available_fields:
            total_weight = total_weight * events.MuonHLTWeight
        if "MuonIDWeight" in available_fields:
            total_weight = total_weight * events.MuonIDWeight
        if "LHEWeightSign" in available_fields:
            total_weight = total_weight * events.LHEWeightSign
        if "bTaggingWeight" in available_fields:
            total_weight = total_weight * events.bTaggingWeight
        if "L1PreFiringWeight_Nom" in available_fields:
            total_weight = total_weight * events.L1PreFiringWeight_Nom
        if "puWeight" in available_fields:
            total_weight = total_weight * events.puWeight

        # Fill histograms (compute dask arrays)
        histograms["cosTheta"].fill(cosTheta=cosTheta.compute(), weight=total_weight.compute())
        histograms["anticosTheta"].fill(anticosTheta=anticosTheta.compute(), weight=total_weight.compute())
        histograms["LabcosTheta"].fill(LabcosTheta=LabcosTheta.compute(), weight=total_weight.compute())
        histograms["yt"].fill(yt=yt.compute(), weight=total_weight.compute())
        histograms["ytbar"].fill(ytbar=ytbar.compute(), weight=total_weight.compute())
        histograms["ttbar_pz"].fill(ttbar_pz=ttbar_pz.compute(), weight=total_weight.compute())
        histograms["ttbar_mass"].fill(ttbar_mass=ttbar_mass.compute(), weight=total_weight.compute())

        return {
            "entries": ak.num(events, axis=0),
            "histos": histograms
        }

    def postprocess(self, accumulator):
        return accumulator


def main():
    parser = argparse.ArgumentParser(
        description="Process ttbar observables ROOT files to create histograms",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    allowed_eras = ['UL2016preVFP', 'UL2016postVFP', 'UL2017', 'UL2018']
    parser.add_argument('-e', '--era', choices=allowed_eras, required=True, 
                        help='Era to process')
    parser.add_argument('-t', '--tag', type=str, required=True, 
                        help='Tag to identify input files (e.g., midNov)')
    parser.add_argument('--config', type=str, default='../config.yaml',
                        help='Path to config.yaml file')
    args = parser.parse_args()

    logger.info(f"Selected era: {args.era}")
    logger.info(f"Output tag: {args.tag}")

    # Load configuration
    config_path = Path(args.config)
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
    else:
        logger.warning(f"Config file not found: {config_path}, using defaults")
        config = {}

    # Determine output directory
    script_dir = Path(__file__).parent
    output_dir = script_dir.parent / "outputs"
    output_dir.mkdir(exist_ok=True)

    # Build path to dataFiles.json
    data_dir = script_dir.parent / "data" / "Datasets"
    datafiles_path = data_dir / f"{args.tag}_{args.era}_observables_dataFiles.json"
    
    if not datafiles_path.exists():
        logger.error(f"DataFiles not found: {datafiles_path}")
        logger.error("Please ensure observables ROOT files are indexed in data/Datasets/")
        return

    logger.info(f"Reading dataFiles from: {datafiles_path}")

    # Load fileset
    fileset = {}
    with open(datafiles_path, 'r') as json_file:
        dicti = json.load(json_file)
        for pr in dicti.get('Data_mu', {}):
            datasetName = f'{args.era}_{pr}'
            fileset[datasetName] = {"files": dicti['Data_mu'][pr]}
        for pr in dicti.get('MC_mu', {}):
            datasetName = f'{args.era}_{pr}'
            fileset[datasetName] = {"files": dicti['MC_mu'][pr]}

    if not fileset:
        logger.error("No datasets found in dataFiles.json!")
        return

    logger.info(f"Loaded {len(fileset)} datasets")
    
    # Clean empty/bad files
    fileset = remove_empty_files(fileset)
    
    if not fileset:
        logger.error("No valid files remaining after cleaning!")
        return

    logger.info("Preprocessing fileset...")
    dataset_runnable, dataset_updated = preprocess(
        fileset,
        align_clusters=False,
        files_per_batch=1,
        skip_bad_files=True,
        save_form=False,
    )

    # Run processor with BaseSchema
    logger.info("Running ObservablesProcessor...")
    to_compute = apply_to_fileset(
        ObservablesProcessor(),
        max_chunks(dataset_runnable, 300),
        schemaclass=BaseSchema,  # Use BaseSchema for flat ROOT trees
    )

    (out,) = dask.compute(to_compute, scheduler='threads')

    # Save output .coffea file
    outputFile = f"{args.tag}_{args.era}_observables.coffea"
    output_path = output_dir / outputFile
    save(out, str(output_path))
    logger.info(f"✓ Saved output to {output_path}")
    logger.info(f"  Datasets processed: {len(out)}")
    logger.info(f"  Histograms per dataset: {len(out[list(out.keys())[0]]['histos']) if out else 0}")


if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()
