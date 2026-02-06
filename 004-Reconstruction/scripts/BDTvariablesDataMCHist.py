#!/usr/bin/env python3
"""
BDTvariablesDataMCHist.py - Process BDT variables ROOT files to create Data/MC histograms

This script reads ROOT files with BDT variable branches (from BDTvariableModule.py)
and creates histograms for the 17 event-shape and kinematic variables using Coffea framework.

Usage:
    python scripts/BDTvariablesDataMCHist.py -e UL2017 -t midNov
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


class BDTvariablesProcessor(processor.ProcessorABC):
    def __init__(self):
        pass

    def process(self, events):
        dataset = events.metadata['dataset']
        logger.info(f"Processing dataset: {dataset}")

        # Initialize histograms for 17 BDT variable branches
        logger.debug("Initializing BDT variable histograms...")
        histograms = {
            # Jet kinematic variables
            "JetHT": hist.Hist.new.Reg(50, 0.0, 1000.0, name="JetHT", label="Jet H_{T} [GeV]").Double(),
            "pTSum": hist.Hist.new.Reg(50, 0.0, 1200.0, name="pTSum", label="p_{T} Sum [GeV]").Double(),
            
            # Fox-Wolfram moments
            "FW1": hist.Hist.new.Reg(50, -1.0, 1.0, name="FW1", label="Fox-Wolfram H_{1}").Double(),
            "FW2": hist.Hist.new.Reg(50, -1.0, 1.0, name="FW2", label="Fox-Wolfram H_{2}").Double(),
            "FW3": hist.Hist.new.Reg(50, -1.0, 1.0, name="FW3", label="Fox-Wolfram H_{3}").Double(),
            
            # Longitudinal alignment
            "AL": hist.Hist.new.Reg(50, -1.0, 1.0, name="AL", label="Alignment A_{L}").Double(),
            
            # Sphericity tensor elements
            "Sxx": hist.Hist.new.Reg(50, 0.0, 1.0, name="Sxx", label="S_{xx}").Double(),
            "Syy": hist.Hist.new.Reg(50, 0.0, 1.0, name="Syy", label="S_{yy}").Double(),
            "Sxy": hist.Hist.new.Reg(50, -0.5, 0.5, name="Sxy", label="S_{xy}").Double(),
            "Sxz": hist.Hist.new.Reg(50, -0.5, 0.5, name="Sxz", label="S_{xz}").Double(),
            "Syz": hist.Hist.new.Reg(50, -0.5, 0.5, name="Syz", label="S_{yz}").Double(),
            "Szz": hist.Hist.new.Reg(50, 0.0, 1.0, name="Szz", label="S_{zz}").Double(),
            
            # Event shape variables
            "S": hist.Hist.new.Reg(50, 0.0, 1.0, name="S", label="Sphericity").Double(),
            "P": hist.Hist.new.Reg(50, 0.0, 1.0, name="P", label="Planarity").Double(),
            "A": hist.Hist.new.Reg(50, 0.0, 1.0, name="A", label="Aplanarity").Double(),
            "p2in": hist.Hist.new.Reg(50, 0.0, 1.0, name="p2in", label="p_{2}^{in}").Double(),
            "p2out": hist.Hist.new.Reg(50, 0.0, 1.0, name="p2out", label="p_{2}^{out}").Double(),
        }

        # Access BDT variable branches directly (flat branches from ROOT file)
        # BaseSchema should provide direct attribute access
        try:
            JetHT = events.JetHT
            pTSum = events.pTSum
            FW1 = events.FW1
            FW2 = events.FW2
            FW3 = events.FW3
            AL = events.AL
            Sxx = events.Sxx
            Syy = events.Syy
            Sxy = events.Sxy
            Sxz = events.Sxz
            Syz = events.Syz
            Szz = events.Szz
            S = events.S
            P = events.P
            A = events.A
            p2in = events.p2in
            p2out = events.p2out
        except AttributeError as e:
            logger.error(f"Error accessing BDT variable branches: {e}")
            logger.info("Attempting dictionary-style access...")
            # Fallback to dictionary-style access if attribute access fails
            JetHT = events["JetHT"]
            pTSum = events["pTSum"]
            FW1 = events["FW1"]
            FW2 = events["FW2"]
            FW3 = events["FW3"]
            AL = events["AL"]
            Sxx = events["Sxx"]
            Syy = events["Syy"]
            Sxy = events["Sxy"]
            Sxz = events["Sxz"]
            Syz = events["Syz"]
            Szz = events["Szz"]
            S = events["S"]
            P = events["P"]
            A = events["A"]
            p2in = events["p2in"]
            p2out = events["p2out"]

        # No chi2_status filter needed for BDT variables (event-level information)
        
        # --- Calculate Total Weights ---
        # Initialize with ones
        total_weight = ak.ones_like(JetHT, dtype=np.float32)
        
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
        histograms["JetHT"].fill(JetHT=JetHT.compute(), weight=total_weight.compute())
        histograms["pTSum"].fill(pTSum=pTSum.compute(), weight=total_weight.compute())
        histograms["FW1"].fill(FW1=FW1.compute(), weight=total_weight.compute())
        histograms["FW2"].fill(FW2=FW2.compute(), weight=total_weight.compute())
        histograms["FW3"].fill(FW3=FW3.compute(), weight=total_weight.compute())
        histograms["AL"].fill(AL=AL.compute(), weight=total_weight.compute())
        histograms["Sxx"].fill(Sxx=Sxx.compute(), weight=total_weight.compute())
        histograms["Syy"].fill(Syy=Syy.compute(), weight=total_weight.compute())
        histograms["Sxy"].fill(Sxy=Sxy.compute(), weight=total_weight.compute())
        histograms["Sxz"].fill(Sxz=Sxz.compute(), weight=total_weight.compute())
        histograms["Syz"].fill(Syz=Syz.compute(), weight=total_weight.compute())
        histograms["Szz"].fill(Szz=Szz.compute(), weight=total_weight.compute())
        histograms["S"].fill(S=S.compute(), weight=total_weight.compute())
        histograms["P"].fill(P=P.compute(), weight=total_weight.compute())
        histograms["A"].fill(A=A.compute(), weight=total_weight.compute())
        histograms["p2in"].fill(p2in=p2in.compute(), weight=total_weight.compute())
        histograms["p2out"].fill(p2out=p2out.compute(), weight=total_weight.compute())

        return {
            "entries": ak.num(events, axis=0),
            "histos": histograms
        }

    def postprocess(self, accumulator):
        return accumulator


def main():
    parser = argparse.ArgumentParser(
        description="Process BDT variables ROOT files to create histograms",
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
    datafiles_path = data_dir / f"{args.tag}_{args.era}_bdtvariables_dataFiles.json"
    
    if not datafiles_path.exists():
        logger.error(f"DataFiles not found: {datafiles_path}")
        logger.error("Please ensure BDT variables ROOT files are indexed in data/Datasets/")
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
    logger.info("Running BDTvariablesProcessor...")
    to_compute = apply_to_fileset(
        BDTvariablesProcessor(),
        max_chunks(dataset_runnable, 300),
        schemaclass=BaseSchema,  # Use BaseSchema for flat ROOT trees
    )

    (out,) = dask.compute(to_compute, scheduler='threads')

    # Save output .coffea file
    outputFile = f"{args.tag}_{args.era}_bdtvariables.coffea"
    output_path = output_dir / outputFile
    save(out, str(output_path))
    logger.info(f"✓ Saved output to {output_path}")
    logger.info(f"  Datasets processed: {len(out)}")
    logger.info(f"  Histograms per dataset: {len(out[list(out.keys())[0]]['histos']) if out else 0}")


if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()
