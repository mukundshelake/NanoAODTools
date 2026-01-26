#!/usr/bin/env python3
"""
RecoDataMCHist.py - Process reconstructed ttbar ROOT files to create Data/MC histograms

This script reads post-reconstruction ROOT files (with branches from RecoModule.py)
and creates histograms for the 13 reconstruction branches using Coffea framework.

Usage:
    python scripts/RecoDataMCHist.py -e UL2017 -t midNov [--sample] [--no-filter]
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


class RecoProcessor(processor.ProcessorABC):
    def __init__(self, apply_chi2_filter=True):
        self.apply_chi2_filter = apply_chi2_filter

    def process(self, events):
        dataset = events.metadata['dataset']
        logger.info(f"Processing dataset: {dataset}")

        # Initialize histograms for 13 reconstruction branches
        logger.debug("Initializing reconstruction histograms...")
        histograms = {
            # Leptonic top 4-vector
            "Top_lep_pt": hist.Hist.new.Reg(50, 0.0, 500.0, name="pt", label="Leptonic Top $p_T$ [GeV]").Double(),
            "Top_lep_eta": hist.Hist.new.Reg(50, -2.5, 2.5, name="eta", label="Leptonic Top $\\eta$").Double(),
            "Top_lep_phi": hist.Hist.new.Reg(64, -3.2, 3.2, name="phi", label="Leptonic Top $\\phi$").Double(),
            "Top_lep_mass": hist.Hist.new.Reg(50, 100.0, 250.0, name="mass", label="Leptonic Top Mass [GeV]").Double(),
            
            # Hadronic top 4-vector
            "Top_had_pt": hist.Hist.new.Reg(50, 0.0, 500.0, name="pt", label="Hadronic Top $p_T$ [GeV]").Double(),
            "Top_had_eta": hist.Hist.new.Reg(50, -2.5, 2.5, name="eta", label="Hadronic Top $\\eta$").Double(),
            "Top_had_phi": hist.Hist.new.Reg(64, -3.2, 3.2, name="phi", label="Hadronic Top $\\phi$").Double(),
            "Top_had_mass": hist.Hist.new.Reg(50, 100.0, 250.0, name="mass", label="Hadronic Top Mass [GeV]").Double(),
            
            # Fit quality variables
            "Chi2_prefit": hist.Hist.new.Reg(50, 0.0, 50.0, name="chi2", label="Pre-fit $\\chi^2$").Double(),
            "Chi2": hist.Hist.new.Reg(50, 0.0, 50.0, name="chi2", label="Fitted $\\chi^2$").Double(),
            "Pgof": hist.Hist.new.Reg(50, 0.0, 1.0, name="pgof", label="P($\\chi^2$)").Double(),
            "chi2_status": hist.Hist.new.Reg(5, 0, 5, name="status", label="Reconstruction Status").Double(),
        }

        # Access reconstruction branches directly (flat branches from ROOT file)
        # BaseSchema should provide direct attribute access
        try:
            top_lep_pt = events.Top_lep_pt
            top_lep_eta = events.Top_lep_eta
            top_lep_phi = events.Top_lep_phi
            top_lep_mass = events.Top_lep_mass
            
            top_had_pt = events.Top_had_pt
            top_had_eta = events.Top_had_eta
            top_had_phi = events.Top_had_phi
            top_had_mass = events.Top_had_mass
            
            chi2_prefit = events.Chi2_prefit
            chi2 = events.Chi2
            pgof = events.Pgof
            chi2_status = events.chi2_status
        except AttributeError as e:
            logger.error(f"Error accessing reconstruction branches: {e}")
            logger.info("Attempting dictionary-style access...")
            # Fallback to dictionary-style access if attribute access fails
            top_lep_pt = events["Top_lep_pt"]
            top_lep_eta = events["Top_lep_eta"]
            top_lep_phi = events["Top_lep_phi"]
            top_lep_mass = events["Top_lep_mass"]
            
            top_had_pt = events["Top_had_pt"]
            top_had_eta = events["Top_had_eta"]
            top_had_phi = events["Top_had_phi"]
            top_had_mass = events["Top_had_mass"]
            
            chi2_prefit = events["Chi2_prefit"]
            chi2 = events["Chi2"]
            pgof = events["Pgof"]
            chi2_status = events["chi2_status"]

        # Apply chi2_status filter if requested (only successful reconstructions)
        if self.apply_chi2_filter:
            good_reco = chi2_status == 0
            n_before = len(chi2_status)
            n_after = ak.sum(good_reco)
            logger.debug(f"Chi2 filter: {n_after}/{n_before} events pass (chi2_status==0)")
            
            # Apply filter to all variables
            top_lep_pt = top_lep_pt[good_reco]
            top_lep_eta = top_lep_eta[good_reco]
            top_lep_phi = top_lep_phi[good_reco]
            top_lep_mass = top_lep_mass[good_reco]
            
            top_had_pt = top_had_pt[good_reco]
            top_had_eta = top_had_eta[good_reco]
            top_had_phi = top_had_phi[good_reco]
            top_had_mass = top_had_mass[good_reco]
            
            chi2_prefit = chi2_prefit[good_reco]
            chi2 = chi2[good_reco]
            pgof = pgof[good_reco]
            chi2_status = chi2_status[good_reco]
            
            # Filter events object for weight calculation
            events = events[good_reco]

        # --- Calculate Total Weights ---
        # Initialize with ones (appropriate length after filtering)
        total_weight = ak.ones_like(chi2_status, dtype=np.float32)
        
        # Multiply by available weights (check with hasattr for MC vs Data)
        if hasattr(events, "MuonHLTWeight"):
            total_weight = total_weight * events.MuonHLTWeight
        if hasattr(events, "MuonIDWeight"):
            total_weight = total_weight * events.MuonIDWeight
        if hasattr(events, "LHEWeightSign"):
            total_weight = total_weight * events.LHEWeightSign
        if hasattr(events, "bTaggingWeight"):
            total_weight = total_weight * events.bTaggingWeight
        if hasattr(events, "L1PreFiringWeight_Nom"):
            total_weight = total_weight * events.L1PreFiringWeight_Nom
        if hasattr(events, "puWeight"):
            total_weight = total_weight * events.puWeight

        # Fill histograms (compute dask arrays)
        histograms["Top_lep_pt"].fill(pt=top_lep_pt.compute(), weight=total_weight.compute())
        histograms["Top_lep_eta"].fill(eta=top_lep_eta.compute(), weight=total_weight.compute())
        histograms["Top_lep_phi"].fill(phi=top_lep_phi.compute(), weight=total_weight.compute())
        histograms["Top_lep_mass"].fill(mass=top_lep_mass.compute(), weight=total_weight.compute())
        
        histograms["Top_had_pt"].fill(pt=top_had_pt.compute(), weight=total_weight.compute())
        histograms["Top_had_eta"].fill(eta=top_had_eta.compute(), weight=total_weight.compute())
        histograms["Top_had_phi"].fill(phi=top_had_phi.compute(), weight=total_weight.compute())
        histograms["Top_had_mass"].fill(mass=top_had_mass.compute(), weight=total_weight.compute())
        
        histograms["Chi2_prefit"].fill(chi2=chi2_prefit.compute(), weight=total_weight.compute())
        histograms["Chi2"].fill(chi2=chi2.compute(), weight=total_weight.compute())
        histograms["Pgof"].fill(pgof=pgof.compute(), weight=total_weight.compute())
        histograms["chi2_status"].fill(status=chi2_status.compute(), weight=total_weight.compute())

        return {
            "entries": ak.num(events, axis=0),
            "histos": histograms
        }

    def postprocess(self, accumulator):
        return accumulator


def main():
    parser = argparse.ArgumentParser(
        description="Process reconstructed ttbar ROOT files to create histograms",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    allowed_eras = ['UL2016preVFP', 'UL2016postVFP', 'UL2017', 'UL2018']
    parser.add_argument('-e', '--era', choices=allowed_eras, required=True, 
                        help='Era to process')
    parser.add_argument('-t', '--tag', type=str, required=True, 
                        help='Tag to identify input files (e.g., midNov)')
    parser.add_argument('--no-filter', action='store_true',
                        help='Disable chi2_status==0 filter (include failed reconstructions)')
    parser.add_argument('--config', type=str, default='../config.yaml',
                        help='Path to config.yaml file')
    args = parser.parse_args()

    logger.info(f"Selected era: {args.era}")
    logger.info(f"Output tag: {args.tag}")
    logger.info(f"Chi2 filter: {not args.no_filter}")

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
    datafiles_path = data_dir / f"{args.tag}_{args.era}_reco_dataFiles.json"
    
    if not datafiles_path.exists():
        logger.error(f"DataFiles not found: {datafiles_path}")
        logger.error("Please ensure reconstructed ROOT files are indexed in data/Datasets/")
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
    logger.info("Running RecoProcessor...")
    to_compute = apply_to_fileset(
        RecoProcessor(apply_chi2_filter=not args.no_filter),
        max_chunks(dataset_runnable, 300),
        schemaclass=BaseSchema,  # Use BaseSchema for flat ROOT trees
    )

    (out,) = dask.compute(to_compute, scheduler='threads')

    # Save output .coffea file
    outputFile = f"{args.tag}_{args.era}_reco.coffea"
    output_path = output_dir / outputFile
    save(out, str(output_path))
    logger.info(f"✓ Saved output to {output_path}")
    logger.info(f"  Datasets processed: {len(out)}")
    logger.info(f"  Histograms per dataset: {len(out[list(out.keys())[0]]['histos']) if out else 0}")


if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()
