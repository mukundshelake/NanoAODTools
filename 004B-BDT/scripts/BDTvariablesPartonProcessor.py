#!/usr/bin/env python3
"""
BDTvariablesPartonProcessor.py - Process BDT variables for parton-classification analysis

This script reads ROOT files with BDT variable branches and GenPart information
to classify ttbar_SemiLeptonic MC events into three categories based on initial partons:
- qq (quark-quark): PDG_ID[0] + PDG_ID[1] == 0
- gg (gluon-gluon): PDG_ID[0] + PDG_ID[1] == 42
- qg (quark-gluon): PDG_ID[0] + PDG_ID[1] != 0 and != 42

Creates separate histograms for each category (3×17=51 histograms total).

Usage:
    python scripts/BDTvariablesPartonProcessor.py -e UL2017 -t midNov
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


class BDTvariablesPartonProcessor(processor.ProcessorABC):
    def __init__(self):
        pass

    def process(self, events):
        dataset = events.metadata['dataset']
        logger.info(f"Processing dataset: {dataset}")

        # Initialize histograms for 17 BDT variables × 3 parton categories = 51 histograms
        logger.debug("Initializing BDT variable histograms for parton categories...")
        histograms = {}
        
        # Define categories
        categories = ["qq", "gg", "qg"]
        
        # Create histograms for each category
        for cat in categories:
            histograms.update({
                # Jet kinematic variables
                f"{cat}_nJet": hist.Hist.new.Reg(15, 0.0, 15.0, name=f"{cat}_nJet", label="Number of Jets").Double(),
                f"{cat}_JetHT": hist.Hist.new.Reg(50, 0.0, 1200.0, name=f"{cat}_JetHT", label="Jet H_{T} [GeV]").Double(),
                f"{cat}_pTSum": hist.Hist.new.Reg(50, 0.0, 1300.0, name=f"{cat}_pTSum", label="p_{T} Sum [GeV]").Double(),
                
                # Fox-Wolfram moments
                f"{cat}_FW1": hist.Hist.new.Reg(50, -0.1, 1.0, name=f"{cat}_FW1", label="Fox-Wolfram H_{1}").Double(),
                f"{cat}_FW2": hist.Hist.new.Reg(50, 0.0, 1.0, name=f"{cat}_FW2", label="Fox-Wolfram H_{2}").Double(),
                f"{cat}_FW3": hist.Hist.new.Reg(50, 0.0, 1.0, name=f"{cat}_FW3", label="Fox-Wolfram H_{3}").Double(),
                
                # Longitudinal alignment
                f"{cat}_AL": hist.Hist.new.Reg(50, -1.0, 1.0, name=f"{cat}_AL", label="Aplanarity L").Double(),
                
                # Sphericity tensor components
                f"{cat}_Sxx": hist.Hist.new.Reg(50, 0.0, 1.0, name=f"{cat}_Sxx", label="S_{xx}").Double(),
                f"{cat}_Syy": hist.Hist.new.Reg(50, 0.0, 1.0, name=f"{cat}_Syy", label="S_{yy}").Double(),
                f"{cat}_Sxy": hist.Hist.new.Reg(50, -0.5, 0.5, name=f"{cat}_Sxy", label="S_{xy}").Double(),
                f"{cat}_Sxz": hist.Hist.new.Reg(50, -0.5, 0.5, name=f"{cat}_Sxz", label="S_{xz}").Double(),
                f"{cat}_Syz": hist.Hist.new.Reg(50, -0.5, 0.5, name=f"{cat}_Syz", label="S_{yz}").Double(),
                f"{cat}_Szz": hist.Hist.new.Reg(50, 0.0, 1.0, name=f"{cat}_Szz", label="S_{zz}").Double(),
                
                # Event shape variables
                f"{cat}_S": hist.Hist.new.Reg(50, 0.0, 1.0, name=f"{cat}_S", label="Sphericity").Double(),
                f"{cat}_P": hist.Hist.new.Reg(50, 0.0, 0.5, name=f"{cat}_P", label="Planarity").Double(),
                f"{cat}_A": hist.Hist.new.Reg(50, 0.0, 0.3, name=f"{cat}_A", label="Aplanarity").Double(),
                
                # Momentum flow variables
                f"{cat}_p2in": hist.Hist.new.Reg(50, 0.0, 0.1, name=f"{cat}_p2in", label="p^{2}_{in}").Double(),
                f"{cat}_p2out": hist.Hist.new.Reg(50, 0.0, 0.05, name=f"{cat}_p2out", label="p^{2}_{out}").Double(),
            })

        # Extract BDT variables
        logger.debug("Extracting BDT variable branches...")
        try:
            nJet = events.nJet
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
            nJet = events["nJet"]
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

        # Extract GenPart PDG IDs for parton classification
        logger.debug("Extracting GenPart PDG IDs for parton classification...")
        try:
            GenPart_pdgId = events.GenPart_pdgId
        except AttributeError:
            GenPart_pdgId = events["GenPart_pdgId"]
        
        # Calculate parton sum: PDG_ID[0] + PDG_ID[1]
        # qq: sum = 0, gg: sum = 42, qg: sum != 0 and != 42
        parton_sum = GenPart_pdgId[:, 0] + GenPart_pdgId[:, 1]
        
        # Create masks for each category
        qq_mask = (parton_sum == 0)
        gg_mask = (parton_sum == 42)
        qg_mask = (parton_sum != 0) & (parton_sum != 42)
        
        # logger.info(f"Event classification: qq={ak.sum(qq_mask).compute()}, gg={ak.sum(gg_mask).compute()}, qg={ak.sum(qg_mask).compute()}")

        # Calculate total weights
        logger.debug("Calculating event weights...")
        total_weight = ak.ones_like(JetHT, dtype=np.float32)
        
        # Multiply by available weights (MC-only analysis)
        # Check fields to avoid dask lazy evaluation issues
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

        # Fill histograms for each category
        logger.debug("Filling histograms...")
        
        # qq category
        histograms["qq_nJet"].fill(qq_nJet=nJet[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_JetHT"].fill(qq_JetHT=JetHT[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_pTSum"].fill(qq_pTSum=pTSum[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_FW1"].fill(qq_FW1=FW1[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_FW2"].fill(qq_FW2=FW2[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_FW3"].fill(qq_FW3=FW3[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_AL"].fill(qq_AL=AL[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_Sxx"].fill(qq_Sxx=Sxx[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_Syy"].fill(qq_Syy=Syy[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_Sxy"].fill(qq_Sxy=Sxy[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_Sxz"].fill(qq_Sxz=Sxz[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_Syz"].fill(qq_Syz=Syz[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_Szz"].fill(qq_Szz=Szz[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_S"].fill(qq_S=S[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_P"].fill(qq_P=P[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_A"].fill(qq_A=A[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_p2in"].fill(qq_p2in=p2in[qq_mask].compute(), weight=total_weight[qq_mask].compute())
        histograms["qq_p2out"].fill(qq_p2out=p2out[qq_mask].compute(), weight=total_weight[qq_mask].compute())

        # gg category
        histograms["gg_nJet"].fill(gg_nJet=nJet[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_JetHT"].fill(gg_JetHT=JetHT[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_pTSum"].fill(gg_pTSum=pTSum[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_FW1"].fill(gg_FW1=FW1[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_FW2"].fill(gg_FW2=FW2[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_FW3"].fill(gg_FW3=FW3[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_AL"].fill(gg_AL=AL[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_Sxx"].fill(gg_Sxx=Sxx[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_Syy"].fill(gg_Syy=Syy[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_Sxy"].fill(gg_Sxy=Sxy[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_Sxz"].fill(gg_Sxz=Sxz[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_Syz"].fill(gg_Syz=Syz[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_Szz"].fill(gg_Szz=Szz[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_S"].fill(gg_S=S[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_P"].fill(gg_P=P[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_A"].fill(gg_A=A[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_p2in"].fill(gg_p2in=p2in[gg_mask].compute(), weight=total_weight[gg_mask].compute())
        histograms["gg_p2out"].fill(gg_p2out=p2out[gg_mask].compute(), weight=total_weight[gg_mask].compute())

        # qg category
        histograms["qg_nJet"].fill(qg_nJet=nJet[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_JetHT"].fill(qg_JetHT=JetHT[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_pTSum"].fill(qg_pTSum=pTSum[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_FW1"].fill(qg_FW1=FW1[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_FW2"].fill(qg_FW2=FW2[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_FW3"].fill(qg_FW3=FW3[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_AL"].fill(qg_AL=AL[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_Sxx"].fill(qg_Sxx=Sxx[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_Syy"].fill(qg_Syy=Syy[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_Sxy"].fill(qg_Sxy=Sxy[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_Sxz"].fill(qg_Sxz=Sxz[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_Syz"].fill(qg_Syz=Syz[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_Szz"].fill(qg_Szz=Szz[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_S"].fill(qg_S=S[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_P"].fill(qg_P=P[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_A"].fill(qg_A=A[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_p2in"].fill(qg_p2in=p2in[qg_mask].compute(), weight=total_weight[qg_mask].compute())
        histograms["qg_p2out"].fill(qg_p2out=p2out[qg_mask].compute(), weight=total_weight[qg_mask].compute())

        return {
            dataset: histograms
        }

    def postprocess(self, accumulator):
        pass


def main(era, tag):
    """
    Main execution function for BDT variables parton classification analysis.
    
    Args:
        era: Data-taking era (e.g., 'UL2016preVFP', 'UL2017')
        tag: Analysis tag (e.g., 'midNov')
    """
    # Path to dataFiles JSON (only ttbar_SemiLeptonic MC)
    data_files_path = f"data/Datasets/{tag}_bdtvariables_parton_{era}_dataFiles.json"
    
    if not os.path.exists(data_files_path):
        logger.error(f"Data files not found: {data_files_path}")
        return
    
    logger.info(f"Loading data files from: {data_files_path}")
    with open(data_files_path, 'r') as f:
        dataFiles = json.load(f)
    
    # Convert to Coffea fileset format
    fileset = {}
    for channel, samples in dataFiles.items():
        for sample, file_dict in samples.items():
            dataset_name = f"{channel}_{sample}"
            fileset[dataset_name] = {"files": file_dict}
    
    logger.info(f"Loaded {len(fileset)} dataset(s)")
    
    # Remove empty files
    logger.info("Cleaning fileset (removing empty files)...")
    fileset = remove_empty_files(fileset)
    
    if not fileset:
        logger.error("No valid files found after cleaning. Exiting.")
        return
    
    # Preprocess fileset
    logger.info("Preprocessing fileset...")
    fileset_available, fileset_updated = preprocess(
        fileset,
        step_size=100000,
        align_clusters=False,
        skip_bad_files=True,
        save_form=False,
    )
    
    # Initialize processor
    logger.info("Initializing BDTvariablesPartonProcessor...")
    bdtvar_processor = BDTvariablesPartonProcessor()
    
    # Run processing with Dask
    logger.info("Running Coffea processor with Dask...")
    to_compute = apply_to_fileset(
        data_manipulation=bdtvar_processor,
        fileset=fileset_updated,
        schemaclass=BaseSchema,
    )
    
    (output_hists,) = dask.compute(to_compute)
    
    # Save output
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/{tag}_{era}_bdtvariables_parton.coffea"
    
    logger.info(f"Saving histograms to: {output_file}")
    save(output_hists, output_file)
    logger.info("✓ Processing complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process BDT variables for parton-classification analysis"
    )
    parser.add_argument("-e", "--era", required=True, help="Era (e.g., UL2016preVFP, UL2017)")
    parser.add_argument("-t", "--tag", required=True, help="Analysis tag (e.g., midNov)")
    args = parser.parse_args()
    
    main(args.era, args.tag)
