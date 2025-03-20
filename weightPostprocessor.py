#!/usr/bin/env python3
import sys
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from python.postprocessing.examples.lheWeightSignModule import lheWeightSignModule
from python.postprocessing.examples.MuonIDWeightProducer import muonIDWeightModule
from python.postprocessing.examples.MuonHLTWeightProducer import muonHLTWeightModule
from python.postprocessing.examples.bTaggingWeights import bTaggingWeightModule

# Define input and output files
input_files = ["/mnt/disk1/skimmed_Run2/UL2016preVFP/MC_mu/ttbar_FullyLeptonic/tree_13_Skim.root"]  # Your input NanoAOD file
output_dir = "outputs/"  # Where the processed file will be saved

# NanoAODTools PostProcessor
p = PostProcessor(
    output_dir, 
    input_files, 
    cut=None, 
    branchsel=None,  # Process all branches
    modules=[lheWeightSignModule(), muonIDWeightModule("UL2016preVFP"), muonHLTWeightModule("UL2016preVFP"), bTaggingWeightModule("UL2016preVFP", "ttbar_SemiLeptonic")],  # Add modules
    noOut=False, 
    haddFileName="skimmed_with_LHEWeightSign.root"  # Output file
)

# Run the processing
p.run()

print("✅ Successfully added LHEWeightSign!")
