#!/usr/bin/env python3
import os, json
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from python.postprocessing.examples.lheWeightSignModule import lheWeightSignModule
from python.postprocessing.examples.MuonIDWeightProducer import muonIDWeightModule
from python.postprocessing.examples.MuonHLTWeightProducer import muonHLTWeightModule
from python.postprocessing.examples.bTaggingWeights import bTaggingWeightModule
from multiprocessing import Pool

def process_dataset(data):
    """Function to process a single dataset."""
    DataMC, key, files, outDir, cut_string = data

    print(f"Processing {key} in {DataMC}")

    if not os.path.exists(outDir):
        os.makedirs(outDir)

    # Determine if the dataset is Data or MC
    if "Data" in DataMC:
        # For Data, apply the JSON file for luminosity masking
        json_input = "python/postprocessing/examples/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.json"
        modules = []  # Add any Data-specific modules if needed
    else:
        # For MC, do not apply the JSON file
        json_input = None
        modules = [
            lheWeightSignModule(),
            # muonIDWeightModule("UL2016preVFP"),
            # muonHLTWeightModule("UL2016preVFP"),
            # bTaggingWeightModule("UL2016preVFP", "ttbar_SemiLeptonic"),
        ]  # Add MC-specific modules

    # Set up the PostProcessor
    post_processor = PostProcessor(
        outDir,
        files,
        jsonInput=json_input,
        cut=cut_string,
        modules=modules,
        noOut=False,
        justcount=False,
    )

    # Run the PostProcessor
    post_processor.run()
    print(f"Finished processing {key} in {DataMC}")

if __name__ == "__main__":
    import sys

    # Load the dataset configuration
    with open('/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/Datasets/skimmed_sampleFiles_UL2016preVFP.json', 'r') as json_file:
        dicti = json.load(json_file)

    cut_string = (
        "Sum$(Muon_pt > 26 && abs(Muon_eta) < 2.4 && Muon_tightId) > 0 && "
        "Sum$(Jet_pt > 25 && abs(Jet_eta) < 2.4 && Jet_btagDeepFlavB > 0.2598) >= 2 && "
        "Sum$(Jet_pt > 25 && abs(Jet_eta) < 2.4) > 3 && "
        "(HLT_IsoMu24 || HLT_IsoTkMu24) && "
        "Flag_goodVertices && "
        "Flag_globalSuperTightHalo2016Filter && "
        "Flag_HBHENoiseFilter && "
        "Flag_HBHENoiseIsoFilter && "
        "Flag_EcalDeadCellTriggerPrimitiveFilter && "
        "Flag_BadPFMuonFilter && "
        "Flag_BadPFMuonDzFilter && "
        "Flag_eeBadScFilter"
    )

    print("Cut string being applied:", cut_string)
    # exit(0)
    # Prepare datasets for parallel processing
    dataset_list = []
    for DataMC in dicti:
        if "Data" not in DataMC:
            for key in dicti[DataMC]:
                # if 'QCD_Pt-30To50' not in key:
                #     continue
                outDir = f'/mnt/disk1/skimmed_Run2/selection/March312025/UL2016preVFP/{DataMC}/{key}'
                input_files = dicti[DataMC][key]
                dataset_list.append((DataMC, key, input_files, outDir, cut_string))

    # Use multiprocessing to process datasets in parallel
    num_cores = 20
    with Pool(num_cores) as pool:
        pool.map(process_dataset, dataset_list)
    # print(dataset_list)