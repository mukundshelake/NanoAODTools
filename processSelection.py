#!/usr/bin/env python3
import os, json, argparse, logging
try:
    import ROOT
    HAS_ROOT = True
except ImportError:
    HAS_ROOT = False
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from python.postprocessing.examples.lheWeightSignModule import lheWeightSignModule
from python.postprocessing.examples.MuonIDWeightProducer import muonIDWeightModule
from python.postprocessing.examples.MuonHLTWeightProducer import muonHLTWeightModule
from python.postprocessing.examples.bTaggingWeights import bTaggingWeightModule
from python.postprocessing.examples.JetPUIdWeightModule import jetPUIdWeightModule
from multiprocessing import Pool

def process_dataset(data):
    """Function to process a single dataset."""
    DataMC, key, files, outDir, cut_string, era = data

    logging.info(f"Processing {key} in {DataMC}")

    if not os.path.exists(outDir):
        os.makedirs(outDir)
    else:
        # Check for existing output files
        existing_files = [f for f in os.listdir(outDir) if f.endswith('.root')]
        if existing_files:
            # Get input file names without paths
            input_basenames = [os.path.basename(f) for f in files]
            # Find which input files don't have good output
            files_to_process = []
            for f in files:
                if "tree_1" not in f:
                    continue
                output_name = os.path.basename(f).replace('.root', '_Skim.root')
                output_path = os.path.join(outDir, output_name)
                
                # Check if output exists and is healthy
                if os.path.exists(output_path):
                    # continue
                    try:
                        # Basic health checks
                        if os.path.getsize(output_path) == 0:
                            logging.warning(f"Empty output file found: {output_path}")
                            files_to_process.append(f)
                            continue
                            
                        # Try to open the file to verify it's valid
                        import ROOT
                        root_file = ROOT.TFile(output_path)
                        if not root_file or root_file.IsZombie():
                            logging.warning(f"Corrupted ROOT file found: {output_path}")
                            files_to_process.append(f)
                            root_file.Close()
                            continue
                        root_file.Close()
                    except Exception as e:
                        logging.warning(f"Error checking {output_path}: {str(e)}")
                        files_to_process.append(f)
                        continue
                else:
                    files_to_process.append(f)
            
            files = files_to_process
            if not files:
                logging.info(f"All files already processed for {key} in {DataMC}")
                return
            logging.info(f"Processing {len(files)} remaining files out of {len(input_basenames)} for {key} in {DataMC}")

    # Define JSON files for each era
    json_files = {
        "UL2016preVFP": "python/postprocessing/examples/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.json",
        "UL2016postVFP": "python/postprocessing/examples/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.json",
        "UL2017": "python/postprocessing/examples/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.json",
        "UL2018": "python/postprocessing/examples/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.json"
    }

    # Determine if the dataset is Data or MC
    if "Data" in DataMC:
        # For Data, apply the JSON file for luminosity masking
        json_input = json_files[era]
        modules = []  # Add any Data-specific modules if needed
    else:
        # For MC, do not apply the JSON file
        json_input = None
        modules = [
            lheWeightSignModule(), # Era independent
            muonIDWeightModule(era), # Use era argument
            muonHLTWeightModule(era), # Use era argument
            bTaggingWeightModule(era, key), # Use era argument
            jetPUIdWeightModule(era, key) # Use era argument
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
    logging.info(f"Finished processing {key} in {DataMC}")

if __name__ == "__main__":
    import sys

    # --- Configure Logging ---
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info("Starting selection processing script.")

    # --- Argument Parser ---
    parser = argparse.ArgumentParser(description="Process NanoAOD files with specified era and output tag.")
    parser.add_argument('--era', required=True, help='Analysis era (e.g., UL2016preVFP, UL2016postVFP)')
    parser.add_argument('--outputTag', required=True, help='Tag for the output directory (e.g., April152025)')
    args = parser.parse_args()
    era = args.era
    outputTag = args.outputTag
    logging.info(f"Using era: {era}")
    logging.info(f"Using output tag: {outputTag}")

    # --- Load the dataset configuration ---
    json_file_path = f'/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/Datasets/skimmed_dataFiles_{era}.json'
    logging.info(f"Loading dataset configuration from: {json_file_path}")
    try:
        with open(json_file_path, 'r') as json_file:
            dicti = json.load(json_file)
    except FileNotFoundError:
        logging.error(f"JSON file not found at {json_file_path}")
        sys.exit(1)

    # --- Define cut strings based on era ---
    # Using b-tag value for UL2016preVFP: 0.2598
    # Using b-tag value for UL2016postVFP: 0.2489
    # Using b-tag value for UL2017: 
    # Using b-tag value for UL2018: 
    common_flags = (
        "Flag_goodVertices && "
        "Flag_globalSuperTightHalo2016Filter && "
        "Flag_HBHENoiseFilter && "
        "Flag_HBHENoiseIsoFilter && "
        "Flag_EcalDeadCellTriggerPrimitiveFilter && "
        "Flag_BadPFMuonFilter && "
        "Flag_BadPFMuonDzFilter && "
        "Flag_eeBadScFilter" # Common flags for all eras
    )
    
    cut_strings = {
        "UL2016preVFP": (
            "Sum$(Muon_pt > 26 && abs(Muon_eta) < 2.4 && Muon_tightId && Muon_pfRelIso04_all <=0.06) > 0 && "
            "Sum$(Jet_pt > 25 && abs(Jet_eta) < 2.4 && Jet_btagDeepFlavB > 0.2598) >= 2 && "
            "Sum$(Jet_pt > 25 && abs(Jet_eta) < 2.4) > 3 && "
            "(HLT_IsoMu24 || HLT_IsoTkMu24) && " + common_flags
        ),
        "UL2016postVFP": (
            "Sum$(Muon_pt > 26 && abs(Muon_eta) < 2.4 && Muon_tightId && Muon_pfRelIso04_all <=0.06) > 0 && "
            "Sum$(Jet_pt > 25 && abs(Jet_eta) < 2.4 && Jet_btagDeepFlavB > 0.2489) >= 2 && "
            "Sum$(Jet_pt > 25 && abs(Jet_eta) < 2.4) > 3 && "
            "(HLT_IsoMu24 || HLT_IsoTkMu24) && " + common_flags
        ),
         "UL2017": (
            "Sum$(Muon_pt > 29 && abs(Muon_eta) < 2.4 && Muon_tightId && Muon_pfRelIso04_all <=0.06) > 0 && " # Muon pT > 29 for 2017/18
            "Sum$(Jet_pt > 25 && abs(Jet_eta) < 2.4 && Jet_btagDeepFlavB > 0.2589) >= 2 && "
            "Sum$(Jet_pt > 25 && abs(Jet_eta) < 2.4) > 3 && "
            "(HLT_IsoMu27) && Flag_ecalBadCalibFilter && " + common_flags # HLT_IsoMu27 for 2017/18, added Flag_ecalBadCalibFilter
        ),
         "UL2018": (
            "Sum$(Muon_pt > 29 && abs(Muon_eta) < 2.4 && Muon_tightId && Muon_pfRelIso04_all <=0.06) > 0 && " # Muon pT > 29 for 2017/18
            "Sum$(Jet_pt > 25 && abs(Jet_eta) < 2.4 && Jet_btagDeepFlavB > 0.2432) >= 2 && "
            "Sum$(Jet_pt > 25 && abs(Jet_eta) < 2.4) > 3 && "
            "(HLT_IsoMu24) && Flag_ecalBadCalibFilter && " + common_flags # HLT_IsoMu24 for 2018, added Flag_ecalBadCalibFilter
        ),
    }

    if era not in cut_strings:
        logging.error(f"Cut string not defined for era '{era}'. Please add it to the script.")
        sys.exit(1)

    cut_string = cut_strings[era]
    logging.info(f"Cut string being applied for {era}: {cut_string}")
    # exit(0)
    # Prepare datasets for parallel processing
    dataset_list = []
    for DataMC in dicti:
        if 'mu' in DataMC:
            if "Data" in DataMC:
                continue
            for key in dicti[DataMC]:
                if 'Semi' in key:
                    continue
                if "Fully" not in key:
                    continue
                outDir = f'/mnt/disk1/skimmed_Run2/selection/{outputTag}/{era}/{DataMC}/{key}' # Use outputTag and era
                input_files = dicti[DataMC][key]
                dataset_list.append((DataMC, key, input_files, outDir, cut_string, era)) # Add era to tuple

    # Use multiprocessing to process datasets in parallel
    num_cores = 5
    with Pool(num_cores) as pool:
        pool.map(process_dataset, dataset_list)
    
    logging.info("Finished all processing.")
    # print(dataset_list)