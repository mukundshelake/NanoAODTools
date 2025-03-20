import os, json
import sys, ROOT
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from multiprocessing import Pool

def process_dataset(data):
    """Function to process a single dataset."""
    DataMC, key, files, outDir, cut_string = data

    print(f"Processing {key} in {DataMC}")

    if not os.path.exists(outDir):
        os.makedirs(outDir)

    # Set up the PostProcessor
    post_processor = PostProcessor(
        outDir,
        files,
        cut=cut_string,
        branchsel="python/postprocessing/examples/keep_and_drop.txt",  # Adjust this if needed
        modules=[],  # Add your custom modules if needed
        noOut=False,
        justcount=False,
    )

    # Run the PostProcessor
    post_processor.run()
    print(f"Finished processing {key} in {DataMC}")

if __name__ == "__main__":
    import sys

    # Load the dataset configuration
    with open('/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/Datasets/dataFiles_UL2016preVFP.json', 'r') as json_file:
        dicti = json.load(json_file)

    cut_string = (
        "Sum$(Muon_pt > 20 && abs(Muon_eta) < 3.0 && Muon_tightId) > 0 && "
        "Sum$(Jet_pt > 20 && abs(Jet_eta) < 3.0 && Jet_btagDeepFlavB > 0.2598) >= 2 && "
        "Sum$(Jet_pt > 20 && abs(Jet_eta) < 3.0) > 3 && "
        "(HLT_IsoMu24 || HLT_IsoTkMu24)"
    )

    print("Cut string being applied:", cut_string)
    # exit(0)
    # Prepare datasets for parallel processing
    dataset_list = []
    for DataMC in dicti:
        if 'mu' in DataMC:
            for key in dicti[DataMC]:
                if 'SemiLep' not in key:
                    continue
                outDir = f'outputs/UL2016preVFP/{DataMC}/{key}'
                # outDir = 'outputs'
                input_files = dicti[DataMC][key]
                dataset_list.append((DataMC, key, input_files, outDir, cut_string))

    # Use multiprocessing to process datasets in parallel
    num_cores = 20
    with Pool(num_cores) as pool:
        pool.map(process_dataset, dataset_list)

'''
# Define the input and output files
# input_files = ["UL2016_preVFP_ttbarSemileptonic.root"]
with open(f'/nfs/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/Datasets/dataFiles_UL2016preVFP.json', 'r') as json_file:
    dicti = json.load(json_file)



cut_string = (
    "Sum$(Muon_pt > 35 && abs(Muon_eta) < 2.4 && Muon_tightId) > 0 && "
    "Sum$(Jet_pt > 30 && abs(Jet_eta) < 2.4 && Jet_btagDeepFlavB > 0.2598) >= 2 && "
    "Sum$(Jet_pt > 30 && abs(Jet_eta) < 2.4) > 3 && "
    "HLT_IsoMu24 || HLT_IsoTkMu24"
)


for DataMC in dicti:
    if 'mu' in DataMC:
        for key in dicti[DataMC]:
            outDir = 'outputs/UL2016preVFP/'+DataMC+'/'+key
            print(outDir)
            if not os.path.exists(outDir):
                os.makedirs(outDir)
            input_files = []
            for file in dicti[DataMC][key]:
                input_files.append(file)
                # print(file)
                # print('\n')
            # print(input_files)
            # Set up the PostProcessor
            print(f"Processing {key} in {DataMC}")
            post_processor = PostProcessor(
                outDir,
                input_files,
                cut= cut_string,  # You can specify a cut string here if needed
                branchsel="python/postprocessing/examples/keep_and_drop.txt",  # You can specify a branch selection file here if needed
                # modules= [exampleModuleConstr()],
                modules=[],   # You can specify a list of modules here if needed
                noOut=False,  # Set to True if you don't want to write output files
                justcount=False,  # Set to True if you just want to count events
            )

            # Run the PostProcessor
            post_processor.run()
'''