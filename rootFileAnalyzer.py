import uproot
import numpy as np
import os
import glob
import argparse

def analyze_root_file(input_file, output_file, checkpreselection, checkselection):
    isData = "Data" in input_file
    # Open the ROOT file using uproot
    try:
        root_file = uproot.open(input_file)
    except Exception as e:
        print(f"Error opening file: {input_file}, {e}")
        return False

    # Get the tree from the ROOT file
    try:
        tree = root_file["Events"]
    except KeyError:
        print(f"Error: 'Events' tree not found in file: {input_file}")
        return False

    # Check if the tree has events
    num_events = tree.num_entries
    if num_events == 0:
        print(f"No events found in file: {input_file}")
        return False

    # Get the branch names
    branch_names = tree.keys()

    # Get min and max values for nJet and nMuon
    try:
        nJet = tree["nJet"].array()
        nMuon = tree["nMuon"].array()
        if len(nJet) > 0:
            nJet_min, nJet_max = np.min(nJet), np.max(nJet)
        else:
            print(f"Error: nJet array is empty in file: {input_file}")
            return False
        if len(nMuon) > 0:
            nMuon_min, nMuon_max = np.min(nMuon), np.max(nMuon)
        else:
            print(f"Error: nMuon array is empty in file: {input_file}")
            return False
    except KeyError as e:
        print(f"Error: {e} branch not found in file: {input_file}")
        return False

    if checkpreselection or checkselection:
        # Find the minimum Jet_pt for events where nJet == nJet_min
        try:
            Jet_pt = tree["Jet_pt"].array()
            min_jet_pt = np.min(Jet_pt[nJet == nJet_min])
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Find the maximum abs(Jet_eta) for events where nJet == nJet_min
        try:
            Jet_eta = tree["Jet_eta"].array()
            max_jet_eta = np.max(np.abs(Jet_eta[nJet == nJet_min]))
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Find the minimum Muon_pt for events where nMuon == nMuon_min
        try:
            Muon_pt = tree["Muon_pt"].array()
            min_muon_pt = np.min(Muon_pt[nMuon == nMuon_min])
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Find the maximum abs(Muon_eta) for events where nMuon == nMuon_min
        try:
            Muon_eta = tree["Muon_eta"].array()
            max_muon_eta = np.max(np.abs(Muon_eta[nMuon == nMuon_min]))
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Find the unique entries in Muon_tightId for events where nMuon == nMuon_min
        try:
            Muon_tightId = tree["Muon_tightId"].array()
            unique_muon_tightId = np.unique(Muon_tightId[nMuon == nMuon_min])
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check if HLT_IsoMu24 or HLT_IsoTkMu24 is satisfied
        try:
            if "UL2016" in input_file:
                HLT_IsoMu24 = tree["HLT_IsoMu24"].array()
                HLT_IsoTkMu24 = tree["HLT_IsoTkMu24"].array()
                trigger_satisfied = np.any(HLT_IsoMu24 | HLT_IsoTkMu24)
            elif "UL2017" in input_file:
                HLT_IsoMu27 = tree["HLT_IsoMu27"].array()
                trigger_satisfied = np.any(HLT_IsoMu27)
            elif "UL2018" in input_file:
                HLT_IsoMu24 = tree["HLT_IsoMu24"].array()
                trigger_satisfied = np.any(HLT_IsoMu24)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

    if checkselection and not isData:
        # Check min and max values of LHEWeightSign
        try:
            LHEWeightSign = tree["LHEWeightSign"].array()
            min_lhe_weight_sign = np.min(LHEWeightSign)
            max_lhe_weight_sign = np.max(LHEWeightSign)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of MuonIDWeight
        try:
            MuonIDWeight = tree["MuonIDWeight"].array()
            min_muon_id_weight = np.min(MuonIDWeight)
            max_muon_id_weight = np.max(MuonIDWeight)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of MuonIDWeightUp
        try:
            MuonIDWeightUp = tree["MuonIDWeightUp"].array()
            min_muon_id_weight_up = np.min(MuonIDWeightUp)
            max_muon_id_weight_up = np.max(MuonIDWeightUp)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of MuonIDWeightDown
        try:
            MuonIDWeightDown = tree["MuonIDWeightDown"].array()
            min_muon_id_weight_down = np.min(MuonIDWeightDown)
            max_muon_id_weight_down = np.max(MuonIDWeightDown)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of MuonHLTWeight
        try:
            MuonHLTWeight = tree["MuonHLTWeight"].array()
            min_muon_hlt_weight = np.min(MuonHLTWeight)
            max_muon_hlt_weight = np.max(MuonHLTWeight)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of MuonHLTWeightStat
        try:
            MuonHLTWeightStat = tree["MuonHLTWeightStat"].array()
            min_muon_hlt_weight_stat = np.min(MuonHLTWeightStat)
            max_muon_hlt_weight_stat = np.max(MuonHLTWeightStat)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of MuonHLTWeightSyst
        try:
            MuonHLTWeightSyst = tree["MuonHLTWeightSyst"].array()
            min_muon_hlt_weight_syst = np.min(MuonHLTWeightSyst)
            max_muon_hlt_weight_syst = np.max(MuonHLTWeightSyst)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of bTaggingWeight
        try:
            bTaggingWeight = tree["bTaggingWeight"].array()
            min_btagging_weight = np.min(bTaggingWeight)
            max_btagging_weight = np.max(bTaggingWeight)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of bTaggingWeightUp
        try:
            bTaggingWeightUp = tree["bTaggingWeightUp"].array()
            min_btagging_weight_up = np.min(bTaggingWeightUp)
            max_btagging_weight_up = np.max(bTaggingWeightUp)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of bTaggingWeightDown
        try:
            bTaggingWeightDown = tree["bTaggingWeightDown"].array()
            min_btagging_weight_down = np.min(bTaggingWeightDown)
            max_btagging_weight_down = np.max(bTaggingWeightDown)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of jetPUIdWeight
        try:
            jetPUIdWeight = tree["jetPUIdWeight"].array()
            min_jet_pu_id_weight = np.min(jetPUIdWeight)
            max_jet_pu_id_weight = np.max(jetPUIdWeight)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of jetPUIdWeightUp
        try:
            jetPUIdWeightUp = tree["jetPUIdWeightUp"].array()
            min_jet_pu_id_weight_up = np.min(jetPUIdWeightUp)
            max_jet_pu_id_weight_up = np.max(jetPUIdWeightUp)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

        # Check min and max values of jetPUIdWeightDown
        try:
            jetPUIdWeightDown = tree["jetPUIdWeightDown"].array()
            min_jet_pu_id_weight_down = np.min(jetPUIdWeightDown)
            max_jet_pu_id_weight_down = np.max(jetPUIdWeightDown)
        except KeyError as e:
            print(f"Error: {e} branch not found in file: {input_file}")
            return False

            # Write the details to the output file
    with open(output_file, 'w') as f:
        f.write(f"Sample ROOT file used: {input_file}\n")
        f.write(f"Number of events: {num_events}\n")
        f.write("Branch names:\n")
        for name in branch_names:
            f.write(f"{name}\n")
        f.write(f"\nnJet min: {nJet_min}, nJet max: {nJet_max}\n")
        f.write(f"nMuon min: {nMuon_min}, nMuon max: {nMuon_max}\n")
        if checkpreselection or checkselection:
            f.write(f"Minimum Jet_pt for events with nJet == {nJet_min}: {min_jet_pt}\n")
            f.write(f"Maximum abs(Jet_eta) for events with nJet == {nJet_min}: {max_jet_eta}\n")
            f.write(f"Minimum Muon_pt for events with nMuon == {nMuon_min}: {min_muon_pt}\n")
            f.write(f"Maximum abs(Muon_eta) for events with nMuon == {nMuon_min}: {max_muon_eta}\n")
            f.write(f"Unique Muon_tightId for events with nMuon == {nMuon_min}: {unique_muon_tightId}\n")
            f.write(f"HLT_IsoMu24 or HLT_IsoTkMu24 satisfied: {trigger_satisfied}\n")
        if checkselection and not isData:
            f.write(f"Minimum LHEWeightSign: {min_lhe_weight_sign}\n")
            f.write(f"Maximum LHEWeightSign: {max_lhe_weight_sign}\n")
            f.write(f"Minimum MuonIDWeight: {min_muon_id_weight}\n")
            f.write(f"Maximum MuonIDWeight: {max_muon_id_weight}\n")
            f.write(f"Minimum MuonIDWeightUp: {min_muon_id_weight_up}\n")
            f.write(f"Maximum MuonIDWeightUp: {max_muon_id_weight_up}\n")
            f.write(f"Minimum MuonIDWeightDown: {min_muon_id_weight_down}\n")
            f.write(f"Maximum MuonIDWeightDown: {max_muon_id_weight_down}\n")
            f.write(f"Minimum MuonHLTWeight: {min_muon_hlt_weight}\n")
            f.write(f"Maximum MuonHLTWeight: {max_muon_hlt_weight}\n")
            f.write(f"Minimum MuonHLTWeightStat: {min_muon_hlt_weight_stat}\n")
            f.write(f"Maximum MuonHLTWeightStat: {max_muon_hlt_weight_stat}\n")
            f.write(f"Minimum MuonHLTWeightSyst: {min_muon_hlt_weight_syst}\n")
            f.write(f"Maximum MuonHLTWeightSyst: {max_muon_hlt_weight_syst}\n")
            f.write(f"Minimum bTaggingWeight: {min_btagging_weight}\n")
            f.write(f"Maximum bTaggingWeight: {max_btagging_weight}\n")
            f.write(f"Minimum bTaggingWeightUp: {min_btagging_weight_up}\n")
            f.write(f"Maximum bTaggingWeightUp: {max_btagging_weight_up}\n")
            f.write(f"Minimum bTaggingWeightDown: {min_btagging_weight_down}\n")
            f.write(f"Maximum bTaggingWeightDown: {max_btagging_weight_down}\n")
            f.write(f"Minimum jetPUIdWeight: {min_jet_pu_id_weight}\n")
            f.write(f"Maximum jetPUIdWeight: {max_jet_pu_id_weight}\n")
            f.write(f"Minimum jetPUIdWeightUp: {min_jet_pu_id_weight_up}\n")
            f.write(f"Maximum jetPUIdWeightUp: {max_jet_pu_id_weight_up}\n")
            f.write(f"Minimum jetPUIdWeightDown: {min_jet_pu_id_weight_down}\n")
            f.write(f"Maximum jetPUIdWeightDown: {max_jet_pu_id_weight_down}\n")
                
    return True

def analyze_folder(root_folder, checkpreselection, checkselection):
    print(f"Analyzing ROOT files in folder: {root_folder}")
    for dirpath, dirnames, filenames in os.walk(root_folder):
        root_files = glob.glob(os.path.join(dirpath, "*.root"))
        if root_files:
            print(f"Found some root files in folder: {dirpath}")
            print(f"Analyzing a sample file from...{root_files}")
            for sample_file in root_files:
                output_file = os.path.join(dirpath, "rootFileHealth.txt")
                if analyze_root_file(sample_file, output_file, checkpreselection, checkselection):
                    break
            else:
                with open(output_file, 'w') as f:
                    f.write("All ROOT files in this folder have zero events.\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze ROOT files in a folder.")
    parser.add_argument("-d", "--root_folder", type=str, required=True, help="Path to the root folder containing ROOT files.")
    parser.add_argument("--checkpreselection", action="store_true", default=False, help="Check additional cuts in the ROOT files.")
    parser.add_argument("--checkselection", action="store_true", default=False, help="Check selection criteria in the ROOT files (e.g., min/max LHEWeightSign).")
    args = parser.parse_args()

    analyze_folder(args.root_folder, args.checkpreselection, args.checkselection)