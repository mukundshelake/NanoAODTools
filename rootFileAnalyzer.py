import uproot
import numpy as np
import os
import glob
import argparse

def analyze_root_file(input_file, output_file, checkcuts):
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

    if checkcuts:
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
            HLT_IsoMu24 = tree["HLT_IsoMu24"].array()
            HLT_IsoTkMu24 = tree["HLT_IsoTkMu24"].array()
            trigger_satisfied = np.any(HLT_IsoMu24 | HLT_IsoTkMu24)
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
        if checkcuts:
            f.write(f"Minimum Jet_pt for events with nJet == {nJet_min}: {min_jet_pt}\n")
            f.write(f"Maximum abs(Jet_eta) for events with nJet == {nJet_min}: {max_jet_eta}\n")
            f.write(f"Minimum Muon_pt for events with nMuon == {nMuon_min}: {min_muon_pt}\n")
            f.write(f"Maximum abs(Muon_eta) for events with nMuon == {nMuon_min}: {max_muon_eta}\n")
            f.write(f"Unique Muon_tightId for events with nMuon == {nMuon_min}: {unique_muon_tightId}\n")
            f.write(f"HLT_IsoMu24 or HLT_IsoTkMu24 satisfied: {trigger_satisfied}\n")
    
    return True

def analyze_folder(root_folder, checkcuts):
    print(f"Analyzing ROOT files in folder: {root_folder}")
    for dirpath, dirnames, filenames in os.walk(root_folder):
        root_files = glob.glob(os.path.join(dirpath, "*.root"))
        if root_files:
            print(f"Found some root files in folder: {dirpath}")
            print(f"Analyzing a sample file from...{root_files}")
            for sample_file in root_files:
                output_file = os.path.join(dirpath, "rootFileHealth.txt")
                # output_file = "rootFileHealth.txt"
                if analyze_root_file(sample_file, output_file, checkcuts):
                    break
            else:
                with open(output_file, 'w') as f:
                    f.write("All ROOT files in this folder have zero events.\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze ROOT files in a folder.")
    parser.add_argument("-d", "--root_folder", type=str, required=True, help="Path to the root folder containing ROOT files.")
    parser.add_argument("--checkcuts", action="store_true", help="Check additional cuts in the ROOT files.")
    args = parser.parse_args()

    analyze_folder(args.root_folder, args.checkcuts)