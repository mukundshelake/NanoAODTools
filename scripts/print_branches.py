#!/usr/bin/env python3
"""Print branch names from a ROOT file."""

import sys

try:
    import uproot
except ImportError:
    print("uproot not found, trying ROOT library...")
    try:
        import ROOT
        ROOT.gROOT.SetBatch(True)
    except ImportError:
        print("Error: Neither uproot nor ROOT library is available")
        sys.exit(1)

def print_branches_uproot(filepath):
    """Print branches using uproot library."""
    try:
        file = uproot.open(filepath)
        print(f"File: {filepath}")
        print(f"\nBranches in tree:")
        
        # Get the first tree in the file
        tree = None
        for key in file.keys():
            if isinstance(file[key], uproot.TTree):
                tree = file[key]
                break
        
        if tree is None:
            print("No TTree found in file")
            return
            
        for branch_name in sorted(tree.keys()):
            print(f"  {branch_name}")
        print(f"\nTotal branches: {len(tree.keys())}")
    except Exception as e:
        print(f"Error reading with uproot: {e}")

def print_branches_root(filepath):
    """Print branches using ROOT library."""
    try:
        file = ROOT.TFile.Open(filepath)
        if not file or file.IsZombie():
            print(f"Error: Could not open file {filepath}")
            return
        
        print(f"File: {filepath}")
        print(f"\nBranches in tree:")
        
        # Get the first tree in the file
        tree = None
        for key in file.GetListOfKeys():
            obj = key.ReadObj()
            if isinstance(obj, ROOT.TTree):
                tree = obj
                break
        
        if tree is None:
            print("No TTree found in file")
            file.Close()
            return
            
        branch_list = tree.GetListOfBranches()
        branches = [branch_list.At(i).GetName() for i in range(branch_list.GetEntries())]
        
        for branch_name in sorted(branches):
            print(f"  {branch_name}")
        print(f"\nTotal branches: {len(branches)}")
        
        file.Close()
    except Exception as e:
        print(f"Error reading with ROOT: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 print_branches.py <filepath>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    
    try:
        import uproot
        print_branches_uproot(filepath)
    except ImportError:
        print_branches_root(filepath)
