import uproot
import awkward as ak
import numpy as np
import sys

# Usage: python script.py input.root
if len(sys.argv) != 2:
    print("Usage: python script.py <input.root>")
    sys.exit(1)

filename = sys.argv[1]

with uproot.open(filename) as file:
    if "Events" not in file:
        print("No 'Events' tree found in the file.")
        sys.exit(1)
    
    tree = file["Events"]
    print(f"Scanning {filename} with {tree.num_entries} entries\n")

    for branch_name in tree.keys():
        try:
            # Load branch as flat array
            array = tree[branch_name].array(library="ak")
            flat = ak.flatten(array, axis=None)

            # Check if it's numeric
            # if not np.issubdtype(flat.type.trait.dtype, np.number):
            #     continue

            min_val = ak.min(flat)
            max_val = ak.max(flat)

            print(f"{branch_name:40}  min: {min_val:12.3f}  max: {max_val:12.3f}")

        except Exception as e:
            print(f"{branch_name:40}  -> skipped (reason: {e})")

