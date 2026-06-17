from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
import numpy as np

class LHEWeightSignProducer(Module):
    def __init__(self, config):
        super().__init__()
        self.warn_once = False  # To limit warnings
        self.bNames = config.get('branchNames', {})

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"], "F")  # 'F' = float

        # Check if the branch exists before processing events
        self.has_lhe_weight = hasattr(inputTree, "LHEWeight_originalXWGTUP")

    def analyze(self, event):
        """Process each event and compute LHEWeightSign"""

        if not self.has_lhe_weight:
            if not self.warn_once:
                print("Warning: 'LHEWeight_originalXWGTUP' branch is missing in this dataset.")
                self.warn_once = True
            self.out.fillBranch(self.bNames["sf"], 1.0)  # Default for missing branch
            return True

        # Get the LHE weight
        lhe_weight = event.LHEWeight_originalXWGTUP

        # Compute the sign of LHE weight
        lhe_weight_sign = np.sign(lhe_weight) if lhe_weight != 0 else 0.0

        # Fill the new branch
        self.out.fillBranch(self.bNames["sf"], lhe_weight_sign)

        return True  # Keep event

def lheWeightSignModule(config):
    return LHEWeightSignProducer(config)

