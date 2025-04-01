from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
import numpy as np

class LHEWeightSignProducer(Module):
    def __init__(self):
        super().__init__()
        self.warn_once = False  # To limit warnings

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        self.out.branch("LHEWeightSign", "F")  # 'F' = float

        # Check if the branch exists before processing events
        self.has_lhe_weight = hasattr(inputTree, "LHEWeight_originalXWGTUP")

    def analyze(self, event):
        """Process each event and compute LHEWeightSign"""

        if not self.has_lhe_weight:
            if not self.warn_once:
                print("Warning: 'LHEWeight_originalXWGTUP' branch is missing in this dataset.")
                self.warn_once = True
            self.out.fillBranch("LHEWeightSign", 1.0)  # Default for missing branch
            return True

        # Get the LHE weight
        lhe_weight = event.LHEWeight_originalXWGTUP

        # Compute the sign of LHE weight
        lhe_weight_sign = np.sign(lhe_weight) if lhe_weight != 0 else 0.0

        # Fill the new branch
        self.out.fillBranch("LHEWeightSign", lhe_weight_sign)

        return True  # Keep event

def lheWeightSignModule():
    return LHEWeightSignProducer()


'''


from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
import numpy as np

class LHEWeightSignProducer(Module):
    def __init__(self):
        super().__init__()

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        self.out.branch("LHEWeightSign", "F")  # 'F' = float


    def analyze(self, event):
        """Process each event and compute LHEWeightSign"""
        # Get the original LHE weight, default to None if the branch is missing
        lhe_weight = getattr(event, "LHEWeight_originalXWGTUP", None)

        if lhe_weight is None:
            # Handle missing branch or inaccessible branch
            print("Warning: 'LHEWeight_originalXWGTUP' branch is missing or inaccessible in this event.")
            self.out.fillBranch("LHEWeightSign", 1.0)  # Default value for missing branch
            return True  # Keep event

        # Compute the sign of LHE weight
        lhe_weight_sign = lhe_weight / abs(lhe_weight) if lhe_weight != 0 else 0.0

        # Fill the new branch with computed value
        self.out.fillBranch("LHEWeightSign", lhe_weight_sign)

        return True  # Keep event

def lheWeightSignModule():
    return LHEWeightSignProducer()
'''