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
        # Get the original LHE weight
        lhe_weight = getattr(event, "LHEWeight_originalXWGTUP", 0.0)

        # Compute the sign of LHE weight
        lhe_weight_sign = lhe_weight / abs(lhe_weight) if lhe_weight != 0 else 0.0

        # Fill the new branch with computed value
        self.out.fillBranch("LHEWeightSign", lhe_weight_sign)

        return True  # Keep event

def lheWeightSignModule():
    return LHEWeightSignProducer()
