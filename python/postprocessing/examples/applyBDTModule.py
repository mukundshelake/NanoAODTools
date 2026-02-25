import ROOT
import numpy as np
import joblib
from ROOT import TLorentzVector
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection

class ApplyBDT(Module):
    def __init__(self, model_path, branch_map, branch_name="BDTScore"):
        self.model_path = model_path
        self.branch_map = branch_map
        self.branch_name = branch_name

    def beginJob(self):
        self.model = joblib.load(self.model_path)

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(self.branch_name, "F")

    def analyze(self, event):
        try:
            features = []
            for model_var in self.branch_map:
                branch = self.branch_map[model_var]
                features.append(getattr(event, branch))
        except AttributeError as e:
            print(f"[ERROR] Missing variable in event: {e}")
            self.out.fillBranch(self.branch_name, -999.0)
            return True

        values_array = np.array(features).reshape(1, -1)
        score = float(self.model.predict_proba(values_array)[0][1])
        self.out.fillBranch(self.branch_name, score)
        return True


def applyBDTModule(config):
    """
    Factory function to create ApplyBDT module from config.
    
    Args:
        config: Dictionary with keys:
            - model_path: Path to the trained BDT model (.pkl file)
            - branch_map: Dictionary mapping model feature names to ROOT branch names
            - branch_name (optional): Name of output branch (default: "BDT_score")
    
    Example config:
        {
            "model_path": "/path/to/model.pkl",
            "branch_map": {
                "JetHM": "JetHM",
                "pTSum": "pTSum",
                "FW1": "FW1",
                ...
            },
            "branch_name": "BDT_score"
        }
    """
    model_path = config["model_path"]
    branch_map = config["branch_map"]
    branch_name = config.get("branch_name", "BDT_score")
    
    return ApplyBDT(model_path, branch_map, branch_name)