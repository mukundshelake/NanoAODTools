import ROOT
import numpy as np
import joblib
from ROOT import TLorentzVector
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection

class ApplyBDT(Module):
    def __init__(self, model_path, branch_map, branch_name="BDT_score"):
        self.model_path = model_path
        self.branch_map = branch_map
        self.branch_name = branch_name

    def beginJob(self):
        self.model = joblib.load(self.model_path)

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(self.branch_name, "F")

    def compute_ttbarpz(self, event):
        lep = TLorentzVector()
        had = TLorentzVector()
        lep.SetPtEtaPhiM(event.Top_lep_pt, event.Top_lep_eta, event.Top_lep_phi, event.Top_lep_mass)
        had.SetPtEtaPhiM(event.Top_had_pt, event.Top_had_eta, event.Top_had_phi, event.Top_had_mass)
        return (lep + had).Pz()

    def analyze(self, event):
        try:
            features = []
            for model_var in self.branch_map:
                branch = self.branch_map[model_var]
                if model_var == "ttbarpz":
                    features.append(self.compute_ttbarpz(event))
                else:
                    features.append(getattr(event, branch))
        except AttributeError as e:
            print(f"[ERROR] Missing variable in event: {e}")
            self.out.fillBranch(self.branch_name, -999.0)
            return True

        values_array = np.array(features).reshape(1, -1)
        score = float(self.model.predict_proba(values_array)[0][1])
        self.out.fillBranch(self.branch_name, score)
        return True


def applyBDTModule(model_path, branch_map, branch_name="BDT_score"):
    return ApplyBDT(model_path, branch_map, branch_name)