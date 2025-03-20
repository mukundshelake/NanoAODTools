from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib
import numpy as np
import awkward as ak

class MuonIDWeightProducer(Module):
    def __init__(self, era):
        super().__init__()
        self.era = era
        IDFile = f"/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/SFs/{era}_mu_ID.json"
        self.IDeval = correctionlib.CorrectionSet.from_file(IDFile)

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        self.out.branch("MuonIDWeight", "F")  # Float
        self.out.branch("MuonIDWeightUp", "F")  # Float
        self.out.branch("MuonIDWeightDown", "F")  # Float

    def analyze(self, event):
        """Process each event and compute highest muon pt"""
        # Select leading (highest pT) muon
        muons = Collection(event, "Muon")
        muons = [muon for muon in muons if muon.pt > 35 and abs(muon.eta) < 2.4 and muon.tightId]
        leading_muon_pt = 0.0
        for muon in muons:
            if muon.pt > leading_muon_pt:
                leading_muon_pt = muon.pt
                leading_muon_eta = muon.eta
        IDSF = self.IDeval["NUM_TightID_DEN_TrackerMuons"].evaluate(f"{self.era[2:]}_UL", abs(leading_muon_eta), leading_muon_pt, 'sf')
        IDSFUp = self.IDeval["NUM_TightID_DEN_TrackerMuons"].evaluate(f"{self.era[2:]}_UL", abs(leading_muon_eta), leading_muon_pt, 'systup')
        IDSFDown = self.IDeval["NUM_TightID_DEN_TrackerMuons"].evaluate(f"{self.era[2:]}_UL", abs(leading_muon_eta), leading_muon_pt, 'systdown')
        self.out.fillBranch("MuonIDWeight", IDSF)
        self.out.fillBranch("MuonIDWeightUp", IDSFUp)
        self.out.fillBranch("MuonIDWeightDown", IDSFDown)

        return True  # Keep event

def muonIDWeightModule(era):
    return MuonIDWeightProducer(era)
