from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib
import numpy as np
import awkward as ak

class MuonHLTWeightProducer(Module):
    def __init__(self, era):
        super().__init__()
        self.era = era
        HLTFile = f"/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/SFs/{era}_mu_HLT.json"
        self.HLTeval = correctionlib.CorrectionSet.from_file(HLTFile)

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        self.out.branch("MuonHLTWeight", "F")  # Float

    def analyze(self, event):
        """Process each event and compute highest muon pt"""
        # Select leading (highest pT) muon
        muons = Collection(event, "Muon")
        leading_muon_pt = 0.0
        for muon in muons:
            if muon.pt > leading_muon_pt:
                leading_muon_pt = muon.pt
                leading_muon_eta = muon.eta
        
        if leading_muon_pt > 200.0:
            leading_muon_pt = 199.9

        if leading_muon_pt < 26.0:
            leading_muon_pt = 26.01
        if abs(leading_muon_eta) > 2.4:
            leading_muon_eta = 2.39
        if self.era == "UL2016preVFP" or self.era == "UL2016postVFP":
            sfString = "NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight"
        elif self.era == "UL2017":
            sfString = "NUM_IsoMu27_DEN_CutBasedIdTight_and_PFIsoTight"
        else:
            sfString = "NUM_IsoMu24_DEN_CutBasedIdTight_and_PFIsoTight"
        HLTSF = self.HLTeval[sfString].evaluate(abs(leading_muon_eta), leading_muon_pt, 'nominal')
        self.out.fillBranch("MuonHLTWeight", HLTSF)

        return True  # Keep event

def muonHLTWeightModule(era):
    return MuonHLTWeightProducer(era)
