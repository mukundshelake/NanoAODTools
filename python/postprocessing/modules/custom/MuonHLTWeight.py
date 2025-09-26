from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib
import numpy as np
import awkward as ak

class MuonHLTWeightProducer(Module):
    def __init__(self, config):
        super().__init__()
        self.HLTeval = correctionlib.CorrectionSet.from_file(config['HLTSFFile'])
        self.muonCut = config['kinematics']['Muon']
        self.clibConfig = config['correctionLib']
        self.bNames = config['branchNames']

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"], "F")  # Float
        self.out.branch(self.bNames["sfstat"], "F")  # Float
        self.out.branch(self.bNames["sfsyst"], "F")  # Float


    def analyze(self, event):
        """Process each event and compute highest muon pt"""
        # Select leading (highest pT) muon
        muons = Collection(event, "Muon")
        leading_muon_pt = 0.0
        for muon in muons:
            isGoodMuon = True
            for lowhighCut in self.muonCut["lohi"]:
                if muon[lowhighCut] < self.muonCut["lohi"][lowhighCut]['low'] or muon[lowhighCut] > self.muonCut["lohi"][lowhighCut]['high']:
                    isGoodMuon = False
                    break
            for valueCut in self.muonCut["value"]:
                if not muon[valueCut] == self.muonCut["value"][valueCut]:
                    isGoodMuon = False
                    break
            if not isGoodMuon:
                continue
            if muon.pt > leading_muon_pt:
                leading_muon_pt = muon.pt
                leading_muon_eta = muon.eta
        if leading_muon_pt > 200:
            leading_muon_pt = 199.9
        if leading_muon_pt > 0:
            HLTSF = self.HLTeval[self.clibConfig["weightName"]].evaluate(abs(leading_muon_eta), leading_muon_pt, 'nominal')
            HLTSF_stat = self.HLTeval[self.clibConfig["weightName"]].evaluate(abs(leading_muon_eta), leading_muon_pt, 'stat')
            HLTSF_syst = self.HLTeval[self.clibConfig["weightName"]].evaluate(abs(leading_muon_eta), leading_muon_pt, 'syst')
        else:  # Handle cases where no muons pass the selection
            HLTSF = HLTSF_stat = HLTSF_syst = 1.0  # Default scale factor
            print("No muons passed the selection. Default scale factors applied.")
        self.out.fillBranch(self.bNames["sf"], HLTSF)
        self.out.fillBranch(self.bNames["sfstat"], HLTSF_stat)
        self.out.fillBranch(self.bNames["sfsyst"], HLTSF_syst)
        return True  # Keep event

def muonHLTWeightModule(config):
    return MuonHLTWeightProducer(config=config)
