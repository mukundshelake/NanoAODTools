from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import numpy as np

import correctionlib

# Global cache inside this module
_CORRECTION_CACHE = {}


class MuonHLTWeightProducer(Module):
    def __init__(self, config):
        super().__init__()
        self.HLTSFFile = config['HLTSFFile']
        self.muonCut = config['kinematics']['Muon']
        self.clibConfig = config['correctionLib']
        self.bNames = config['branchNames']
        self.HLTeval = None

    def beginJob(self):
        if self.HLTSFFile not in _CORRECTION_CACHE:
            # Load only if not already loaded in this process
            _CORRECTION_CACHE[self.HLTSFFile] = correctionlib.CorrectionSet.from_file(self.HLTSFFile)
        self.HLTeval = _CORRECTION_CACHE[self.HLTSFFile]


    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"], "F")
        self.out.branch(self.bNames["sfstat"], "F")
        self.out.branch(self.bNames["sfsyst"], "F")

    def analyze(self, event):
        muons = Collection(event, "Muon")
        leading_muon_pt = 0.0
        leading_muon_eta = 0.0

        for muon in muons:
            # apply kinematic cuts
            isGoodMuon = True
            for var, cut in self.muonCut["lohi"].items():
                if muon[var] < cut['low'] or muon[var] > cut['high']:
                    isGoodMuon = False
                    break
            if isGoodMuon:
                for var, val in self.muonCut["value"].items():
                    if muon[var] != val:
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
            weightName = self.clibConfig["weightName"]
            HLTSF      = self.HLTeval[weightName].evaluate(abs(leading_muon_eta), leading_muon_pt, 'nominal')
            HLTSF_stat = self.HLTeval[weightName].evaluate(abs(leading_muon_eta), leading_muon_pt, 'stat')
            HLTSF_syst = self.HLTeval[weightName].evaluate(abs(leading_muon_eta), leading_muon_pt, 'syst')
        else:
            HLTSF = HLTSF_stat = HLTSF_syst = 1.0

        self.out.fillBranch(self.bNames["sf"], HLTSF)
        self.out.fillBranch(self.bNames["sfstat"], HLTSF_stat)
        self.out.fillBranch(self.bNames["sfsyst"], HLTSF_syst)
        return True

def muonHLTWeightModule(config):
    return MuonHLTWeightProducer(config=config)

