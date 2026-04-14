from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
import correctionlib

# Global cache inside this module
_CORRECTION_CACHE = {}

# Sentinel written by SelectedObjectsProducer when no muon was found.
_SENTINEL_PT = -1.0

# HLT SF maps are only defined up to 200 GeV.
_HLT_PT_MAX = 199.9


class MuonHLTWeightProducer(Module):
    def __init__(self, config):
        super().__init__()
        self.HLTSFFile = config['HLTSFFile']
        self.clibConfig = config['correctionLib']
        self.bNames = config['branchNames']
        self.selMuonBranch = config['selMuonBranch']  # e.g. "SelMuon"
        self.HLTeval = None

    def beginJob(self):
        if self.HLTSFFile not in _CORRECTION_CACHE:
            _CORRECTION_CACHE[self.HLTSFFile] = correctionlib.CorrectionSet.from_file(self.HLTSFFile)
        self.HLTeval = _CORRECTION_CACHE[self.HLTSFFile]

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"],     "F")
        self.out.branch(self.bNames["sfstat"], "F")
        self.out.branch(self.bNames["sfsyst"], "F")

    def analyze(self, event):
        pt  = getattr(event, f"{self.selMuonBranch}_pt")
        eta = getattr(event, f"{self.selMuonBranch}_eta")

        if pt > _SENTINEL_PT:
            pt = min(pt, _HLT_PT_MAX)
            weightName = self.clibConfig["weightName"]
            HLTSF      = self.HLTeval[weightName].evaluate(abs(eta), pt, 'nominal')
            HLTSF_stat = self.HLTeval[weightName].evaluate(abs(eta), pt, 'stat')
            HLTSF_syst = self.HLTeval[weightName].evaluate(abs(eta), pt, 'syst')
        else:
            HLTSF = HLTSF_stat = HLTSF_syst = 1.0

        self.out.fillBranch(self.bNames["sf"],     HLTSF)
        self.out.fillBranch(self.bNames["sfstat"], HLTSF_stat)
        self.out.fillBranch(self.bNames["sfsyst"], HLTSF_syst)
        return True


def muonHLTWeightModule(config):
    return MuonHLTWeightProducer(config=config)

