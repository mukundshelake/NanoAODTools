from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
import correctionlib

# Sentinel written by SelectedObjectsProducer when no muon was found.
_SENTINEL_PT = -1.0


class MuonIDWeightProducer(Module):
    def __init__(self, config):
        super().__init__()
        self.IDeval = correctionlib.CorrectionSet.from_file(config['IDSFFile'])
        self.clibConfig = config['correctionLib']
        self.bNames = config['branchNames']
        self.selMuonBranch = config['selMuonBranch']  # e.g. "SelMuon"

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"],     "F")
        self.out.branch(self.bNames["sfup"],   "F")
        self.out.branch(self.bNames["sfdown"], "F")

    def analyze(self, event):
        pt  = getattr(event, f"{self.selMuonBranch}_pt")
        eta = getattr(event, f"{self.selMuonBranch}_eta")

        if pt > _SENTINEL_PT:
            wName = self.clibConfig["weightName"]
            eraName = self.clibConfig["eraName"]
            IDSF     = self.IDeval[wName].evaluate(eraName, abs(eta), pt, 'sf')
            IDSFUp   = self.IDeval[wName].evaluate(eraName, abs(eta), pt, 'systup')
            IDSFDown = self.IDeval[wName].evaluate(eraName, abs(eta), pt, 'systdown')
        else:
            IDSF = IDSFUp = IDSFDown = 1.0

        self.out.fillBranch(self.bNames["sf"],     IDSF)
        self.out.fillBranch(self.bNames["sfup"],   IDSFUp)
        self.out.fillBranch(self.bNames["sfdown"], IDSFDown)
        return True


def muonIDWeightModule(config):
    return MuonIDWeightProducer(config)
