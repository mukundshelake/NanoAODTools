from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from coffea.lookup_tools import extractor

# Sentinel written by SelectedObjectsProducer (003-ObjectSelectionI) when no muon was found.
_SENTINEL_PT = -1.0


class ABCDTransferWeightProducer(Module):
    """
    Looks up the (SelMuon_pt, |SelMuon_eta|)-binned ABCD QCD transfer factor
    R = N_C/N_D -- computed by 003-ObjectSelectionI's computeABCDScaleFactor.py
    and shipped here as the 'ABCD_transferFactor_R' TH2 in scaleFactorFile --
    and writes it as a per-event weight branch, qcdABCDWeight.

    This weight has no meaning on its own for the "nominal" (region A)
    histograms 003-ObjectSelectionIII builds -- it's used only when building
    the background-subtracted, per-event-reweighted region-D QCD template:
    Data contributes +1*qcdABCDWeight, background MC contributes
    -Lumi*Xsec/Ngen*sign(LHEWeight)*qcdABCDWeight to that subtraction, via
    003-ObjectSelectionIII's existing weightList multiplication.

    Written for both Data and MC (ModuleList.Data and ModuleList.MC) since
    both sides contribute to that subtraction -- the first Data-side module
    in this chapter.

    Written as 0.0 when no muon was selected (SelMuon_pt sentinel, -1.0): an
    undefined-region event can't contribute to the QCD template regardless
    of sign.
    """

    def __init__(self, config):
        super().__init__()
        scaleFactorFile = config['scaleFactorFile']
        ext = extractor()
        ext.add_weight_sets([f"* * {scaleFactorFile}"])
        ext.finalize()
        self.evaluator = ext.make_evaluator()
        self.histName = config.get('histName', 'ABCD_transferFactor_R')
        self.bNames = config['branchNames']
        self.selMuonBranch = config['selMuonBranch']  # e.g. "SelMuon"

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"], "F")

    def analyze(self, event):
        pt = getattr(event, f"{self.selMuonBranch}_pt")
        if pt > _SENTINEL_PT:
            eta = getattr(event, f"{self.selMuonBranch}_eta")
            weight = float(self.evaluator[self.histName](pt, abs(eta)))
        else:
            weight = 0.0

        self.out.fillBranch(self.bNames["sf"], weight)
        return True


def abcdTransferWeightModule(config):
    return ABCDTransferWeightProducer(config)
