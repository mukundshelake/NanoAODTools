from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
import correctionlib


class METXYCorrProducer(Module):
    """
    MET-phi (XY-shift) modulation correction, Data and MC both.

    Real detector effects (anisotropic calorimeter response, imperfect
    conditions) impose a small pileup-dependent sinusoidal modulation on
    MET_phi that isn't physical -- both Data and simulation show it, with
    different fitted coefficients, hence separate _data/_mc corrections in
    one file (JME POG's met.json.gz). Runs as an ordinary ModuleList entry
    (both ModuleList.MC and ModuleList.Data), placed before selectedObjects:
    unlike JER, MET_pt/MET_phi are never referenced in SelectionCuts' cut
    string (only inside SelectedObjectsProducer's own mTW computation), and
    NanoAODTools' OutputTree.fillBranch() registers the new value via
    setExtraBranch() on the *input* tree object -- Event.__getattr__ checks
    that override before falling back to the raw branch read, so a later
    module in the same ModuleList/pass genuinely sees this one's corrected
    MET_pt/MET_phi through plain event.MET_pt access. No separate pass or
    intermediate storage needed here.

    Purely deterministic (met_pt, met_phi, npvs, run) -> corrected value --
    no stochastic component, unlike JER.
    """

    def __init__(self, metFile, isData, npvsBranch="PV_npvs", metBranch="MET"):
        super().__init__()
        self.evaluator = correctionlib.CorrectionSet.from_file(metFile)
        variant = "data" if isData else "mc"
        self.ptCorr = self.evaluator[f"pt_metphicorr_pfmet_{variant}"]
        self.phiCorr = self.evaluator[f"phi_metphicorr_pfmet_{variant}"]
        self.npvsBranch = npvsBranch
        self.metBranch = metBranch

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(f"{self.metBranch}_pt", "F")
        self.out.branch(f"{self.metBranch}_phi", "F")

    def analyze(self, event):
        met_pt = getattr(event, f"{self.metBranch}_pt")
        met_phi = getattr(event, f"{self.metBranch}_phi")
        npvs = float(getattr(event, self.npvsBranch))
        run = float(event.run)

        corr_pt = self.ptCorr.evaluate(met_pt, met_phi, npvs, run)
        corr_phi = self.phiCorr.evaluate(met_pt, met_phi, npvs, run)

        self.out.fillBranch(f"{self.metBranch}_pt", corr_pt)
        self.out.fillBranch(f"{self.metBranch}_phi", corr_phi)

        return True


def METXYCorrModule(config):
    return METXYCorrProducer(
        metFile=config["metFile"],
        isData=config["isData"],
        npvsBranch=config.get("npvsBranch", "PV_npvs"),
        metBranch=config.get("metBranch", "MET"),
    )
