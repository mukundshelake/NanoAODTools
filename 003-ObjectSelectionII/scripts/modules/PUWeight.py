from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
import correctionlib


class PUWeightProducer(Module):
    """
    Pileup reweighting scale factor, from Pileup_nTrueInt (MC truth) and the
    LUM POG's puWeights.json.gz correctionlib file (SF_FETCH_SPECS in
    utils.py).

    config['correctionLib']['weightName'] ("Collisions{16,17,18}_UltraLegacy_goldenJSON",
    2016preVFP and 2016postVFP both use "Collisions16_...") and the "weights"
    category's 'nominal'/'up'/'down' keys are VERIFIED against the real fetched
    files for all four eras (2025-09-03, via --fetchSFFiles + correctionlib.
    CorrectionSet.from_file(path).keys() / .evaluate() on each) -- both match
    exactly, no config.yaml changes needed.

    Also requires Pileup_nTrueInt to actually be on the skim -- checked
    directly on a real UL2016preVFP ttbar sample and found it wasn't:
    002-Samples/config.yaml's branch_selection dropped it (only "puWeight*"
    was ever kept, and never matched anything -- this campaign's central
    NanoAOD has no baked-in puWeight branch, which is why this module exists
    at all). That's fixed in 002-Samples/config.yaml now, but needs a fresh
    002-Samples reprocessing run before Pileup_nTrueInt actually reaches this
    module -- existing skims produced before that change won't have it.
    """

    def __init__(self, config):
        super().__init__()
        self.puEval = correctionlib.CorrectionSet.from_file(config['PUSFFile'])
        self.weightName = config['correctionLib']['weightName']
        self.bNames = config['branchNames']

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"],     "F")
        self.out.branch(self.bNames["sfup"],   "F")
        self.out.branch(self.bNames["sfdown"], "F")

    def analyze(self, event):
        nTrueInt = event.Pileup_nTrueInt
        corr = self.puEval[self.weightName]
        puWeight     = corr.evaluate(nTrueInt, 'nominal')
        puWeightUp   = corr.evaluate(nTrueInt, 'up')
        puWeightDown = corr.evaluate(nTrueInt, 'down')

        self.out.fillBranch(self.bNames["sf"],     puWeight)
        self.out.fillBranch(self.bNames["sfup"],   puWeightUp)
        self.out.fillBranch(self.bNames["sfdown"], puWeightDown)
        return True


def puWeightModule(config):
    return PUWeightProducer(config)
