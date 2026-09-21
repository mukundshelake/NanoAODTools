from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import math

# GenPart_statusFlags bit 13 = isLastCopy (after radiation, before decay) --
# the parton-level top definition TOP PAG's reweighting recipe requires.
_IS_LAST_COPY_BIT = 1 << 13


class TopPtWeightProducer(Module):
    """
    TOP PAG data-NLO top-pT reweighting (TopPtReweighting twiki):
    weight = sqrt(SF(top_pt) * SF(antitop_pt)), SF(pT) = exp(a - b*pT), using
    the parton-level top/antitop from the 'isLastCopy' definition (after
    radiation, before decay) -- GenPart entries with pdgId==+-6 and bit 13
    (isLastCopy) set in GenPart_statusFlags. pT capped at 500 GeV per the
    recommendation ("beyond top pT >= 500 GeV the value of the weight
    function at 500 GeV should be used").

    a=0.0615, b=0.0005 is the data/POWHEG+Pythia8 parametrization -- matches
    this analysis's actual ttbar generator setup (TTToSemiLeptonic/TTTo2L2Nu
    ...-powheg-pythia8).

    Use case: this analysis's signal (ttbar_SemiLeptonic) is measured
    directly, not treated as background to something else -- TOP PAG's
    "Use case 1" (tt MC used to model the detector response) applies, hence
    the data-NLO parametrization rather than NNLO-NLO (reserved for cases
    where data-NLO can't be used, e.g. BSM searches using tt as background).

    Restricted to genuine ttbar production by construction, not group-name
    matching: the weight only activates when BOTH a last-copy top AND a
    last-copy antitop are found in the same event. Single-top production
    (Schannel/Tchannel/tW) has only one top quark -- no antitop partner --
    so it naturally falls through to weight=1.0, matching the explicit TOP
    PAG recommendation that this correction must NOT be applied to single
    top ("differential measurements in single-top have not shown such
    mis-modeling"). Every other MC process has no top quark at all, so it's
    equally a no-op there -- safe to add to ModuleList.MC universally,
    no per-group gating needed in run_all.py.

    Per TOP PAG, the associated systematic should be assessed as "with vs
    without" this reweighting (compare full results both ways), not as an
    up/down shape pair -- so this module writes only the central weight,
    no systematic variant branches.
    """

    _A = 0.0615
    _B = 0.0005
    _PT_CAP = 500.0

    def __init__(self, config):
        super().__init__()
        self.bNames = config.get('branchNames', {})

    @classmethod
    def _sf(cls, pt):
        return math.exp(cls._A - cls._B * min(pt, cls._PT_CAP))

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"], "F")

    def analyze(self, event):
        genparts = Collection(event, "GenPart")
        top_pt = None
        antitop_pt = None
        for gp in genparts:
            if not (gp.statusFlags & _IS_LAST_COPY_BIT):
                continue
            if gp.pdgId == 6:
                top_pt = gp.pt
            elif gp.pdgId == -6:
                antitop_pt = gp.pt
            if top_pt is not None and antitop_pt is not None:
                break

        if top_pt is not None and antitop_pt is not None:
            weight = math.sqrt(self._sf(top_pt) * self._sf(antitop_pt))
        else:
            weight = 1.0

        self.out.fillBranch(self.bNames["sf"], weight)
        return True


def topPtWeightModule(config):
    return TopPtWeightProducer(config)
