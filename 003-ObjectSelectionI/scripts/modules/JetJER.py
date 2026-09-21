from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
from PhysicsTools.NanoAODTools.postprocessing.tools import deltaR
import correctionlib
import math
import random


class JetJERProducer(Module):
    """
    Hybrid-method JER smearing (MC only), with Type-1 MET repropagation.

    Central UL NanoAOD's Jet_pt already has final JES (L1L2L3+Residual)
    baked in at production (RunIISummer20UL*NanoAOD(APV)v9, JME POG's
    terminal Run 2 UL calibration) -- no JES re-derivation needed. What's
    genuinely missing is JER: MC jets carry the generator-level resolution,
    not the (worse) resolution actually seen in Data, and nothing in this
    pipeline currently corrects for that.

    Runs as its own PostProcessor pass, before 003-I's SelectionCuts/
    SelectedObjectsProducer -- NOT as a 003-II-style post-selection weight
    module. SelectionCuts' jetCut/bjetCut are evaluated by PostProcessor's
    cut= string directly against the *input* tree, before any module in that
    same pass runs (see 003-I's own README on why a later module can't
    influence an earlier cut). Smearing jets after the event/jet-count cuts
    already happened would smear the wrong, already-selected population, so
    this has to be a separate, earlier pass over the preselection file, and
    003-I's existing selection pass then reads its output instead of the raw
    preselection file. Overwrites Jet_pt/Jet_mass IN PLACE (same branch
    names) so nothing downstream needs to change.

    Also overwrites MET_pt/MET_phi with the Type-1 propagation of the
    per-jet pt shift (MET -= sum of (smeared - original) jet 2-vectors).
    Skipping this would leave MET computed from the pre-smear jets while the
    jets themselves changed -- a new jet/MET inconsistency, not a fix. This
    repo's ABCD region tagging is built on mTW, itself built from MET_pt/
    MET_phi, so this directly matters here, not just as a general nicety.

    Hybrid method (JME POG standard): if a jet has a matched GenJet within
    dR < 0.5*coneSize and |pt-genpt| < 3*sigma_JER*pt, scale deterministically
    toward the GenJet's pt (pt' = genpt + SF*(pt-genpt)); otherwise smear
    stochastically (pt' = pt*(1 + N(0,1)*sqrt(max(SF^2-1,0))*resolution)).
    The stochastic branch is seeded off (run, luminosityBlock, event, jet
    index) for reproducibility -- rerunning this module twice on the same
    input gives identical output.
    """

    def __init__(self, jerFile, resolutionKey, scaleFactorKey,
                 jetBranch="Jet", metBranch="MET", coneSize=0.4):
        super().__init__()
        self.evaluator = correctionlib.CorrectionSet.from_file(jerFile)
        if resolutionKey not in self.evaluator:
            raise KeyError(
                f"'{resolutionKey}' not found in {jerFile}. Available keys: "
                f"{list(self.evaluator.keys())}"
            )
        if scaleFactorKey not in self.evaluator:
            raise KeyError(
                f"'{scaleFactorKey}' not found in {jerFile}. Available keys: "
                f"{list(self.evaluator.keys())}"
            )
        self.resolution = self.evaluator[resolutionKey]
        self.scalefactor = self.evaluator[scaleFactorKey]
        self.jetBranch = jetBranch
        self.metBranch = metBranch
        self.coneSize = coneSize

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(f"{self.jetBranch}_pt", "F", lenVar=f"n{self.jetBranch}")
        self.out.branch(f"{self.jetBranch}_mass", "F", lenVar=f"n{self.jetBranch}")
        self.out.branch(f"{self.metBranch}_pt", "F")
        self.out.branch(f"{self.metBranch}_phi", "F")

    def analyze(self, event):
        jets = Collection(event, self.jetBranch)
        genJets = Collection(event, "GenJet")
        rho = event.fixedGridRhoFastjetAll

        met_pt = getattr(event, f"{self.metBranch}_pt")
        met_phi = getattr(event, f"{self.metBranch}_phi")
        met_px = met_pt * math.cos(met_phi)
        met_py = met_pt * math.sin(met_phi)

        new_pt, new_mass = [], []
        for i, jet in enumerate(jets):
            reso = self.resolution.evaluate(jet.eta, jet.pt, rho)
            sf = self.scalefactor.evaluate(jet.eta, "nom")

            matched_pt = None
            genIdx = jet.genJetIdx
            if 0 <= genIdx < len(genJets):
                gj = genJets[genIdx]
                if (deltaR(jet.eta, jet.phi, gj.eta, gj.phi) < 0.5 * self.coneSize
                        and abs(jet.pt - gj.pt) < 3.0 * reso * jet.pt):
                    matched_pt = gj.pt

            if matched_pt is not None:
                smear = 1.0 + (sf - 1.0) * (jet.pt - matched_pt) / jet.pt
            else:
                seed = hash((event.run, event.luminosityBlock, event.event, i)) & 0xffffffff
                sigma = reso * math.sqrt(max(sf * sf - 1.0, 0.0))
                smear = 1.0 + random.Random(seed).gauss(0.0, sigma)
            smear = max(smear, 0.0)

            pt_new = jet.pt * smear
            met_px -= (pt_new - jet.pt) * math.cos(jet.phi)
            met_py -= (pt_new - jet.pt) * math.sin(jet.phi)

            new_pt.append(pt_new)
            new_mass.append(jet.mass * smear)

        self.out.fillBranch(f"{self.jetBranch}_pt", new_pt)
        self.out.fillBranch(f"{self.jetBranch}_mass", new_mass)
        self.out.fillBranch(f"{self.metBranch}_pt", math.hypot(met_px, met_py))
        self.out.fillBranch(f"{self.metBranch}_phi", math.atan2(met_py, met_px))

        return True


def JetJERModule(config):
    return JetJERProducer(
        jerFile=config["jerFile"],
        resolutionKey=config["resolutionKey"],
        scaleFactorKey=config["scaleFactorKey"],
        jetBranch=config.get("jetBranch", "Jet"),
        metBranch=config.get("metBranch", "MET"),
        coneSize=config.get("coneSize", 0.4),
    )
