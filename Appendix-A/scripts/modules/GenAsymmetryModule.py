import ROOT
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection

# GenPart_statusFlags bit for isLastCopy
IS_LAST_COPY = 1 << 13

# Gen_prodType codes
PROD_OTHER = -1   # tops or incoming partons not found, or a qq / qbarqbar pair
PROD_GG    = 0
PROD_QQBAR = 1
PROD_QG    = 2


def _is_quark(pdgId):
    return 1 <= abs(pdgId) <= 5


class GenAsymmetryProducer(Module):
    """Gen-level ttbar forward-backward asymmetry inputs.

    Writes, per event:
      Gen_Mtt                 invariant mass of the last-copy t + tbar [GeV]
      Gen_cosThetaStar        cos of the angle between the top and the reference
                              direction, both in the ttbar rest frame
      Gen_cosThetaStar_boost  same, with the reference always taken as the ttbar
                              boost direction along z (the proxy used for gg),
                              for comparing against the true-quark reference
      Gen_prodType            -1 other, 0 gg, 1 qqbar, 2 qg

    Reference direction (a unit vector along +-z in the lab):
      qqbar -> direction of the incoming quark
      qg    -> direction of the incoming quark, or opposite the incoming antiquark
      gg    -> sign of the ttbar pz

    The incoming partons are GenPart[0] and GenPart[1] (status 21, mother -1).
    NanoAOD stores them with pt = 0, so only the sign of their eta carries
    their direction; that is all a massless reference along the beam needs,
    since scaling its energy does not change its direction after a boost.
    """

    def __init__(self, era, cfg={}):
        self.era = era

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch("Gen_Mtt", "F")
        self.out.branch("Gen_cosThetaStar", "F")
        self.out.branch("Gen_cosThetaStar_boost", "F")
        self.out.branch("Gen_prodType", "I")

    def analyze(self, event):
        genParts = Collection(event, "GenPart")

        top = antitop = None
        for p in genParts:
            if not (p.statusFlags & IS_LAST_COPY):
                continue
            if p.pdgId == 6 and top is None:
                top = p
            elif p.pdgId == -6 and antitop is None:
                antitop = p
        if top is None or antitop is None or len(genParts) < 2:
            self._fill(-1.0, -99.0, -99.0, PROD_OTHER)
            return True

        t_p4 = ROOT.TLorentzVector()
        t_p4.SetPtEtaPhiM(top.pt, top.eta, top.phi, top.mass)
        tbar_p4 = ROOT.TLorentzVector()
        tbar_p4.SetPtEtaPhiM(antitop.pt, antitop.eta, antitop.phi, antitop.mass)
        tt_p4 = t_p4 + tbar_p4

        boost_sign = 1.0 if tt_p4.Pz() >= 0 else -1.0
        prod_type, ref_sign = self._classify(genParts[0], genParts[1])
        if prod_type == PROD_GG:
            ref_sign = boost_sign

        # Top direction in the ttbar rest frame
        beta = -tt_p4.BoostVector()
        t_rest = ROOT.TLorentzVector(t_p4)
        t_rest.Boost(beta)
        t_dir = t_rest.Vect().Unit()

        cos_boost = self._cos_to_beam(t_dir, boost_sign, beta)
        cos_ref = self._cos_to_beam(t_dir, ref_sign, beta) if ref_sign != 0 else -99.0

        self._fill(tt_p4.M(), cos_ref, cos_boost, prod_type)
        return True

    @staticmethod
    def _classify(p1, p2):
        """Return (prodType, sign of the reference direction along z; 0 if undefined)."""
        id1, id2 = p1.pdgId, p2.pdgId
        z1 = 1.0 if p1.eta > 0 else -1.0
        z2 = 1.0 if p2.eta > 0 else -1.0
        if id1 == 21 and id2 == 21:
            return PROD_GG, 0.0
        if _is_quark(id1) and _is_quark(id2):
            if id1 * id2 > 0:
                return PROD_OTHER, 0.0
            return PROD_QQBAR, (z1 if id1 > 0 else z2)
        if _is_quark(id1) and id2 == 21:
            return PROD_QG, (z1 if id1 > 0 else -z1)
        if _is_quark(id2) and id1 == 21:
            return PROD_QG, (z2 if id2 > 0 else -z2)
        return PROD_OTHER, 0.0

    @staticmethod
    def _cos_to_beam(t_dir, z_sign, beta):
        """cos(angle) between t_dir and a massless beam particle moving along z_sign*z,
        with the beam particle boosted into the same ttbar rest frame."""
        beam = ROOT.TLorentzVector(0.0, 0.0, z_sign, 1.0)
        beam.Boost(beta)
        return t_dir.Dot(beam.Vect().Unit())

    def _fill(self, mtt, cos_ref, cos_boost, prod_type):
        self.out.fillBranch("Gen_Mtt", mtt)
        self.out.fillBranch("Gen_cosThetaStar", cos_ref)
        self.out.fillBranch("Gen_cosThetaStar_boost", cos_boost)
        self.out.fillBranch("Gen_prodType", prod_type)


def GenAsymmetryModule(era, cfg={}):
    return GenAsymmetryProducer(era, cfg)
