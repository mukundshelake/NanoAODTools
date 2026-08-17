from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import numpy as np


class BDTvariableProducer(Module):
    def __init__(self):
        super().__init__()

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        # BDT and event-shape variables
        for name in ["JetHT", "pTSum", "FW1", "FW2", "FW3", "AL",
                     "Sxx", "Syy", "Sxy", "Sxz", "Syz", "Szz",
                     "S", "P", "A", "p2in", "p2out"]:
            self.out.branch(name, "F")

    def analyze(self, event):
        """
        Process each event: compute JetHT, Fox-Wolfram moments (l=1,2,3),
        longitudinal alignment AL, sphericity tensor elements, and
        derived Sphericity, Planarity, Alignment, p2in, p2out.
        """
        # Retrieve jets and MET
        jets = Collection(event, "Jet")
        # filter out any pathological eta
        good_jets = [j for j in jets if abs(j.eta) <= 10]
        # MET: prefer MET_pt attribute if available
        if hasattr(event, 'MET_pt'):
            met_pt = event.MET_pt
        elif hasattr(event, 'MET') and hasattr(event.MET, 'pt'):
            met_pt = event.MET.pt
        else:
            met_pt = 0.0

        # initialize accumulators
        JetHT_ = 0.0
        sqrt_s = 0.0
        s_sum = 0.0
        AL = 0.0
        Sxx = Syy = Sxy = Sxz = Syz = Szz = 0.0
        FW1 = FW2 = FW3 = 0.0

        # need at least one good jet for JetHT, but Fox-Wolfram and sphericity require >=2
        if good_jets:
            # cache kinematic projections per jet
            kinematics = []  # list of dicts with pt, cosh, sinh, cos, sin
            for jet in good_jets:
                pt = jet.pt
                eta = jet.eta
                phi = jet.phi
                cosh_eta = np.cosh(eta)
                sinh_eta = np.sinh(eta)
                cos_phi  = np.cos(phi)
                sin_phi  = np.sin(phi)
                p_mag     = pt * cosh_eta  # |p|
                # accumulators
                JetHT_   += pt
                sqrt_s   += p_mag
                s_sum    += p_mag * p_mag
                AL       += pt * sinh_eta
                Sxx      += pt * cos_phi * pt * cos_phi
                Syy      += pt * sin_phi * pt * sin_phi
                Sxy      += pt * cos_phi * pt * sin_phi
                Sxz      += pt * cos_phi * pt * sinh_eta
                Syz      += pt * sin_phi * pt * sinh_eta
                Szz      += pt * sinh_eta * pt * sinh_eta
                kinematics.append({
                    'pt': pt,
                    'cosh': cosh_eta,
                    'sinh': sinh_eta,
                    'cos': cos_phi,
                    'sin': sin_phi,
                })

            # Fox-Wolfram moments over all pairs
            for kin1 in kinematics:
                for kin2 in kinematics:
                    cos_theta = (kin1['cos']*kin2['cos'] + kin1['sin']*kin2['sin']
                                 + kin1['sinh']*kin2['sinh']) / (kin1['cosh']*kin2['cosh'])
                    # weight = |p_i| * |p_j| = pt_i*cosh_i * pt_j*cosh_j
                    w = kin1['pt']*kin1['cosh'] * kin2['pt']*kin2['cosh']
                    FW1 += w * cos_theta
                    FW2 += w * (3*cos_theta**2 - 1)/2
                    FW3 += w * (5*cos_theta**3 - 3*cos_theta)/2

        # finalize Fox-Wolfram and AL
        if sqrt_s > 0:
            FW1 /= (sqrt_s*sqrt_s)
            FW2 /= (sqrt_s*sqrt_s)
            FW3 /= (sqrt_s*sqrt_s)
            AL  /= sqrt_s
        else:
            FW1 = FW2 = FW3 = AL = 0.0

        # finalize sphericity tensor elements
        if s_sum > 0:
            Sxx /= s_sum; Syy /= s_sum; Sxy /= s_sum
            Sxz /= s_sum; Syz /= s_sum; Szz /= s_sum
        else:
            Sxx = Syy = Sxy = Sxz = Syz = Szz = 0.0

        # pT sum
        pTSum = JetHT_ + met_pt

        # build sphericity matrix and get eigenvalues
        SMatrix = np.array([
            [Sxx, Sxy, Sxz],
            [Sxy, Syy, Syz],
            [Sxz, Syz, Szz]
        ])
        eigs = np.linalg.eigvalsh(SMatrix)
        # numerical safety: clip tiny negatives
        eigs = np.clip(eigs, 0.0, None)
        # sort descending
        lambda1, lambda2, lambda3 = np.sort(eigs)[::-1]

        # derive event-shape variables
        sphericity = 1.5 * (lambda2 + lambda3)
        planarity  = (lambda3/lambda2) if lambda2 > 1e-8 else 0.0
        alignment  = (lambda2/lambda1) if lambda1 > 1e-8 else 0.0
        Njets      = len(good_jets) or 1
        p2in       = lambda2 / Njets
        p2out      = lambda3 / Njets

        # fill output branches
        self.out.fillBranch("JetHT", JetHT_)
        self.out.fillBranch("pTSum", pTSum)
        self.out.fillBranch("FW1", FW1)
        self.out.fillBranch("FW2", FW2)
        self.out.fillBranch("FW3", FW3)
        self.out.fillBranch("AL", AL)
        self.out.fillBranch("Sxx", Sxx)
        self.out.fillBranch("Syy", Syy)
        self.out.fillBranch("Sxy", Sxy)
        self.out.fillBranch("Sxz", Sxz)
        self.out.fillBranch("Syz", Syz)
        self.out.fillBranch("Szz", Szz)
        self.out.fillBranch("S", sphericity)
        self.out.fillBranch("P", planarity)
        self.out.fillBranch("A", alignment)
        self.out.fillBranch("p2in", p2in)
        self.out.fillBranch("p2out", p2out)

        return True  # keep event


def BDTvariableModule():
    return BDTvariableProducer()