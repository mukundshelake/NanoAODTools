import ROOT
import math
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module

# Load the CMS HitFit library (adjust path if needed)
ROOT.gSystem.Load('libTopHitFit')

class TTbarSemilepReconstructor(Module):
    def __init__(self, era):
        era_thresholds = {
            'UL2016preVFP': 0.2598,
            'UL2016postVFP': 0.2489,
            'UL2017': 0.3040,
            'UL2018': 0.2783
        }
        self.btagWP = era_thresholds.get(era, 0.2598)  # default to 2016preVFP if unknown
        # mass and resolution constants
        self.mW = 80.4
        self.mt = 172.5
        self.sigmaW = 10.0  # GeV, published W mass resolution
        self.sigmatt = 13.0  # GeV, published top mass resolution

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        # Define output branches
        for name in ["Top_lep", "Top_had"]:
            for var in ["pt", "eta", "phi", "mass"]:
                self.out.branch(f"{name}_{var}", "F")
        self.out.branch("Chi2_prefit", "F")
        self.out.branch("Chi2", "F")
        self.out.branch("Pgof", "F")

    def analyze(self, event):
        muons = Collection(event, "Muon")
        jets  = Collection(event, "Jet")
        # Compute MET components
        met_px = event.MET_pt * math.cos(event.MET_phi)
        met_py = event.MET_pt * math.sin(event.MET_phi)

        # select leading muon
        mu = max(muons, key=lambda m: m.pt)
        mu_p4 = self.make_p4(mu)

        # select 4 leading jets
        jets4 = sorted(jets, key=lambda j: j.pt, reverse=True)[:4]
        # pick top 2 b-tags
        bjets = sorted(jets4, key=lambda j: j.btagDeepFlavB, reverse=True)
        bjets = [j for j in bjets if j.btagDeepFlavB > self.btagWP][:2]
        ljets = [j for j in jets4 if j not in bjets][:2]
        if len(bjets) < 2 or len(ljets) < 2:
            return False

        # full chi2 prefit: loop over jet permutations and neutrino solutions
        best_chi2_prefit = float('inf')
        best_perm = None
        # neutrino pz solutions for fixed MET, muon
        def nu_pz_solutions():
            px, py = met_px, met_py
            pxl, pyl, pzl = mu_p4.Px(), mu_p4.Py(), mu_p4.Pz()
            El = mu_p4.E()
            a = self.mW**2 + 2*(pxl*px + pyl*py)
            A = 4*(El**2 - pzl**2)
            B = -4*a*pzl
            C = 4*El**2*(px**2+py**2) - a**2
            disc = B*B - 4*A*C
            if disc < 0:
                return [-B/(2*A)]
            sqrt_disc = math.sqrt(disc)
            return [(-B+sqrt_disc)/(2*A), (-B-sqrt_disc)/(2*A)]
        pz_list = nu_pz_solutions()

        for br, bh in [(bjets[0],bjets[1]), (bjets[1],bjets[0])]:
            for q1, q2 in [(ljets[0],ljets[1]), (ljets[1],ljets[0])]:
                # get 4-vectors
                br_p4 = self.make_p4(br)
                bh_p4 = self.make_p4(bh)
                q1_p4 = self.make_p4(q1)
                q2_p4 = self.make_p4(q2)
                # hadronic W and top
                w_had_p4 = q1_p4 + q2_p4
                top_had_p4 = w_had_p4 + bh_p4
                # mass terms
                chi2_jets = ((w_had_p4.M()-self.mW)/self.sigmaW)**2
                # loop neutrino solutions
                for pz in pz_list:
                    E_nu = math.sqrt(met_px**2+met_py**2+pz**2)
                    nu_p4 = ROOT.TLorentzVector(met_px, met_py, pz, E_nu)
                    # leptonic W and top
                    w_lep_p4 = mu_p4 + nu_p4
                    top_lep_p4 = w_lep_p4 + br_p4
                    # chi2 terms
                    chi2_wlep = ((w_lep_p4.M()-self.mW)/self.sigmaW)**2
                    chi2_top = ((top_lep_p4.M()-top_had_p4.M())/self.sigmatt)**2
                    total_chi2 = chi2_jets + chi2_wlep + chi2_top
                    if total_chi2 < best_chi2_prefit:
                        best_chi2_prefit = total_chi2
                        best_perm = (br, bh, q1, q2)

        # fill pre-fit chi2
        self.out.fillBranch("Chi2_prefit", best_chi2_prefit)

        # now perform full HitFit on best_perm
        br, bh, q1r, q2r = best_perm
        fitter = self.init_hitfit(mu, br, bh, q1r, q2r, met_px, met_py)
        if fitter.fit() != 0:
            return False
        chi2 = fitter.getChi2()
        pgof = fitter.getProb()
        tlp  = fitter.getFittedParticle(0)
        thd  = fitter.getFittedParticle(1)
        # fill branches
        for prefix, obj in [("Top_lep", tlp), ("Top_had", thd)]:
            self.out.fillBranch(f"{prefix}_pt",   obj.Pt())
            self.out.fillBranch(f"{prefix}_eta",  obj.Eta())
            self.out.fillBranch(f"{prefix}_phi",  obj.Phi())
            self.out.fillBranch(f"{prefix}_mass", obj.M())
        self.out.fillBranch("Chi2", chi2)
        self.out.fillBranch("Pgof", pgof)
        return True

    def init_hitfit(self, mu, br, bh, q1, q2, met_px, met_py):
        args = ROOT.hitfit.Constrained_Top_Args()
        args.resolutionJet = 0.10
        args.resolutionLep = 0.02
        args.resolutionMET = 0.10
        args.setMassW(self.mW)
        args.setMassTop(self.mt)
        # Lep
        args.LepPt  = mu.pt;  args.LepEta = mu.eta;  args.LepPhi = mu.phi;  args.LepE = mu.p4().E()
        # Jets
        for i, j in enumerate([br,bh,q1,q2]):
            setattr(args, f"Jet{i+1}Pt",  j.pt)
            setattr(args, f"Jet{i+1}Eta", j.eta)
            setattr(args, f"Jet{i+1}Phi", j.phi)
            setattr(args, f"Jet{i+1}E",   j.p4().E())
        args.METx = met_px; args.METy = met_py
        return ROOT.hitfit.Constrained_Top(args)

    def make_p4(self, obj):
        p4 = ROOT.TLorentzVector()
        p4.SetPtEtaPhiM(obj.pt, obj.eta, obj.phi, obj.mass)
        return p4


def RecoModule(era):
    return TTbarSemilepReconstructor(era)