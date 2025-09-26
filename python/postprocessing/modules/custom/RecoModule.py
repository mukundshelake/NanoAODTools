import ROOT
import math
import numpy as np
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from scipy.optimize import minimize
import yaml
class TTbarSemilepReconstructor(Module):
    def __init__(self, era):
        era_thresholds = {
            'UL2016preVFP': 0.2598,
            'UL2016postVFP': 0.2489,
            'UL2017': 0.3040,
            'UL2018': 0.2783
        }
        self.btagWP = era_thresholds.get(era, 0.2598)
        self.mW = 80.4
        self.mt = 172.5
        self.sigmaW = 10.0
        self.sigmatt = 13.0

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        for name in ["Top_lep", "Top_had"]:
            for var in ["pt", "eta", "phi", "mass"]:
                self.out.branch(f"{name}_{var}", "F")
        self.out.branch("Chi2_prefit", "F")
        self.out.branch("Chi2", "F")
        self.out.branch("Pgof", "F")
        self.out.branch("chi2_status", "I")

    def analyze(self, event):
        muons = Collection(event, "Muon")
        jets = Collection(event, "Jet")
        met_px = event.MET_pt * math.cos(event.MET_phi)
        met_py = event.MET_pt * math.sin(event.MET_phi)

        # Start with default status (assume success)
        chi2_status = 0
        best_chi2_prefit = float('inf')
        best_perm = None

        # Muon
        mu = max(muons, key=lambda m: m.pt)
        mu_p4 = self.make_lep_p4(mu)

        # Jet selection
        selected_jets = [jet for jet in jets if jet.pt > 25 and abs(jet.eta) < 2.4]
        sorted_jets = sorted(selected_jets, key=lambda jet: jet.pt, reverse=True)
        bjets = []
        remaining_jets = []
        for jet in sorted_jets:
            if jet.btagDeepFlavB > self.btagWP and len(bjets) < 2:
                bjets.append(jet)
            else:
                remaining_jets.append(jet)
        ljets = remaining_jets[:2]

        if len(bjets) != 2 or len(ljets) != 2:
            chi2_status = 1
            self.out.fillBranch("Chi2_prefit", -1)
            self.out.fillBranch("Chi2", -1)
            self.out.fillBranch("Pgof", -1)
            self.out.fillBranch("chi2_status", chi2_status)
            for prefix in ["Top_lep", "Top_had"]:
                self.out.fillBranch(f"{prefix}_pt",   -1)
                self.out.fillBranch(f"{prefix}_eta",  -1)
                self.out.fillBranch(f"{prefix}_phi",  -1)
                self.out.fillBranch(f"{prefix}_mass", -1)
            return True  # KEEP the event, mark it as failed at this step

        # Neutrino solutions
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

        for br, bh in [(bjets[0], bjets[1]), (bjets[1], bjets[0])]:
            for q1, q2 in [(ljets[0], ljets[1])]:
                br_p4 = self.make_jet_p4(br)
                bh_p4 = self.make_jet_p4(bh)
                q1_p4 = self.make_jet_p4(q1)
                q2_p4 = self.make_jet_p4(q2)

                w_had_p4 = q1_p4 + q2_p4
                top_had_p4 = w_had_p4 + bh_p4
                chi2_jets = ((w_had_p4.M()-self.mW)/self.sigmaW)**2

                for pz in pz_list:
                    E_nu = math.sqrt(met_px**2 + met_py**2 + pz**2)
                    nu_p4 = ROOT.TLorentzVector(met_px, met_py, pz, E_nu)

                    w_lep_p4 = mu_p4 + nu_p4
                    top_lep_p4 = w_lep_p4 + br_p4

                    chi2_wlep = ((w_lep_p4.M()-self.mW)/self.sigmaW)**2
                    chi2_top = ((top_lep_p4.M()-top_had_p4.M())/self.sigmatt)**2
                    total_chi2 = chi2_jets + chi2_wlep + chi2_top
                    if total_chi2 < best_chi2_prefit:
                        best_chi2_prefit = total_chi2
                        best_perm = {
                            "mu_p4": mu_p4,
                            "br_p4": br_p4,
                            "bh_p4": bh_p4,
                            "q1_p4": q1_p4,
                            "q2_p4": q2_p4,
                            "nu_p4": nu_p4
                        }

        self.out.fillBranch("Chi2_prefit", best_chi2_prefit if best_perm else -1)
        if not best_perm:
            chi2_status = 2
            self.out.fillBranch("Chi2", -1)
            self.out.fillBranch("Pgof", -1)
            self.out.fillBranch("chi2_status", chi2_status)
            for prefix in ["Top_lep", "Top_had"]:
                self.out.fillBranch(f"{prefix}_pt",   -1)
                self.out.fillBranch(f"{prefix}_eta",  -1)
                self.out.fillBranch(f"{prefix}_phi",  -1)
                self.out.fillBranch(f"{prefix}_mass", -1)
            return True

        res = self.full_chi2_fit_soft_constraints(best_perm)
        if not res["success"]:
            # Use prefit best guess instead
            chi2_status = 3  # New status code to indicate fallback

            # Extract TLorentzVectors from best_perm dict
            mu_p4 = best_perm["mu_p4"]
            br_p4 = best_perm["br_p4"]
            bh_p4 = best_perm["bh_p4"]
            q1_p4 = best_perm["q1_p4"]
            q2_p4 = best_perm["q2_p4"]

            # For neutrino, if best_perm has it, use that; else build from met components
            if "nu_p4" in best_perm:
                nu_p4 = best_perm["nu_p4"]
            else:
                # fallback: build nu_p4 from MET components (you may have them somewhere)
                E_nu = math.sqrt(met_px**2 + met_py**2 + met_pz**2)
                nu_p4 = ROOT.TLorentzVector(met_px, met_py, met_pz, E_nu)

            lep_top = mu_p4 + nu_p4 + br_p4
            had_top = q1_p4 + q2_p4 + bh_p4
            chi2 = best_chi2_prefit
            pgof = math.exp(-0.5 * chi2)

            for prefix, obj in [("Top_lep", lep_top), ("Top_had", had_top)]:
                self.out.fillBranch(f"{prefix}_pt",   obj.Pt())
                self.out.fillBranch(f"{prefix}_eta",  obj.Eta())
                self.out.fillBranch(f"{prefix}_phi",  obj.Phi())
                self.out.fillBranch(f"{prefix}_mass", obj.M())
            self.out.fillBranch("Chi2", chi2)
            self.out.fillBranch("Pgof", pgof)
            self.out.fillBranch("chi2_status", chi2_status)
            return True

        lep_top, had_top, chi2 = res["lep_top"], res["had_top"], res["chi2"]
        pgof = math.exp(-0.5 * chi2)

        for prefix, obj in [("Top_lep", lep_top), ("Top_had", had_top)]:
            self.out.fillBranch(f"{prefix}_pt",   obj.Pt())
            self.out.fillBranch(f"{prefix}_eta",  obj.Eta())
            self.out.fillBranch(f"{prefix}_phi",  obj.Phi())
            self.out.fillBranch(f"{prefix}_mass", obj.M())
        self.out.fillBranch("Chi2", chi2)
        self.out.fillBranch("Pgof", pgof)
        self.out.fillBranch("chi2_status", chi2_status)
        return True

    def full_chi2_fit_soft_constraints(self, best_perm):
        mu_p4 = best_perm["mu_p4"]
        br_p4 = best_perm["br_p4"]
        bh_p4 = best_perm["bh_p4"]
        q1_p4 = best_perm["q1_p4"]
        q2_p4 = best_perm["q2_p4"]
        nu_p4 = best_perm["nu_p4"]

        particles = [mu_p4, nu_p4, br_p4, bh_p4, q1_p4, q2_p4]

        p_meas = []
        for vec in particles:
            p_meas.extend([vec.Px(), vec.Py(), vec.Pz()])
        p_meas = np.array(p_meas)

        def get_sigma(idx, val):
            if idx < 3:        # muon
                return 0.05 * abs(val)
            elif idx < 6:      # neutrino
                return 0.10 * abs(val)
            else:              # jets
                return 0.15 * abs(val)

        sigma = np.array([get_sigma(i, p_meas[i]) for i in range(len(p_meas))])

        def get_p4(px, py, pz, mass=0.0):
            E = np.sqrt(px**2 + py**2 + pz**2 + mass**2)
            vec = ROOT.TLorentzVector()
            vec.SetPxPyPzE(px, py, pz, E)
            return vec

        def chi2_fn(p):
            chi2 = np.sum(((p - p_meas) / sigma) ** 2)

            # Reconstruct TLorentzVectors
            mu_vec = get_p4(*p[0:3], mu_p4.M())
            nu_vec = get_p4(*p[3:6], 0.0)
            br_vec = get_p4(*p[6:9], br_p4.M())
            bh_vec = get_p4(*p[9:12], bh_p4.M())
            q1_vec = get_p4(*p[12:15], q1_p4.M())
            q2_vec = get_p4(*p[15:18], q2_p4.M())

            chi2 += (( (mu_vec + nu_vec).M() - self.mW ) / self.sigmaW) ** 2
            chi2 += (( (q1_vec + q2_vec).M() - self.mW ) / self.sigmaW) ** 2

            chi2 += (( (mu_vec + nu_vec + br_vec).M() - self.mt ) / self.sigmatt) ** 2
            chi2 += (( (q1_vec + q2_vec + bh_vec).M() - self.mt ) / self.sigmatt) ** 2

            return chi2

        # Run optimization without constraints
        result = minimize(
            chi2_fn,
            p_meas,
            method='SLSQP',
            options={'maxiter': 1000, 'ftol': 1e-6}
        )

        if not result.success:
            return {'success': False}

        p_fit = result.x
        mu_fit = get_p4(*p_fit[0:3], mu_p4.M())
        nu_fit = get_p4(*p_fit[3:6], 0.0)
        br_fit = get_p4(*p_fit[6:9], br_p4.M())
        bh_fit = get_p4(*p_fit[9:12], bh_p4.M())
        q1_fit = get_p4(*p_fit[12:15], q1_p4.M())
        q2_fit = get_p4(*p_fit[15:18], q2_p4.M())

        return {
            'success': True,
            'lep_top': mu_fit + nu_fit + br_fit,
            'had_top': q1_fit + q2_fit + bh_fit,
            'chi2': float(result.fun)
        }


    def make_jet_p4(self, jet):
        p4 = ROOT.TLorentzVector()
        p4.SetPtEtaPhiM(jet.pt, jet.eta, jet.phi, 0.0)  # Assume jets are massless or assign fixed mass if needed
        return p4

    def make_lep_p4(self, lep):
        p4 = ROOT.TLorentzVector()
        p4.SetPtEtaPhiM(lep.pt, lep.eta, lep.phi, lep.mass)
        return p4

def RecoModule(era):
    return TTbarSemilepReconstructor(era)
