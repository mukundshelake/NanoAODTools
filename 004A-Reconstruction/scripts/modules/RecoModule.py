import ROOT
import math
import numpy as np
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from scipy.optimize import minimize


class TTbarSemilepReconstructor(Module):
    def __init__(self, era, cfg={}):
        self.mW      = cfg.get("mW",      80.4)
        self.mt      = cfg.get("mt",     172.5)
        self.sigmaW  = cfg.get("sigmaW",  10.0)
        self.sigmatt = cfg.get("sigmatt", 13.0)

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
        met_px = event.MET_pt * math.cos(event.MET_phi)
        met_py = event.MET_pt * math.sin(event.MET_phi)

        # Guard against upstream selection failure: SelectedObjectsProducer fills
        # sentinel pt = -1 when no qualifying muon, or no 2-b-tagged + 2-light
        # split among the selected jets, was found. In production this never
        # fires -- 003-ObjectSelectionI skims out events failing those cuts
        # before any module runs -- but it's a cheap safety net against silently
        # "reconstructing" from garbage momenta if that skim is ever relaxed.
        if (event.SelMuon_pt < 0 or event.leadingbJet_pt < 0 or
                event.subleadingbJet_pt < 0 or event.leadingJet_pt < 0 or
                event.subleadingJet_pt < 0):
            self._fill_failure(1)
            return True

        # Read pre-selected objects from upstream scalar branches
        mu_p4 = ROOT.TLorentzVector()
        mu_p4.SetPtEtaPhiM(event.SelMuon_pt, event.SelMuon_eta, event.SelMuon_phi, event.SelMuon_mass)

        lb_p4 = ROOT.TLorentzVector()
        lb_p4.SetPtEtaPhiM(event.leadingbJet_pt, event.leadingbJet_eta, event.leadingbJet_phi, event.leadingbJet_mass)

        slb_p4 = ROOT.TLorentzVector()
        slb_p4.SetPtEtaPhiM(event.subleadingbJet_pt, event.subleadingbJet_eta, event.subleadingbJet_phi, event.subleadingbJet_mass)

        q1_p4 = ROOT.TLorentzVector()
        q1_p4.SetPtEtaPhiM(event.leadingJet_pt, event.leadingJet_eta, event.leadingJet_phi, event.leadingJet_mass)

        q2_p4 = ROOT.TLorentzVector()
        q2_p4.SetPtEtaPhiM(event.subleadingJet_pt, event.subleadingJet_eta, event.subleadingJet_phi, event.subleadingJet_mass)

        pz_list = self._nu_pz_solutions(mu_p4, met_px, met_py)
        if not pz_list:
            # Degenerate quadratic (El == |pzl|); essentially unreachable for a
            # muon passing the pt cuts, kept as a defensive fallback.
            self._fill_failure(2)
            return True

        # Build all 2 (b-assignment) x len(pz_list) permutations (4 in the
        # normal case), each carrying its pre-fit (no-fit) chi2 for diagnostics.
        permutations = []
        for br, bh in [(lb_p4, slb_p4), (slb_p4, lb_p4)]:
            w_had_p4 = q1_p4 + q2_p4
            top_had_p4 = w_had_p4 + bh
            chi2_jets = ((w_had_p4.M() - self.mW) / self.sigmaW) ** 2

            for pz in pz_list:
                E_nu = math.sqrt(met_px**2 + met_py**2 + pz**2)
                nu_p4 = ROOT.TLorentzVector(met_px, met_py, pz, E_nu)

                w_lep_p4 = mu_p4 + nu_p4
                top_lep_p4 = w_lep_p4 + br

                chi2_wlep = ((w_lep_p4.M() - self.mW) / self.sigmaW) ** 2
                chi2_top  = ((top_lep_p4.M() - top_had_p4.M()) / self.sigmatt) ** 2

                permutations.append({
                    "mu_p4": mu_p4, "br_p4": br, "bh_p4": bh,
                    "q1_p4": q1_p4, "q2_p4": q2_p4, "nu_p4": nu_p4,
                    "prefit_chi2": chi2_jets + chi2_wlep + chi2_top,
                })

        # Doc prescription (Sec. 2.3.1): run the full kinematic fit for *all*
        # permutations and rank by the post-fit chi2/Pgof -- not by the cheap
        # pre-fit proxy above, which is kept only as a diagnostic (Chi2_prefit).
        best_fit = None
        best_fit_perm = None
        for perm in permutations:
            res = self.full_chi2_fit_soft_constraints(perm)
            perm["fit_result"] = res
            if res["success"] and (best_fit is None or res["chi2"] < best_fit["chi2"]):
                best_fit = res
                best_fit_perm = perm

        if best_fit is not None:
            self.out.fillBranch("Chi2_prefit", best_fit_perm["prefit_chi2"])
            pgof = math.exp(-0.5 * best_fit["chi2"])
            self._fill_success(best_fit["lep_top"], best_fit["had_top"], best_fit["chi2"], pgof, chi2_status=0)
            return True

        # None of the permutations converged: fall back to the one with the
        # lowest chi2 evaluated at the *measured* (unfit) momenta, using the
        # exact same chi2 formula as the successful-fit branch above so that
        # "Chi2"/"Pgof" mean the same thing regardless of chi2_status.
        fallback_perm = min(permutations, key=lambda p: p["fit_result"]["chi2_at_meas"])
        lep_top = fallback_perm["mu_p4"] + fallback_perm["nu_p4"] + fallback_perm["br_p4"]
        had_top = fallback_perm["q1_p4"] + fallback_perm["q2_p4"] + fallback_perm["bh_p4"]
        chi2 = fallback_perm["fit_result"]["chi2_at_meas"]
        pgof = math.exp(-0.5 * chi2)

        self.out.fillBranch("Chi2_prefit", fallback_perm["prefit_chi2"])
        self._fill_success(lep_top, had_top, chi2, pgof, chi2_status=3)
        return True

    def _nu_pz_solutions(self, mu_p4, met_px, met_py):
        pxl, pyl, pzl = mu_p4.Px(), mu_p4.Py(), mu_p4.Pz()
        El = mu_p4.E()
        a = self.mW**2 + 2 * (pxl * met_px + pyl * met_py)
        A = 4 * (El**2 - pzl**2)
        B = -4 * a * pzl
        C = 4 * El**2 * (met_px**2 + met_py**2) - a**2

        if abs(A) < 1e-9:
            return []

        disc = B * B - 4 * A * C
        if disc < 0:
            return [-B / (2 * A)]
        sqrt_disc = math.sqrt(disc)
        return [(-B + sqrt_disc) / (2 * A), (-B - sqrt_disc) / (2 * A)]

    def _fill_success(self, lep_top, had_top, chi2, pgof, chi2_status):
        for prefix, obj in [("Top_lep", lep_top), ("Top_had", had_top)]:
            self.out.fillBranch(f"{prefix}_pt",   obj.Pt())
            self.out.fillBranch(f"{prefix}_eta",  obj.Eta())
            self.out.fillBranch(f"{prefix}_phi",  obj.Phi())
            self.out.fillBranch(f"{prefix}_mass", obj.M())
        self.out.fillBranch("Chi2", chi2)
        self.out.fillBranch("Pgof", pgof)
        self.out.fillBranch("chi2_status", chi2_status)

    def _fill_failure(self, chi2_status):
        """Fill all output branches with sentinel values for failed events."""
        self.out.fillBranch("Chi2_prefit", -1)
        self.out.fillBranch("Chi2",        -1)
        self.out.fillBranch("Pgof",        -1)
        self.out.fillBranch("chi2_status", chi2_status)
        for prefix in ["Top_lep", "Top_had"]:
            self.out.fillBranch(f"{prefix}_pt",   -1)
            self.out.fillBranch(f"{prefix}_eta",  -1)
            self.out.fillBranch(f"{prefix}_phi",  -1)
            self.out.fillBranch(f"{prefix}_mass", -1)

    def full_chi2_fit_soft_constraints(self, perm):
        mu_p4 = perm["mu_p4"]
        br_p4 = perm["br_p4"]
        bh_p4 = perm["bh_p4"]
        q1_p4 = perm["q1_p4"]
        q2_p4 = perm["q2_p4"]
        nu_p4 = perm["nu_p4"]

        particles = [mu_p4, nu_p4, br_p4, bh_p4, q1_p4, q2_p4]

        p_meas = []
        for vec in particles:
            p_meas.extend([vec.Px(), vec.Py(), vec.Pz()])
        p_meas = np.array(p_meas)

        def get_sigma(idx, val):
            if idx < 3:        # muon
                rel_sigma = 0.05 * abs(val)
            elif idx < 6:      # neutrino
                rel_sigma = 0.10 * abs(val)
            else:              # jets
                rel_sigma = 0.15 * abs(val)
            return max(rel_sigma, 1e-3)

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

            chi2 += (((mu_vec + nu_vec).M() - self.mW) / self.sigmaW) ** 2
            chi2 += (((q1_vec + q2_vec).M() - self.mW) / self.sigmaW) ** 2

            # Equal-top-mass constraint (Sec. 2.3.1): the two top candidates'
            # invariant masses must match each other -- the top mass itself is
            # a free parameter of the fit, not pinned to an external value
            # such as 172.5 GeV.
            top_lep_mass = (mu_vec + nu_vec + br_vec).M()
            top_had_mass = (q1_vec + q2_vec + bh_vec).M()
            chi2 += ((top_lep_mass - top_had_mass) / self.sigmatt) ** 2

            return chi2

        chi2_at_meas = float(chi2_fn(p_meas))

        try:
            result = minimize(
                chi2_fn,
                p_meas,
                method='SLSQP',
                options={'maxiter': 1000, 'ftol': 1e-6}
            )
        except Exception:
            return {'success': False, 'chi2_at_meas': chi2_at_meas}

        if not result.success:
            return {'success': False, 'chi2_at_meas': chi2_at_meas}

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
            'chi2': float(result.fun),
            'chi2_at_meas': chi2_at_meas,
        }


def RecoModule(era, cfg={}):
    return TTbarSemilepReconstructor(era, cfg)
