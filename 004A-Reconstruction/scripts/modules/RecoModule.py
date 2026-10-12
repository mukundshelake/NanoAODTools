import ROOT
import math
import os
import sys
import numpy as np
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module

# HitFit is vendored under <repo>/external/HitFit (build it with its build.sh).
# In a CRAB job there is no repo: hitfit_loader.py is shipped flat next to this
# file and imports from the job's working directory.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_HITFIT_DIR = os.path.join(_REPO_ROOT, "external", "HitFit")
if os.path.isdir(_HITFIT_DIR) and _HITFIT_DIR not in sys.path:
    sys.path.insert(0, _HITFIT_DIR)
from hitfit_loader import make_fitter  # noqa: E402

# chi2_status codes. 2 is retired (it was the degenerate neutrino-pz quadratic
# of the pre-HitFit fit; HitFit solves pz itself).
STATUS_OK = 0
STATUS_BAD_INPUT = 1        # upstream selection sentinel (no muon / jets)
STATUS_NO_CONVERGENCE = 3   # no permutation converged -> event not reconstructed
STATUS_OUTSIDE_RES = 4      # an object's eta is outside its resolution table


class TTbarSemilepReconstructor(Module):
    """tt -> mu + jets reconstruction with HitFit, per CMS-TOP-23-003 Sec. 2.3.1.

    Four fits per event: 2 b-jet assignments x 2 neutrino-pz starting values
    (roots of the leptonic-W quadratic). HitFit treats the neutrino pz as an
    unmeasured parameter and imposes m(l nu) = m(qq') = mW and
    m(t_lep) = m(t_had) as hard constraints, so the fit has ndf = 2.
    Permutations are ranked by chi2 (equivalently Pgof at fixed ndf).
    See 004A-Reconstruction/README.md.
    """

    def __init__(self, era, cfg={}):
        self.mW = cfg.get("mW", 80.4)
        hitfit_cfg = cfg["hitfit"]
        # Resolution table paths may carry an {era} placeholder (per-era tables).
        res = {k: self._resolve(v.format(era=era)) for k, v in hitfit_cfg["resolutions"].items()}
        self.fitter = make_fitter(hitfit_cfg["settings"], res["udsc"], res["b"],
                                  res["muon"], res["kt"], mw=self.mW)

    @staticmethod
    def _resolve(path):
        return path if os.path.isabs(path) else os.path.join(_REPO_ROOT, path)

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        for name in ["Top_lep", "Top_had"]:
            for var in ["pt", "eta", "phi", "mass"]:
                self.out.branch(f"{name}_{var}", "F")
        self.out.branch("Chi2", "F")
        self.out.branch("Pgof", "F")
        self.out.branch("chi2_status", "I")
        self.out.branch("Nu_pz", "F")          # fitted neutrino pz
        self.out.branch("Mt_fit", "F")         # HitFit fitted top mass
        self.out.branch("Mt_fit_err", "F")     # and its uncertainty
        self.out.branch("perm_index", "I")     # 2*(b swapped) + (pz root)
        self.out.branch("n_converged", "I")    # converged permutations, 0-4

    def analyze(self, event):
        met_px = event.MET_pt * math.cos(event.MET_phi)
        met_py = event.MET_pt * math.sin(event.MET_phi)

        # Guard against upstream selection failure: SelectedObjectsProducer fills
        # sentinel pt = -1 when no qualifying muon, or no 2-b-tagged + 2-light
        # split among the selected jets, was found. In production this never
        # fires -- 003-ObjectSelectionI skims those events out -- but it is a
        # cheap safety net against fitting garbage momenta.
        if (event.SelMuon_pt < 0 or event.leadingbJet_pt < 0 or
                event.subleadingbJet_pt < 0 or event.leadingJet_pt < 0 or
                event.subleadingJet_pt < 0):
            self._fill_failure(STATUS_BAD_INPUT)
            return True

        b1 = (event.leadingbJet_pt, event.leadingbJet_eta, event.leadingbJet_phi, event.leadingbJet_mass)
        b2 = (event.subleadingbJet_pt, event.subleadingbJet_eta, event.subleadingbJet_phi, event.subleadingbJet_mass)
        q1 = (event.leadingJet_pt, event.leadingJet_eta, event.leadingJet_phi, event.leadingJet_mass)
        q2 = (event.subleadingJet_pt, event.subleadingJet_eta, event.subleadingJet_phi, event.subleadingJet_mass)
        mu = (event.SelMuon_pt, event.SelMuon_eta, event.SelMuon_phi, event.SelMuon_mass)

        # With a negative discriminant both roots collapse onto the real part,
        # so the second seed would repeat the first fit exactly.
        n_seeds = 2 if self._has_two_pz_roots(mu, met_px, met_py) else 1

        best = None
        best_index = -1
        n_converged = 0
        outside_res = False
        for swap, (b_had, b_lep) in enumerate([(b1, b2), (b2, b1)]):
            # Order expected by the wrapper: hadW quark 1, hadW quark 2, hadronic b, leptonic b.
            jets = (q1, q2, b_had, b_lep)
            arrays = [np.array([j[k] for j in jets], dtype=np.float64) for k in range(4)]
            for seed in range(n_seeds):
                r = self.fitter.fit(*arrays, *mu, met_px, met_py, bool(seed))
                if r.status == 2:
                    outside_res = True
                    continue
                if r.status != 0:
                    continue
                n_converged += 1
                if best is None or r.chi2 < best.chi2:
                    best = r
                    best_index = 2 * swap + seed

        if best is None:
            self._fill_failure(STATUS_OUTSIDE_RES if outside_res else STATUS_NO_CONVERGENCE,
                               n_converged=0)
            return True

        lep_top = ROOT.TLorentzVector(*best.lep_top)
        had_top = ROOT.TLorentzVector(*best.had_top)
        for prefix, obj in [("Top_lep", lep_top), ("Top_had", had_top)]:
            self.out.fillBranch(f"{prefix}_pt",   obj.Pt())
            self.out.fillBranch(f"{prefix}_eta",  obj.Eta())
            self.out.fillBranch(f"{prefix}_phi",  obj.Phi())
            self.out.fillBranch(f"{prefix}_mass", obj.M())
        self.out.fillBranch("Chi2", best.chi2)
        self.out.fillBranch("Pgof", ROOT.TMath.Prob(best.chi2, 2))
        self.out.fillBranch("chi2_status", STATUS_OK)
        self.out.fillBranch("Nu_pz", best.nu_pz)
        self.out.fillBranch("Mt_fit", best.mt)
        self.out.fillBranch("Mt_fit_err", best.sigmt)
        self.out.fillBranch("perm_index", best_index)
        self.out.fillBranch("n_converged", n_converged)
        return True

    def _has_two_pz_roots(self, mu, met_px, met_py):
        """True if the leptonic-W quadratic for the neutrino pz has two real roots."""
        pt, eta, phi, m = mu
        pxl, pyl, pzl = pt * math.cos(phi), pt * math.sin(phi), pt * math.sinh(eta)
        El = math.sqrt(pxl**2 + pyl**2 + pzl**2 + m**2)
        a = self.mW**2 + 2 * (pxl * met_px + pyl * met_py)
        A = 4 * (El**2 - pzl**2)
        B = -4 * a * pzl
        C = 4 * El**2 * (met_px**2 + met_py**2) - a**2
        return B * B - 4 * A * C >= 0

    def _fill_failure(self, chi2_status, n_converged=0):
        """Fill all output branches with sentinel values for events without a fit."""
        for prefix in ["Top_lep", "Top_had"]:
            for var in ["pt", "eta", "phi", "mass"]:
                self.out.fillBranch(f"{prefix}_{var}", -1)
        self.out.fillBranch("Chi2",        -1)
        self.out.fillBranch("Pgof",        -1)
        self.out.fillBranch("chi2_status", chi2_status)
        self.out.fillBranch("Nu_pz",       -1)
        self.out.fillBranch("Mt_fit",      -1)
        self.out.fillBranch("Mt_fit_err",  -1)
        self.out.fillBranch("perm_index",  -1)
        self.out.fillBranch("n_converged", n_converged)


def RecoModule(era, cfg={}):
    return TTbarSemilepReconstructor(era, cfg)
