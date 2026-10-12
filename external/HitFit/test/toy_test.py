#!/usr/bin/env python3
"""Toy calibration test of the vendored HitFit through the flat wrapper.

Generates tt -> mu + 4 jets events with HitFit's own toy generator (gentop),
smeared with the resolutions in toy_res_*.txt, fits the TRUE jet assignment
with both neutrino-pz seeds through hitfit_wrap::Fitter (lepw = hadw = 80.4,
top_mass = 0, equal_side = true), keeps the better seed, and checks the chi2
against chi2(ndf = 2) -- the TOP-23-003 lepton+jets fit has 17 measured
quantities, 18 parameters and 3 constraints.

Pass criteria (loose, statistical): >= 98% of events converge, the fraction with
Pgof = exp(-chi2/2) > 0.2 is within 0.05 of 0.80, and the median chi2 is within
0.25 of 1.386. Exit code 1 on failure.
"""

import argparse
import math
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from hitfit_loader import ROOT, make_fitter, load, HITFIT_DIR, write_settings  # noqa: E402

# Fitter settings: the same values 004A uses (config.yaml Modules.reconstruction.hitfit).
SETTINGS = {
    "mwhad_min_cut": 0, "mwhad_max_cut": 10000, "mtdiff_max_cut": 10000, "jet_mass_cut": 10000,
    "nkeep": 1680, "solve_nu_tmass": False, "do_higgs_flag": False, "print_event_flag": False,
    "bmass": 4.7, "equal_side": True,
    "use_e": True, "e_com": 13000, "ignore_met": False,
    "printfit": False, "use_G": False, "constraint_sum_eps": 0.01, "chisq_diff_eps": 0.01,
    "maxit": 10000, "maxcut": 20, "cutsize": 0.5, "min_tot_cutsize": 1e-10, "chisq_test_eps": 1e-5,
    "test_gradient": False, "test_step": 0.002, "test_eps": 0.035,
}

# Toy generator settings (gentop). Smearing strings must match toy_res_*.txt.
GENTOP = {
    "t_pt_mean": 80, "mt": 172.5, "sigma_mt": 0, "mh": 125, "sigma_mh": 0, "recoil_pt_mean": 15,
    "boost_sigma": 0.7, "m_boost": 500, "mb": 4.7, "sigma_mb": 0, "mw": 80.4, "sigma_mw": 0,
    "svx_tageff": 1, "smear": True, "smear_dir": True, "muon": True,
    "ele_res_str": "0.02/0,0,0.001/0,0,0.001",
    "muo_res_str": "0.02/0,0,0.001/0,0,0.001",
    "jet_res_str": "0.08,1.0,2.0/0,0,0.03/0,0,0.03",
    "kt_res_str": "0,0,12",
}

TOY_CPP = r'''
#include "TopQuarkAnalysis/TopHitFit/interface/Defaults_Text.h"
#include "TopQuarkAnalysis/TopHitFit/interface/gentop.h"
#include "TopQuarkAnalysis/TopHitFit/interface/Lepjets_Event.h"
#include "CLHEP/Random/JamesRandom.h"
// Test scaffolding: one gentop event as flat numbers. out[0..15] = jets
// (pt, eta, phi, m) in the order hadW1, hadW2, hadB, lepB; out[16..19] = muon
// (pt, eta, phi, m); out[20..21] = MET (px, py).
struct HitFitToy {
  hitfit::Defaults_Text defs; hitfit::Gentop_Args args; CLHEP::HepJamesRandom engine;
  HitFitToy(const char* cfg, long seed) : defs(cfg), args(defs), engine(seed) {}
  void next(double* out) {
    hitfit::Lepjets_Event ev = hitfit::gentop(args, engine);
    const int want[4] = {hitfit::hadw1_label, hitfit::hadw2_label, hitfit::hadb_label, hitfit::lepb_label};
    for (int k = 0; k < 4; ++k)
      for (std::size_t j = 0; j < ev.njets(); ++j)
        if (ev.jet(j).type() == want[k]) {
          const auto& p = ev.jet(j).p();
          out[4*k] = p.perp(); out[4*k+1] = p.pseudoRapidity(); out[4*k+2] = p.phi(); out[4*k+3] = p.m();
        }
    const auto& l = ev.lep(0).p();
    out[16] = l.perp(); out[17] = l.pseudoRapidity(); out[18] = l.phi(); out[19] = l.m();
    out[20] = ev.met().x(); out[21] = ev.met().y();
  }
};
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--events", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=12345)
    args = parser.parse_args()

    load()
    # build.sh passes the CLHEP include dir of whichever environment it built in.
    clhep_include = os.environ.get("HITFIT_CLHEP_INCLUDE",
                                   os.path.join(HITFIT_DIR, "envs", "clhep", "include"))
    for path in (os.path.join(HITFIT_DIR, "include"), clhep_include):
        ROOT.gInterpreter.AddIncludePath(path)
    if not ROOT.gInterpreter.Declare(TOY_CPP):
        sys.exit("could not declare the toy generator")

    toy_cfg = os.path.join(HERE, "_gentop_settings.txt")
    write_settings(GENTOP, toy_cfg)
    try:
        toy = ROOT.HitFitToy(toy_cfg, args.seed)
    finally:
        os.remove(toy_cfg)

    res = {k: os.path.join(HERE, f"toy_res_{k}.txt") for k in ("jet", "muon", "kt")}
    fitter = make_fitter(SETTINGS, res["jet"], res["jet"], res["muon"], res["kt"], mw=80.4)

    buf = np.zeros(22)
    chi2 = []
    for _ in range(args.events):
        toy.next(buf)
        jets = [np.ascontiguousarray(buf[k:16:4]) for k in range(4)]
        best = None
        for nuz in (False, True):
            r = fitter.fit(*jets, *buf[16:20], buf[20], buf[21], nuz)
            if r.status == 0 and (best is None or r.chi2 < best):
                best = r.chi2
        if best is not None:
            chi2.append(best)

    chi2 = np.asarray(chi2)
    conv = len(chi2) / args.events
    frac = float(np.mean(np.exp(-0.5 * chi2) > 0.2))
    median = float(np.median(chi2))
    print(f"converged {len(chi2)}/{args.events} ({conv:.3f}); mean chi2 {chi2.mean():.3f} (chi2(2): 2); "
          f"median {median:.3f} (chi2(2): {2*math.log(2):.3f}); Pgof>0.2 {frac:.3f} (chi2(2): 0.800)")
    ok = conv >= 0.98 and abs(frac - 0.80) < 0.05 and abs(median - 2 * math.log(2)) < 0.25
    print("PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
