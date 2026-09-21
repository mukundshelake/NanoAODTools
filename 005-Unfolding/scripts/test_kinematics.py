#!/usr/bin/env python3
"""Tests for kinematics.py.

Run directly (`python scripts/test_kinematics.py`) or under pytest.

The important one is test_charge_convention_against_gen_truth, which the
inverted-assignment bug (issue #29) motivated: it pins the t/tbar convention
against *gen truth on real MC*, so a future edit that flips it back fails
loudly instead of quietly flipping the sign of A_C. That test needs the
UL2016preVFP BDTScore skim on local disk and skips if it is absent.
"""

import glob
import os

import numpy as np

import kinematics as k

SKIM = ("/mnt/disk1/skimmed_Run2/BDTScore/midNov/UL2016preVFP/"
        "MC_mu/ttbar_SemiLeptonic/*.root")

# dR matching is imperfect -- tau->mu and b->mu cascades put ~15% of muons on
# the "wrong" side -- so the bar is "clearly better than chance", not perfect.
# With the convention inverted this figure is ~15%, so the gap is unmissable.
MIN_MATCH_RATE = 0.75


def test_rapidity_rejects_failure_sentinels():
    # RecoModule._fill_failure() writes -1 for pt/eta/phi/mass. The naive
    # 0.5*log((E+pz)/(E-pz)) turns that into an innocuous y = 0.757, silently
    # classifying a failed fit as a central top (issue #42).
    y = k.rapidity(np.array([-1.0]), np.array([-1.0]), np.array([-1.0]))
    assert np.isnan(y[0]), f"expected nan for the -1 sentinel, got {y[0]}"


def test_rapidity_matches_closed_form():
    pt, eta, mass = np.array([200.0]), np.array([0.5]), np.array([172.5])
    pz = pt * np.sinh(eta)
    e = np.sqrt((pt * np.cosh(eta)) ** 2 + mass ** 2)
    assert np.allclose(k.rapidity(pt, eta, mass), 0.5 * np.log((e + pz) / (e - pz)))


def test_rapidity_is_odd_under_eta_flip():
    eta = np.array([-2.0, -0.3, 0.0, 0.3, 2.0])
    pt = np.full(5, 150.0)
    mass = np.full(5, 172.5)
    assert np.allclose(k.rapidity(pt, eta, mass), -k.rapidity(pt, -eta, mass))


def test_invariant_mass_of_back_to_back_pair():
    # Two 172.5 GeV objects, back to back at pT = 0 in the lab: m = 2*172.5.
    p1 = (np.array([0.0]), np.array([0.0]), np.array([0.0]), np.array([172.5]))
    p2 = (np.array([0.0]), np.array([0.0]), np.array([np.pi]), np.array([172.5]))
    assert np.allclose(k.invariant_mass(p1, p2), 345.0)


def test_assign_top_antitop_is_a_pure_swap():
    y_lep, y_had = np.array([1.0, 1.0]), np.array([2.0, 2.0])
    y_t, y_tb = k.assign_top_antitop(y_lep, y_had, np.array([+1, -1]))
    # mu+ : leptonic side is the top.  mu- : leptonic side is the antitop.
    assert (y_t[0], y_tb[0]) == (1.0, 2.0)
    assert (y_t[1], y_tb[1]) == (2.0, 1.0)


def test_assign_top_antitop_without_a_muon_is_nan():
    y_t, y_tb = k.assign_top_antitop(np.array([1.0]), np.array([2.0]), np.array([0]))
    assert np.isnan(y_t[0]) and np.isnan(y_tb[0])


def test_charge_convention_against_gen_truth():
    """mu+ must select the top, checked by dR-matching Top_lep to the gen t/tbar.

    This is the regression guard for issue #29. Inverting the convention in
    kinematics.assign_top_antitop turns the measured rate from ~0.85 to ~0.15
    and fails this test.
    """
    import awkward as ak
    import uproot

    files = sorted(glob.glob(SKIM))[:3]
    if not files:
        print("    (skipped: BDTScore skim not on local disk)")
        return

    branches = ["Top_lep_eta", "Top_lep_phi", "Top_lep_pt", "Top_lep_mass",
                "Top_had_eta", "Top_had_phi", "Top_had_pt", "Top_had_mass",
                "Muon_charge", "Muon_pt", "Muon_eta", "Muon_tightId",
                "Muon_pfRelIso04_all",
                "GenPart_pdgId", "GenPart_statusFlags",
                "GenPart_eta", "GenPart_phi"]
    a = uproot.concatenate([f + ":Events" for f in files], branches)

    index, has_muon = k.select_muon_index(
        a["Muon_pt"], a["Muon_eta"], a["Muon_tightId"],
        a["Muon_pfRelIso04_all"], era="UL2016preVFP",
    )
    charge = ak.to_numpy(ak.fill_none(ak.firsts(a["Muon_charge"][index]), 0)).astype(int)

    last_copy = ((a["GenPart_statusFlags"] >> 13) & 1) == 1

    def gen(pdg_id, field):
        sel = last_copy & (a["GenPart_pdgId"] == pdg_id)
        return ak.to_numpy(ak.fill_none(ak.firsts(a[field][sel]), np.nan))

    def delta_r(eta1, phi1, eta2, phi2):
        dphi = np.abs(phi1 - phi2)
        dphi = np.where(dphi > np.pi, 2 * np.pi - dphi, dphi)
        return np.sqrt((eta1 - eta2) ** 2 + dphi ** 2)

    lep_eta = ak.to_numpy(a["Top_lep_eta"])
    lep_phi = ak.to_numpy(a["Top_lep_phi"])
    d_top = delta_r(lep_eta, lep_phi, gen(6, "GenPart_eta"), gen(6, "GenPart_phi"))
    d_atop = delta_r(lep_eta, lep_phi, gen(-6, "GenPart_eta"), gen(-6, "GenPart_phi"))

    usable = has_muon & (charge != 0) & np.isfinite(d_top) & np.isfinite(d_atop)
    leptonic_is_top = d_top < d_atop

    # The assignment under test, expressed on the same events: feed it a
    # marker rapidity for the leptonic side and see which slot it lands in.
    marker_lep = np.ones(len(charge))
    marker_had = np.zeros(len(charge))
    y_t, _ = k.assign_top_antitop(marker_lep, marker_had, charge)
    assignment_says_leptonic_is_top = y_t == 1.0

    agree = (assignment_says_leptonic_is_top == leptonic_is_top)[usable].mean()
    print(f"    convention agrees with gen truth in {agree:.1%} of {usable.sum():,} events")
    assert agree > MIN_MATCH_RATE, (
        f"t/tbar assignment agrees with gen truth only {agree:.1%} of the time "
        f"(expected >{MIN_MATCH_RATE:.0%}). The convention is probably inverted: "
        "t -> W+ b -> mu+, so mu+ means the LEPTONIC side is the TOP. See issue #29."
    )


def test_select_muon_index_rejects_unknown_era():
    import awkward as ak
    empty = ak.Array([[]])
    try:
        k.select_muon_index(empty, empty, empty, empty, era="UL2015")
    except ValueError:
        return
    raise AssertionError("expected ValueError for an unknown era")


def test_muon_thresholds_match_002_preselection():
    """Thresholds here must not drift from 002-Samples/config.yaml."""
    import re
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "..", "002-Samples", "config.yaml")
    if not os.path.exists(path):
        print("    (skipped: 002-Samples/config.yaml not found)")
        return
    import yaml
    with open(path) as fh:
        cuts = yaml.safe_load(fh)["Pre-SelectionCuts"]
    for era, expected in k.MUON_PT_THRESHOLD.items():
        found = float(re.search(r"Muon_pt > ([0-9.]+)", cuts[era]["muonCut"]).group(1))
        assert found == expected, f"{era}: 002 says {found}, kinematics.py says {expected}"


if __name__ == "__main__":
    tests = [v for kk, v in sorted(globals().items()) if kk.startswith("test_")]
    failed = 0
    for test in tests:
        try:
            test()
            print(f"  PASS  {test.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"  FAIL  {test.__name__}: {exc}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    raise SystemExit(1 if failed else 0)
