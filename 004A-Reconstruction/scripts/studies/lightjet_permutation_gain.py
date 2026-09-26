#!/usr/bin/env python3
"""How much would the A_C dilution improve if the hadronic W could use ALL
light-jet pairs instead of only leadingJet + subleadingJet?  (issue #40)

Why this study exists
---------------------
sigma(A_C) ~ 1/(D sqrt(N)) with D = 2p-1, p the probability of getting
sign(delta|y|) right. Measured D = 0.212, so every measurement is ~4.7x less
precise than its event count suggests, and no amount of unfolding recovers it.

D factorises as roughly 0.26 (kinematic fit) x 0.85 (charge assignment). The
charge half is fixed (#29). The kinematic half is dominated by the hadronic
side: RecoModule always assigns leadingJet + subleadingJet to the hadronic W,
and those are the right two light jets only ~28% of the time, so in ~72% of
events the fit is handed the wrong hypothesis and cannot recover.

Widening the permutation set is expensive -- 004A is ~33 h wall per era and
004B/004C/005 all have to follow. This measures the payoff first, without
re-running the fit, by substituting the TRUTH-matched hadronic top for the
fitted one and seeing what D becomes.

What it does NOT do: it does not run a wider fit. The truth-assigned number is
an upper bound on what a perfect wider permutation set could reach, and the
"available" fraction below says how much of that a wider set could access at
all. The realised gain will be between the two.

Usage:
    python scripts/studies/lightjet_permutation_gain.py --files 15
"""

import argparse
import glob
import os
import sys

import awkward as ak
import numpy as np
import uproot

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..",
    "005-Unfolding", "scripts"))
import kinematics  # noqa: E402

# 003-ObjectSelectionI selectedObjects, UL2016preVFP
JET_PT_MIN, JET_ETA_MAX, JET_ID = 25.0, 2.4, 6
BTAG_THRESHOLD = 0.2598
DR_MATCH = 0.4

BRANCHES = [
    "Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass", "Jet_btagDeepFlavB",
    "Jet_jetId", "Jet_puId",
    "Muon_pt", "Muon_eta", "Muon_phi", "Muon_charge", "Muon_tightId",
    "Muon_pfRelIso04_all",
    "Top_lep_pt", "Top_lep_eta", "Top_lep_phi", "Top_lep_mass",
    "Top_had_pt", "Top_had_eta", "Top_had_phi", "Top_had_mass",
    "GenPart_pdgId", "GenPart_genPartIdxMother", "GenPart_statusFlags",
    "GenPart_pt", "GenPart_eta", "GenPart_phi", "GenPart_mass",
    "Pgof", "chi2_status",
]

# LHEWeightSign is added by 003-II's producer and is present in the BDTScore
# skims but not in the 004B output of earlySeptember_corrected, which has the
# raw LHEWeight_originalXWGTUP it is derived from. Either works here: the
# dilution is a ratio of counts and sign weights move it by well under a
# percent.
WEIGHT_BRANCHES = ["LHEWeightSign", "LHEWeight_originalXWGTUP"]


def delta_r(eta1, phi1, eta2, phi2):
    dphi = np.abs(phi1 - phi2)
    dphi = np.where(dphi > np.pi, 2 * np.pi - dphi, dphi)
    return np.sqrt((eta1 - eta2) ** 2 + dphi ** 2)


def analyse(path, counters):
    tree = uproot.open(path)["Events"]
    available = [b for b in WEIGHT_BRANCHES if b in tree.keys()]
    if not available:
        raise KeyError(f"{os.path.basename(path)} has none of {WEIGHT_BRANCHES}")
    a = tree.arrays(BRANCHES + available[:1])
    weight_branch = available[0]
    n = len(a)

    # --- per-event python loop for the decay chain; the vectorised version is
    # unreadable and this is a one-off study on a subsample ---
    counts = ak.to_numpy(ak.num(a["GenPart_pdgId"]))
    g_pdg = ak.to_numpy(ak.flatten(a["GenPart_pdgId"]))
    g_mom = ak.to_numpy(ak.flatten(a["GenPart_genPartIdxMother"]))
    g_eta = ak.to_numpy(ak.flatten(a["GenPart_eta"]))
    g_phi = ak.to_numpy(ak.flatten(a["GenPart_phi"]))
    offsets = np.concatenate(([0], np.cumsum(counts)))

    jet_pt = a["Jet_pt"]; jet_eta = a["Jet_eta"]; jet_phi = a["Jet_phi"]
    jet_m = a["Jet_mass"]; jet_b = a["Jet_btagDeepFlavB"]

    jet_ok = ((jet_pt > JET_PT_MIN) & (abs(jet_eta) < JET_ETA_MAX)
              & (a["Jet_jetId"] == JET_ID)
              & ((jet_pt > 50) | (a["Jet_puId"] > 0)))

    mu_ok = ((a["Muon_pt"] > 26) & (abs(a["Muon_eta"]) < 2.4)
             & a["Muon_tightId"] & (a["Muon_pfRelIso04_all"] <= 0.2))
    mu_idx = ak.argmax(ak.where(mu_ok, a["Muon_pt"], -999.0), axis=1, keepdims=True)
    charge = ak.to_numpy(ak.fill_none(ak.firsts(a["Muon_charge"][mu_idx]), 0))

    lep = [ak.to_numpy(a[f"Top_lep_{v}"]) for v in ("pt", "eta", "phi", "mass")]
    had = [ak.to_numpy(a[f"Top_had_{v}"]) for v in ("pt", "eta", "phi", "mass")]
    weight = np.sign(ak.to_numpy(a[weight_branch]).astype(float))

    y_lep_fit = kinematics.rapidity(lep[0], lep[1], lep[3])
    y_had_fit = kinematics.rapidity(had[0], had[1], had[3])

    jp = ak.to_list(jet_pt); je = ak.to_list(jet_eta); jf = ak.to_list(jet_phi)
    jm = ak.to_list(jet_m); jb = ak.to_list(jet_b); jok = ak.to_list(jet_ok)

    y_had_truth = np.full(n, np.nan)
    flags = {k: np.zeros(n, dtype=bool) for k in
             ("all4", "pair_is_topfour", "pair_available", "had_matched")}

    for ev in range(n):
        lo, hi = offsets[ev], offsets[ev + 1]
        p, mo = g_pdg[lo:hi], g_mom[lo:hi]
        eta, phi = g_eta[lo:hi], g_phi[lo:hi]

        # W daughters: quark or lepton whose mother is a W
        mother_pdg = np.where((mo >= 0) & (mo < len(p)), p[np.clip(mo, 0, len(p) - 1)], 0)
        from_w = np.abs(mother_pdg) == 24
        wq = np.flatnonzero(from_w & (np.abs(p) >= 1) & (np.abs(p) <= 5))
        if len(wq) < 2:
            continue
        wq = wq[:2]
        had_w_sign = np.sign(mother_pdg[wq[0]])          # +24 from t, -24 from tbar
        # b from the same top as the hadronic W
        b_had = np.flatnonzero((np.abs(mother_pdg) == 6) & (np.abs(p) == 5)
                               & (np.sign(p) == had_w_sign))
        if len(b_had) == 0:
            continue
        b_had = b_had[0]

        sel = [j for j in range(len(jp[ev])) if jok[ev][j]]
        if len(sel) < 4:
            continue
        order = sorted(sel, key=lambda j: jp[ev][j], reverse=True)
        bj = [j for j in order if jb[ev][j] > BTAG_THRESHOLD][:2]
        lj = [j for j in order if j not in bj][:2]
        if len(bj) < 2 or len(lj) < 2:
            continue

        # greedy unique dR matching of the 3 hadronic partons to selected jets
        targets = {"q1": wq[0], "q2": wq[1], "bh": b_had}
        assigned, used = {}, set()
        pairs = sorted(
            ((delta_r(eta[t], phi[t], je[ev][j], jf[ev][j]), name, j)
             for name, t in targets.items() for j in sel),
            key=lambda x: x[0])
        for dr, name, j in pairs:
            if dr > DR_MATCH or name in assigned or j in used:
                continue
            assigned[name] = j
            used.add(j)

        if len(assigned) == 3:
            flags["had_matched"][ev] = True
            q_jets = {assigned["q1"], assigned["q2"]}
            flags["pair_is_topfour"][ev] = q_jets == set(lj)
            flags["pair_available"][ev] = True
            # truth-assigned hadronic top from the matched jets
            px = py = pz = e = 0.0
            for j in (assigned["q1"], assigned["q2"], assigned["bh"]):
                pt_, eta_, phi_, m_ = jp[ev][j], je[ev][j], jf[ev][j], jm[ev][j]
                px += pt_ * np.cos(phi_); py += pt_ * np.sin(phi_)
                pz += pt_ * np.sinh(eta_)
                e += np.sqrt((pt_ * np.cosh(eta_)) ** 2 + m_ ** 2)
            if e > abs(pz):
                y_had_truth[ev] = 0.5 * np.log((e + pz) / (e - pz))
            flags["all4"][ev] = True

    counters["n"] += n
    for k, v in flags.items():
        counters[k] += int(v.sum())

    return dict(charge=charge, weight=weight, y_lep_fit=y_lep_fit,
                y_had_fit=y_had_fit, y_had_truth=y_had_truth,
                pgof=ak.to_numpy(a["Pgof"]),
                had_matched=flags["had_matched"],
                pair_is_topfour=flags["pair_is_topfour"],
                gen=gen_rapidities(a))


def gen_rapidities(a):
    last = ((a["GenPart_statusFlags"] >> 13) & 1) == 1
    out = {}
    for sign, key in ((6, "t"), (-6, "tbar")):
        m = last & (a["GenPart_pdgId"] == sign)
        vals = [ak.to_numpy(ak.fill_none(ak.firsts(a[f"GenPart_{v}"][m]), np.nan))
                for v in ("pt", "eta", "mass")]
        out[key] = kinematics.rapidity(*vals)
    return out


def dilution(y_t, y_tbar, gen_t, gen_tbar, weight, mask):
    dy = np.abs(y_t) - np.abs(y_tbar)
    dg = np.abs(gen_t) - np.abs(gen_tbar)
    ok = mask & np.isfinite(dy) & np.isfinite(dg) & (dy != 0) & (dg != 0)
    if ok.sum() == 0:
        return np.nan, 0
    agree = weight[ok][np.sign(dy[ok]) == np.sign(dg[ok])].sum() / weight[ok].sum()
    return 2 * agree - 1, int(ok.sum())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files", type=int, default=15)
    parser.add_argument("--era", default="UL2016preVFP")
    parser.add_argument("--input", default=None,
                        help="glob for the ROOT files; defaults to the midNov "
                             "BDTScore skim. Point it at a 004B output to study "
                             "a campaign that has not been BDT-scored yet.")
    parser.add_argument("--label", default=None, help="name for the report header")
    args = parser.parse_args()

    pattern = args.input or (
        f"/mnt/disk1/skimmed_Run2/BDTScore/midNov/{args.era}"
        f"/MC_mu/ttbar_SemiLeptonic/*.root")
    files = sorted(glob.glob(pattern))[:args.files]
    if not files:
        raise SystemExit(f"no files matching {pattern}")

    counters = {k: 0 for k in
                ("n", "all4", "pair_is_topfour", "pair_available", "had_matched")}
    chunks = []
    for i, path in enumerate(files, 1):
        chunks.append(analyse(path, counters))
        print(f"  [{i}/{len(files)}] {os.path.basename(path)}")

    cat = lambda key: np.concatenate([c[key] for c in chunks])
    charge, weight = cat("charge"), cat("weight")
    y_lep_fit, y_had_fit, y_had_truth = cat("y_lep_fit"), cat("y_had_fit"), cat("y_had_truth")
    had_matched, pair_is_topfour = cat("had_matched"), cat("pair_is_topfour")
    gen_t = np.concatenate([c["gen"]["t"] for c in chunks])
    gen_tbar = np.concatenate([c["gen"]["tbar"] for c in chunks])

    yt_fit, ytbar_fit = kinematics.assign_top_antitop(y_lep_fit, y_had_fit, charge)
    yt_tru, ytbar_tru = kinematics.assign_top_antitop(y_lep_fit, y_had_truth, charge)

    total = counters["n"]
    print(f"\n{'=' * 72}")
    print(f"Events analysed: {total:,}  ({len(files)} files, {args.era}"
          f"{', ' + args.label if args.label else ''})")
    print(f"\n  parton matching (DeltaR < {DR_MATCH}, unique assignment):")
    for k, label in (("had_matched", "hadronic side fully matched (b + 2 light)"),
                     ("pair_is_topfour", "  ... and the light pair IS {leadingJet, subleadingJet}")):
        print(f"    {label:<56} {counters[k]:>9,}  {counters[k] / total:6.2%}")
    matched = counters["had_matched"]
    if matched:
        print(f"    {'  ... light pair is some OTHER pair of selected jets':<56} "
              f"{matched - counters['pair_is_topfour']:>9,}  "
              f"{(matched - counters['pair_is_topfour']) / total:6.2%}")
        print(f"\n    of matched events, the current permutation set can reach "
              f"{counters['pair_is_topfour'] / matched:.1%}")
        print(f"    a set over all light-jet pairs could reach               100.0%")

    print(f"\n  dilution D = 2p-1 on the SAME event set (hadronic side matched):")
    d_fit, n1 = dilution(yt_fit, ytbar_fit, gen_t, gen_tbar, weight, had_matched)
    d_tru, n2 = dilution(yt_tru, ytbar_tru, gen_t, gen_tbar, weight, had_matched)
    print(f"    current fit                        D = {d_fit:+.4f}   ({n1:,} events)")
    print(f"    truth-assigned hadronic top        D = {d_tru:+.4f}   ({n2:,} events)")
    if np.isfinite(d_fit) and d_fit != 0:
        print(f"    upper bound on the gain              {d_tru / d_fit:.2f} x")
        print(f"    sigma(A_C) would scale by            {d_fit / d_tru:.2f} x")

    print(f"\n  for reference, all events (no matching requirement):")
    d_all, n3 = dilution(yt_fit, ytbar_fit, gen_t, gen_tbar, weight,
                         np.ones(total, dtype=bool))
    print(f"    current fit                        D = {d_all:+.4f}   ({n3:,} events)")

    # Split by whether the current permutation set had the right pair available.
    # This is the key diagnostic: the difference between the two rows is what a
    # wider permutation set could plausibly recover, while the gap to the
    # truth-assigned number is what it could recover only with a perfect fit.
    print(f"\n  current-fit D split by whether the correct pair was reachable:")
    split = {}
    for label, key, m in (("pair IS {leadingJet, subleadingJet}", "reachable",
                           had_matched & pair_is_topfour),
                          ("pair is a DIFFERENT selected pair", "unreachable",
                           had_matched & ~pair_is_topfour)):
        d, nn = dilution(yt_fit, ytbar_fit, gen_t, gen_tbar, weight, m)
        split[key] = (d, nn)
        print(f"    {label:<40} D = {d:+.4f}   ({nn:,} events)")

    d_match, _ = dilution(yt_fit, ytbar_fit, gen_t, gen_tbar, weight, had_matched)
    print(f"\n{'=' * 72}")
    print("  WHAT A WIDER PERMUTATION SET WOULD BUY (on matched events)")
    print(f"    today                                       D = {d_match:+.4f}")
    if split["reachable"][0] and np.isfinite(split["reachable"][0]):
        print(f"    pessimistic: newly-reachable events end up")
        print(f"      only as good as today's reachable ones    D = "
              f"{split['reachable'][0]:+.4f}   ({split['reachable'][0] / d_match:.2f} x)")
    print(f"    optimistic: hadronic assignment perfect     D = {d_tru:+.4f}   "
          f"({d_tru / d_match:.2f} x)")
    print()
    print(f"    sigma(A_C) scales by 1/D, so full Run 2 goes from ~0.0033 to")
    lo = 0.0033 * d_match / split["reachable"][0] if split["reachable"][0] else float("nan")
    hi = 0.0033 * d_match / d_tru
    print(f"      {lo:.4f} (pessimistic)  ...  {hi:.4f} (optimistic)")
    print(f"    against an SM A_C of 0.006 - 0.010, i.e. "
          f"{0.006 / lo:.1f} - {0.010 / hi:.1f} sigma")
    return 0


if __name__ == "__main__":
    sys.exit(main())
