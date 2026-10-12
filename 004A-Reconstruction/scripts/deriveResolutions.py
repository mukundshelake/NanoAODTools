#!/usr/bin/env python3
"""Derive HitFit resolution tables for one era from ttbar_SemiLeptonic MC (#50).

HitFit fits the momenta of the tt decay *partons*, so the resolutions it needs
are reco-object vs parton (jets: including fragmentation and out-of-cone
losses), not detector resolutions such as JER (reco jet vs gen jet). This is
how CMS derived HitFit's original tqaf tables, with b and light (udsc) jets
kept separate. MC jets here are already JER-smeared by 003-ObjectSelectionI,
so the tables describe data as well.

Matching (truth, as in CMS-TOP-23-003): the b quarks from the two tops and the
quarks from the hadronic W -- the direct decay daughters, i.e. the partons
whose masses the fit constrains -- are matched within DeltaR < 0.3 to the four jets
RecoModule fits (leading/subleadingbJet <-> b quarks, leading/subleadingJet
<-> W quarks, either order); the selected muon to the prompt last-copy gen
muon within DeltaR < 0.1. Only matched objects enter the tables.

Quantities, in the form HitFit evaluates them (Vector_Resolution with the
"/et" flag; Resolution sigma(x) = sqrt(C^2 x^2 + R^2 x + N^2)):
  * pT:      jets sigma(pT) as a function of pT; muons sigma(1/pT) as a
             function of 1/pT (curvature resolution, HitFit inverse flag);
  * eta/phi: sigma as a function of 1/pT (HitFit "inverse" flag, written as a
             leading minus on C), i.e. sqrt(C^2/pT^2 + R^2/pT + N^2);
  * kt:      the transverse recoil kt = muon + 4 jets + MET, compared to the
             generator tt pT, per x/y component, as a function of |kt_x|,
             from events with all four jets correctly matched.
Widths are binned in the TRUE quantity (parton/gen pT, generator kt): binning
in the measured one moves mismeasured objects into the wrong bins and fakes a
large constant term. HitFit then evaluates them at the measured value.
The width in each bin is the half-width of the central 68% interval of the
residual (robust against non-Gaussian tails); mean pT responses are reported.

Output (per era, in --outDir): {era}_udscJetResolution.txt,
{era}_bJetResolution.txt, {era}_muonResolution.txt, {era}_ktResolution.txt in
HitFit EtaDepResolution format (eta bins mirrored to negative eta), plus
{era}_resolution_summary.json with the binned widths and fit parameters.

Run in latestcoffea:
    python scripts/deriveResolutions.py --era UL2018 --selectionIITag earlySeptember_rocUL \
        --selectionIIHash 37f710b13012 --nFiles 4
"""

import argparse
import glob
import json
import os
import sys

import awkward as ak
import numpy as np
import uproot
from scipy.optimize import nnls

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import utils  # noqa: E402

CHAPTER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LAST_COPY = 1 << 13
IS_PROMPT = 1 << 0
PARTON_DR = 0.3
MUON_DR = 0.1

JET_ETA_EDGES = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5]
MUON_ETA_EDGES = [0.0, 0.9, 1.2, 2.1, 2.5]
JET_PT_EDGES = [25, 30, 35, 40, 45, 50, 60, 70, 80, 95, 110, 130, 160, 200, 260, 350, 500]
MUON_PT_EDGES = [26, 30, 35, 40, 45, 50, 60, 70, 80, 100, 130, 180, 300]
KT_EDGES = [0, 5, 10, 15, 20, 30, 40, 55, 75, 100, 150, 250]
MIN_ENTRIES = 200

BRANCHES = (
    ["GenPart_pdgId", "GenPart_statusFlags", "GenPart_genPartIdxMother",
     "GenPart_pt", "GenPart_eta", "GenPart_phi", "GenPart_mass",
     "SelMuon_pt", "SelMuon_eta", "SelMuon_phi", "MET_pt", "MET_phi"]
    + [f"{j}_{v}" for j in ("leadingbJet", "subleadingbJet", "leadingJet", "subleadingJet")
       for v in ("pt", "eta", "phi", "mass")]
)


# --------------------------------------------------------------------------- truth

def _delta_r(eta1, phi1, eta2, phi2):
    dphi = np.abs(phi1 - phi2)
    dphi = np.where(dphi > np.pi, 2 * np.pi - dphi, dphi)
    return np.hypot(eta1 - eta2, dphi)


def _first(mask, values):
    """First value in each event where mask is true, else NaN."""
    return ak.to_numpy(ak.fill_none(ak.firsts(values[mask]), np.nan)).astype(float)


def gen_truth(a):
    """Per-event parton and lepton truth as flat numpy arrays (NaN where absent)."""
    pdg, flags, mom = a["GenPart_pdgId"], a["GenPart_statusFlags"], a["GenPart_genPartIdxMother"]
    safe_mom = ak.where(mom >= 0, mom, 0)
    mom_pdg = ak.where(mom >= 0, pdg[safe_mom], 0)

    # Quarks whose direct mother is a W: exactly two, from the same W sign.
    wq = (abs(pdg) >= 1) & (abs(pdg) <= 5) & (abs(mom_pdg) == 24)
    n_wq = ak.sum(wq, axis=1)
    w_sign = ak.fill_none(ak.firsts(np.sign(mom_pdg[wq])), 0)
    same_w = ak.all(np.sign(mom_pdg[wq]) == w_sign, axis=1)
    semilep = ak.to_numpy((n_wq == 2) & same_w)

    # b quarks as they come out of the top decay (direct daughters of a top),
    # like the W quarks above: HitFit's m(bW) = m(t) constraint holds for these,
    # not for the post-radiation last copy.
    is_b = (abs(pdg) == 5) & (abs(mom_pdg) == 6)
    sign = ak.to_numpy(w_sign)
    t = {}
    for v in ("pt", "eta", "phi"):
        arr = a[f"GenPart_{v}"]
        t[f"q1_{v}"] = _first(wq, arr)
        t[f"q2_{v}"] = ak.to_numpy(ak.fill_none(ak.firsts(arr[wq][:, 1:]), np.nan)).astype(float)
        bplus, bminus = _first(is_b & (pdg == 5), arr), _first(is_b & (pdg == -5), arr)
        # t -> b W+: the hadronic b has the W's sign.
        t[f"bhad_{v}"] = np.where(sign > 0, bplus, bminus)
        t[f"blep_{v}"] = np.where(sign > 0, bminus, bplus)
    t["semilep"] = semilep & np.isfinite(t["bhad_pt"]) & np.isfinite(t["blep_pt"])

    # Prompt last-copy gen muons; the one closest to the selected muon.
    is_mu = (abs(pdg) == 13) & ((flags & LAST_COPY) != 0) & ((flags & IS_PROMPT) != 0)
    mu_eta, mu_phi, mu_pt = a["GenPart_eta"][is_mu], a["GenPart_phi"][is_mu], a["GenPart_pt"][is_mu]
    dr = _delta_r(mu_eta, mu_phi, a["SelMuon_eta"], a["SelMuon_phi"])
    best = ak.argmin(dr, axis=1, keepdims=True)
    t["mu_dr"] = ak.to_numpy(ak.fill_none(ak.firsts(dr[best]), np.inf)).astype(float)
    for v, arr in (("pt", mu_pt), ("eta", mu_eta), ("phi", mu_phi)):
        t[f"genmu_{v}"] = ak.to_numpy(ak.fill_none(ak.firsts(arr[best]), np.nan)).astype(float)

    # Generator tt transverse momentum = true kt.
    is_top = (abs(pdg) == 6) & ((flags & LAST_COPY) != 0)
    pt, phi = a["GenPart_pt"][is_top], a["GenPart_phi"][is_top]
    n_top = ak.to_numpy(ak.num(pt))
    t["ktgen_x"] = np.where(n_top == 2, ak.to_numpy(ak.sum(pt * np.cos(phi), axis=1)), np.nan)
    t["ktgen_y"] = np.where(n_top == 2, ak.to_numpy(ak.sum(pt * np.sin(phi), axis=1)), np.nan)
    return t


def match_pair(p1, p2, j1, j2):
    """One-to-one DeltaR < PARTON_DR match of two partons to two jets.

    Returns (ok, crossed): crossed means p1<->j2, p2<->j1. Closer pairing wins.
    """
    s = (_delta_r(p1[0], p1[1], j1[0], j1[1]), _delta_r(p2[0], p2[1], j2[0], j2[1]))
    c = (_delta_r(p1[0], p1[1], j2[0], j2[1]), _delta_r(p2[0], p2[1], j1[0], j1[1]))
    straight = (s[0] < PARTON_DR) & (s[1] < PARTON_DR)
    cross = (c[0] < PARTON_DR) & (c[1] < PARTON_DR)
    crossed = cross & (~straight | (c[0] + c[1] < s[0] + s[1]))
    return straight | cross, crossed


# --------------------------------------------------------------------------- collection

def collect(files, max_events):
    """Matched (reco, truth) pairs for udsc jets, b jets, muons and kt."""
    out = {k: {"pt": [], "eta": [], "phi": [], "tpt": [], "teta": [], "tphi": []}
           for k in ("udsc", "b", "muon")}
    out["kt"] = {"meas": [], "true": []}
    n_events = n_semilep = 0
    for path in files:
        a = uproot.open(path)["Events"].arrays(BRANCHES, entry_stop=max_events - n_events if max_events else None)
        n_events += len(a)
        t = gen_truth(a)
        n_semilep += int(t["semilep"].sum())
        j = {name: {v: ak.to_numpy(a[f"{name}_{v}"]).astype(float) for v in ("pt", "eta", "phi", "mass")}
             for name in ("leadingbJet", "subleadingbJet", "leadingJet", "subleadingJet")}

        def jet(name):
            return (j[name]["eta"], j[name]["phi"])

        okb, crb = match_pair((t["bhad_eta"], t["bhad_phi"]), (t["blep_eta"], t["blep_phi"]),
                              jet("leadingbJet"), jet("subleadingbJet"))
        okq, crq = match_pair((t["q1_eta"], t["q1_phi"]), (t["q2_eta"], t["q2_phi"]),
                              jet("leadingJet"), jet("subleadingJet"))
        okb &= t["semilep"]
        okq &= t["semilep"]

        def add(kind, ok, jet_name, part):
            sel = ok
            out[kind]["pt"].append(j[jet_name]["pt"][sel])
            out[kind]["eta"].append(j[jet_name]["eta"][sel])
            out[kind]["phi"].append(j[jet_name]["phi"][sel])
            for v in ("pt", "eta", "phi"):
                out[kind]["t" + v].append(part[v][sel])

        def parton(prefix):
            return {v: t[f"{prefix}_{v}"] for v in ("pt", "eta", "phi")}

        def pick(cond, p_if, p_else):
            return {v: np.where(cond, p_if[v], p_else[v]) for v in ("pt", "eta", "phi")}

        # leadingbJet gets bhad unless crossed; subleadingbJet the other one.
        add("b", okb, "leadingbJet", pick(crb, parton("blep"), parton("bhad")))
        add("b", okb, "subleadingbJet", pick(crb, parton("bhad"), parton("blep")))
        add("udsc", okq, "leadingJet", pick(crq, parton("q2"), parton("q1")))
        add("udsc", okq, "subleadingJet", pick(crq, parton("q1"), parton("q2")))

        mu = {v: ak.to_numpy(a[f"SelMuon_{v}"]).astype(float) for v in ("pt", "eta", "phi")}
        okmu = (t["mu_dr"] < MUON_DR) & (mu["pt"] > 0)
        out["muon"]["pt"].append(mu["pt"][okmu])
        out["muon"]["eta"].append(mu["eta"][okmu])
        out["muon"]["phi"].append(mu["phi"][okmu])
        for v in ("pt", "eta", "phi"):
            out["muon"]["t" + v].append(t[f"genmu_{v}"][okmu])

        # kt = muon + 4 jets + MET (what HitFit computes), vs generator tt pT.
        met = ak.to_numpy(a["MET_pt"]).astype(float)
        metphi = ak.to_numpy(a["MET_phi"]).astype(float)
        ktx = mu["pt"] * np.cos(mu["phi"]) + met * np.cos(metphi)
        kty = mu["pt"] * np.sin(mu["phi"]) + met * np.sin(metphi)
        for name in j:
            ktx += j[name]["pt"] * np.cos(j[name]["phi"])
            kty += j[name]["pt"] * np.sin(j[name]["phi"])
        okkt = okb & okq & okmu & np.isfinite(t["ktgen_x"])
        out["kt"]["meas"] += [ktx[okkt], kty[okkt]]
        out["kt"]["true"] += [t["ktgen_x"][okkt], t["ktgen_y"][okkt]]
        print(f"  {os.path.basename(path)}: {len(a)} events, b {okb.sum()} pairs, "
              f"udsc {okq.sum()} pairs, muon {okmu.sum()}", flush=True)
        if max_events and n_events >= max_events:
            break
    for kind in out:
        for k in out[kind]:
            out[kind][k] = np.concatenate(out[kind][k]) if out[kind][k] else np.array([])
    return out, n_events, n_semilep


# --------------------------------------------------------------------------- fitting

def width68(x):
    lo, hi = np.percentile(x, [15.865, 84.135])
    return 0.5 * (hi - lo)


def binned_widths(xbin, resid, edges):
    """(bin centre, width, width error, n) per bin with enough entries."""
    rows = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        sel = (xbin >= lo) & (xbin < hi)
        n = int(sel.sum())
        if n < MIN_ENTRIES:
            continue
        w = width68(resid[sel])
        rows.append((float(np.median(xbin[sel])), float(w), float(w / np.sqrt(2 * n)), n))
    return rows


def fit_crn(rows, inverse):
    """Fit sigma = sqrt(C^2 x^2 + R^2 x + N^2), x = pT (or 1/pT if inverse).

    sigma^2 is linear in (C^2, R^2, N^2), so this is a weighted non-negative
    linear least-squares problem (scipy nnls): exact, no starting values, and
    C^2, R^2, N^2 >= 0 by construction.
    """
    x = np.array([r[0] for r in rows])
    y = np.array([r[1] for r in rows])
    e = np.array([r[2] for r in rows])
    if inverse:
        x = 1.0 / x
    s2_err = 2.0 * y * e                      # error on sigma^2
    design = np.column_stack([x * x, x, np.ones_like(x)]) / s2_err[:, None]
    coef, _ = nnls(design, y * y / s2_err)
    return [float(np.sqrt(v)) for v in coef]


def fmt_res(c, r, n, inverse=False):
    return ("-" if inverse else "") + f"{c:.8f},{r:.8f},{n:.8f}"


def derive_object(d, eta_edges, pt_edges, inverse_pt=False):
    """Per-|eta|-bin fits for pT and eta/phi (sigma vs 1/pT).

    inverse_pt (muons): the curvature resolution makes sigma(pT) grow ~pT^2,
    which the C,R,N form cannot follow, so fit sigma(1/pT) vs 1/pT instead.
    HitFit then fits the object in 1/p, like its own tqaf muon tables.
    """
    bins = []
    pt, eta = d["tpt"], np.abs(d["teta"])   # bin in the true quantity
    dpt = (1.0 / d["pt"] - 1.0 / d["tpt"]) if inverse_pt else (d["pt"] - d["tpt"])
    deta = d["eta"] - d["teta"]
    dphi = np.angle(np.exp(1j * (d["phi"] - d["tphi"])))
    for lo, hi in zip(eta_edges[:-1], eta_edges[1:]):
        sel = (eta >= lo) & (eta < hi)
        rows = {k: binned_widths(pt[sel], r[sel], pt_edges)
                for k, r in (("pt", dpt), ("eta", deta), ("phi", dphi))}
        if min(len(v) for v in rows.values()) < 3:
            raise RuntimeError(f"too few populated pT bins in |eta| [{lo},{hi})")
        fits = {"pt": fit_crn(rows["pt"], inverse_pt), "eta": fit_crn(rows["eta"], True),
                "phi": fit_crn(rows["phi"], True)}
        response = float(np.median(d["pt"][sel] / d["tpt"][sel]))
        bins.append({"eta_lo": lo, "eta_hi": hi, "n": int(sel.sum()), "fits": fits, "inverse_pt": inverse_pt,
                     "binned": rows, "median_pt_response": response})
    return bins


def write_etadep(path, bins, header):
    """HitFit EtaDepResolution file; |eta| bins mirrored to negative eta."""
    lines = [f"# {line}" for line in header]
    entries = []
    for b in bins:
        f = b["fits"]
        vecres = f"{fmt_res(*f['pt'], inverse=b['inverse_pt'])}/{fmt_res(*f['eta'], inverse=True)}/{fmt_res(*f['phi'], inverse=True)}/et"
        entries.append((b["eta_lo"], b["eta_hi"], vecres))
        entries.append((-b["eta_hi"], -b["eta_lo"], vecres))
    entries.sort()
    for i, (lo, hi, vecres) in enumerate(entries):
        lines += ["", f"etadep_etamin{i} = {lo:.4f}", f"etadep_etamax{i} = {hi:.4f}", f"etadep_vecres{i} = {vecres}"]
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--era", required=True)
    parser.add_argument("--selectionIITag", required=True)
    parser.add_argument("--selectionIIHash", required=True)
    parser.add_argument("--nFiles", type=int, default=4)
    parser.add_argument("--maxEvents", type=int, default=0, help="0 = all events in the files")
    parser.add_argument("--outDir", default=os.path.join(CHAPTER_DIR, "resolutions"))
    args = parser.parse_args()

    config = utils.load_config(os.path.join(CHAPTER_DIR, "config.yaml"))
    storage = utils.resolve_storage_path(config)
    pattern = os.path.join(storage, "selectionII", args.selectionIITag, args.selectionIIHash, args.era,
                           "MC_mu", "SemiLeptonic", "ttbar_SemiLeptonic", "*.root")
    files = sorted(glob.glob(pattern))[:args.nFiles]
    if not files:
        sys.exit(f"no files match {pattern}")
    print(f"{args.era}: {len(files)} files from {os.path.dirname(pattern)}")

    data, n_events, n_semilep = collect(files, args.maxEvents)
    jets_b = derive_object(data["b"], JET_ETA_EDGES, JET_PT_EDGES)
    jets_q = derive_object(data["udsc"], JET_ETA_EDGES, JET_PT_EDGES)
    muons = derive_object(data["muon"], MUON_ETA_EDGES, MUON_PT_EDGES, inverse_pt=True)

    kt_resid = data["kt"]["meas"] - data["kt"]["true"]
    kt_rows = binned_widths(np.abs(data["kt"]["true"]), kt_resid, KT_EDGES)
    kt_fit = fit_crn(kt_rows, False)

    os.makedirs(args.outDir, exist_ok=True)
    src = (f"source: {args.selectionIITag}/{args.selectionIIHash} ttbar_SemiLeptonic, {len(files)} files, "
           f"{n_events} events ({n_semilep} semileptonic with all partons)")
    common = [f"HitFit resolutions for {args.era}, derived by 004A-Reconstruction/scripts/deriveResolutions.py (#50).",
              src, f"reco vs parton (DeltaR < {PARTON_DR}); sigma(pT) vs pT, eta/phi vs 1/pT (leading '-'); /et."]
    write_etadep(os.path.join(args.outDir, f"{args.era}_udscJetResolution.txt"), jets_q,
                 common + ["Light (udsc) jets: W-decay quarks matched to leading/subleadingJet."])
    write_etadep(os.path.join(args.outDir, f"{args.era}_bJetResolution.txt"), jets_b,
                 common + ["b jets: top-decay b quarks matched to leading/subleadingbJet."])
    write_etadep(os.path.join(args.outDir, f"{args.era}_muonResolution.txt"), muons,
                 [c.replace("parton (DeltaR < 0.3)", f"prompt gen muon (DeltaR < {MUON_DR})") for c in common]
                 + ["Muon: SelMuon vs prompt last-copy gen muon."])
    with open(os.path.join(args.outDir, f"{args.era}_ktResolution.txt"), "w") as fh:
        fh.write("\n".join(f"# {c}" for c in common[:2]) + "\n"
                 "# kt = muon + 4 fitted jets + MET vs generator tt pT, per x/y component, sigma vs |kt_x|.\n"
                 f"met_resolution = {fmt_res(*kt_fit)}\n")

    summary = {"era": args.era, "source": src, "parton_dr": PARTON_DR,
               "n_matched": {k: int(len(data[k]["pt"])) for k in ("udsc", "b", "muon")},
               "udsc": jets_q, "b": jets_b, "muon": muons,
               "kt": {"fit": kt_fit, "binned": kt_rows, "n": int(len(kt_resid))}}
    with open(os.path.join(args.outDir, f"{args.era}_resolution_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=1)

    print(f"matched: udsc {len(data['udsc']['pt'])}, b {len(data['b']['pt'])}, muon {len(data['muon']['pt'])}, "
          f"kt {len(kt_resid)}")
    def worst(rows, fit, inverse):
        """Largest |fit - binned| / binned over the bins (fit quality)."""
        c, r, nn = fit
        dev = []
        for x, w, _, _ in rows:
            xx = 1.0 / x if inverse else x
            dev.append(abs(np.sqrt(c * c * xx * xx + r * r * xx + nn * nn) - w) / w)
        return max(dev)

    for kind, bins in (("udsc", jets_q), ("b", jets_b), ("muon", muons)):
        for b in bins:
            c, r, n = b["fits"]["pt"]
            if b["inverse_pt"]:   # sigma(1/pT) -> sigma(pT) = pT^2 sigma(1/pT)
                s50 = 2500 * np.sqrt(c * c / 2500 + r * r / 50 + n * n)
            else:
                s50 = np.sqrt(c * c * 2500 + r * r * 50 + n * n)
            dev = {k: worst(b["binned"][k], b["fits"][k], k != "pt" or b["inverse_pt"])
                   for k in ("pt", "eta", "phi")}
            print(f"  {kind:4s} |eta| {b['eta_lo']:.1f}-{b['eta_hi']:.1f}: n {b['n']:6d}  "
                  f"sigma(pT=50) {s50:5.2f} GeV  response {b['median_pt_response']:.3f}  "
                  f"worst fit dev pT/eta/phi {dev['pt']:.0%}/{dev['eta']:.0%}/{dev['phi']:.0%}")
    print(f"  kt: C,R,N = {[round(v, 3) for v in kt_fit]}  worst fit dev {worst(kt_rows, kt_fit, False):.0%}")


if __name__ == "__main__":
    main()
