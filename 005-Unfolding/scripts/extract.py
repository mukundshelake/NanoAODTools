#!/usr/bin/env python3
"""BDTScore ROOT -> parquet for 005-Unfolding. Replaces the old getParquet.py.

Three modes:

    signal      the response-matrix sample; adds gen_yt, gen_ytbar, mtt_gen
    background  reco + weights only, one parquet per sample
    data        reco only, no MC weights at all

Usage:
    python scripts/extract.py --era UL2016preVFP --mode signal
    python scripts/extract.py --era UL2016preVFP --mode background          # all
    python scripts/extract.py --era UL2016preVFP --mode background --sample DYJetsToLL
    python scripts/extract.py --era UL2016preVFP --mode data

What changed relative to getParquet.py
--------------------------------------
* The muon is the one the analysis selected, not `ak.firsts` of the unfiltered
  Muon collection. 45% of events have more than one muon and the charge
  disagreed in ~1.4% of them (issue #33). Thresholds are era-dependent and come
  from kinematics.MUON_PT_THRESHOLD, pinned against 002-Samples/config.yaml.

* Weights are built from the explicit per-mode list in config.yaml rather than
  from whichever branches happen to exist. This matters most for data:
  L1PreFiringWeight_Nom *is present in the Data_mu trees*, so a "use what is
  there" approach would silently apply an MC-only correction to data. Guarded
  by assertion, not just by convention (issue #43).

* Missing branches fail with a list of what is missing, instead of a bare
  KeyError from deep inside the weight product (issue #43).

* `yt`/`ytbar` from the tree are ttbar rest-frame rapidities -- they are
  antisymmetric, y*_t = -y*_tbar -- and are NOT the lab-frame quantities the
  analysis uses. Stored as `yt_cm`/`ytbar_cm` so nobody mistakes them for the
  observable (issue #43).

* chi2_status, Chi2 and BDTScore are carried through so selection.py can apply
  the fit-quality and BDT requirements (issues #42, #37).

* LHEScaleWeight (9) and PSWeight (4) are carried as scalar columns so renorm/
  fact scale and parton-shower variations can be propagated later. LHEPdfWeight
  has 103 members and would add ~2.5 GB per era, so it is behind --pdf-weights;
  the better long-term design is to accumulate PDF variations straight into
  histograms rather than store them per event (issue #43).
"""

import argparse
import glob
import os
import sys

import awkward as ak
import numpy as np
import uproot

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfgmod  # noqa: E402
import kinematics  # noqa: E402

# Reco kinematics every mode needs.
RECO_BRANCHES = [
    "ttbar_mass", "Pgof", "Chi2", "chi2_status", "BDTScore",
    "yt", "ytbar",
    "Top_lep_pt", "Top_lep_eta", "Top_lep_mass",
    "Top_had_pt", "Top_had_eta", "Top_had_mass",
    "Muon_charge", "Muon_pt", "Muon_eta", "Muon_tightId", "Muon_pfRelIso04_all",
]

GEN_BRANCHES = [
    "GenPart_pdgId", "GenPart_statusFlags",
    "GenPart_pt", "GenPart_eta", "GenPart_phi", "GenPart_mass",
]

THEORY_BRANCHES = {"LHEScaleWeight": 9, "PSWeight": 4}
PDF_BRANCH = ("LHEPdfWeight", 103)

# Theory branches skipped because their member count did not match; reported at
# the end of a run so a missing systematic can never pass unnoticed.
SKIPPED_THEORY = set()

# Input files skipped because they lack required weight branches, keyed by
# sample. Written to skipped_files.json so the loss is quantified, never
# implicit -- see check_branches().
SKIPPED_FILES = {}

# Background samples that contributed no events at all.
EMPTY_SAMPLES = []


def weight_branches(cfg, is_mc):
    """Every weight branch this mode needs, nominal plus variations."""
    names = list(cfg["weights"]["MC" if is_mc else "Data"])
    if not is_mc:
        offenders = [n for n in names if "L1PreFiring" in n]
        if offenders:
            raise ValueError(
                f"config weights.Data contains {offenders}. L1 prefiring corrects "
                "simulation for an effect real data already contains intrinsically "
                "and must never be applied to data."
            )
        return names, []

    variations = []
    for spec in cfg["systematics"].values():
        if spec["nominal"] in names:
            variations += [spec["up"], spec["down"]]
    return names, variations


def build_weights(arrays, cfg, is_mc, n_events):
    """Nominal weight and one up/down pair per configured systematic source."""
    names, _ = weight_branches(cfg, is_mc)
    if not names:
        # Data: unit weights, by construction rather than by omission.
        return {"weight_nominal": np.ones(n_events, dtype=np.float64)}

    columns = {b: np.asarray(arrays[b], dtype=np.float64) for b in names}

    def product(substitute_from=None, substitute_to=None):
        # Recomputed as a product rather than divided out, so a zero weight in
        # any single source cannot produce inf/nan in the variations.
        values = [
            np.asarray(arrays[substitute_to], dtype=np.float64)
            if b == substitute_from else columns[b]
            for b in names
        ]
        return np.prod(values, axis=0)

    out = {"weight_nominal": product()}
    for source, spec in cfg["systematics"].items():
        if spec["nominal"] not in names:
            continue
        out[f"weight_{source}Up"] = product(spec["nominal"], spec["up"])
        out[f"weight_{source}Down"] = product(spec["nominal"], spec["down"])
    return out


def check_branches(tree, needed, path, fatal):
    """Return the list of missing branches; raise instead when `fatal`.

    Some background skims are missing the 003/004 scale-factor branches
    entirely -- 43 files across 12 samples in midNov/UL2016preVFP. Defaulting
    those to 1.0 would silently bias the background normalisation, so the file
    is skipped and the loss is counted and reported instead. For the signal any
    gap is fatal: the response matrix must be built from a complete sample.
    """
    missing = [b for b in needed if b not in tree.keys()]
    if missing and fatal:
        raise KeyError(
            f"{os.path.basename(path)} is missing required branches {missing}.\n"
            "Either the input tag is wrong or the branch was dropped upstream; "
            "this is not something to silently default."
        )
    return missing


def process_file(path, cfg, era, mode, with_pdf=False):
    tree = uproot.open(path)["Events"]
    if tree.num_entries == 0:
        return None

    is_mc = mode != "data"
    is_signal = mode == "signal"
    nominal, variations = weight_branches(cfg, is_mc)

    needed = RECO_BRANCHES + nominal + variations + (GEN_BRANCHES if is_signal else [])
    missing = check_branches(tree, needed, path, fatal=is_signal)
    if missing:
        return {"skipped": os.path.basename(path),
                "entries": int(tree.num_entries),
                "missing": missing}

    # Theory weights are carried for the SIGNAL only: they exist to propagate
    # ttbar modelling uncertainties through the unfolding, while backgrounds
    # get a normalisation uncertainty instead. They are also not uniform across
    # samples -- the QCD_Pt-* skims carry a single PSWeight member rather than
    # four -- so restricting them here avoids a per-sample size zoo.
    theory = [b for b in THEORY_BRANCHES if is_signal and b in tree.keys()]
    if with_pdf and is_signal and PDF_BRANCH[0] in tree.keys():
        theory.append(PDF_BRANCH[0])

    arrays = tree.arrays(needed + theory)
    n = len(arrays)

    # --- the analysis-selected muon (issue #33) ---
    index, has_muon = kinematics.select_muon_index(
        arrays["Muon_pt"], arrays["Muon_eta"], arrays["Muon_tightId"],
        arrays["Muon_pfRelIso04_all"], era=era,
    )
    charge = ak.to_numpy(ak.fill_none(ak.firsts(arrays["Muon_charge"][index]), 0))
    charge = np.where(has_muon, charge, 0)

    y_lep = kinematics.rapidity(arrays["Top_lep_pt"], arrays["Top_lep_eta"],
                                arrays["Top_lep_mass"])
    y_had = kinematics.rapidity(arrays["Top_had_pt"], arrays["Top_had_eta"],
                                arrays["Top_had_mass"])
    yt_lab, ytbar_lab = kinematics.assign_top_antitop(y_lep, y_had, charge)

    result = {
        "mtt_reco": np.asarray(arrays["ttbar_mass"], dtype=np.float64),
        "Pgof": np.asarray(arrays["Pgof"], dtype=np.float64),
        "Chi2": np.asarray(arrays["Chi2"], dtype=np.float64),
        "chi2_status": np.asarray(arrays["chi2_status"], dtype=np.int32),
        "bdt_score": np.asarray(arrays["BDTScore"], dtype=np.float64),
        "muon_charge": charge.astype(np.int8),
        "has_selected_muon": has_muon,
        "yt_lab": yt_lab,
        "ytbar_lab": ytbar_lab,
        # ttbar rest-frame, antisymmetric -- NOT the analysis observable.
        "yt_cm": np.asarray(arrays["yt"], dtype=np.float64),
        "ytbar_cm": np.asarray(arrays["ytbar"], dtype=np.float64),
        **build_weights(arrays, cfg, is_mc, n),
    }

    for branch in theory:
        size = dict([PDF_BRANCH], **THEORY_BRANCHES)[branch]
        counts = ak.to_numpy(ak.num(arrays[branch]))
        found = sorted(set(counts.tolist()))
        if found != [size]:
            # Skipped rather than fatal: a sample without the full set of
            # variations should not abort an otherwise good extraction, but it
            # must be visible, so the caller records and reports it.
            SKIPPED_THEORY.add(f"{branch}: {found} members, expected {size}")
            continue
        flat = ak.to_numpy(arrays[branch]).reshape(n, size)
        for i in range(size):
            result[f"{branch}_{i}"] = flat[:, i].astype(np.float32)

    if is_signal:
        pdg = arrays["GenPart_pdgId"]
        last_copy = ((arrays["GenPart_statusFlags"] >> 13) & 1) == 1

        def gen(pdg_id, field):
            sel = last_copy & (pdg == pdg_id)
            return ak.to_numpy(ak.fill_none(ak.firsts(arrays[field][sel]), np.nan))

        top = [gen(6, f) for f in ("GenPart_pt", "GenPart_eta", "GenPart_phi", "GenPart_mass")]
        atop = [gen(-6, f) for f in ("GenPart_pt", "GenPart_eta", "GenPart_phi", "GenPart_mass")]

        result["gen_yt"] = kinematics.rapidity(top[0], top[1], top[3])
        result["gen_ytbar"] = kinematics.rapidity(atop[0], atop[1], atop[3])
        result["mtt_gen"] = kinematics.invariant_mass(tuple(top), tuple(atop))

        combined = ak.Array(result)
        valid = np.isfinite(result["gen_yt"]) & np.isfinite(result["gen_ytbar"])
        return combined[valid]

    return ak.Array(result)


def run_one(cfg, era, mode, sample, datamc, out_path, with_pdf):
    input_dir = cfgmod.input_dir(cfg, era, datamc, sample)
    files = sorted(glob.glob(str(input_dir / "*.root")))
    if not files:
        raise RuntimeError(f"no ROOT files under {input_dir}")

    chunks, total, skipped = [], 0, []
    for path in files:
        chunk = process_file(path, cfg, era, mode, with_pdf)
        if isinstance(chunk, dict):
            skipped.append(chunk)
            continue
        if chunk is not None and len(chunk) > 0:
            chunks.append(chunk)
            total += len(chunk)
    if not chunks:
        # A background sample can legitimately contribute nothing after the
        # upstream skims -- QCD_Pt-15To20_MuEnriched has 26 empty files of 28.
        # Report it and move on; only the signal must be non-empty.
        if mode == "signal":
            raise RuntimeError(f"no events survived extraction for the signal {sample}")
        print(f"  {sample:<34} {0:>10,} events  -> (no parquet written)")
        EMPTY_SAMPLES.append(sample)
        return 0

    lost = sum(s["entries"] for s in skipped)
    if skipped:
        frac = lost / (lost + total) if (lost + total) else 0.0
        print(f"  !! {sample}: SKIPPED {len(skipped)} file(s), {lost:,} events "
              f"({frac:.1%} of the sample) missing "
              f"{sorted({b for s in skipped for b in s['missing']})}")
        SKIPPED_FILES[sample] = {"files": len(skipped), "events_lost": lost,
                                 "fraction_lost": frac, "detail": skipped}

    combined = ak.concatenate(chunks)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ak.to_parquet(combined, str(out_path))
    print(f"  {sample:<34} {total:>10,} events  -> {out_path.name}")
    if SKIPPED_THEORY:
        for note in sorted(SKIPPED_THEORY):
            print(f"      [theory skipped] {note}")
        SKIPPED_THEORY.clear()
    return total


def main():
    parser = argparse.ArgumentParser(description="Extract BDTScore ROOT files to parquet")
    parser.add_argument("--era", required=True)
    parser.add_argument("--mode", required=True, choices=["signal", "background", "data"])
    parser.add_argument("--sample", help="one background sample; default is all of them")
    parser.add_argument("--tag", default="Dump", help="output campaign tag")
    parser.add_argument("--config", default=None)
    parser.add_argument("--pdf-weights", action="store_true",
                        help="also carry the 103 LHEPdfWeight members (~2.5 GB/era)")
    args = parser.parse_args()

    cfg = cfgmod.load(args.config)
    outdir = cfgmod.output_dir(cfg, args.era, args.tag)
    print(f"config hash {cfg['config_hash']}  ->  {outdir}")

    if args.mode == "signal":
        run_one(cfg, args.era, "signal", cfg["Signal"], "MC_mu",
                outdir / "signal.parquet", args.pdf_weights)

    elif args.mode == "background":
        samples = [args.sample] if args.sample else cfgmod.background_samples(cfg, args.era)
        for sample in samples:
            run_one(cfg, args.era, "background", sample, "MC_mu",
                    outdir / f"background_{sample}.parquet", args.pdf_weights)

    else:
        for run in cfg["Data"]:
            run_one(cfg, args.era, "data", run, "Data_mu",
                    outdir / f"data_{run}.parquet", False)

    if EMPTY_SAMPLES:
        print(f"\n  {len(EMPTY_SAMPLES)} sample(s) contributed no events: "
              f"{', '.join(EMPTY_SAMPLES)}")

    if SKIPPED_FILES:
        import json
        sidecar = outdir / f"skipped_files_{args.mode}.json"
        sidecar.write_text(json.dumps(SKIPPED_FILES, indent=2))
        worst = max(SKIPPED_FILES.items(), key=lambda kv: kv[1]["fraction_lost"])
        print(f"\n  !! {len(SKIPPED_FILES)} sample(s) lost files to missing weight "
              f"branches; worst is {worst[0]} at {worst[1]['fraction_lost']:.1%}")
        print(f"  !! detail -> {sidecar}")
        print("  !! background normalisation is biased low by these fractions "
              "until the upstream skims are fixed.")


if __name__ == "__main__":
    main()
