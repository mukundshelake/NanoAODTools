#!/usr/bin/env python3
"""
makeDatacards.py

Create Higgs Combine datacards for measuring charge asymmetries (A_FB, A_out,
A_in, A_c) via impact plots, starting from extractNs.py output.

One Combine datacard is produced per (asymmetry × ttbar_mass bin):

    POI = A  (the asymmetry in that bin)
    N_pos_exp = μ · N_mc_tot · (1+A)/2 · ∏ lnN_i
    N_neg_exp = μ · N_mc_tot · (1-A)/2 · ∏ lnN_j

    μ       — free rateParam (floats, absorbs MC-to-data normalisation)
    lnN κ   — κ = N_syst / N_nom  per pos/neg bin per systematic source

The PhysicsModel (AsymmetryModel.py) and a ready-to-run shell script
(run_impacts.sh) are also written to the output folder.

Usage:
    python makeDatacards.py \\
        --counts_file    Outputs/counts_UL2016preVFP.json \\
        --lumiXinfo_file Inputs/midNov_BDTScore_UL2016preVFP_lumiXinfo.json \\
        --era            UL2016preVFP \\
        --output_folder  Datacards/UL2016preVFP

Output:
    Datacards/UL2016preVFP/
        AsymmetryModel.py                  (copied here for easy deployment)
        run_impacts.sh                     (full Combine impact workflow)
        A_FB_UL2016preVFP_bin0.txt         mttbar [300,400]
        A_FB_UL2016preVFP_bin1.txt         mttbar [400,500]
        ...
        A_c_UL2016preVFP_bin8.txt          mttbar [1100,1200]

Combine impact workflow (run on lxplus after "cmsenv"):
    cd Datacards/UL2016preVFP && bash run_impacts.sh
    → impact_A_FB_UL2016preVFP_bin0.pdf  ...

Notes:
    - combineTool.py and plotImpacts.py require CombineHarvester in addition
      to HiggsAnalysis/CombinedLimit (Combine).
    - By default run_impacts.sh uses -t -1 (Asimov pseudo-data = MC prediction)
      so the fit converges to the nominal MC asymmetry.  Remove -t -1 when
      running on real data.
    - Systematics that were not present in some datasets (fallback weight = 1)
      still produce κ = 1.0 entries, which Combine treats as no constraint —
      this is correct behaviour.
"""

import argparse
import json
import math
import os
import shutil
import textwrap

import numpy as np


# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

MASS_EDGES = [300 + 100 * i for i in range(10)]   # 9 bins: [300,400] … [1100,1200]
N_BINS = 9

# Map asymmetry name → (pos_count_key, neg_count_key) as in counts.json
ASYM_COUNT_KEYS = {
    "A_FB":  ("N_FB_pos",  "N_FB_neg"),
    "A_out": ("N_out_pos", "N_out_neg"),
    "A_in":  ("N_in_pos",  "N_in_neg"),
    "A_c":   ("N_c_pos",   "N_c_neg"),
}

_ALL_COUNT_KEYS = [
    "N_FB_pos", "N_FB_neg",
    "N_out_pos", "N_out_neg",
    "N_in_pos",  "N_in_neg",
    "N_c_pos",   "N_c_neg",
]

# Bins below this total MC yield are skipped (asymmetry ill-defined)
MIN_YIELD = 1e-3


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _base_and_direction(syst_label: str):
    """'muonIDUp' → ('muonID', 'Up'),  'puDown' → ('pu', 'Down')."""
    for suffix in ("Up", "Down"):
        if syst_label.endswith(suffix):
            return syst_label[:-len(suffix)], suffix
    return syst_label, "Up"


def compute_mc_totals(counts: dict, lumiXinfo: dict):
    """
    Lumi-weight MC counts from all MC groups.

    Returns
    -------
    nom   : {count_key: np.ndarray(9)}           — nominal combined MC yields
    syst  : {base_name: {"Up":{k:arr}, "Down":{k:arr}}}  — per-source totals
    """
    lumi  = lumiXinfo["Luminosity"]
    era   = lumiXinfo["era"]
    xsecs = lumiXinfo["cross_sections"]
    ngens = lumiXinfo["generated_events"]

    nom  = {k: np.zeros(N_BINS) for k in _ALL_COUNT_KEYS}
    syst = {}   # {base: {"Up": {k: arr}, "Down": {k: arr}}}

    for group_name, group_datasets in counts.items():
        if not group_name.startswith("MC"):
            continue
        for dataset_name, dc in group_datasets.items():
            if not dc:
                continue
            lumi_key = f"{era}_{dataset_name}"
            if lumi_key not in xsecs or lumi_key not in ngens:
                print(f"  [SKIP] {lumi_key}: missing xsec or n_gen")
                continue
            w = lumi * xsecs[lumi_key] / ngens[lumi_key]

            for k in _ALL_COUNT_KEYS:
                nom[k] += w * np.array(dc[k])

            for syst_label, sv in dc.get("syst", {}).items():
                base, direction = _base_and_direction(syst_label)
                if base not in syst:
                    syst[base] = {
                        "Up":   {k: np.zeros(N_BINS) for k in _ALL_COUNT_KEYS},
                        "Down": {k: np.zeros(N_BINS) for k in _ALL_COUNT_KEYS},
                    }
                for k in _ALL_COUNT_KEYS:
                    syst[base][direction][k] += w * np.array(sv[k])

    return nom, syst


def compute_data_totals(counts: dict):
    """Sum Data group counts (simple sum, no luminosity weighting)."""
    totals = {k: np.zeros(N_BINS) for k in _ALL_COUNT_KEYS}
    for group_name, group_datasets in counts.items():
        if not group_name.startswith("Data"):
            continue
        for dc in group_datasets.values():
            if not dc:
                continue
            for k in _ALL_COUNT_KEYS:
                totals[k] += np.array(dc[k])
    return totals


def fmt_kappa(k_dn: float, k_up: float, eps: float = 1e-6) -> str:
    """
    Format a Combine lnN kappa value.

    Combine asymmetric lnN convention:  'kappa_down/kappa_up'
    where kappa_up  = yield_up  / yield_nominal   (typically > 1)
          kappa_down = yield_down / yield_nominal  (typically < 1)
    """
    # Clamp negative or zero kappas (unphysical, usually from empty bins)
    k_up = max(k_up, eps)
    k_dn = max(k_dn, eps)

    if abs(k_up - 1.0) < eps and abs(k_dn - 1.0) < eps:
        return "1.0"                           # no effect
    if abs(k_up - k_dn) < eps:
        return f"{k_up:.6f}"                   # symmetric
    return f"{k_dn:.6f}/{k_up:.6f}"           # asymmetric: kdn/kup


# ──────────────────────────────────────────────────────────────────────────────
# Datacard writer
# ──────────────────────────────────────────────────────────────────────────────

def write_datacard(
    path: str,
    asym: str,
    era: str,
    bin_idx: int,
    obs_pos: float,
    obs_neg: float,
    mc_pos: float,
    mc_neg: float,
    mc_tot: float,
    kappas: dict,        # {base: (k_pos_dn, k_pos_up, k_neg_dn, k_neg_up)}
    syst_bases: list,
) -> None:
    """
    Write a single Combine .txt datacard.

    Rate layout
    -----------
    Both bins carry rate = mc_tot (the summed MC yield).
    AsymmetryModel scales them by (1+A)/2 and (1-A)/2 respectively,
    so at the best-fit A the expected counts match the MC nominal.

    Systematic layout
    -----------------
    lnN nuisances with kappa = N_syst / N_nom per pos/neg bin.
    A purely normalisation-shifting systematic has the same kappa for pos and neg
    (it goes into μ and leaves A unchanged).  A shape-changing systematic has
    different kappas and impacts A.
    """
    mass_lo = MASS_EDGES[bin_idx]
    mass_hi = MASS_EDGES[bin_idx + 1]
    label   = f"{asym}_{era}_bin{bin_idx}"
    pos_bin = f"{label}_pos"
    neg_bin = f"{label}_neg"

    W1, W2 = 30, 42   # column widths

    def row(*cols):
        parts = [str(cols[0]).ljust(W1)] + [str(c).ljust(W2) for c in cols[1:]]
        return "  ".join(parts).rstrip()

    lines = [
        f"# Combine datacard",
        f"#   Observable : {asym}",
        f"#   Era        : {era}",
        f"#   m_ttbar    : [{mass_lo}, {mass_hi}] GeV",
        f"#   MC nominal : pos = {mc_pos:.3f}   neg = {mc_neg:.3f}   tot = {mc_tot:.3f}",
        f"#",
        f"# PhysicsModel : AsymmetryModel (N_pos ∝ (1+A)/2, N_neg ∝ (1-A)/2)",
        f"# POI          : A ∈ [-1, 1]",
        f"# μ            : freely floating rateParam (overall MC-to-data scale)",
        f"#",
        f"imax 2   # two bins: pos and neg",
        f"jmax 1   # one dummy background (required by Combine parser)",
        f"kmax *   # all systematic uncertainties listed below",
        f"",
        f"{'bin':<{W1}}  {pos_bin:<{W2}}  {neg_bin}",
        f"{'observation':<{W1}}  {obs_pos:<{W2}.6g}  {obs_neg:.6g}",
        f"",
        f"{'bin':<{W1}}  {pos_bin:<{W2}}  {pos_bin:<{W2}}  {neg_bin:<{W2}}  {neg_bin}",
        f"{'process':<{W1}}  {'signal':<{W2}}  {'dummy':<{W2}}  {'signal':<{W2}}  {'dummy'}",
        f"{'process':<{W1}}  {'0':<{W2}}  {'1':<{W2}}  {'0':<{W2}}  {'1'}",
        f"# Both bins carry the total MC rate; AsymmetryModel applies (1±A)/2",
        f"{'rate':<{W1}}  {mc_tot:<{W2}.6g}  {'1e-6':<{W2}}  {mc_tot:<{W2}.6g}  {'1e-6'}",
        f"",
        f"# ── Freely floating overall normalisation (shared between pos/neg) ──",
        f"{'norm':<{W1}}  {'rateParam':<10}  {pos_bin:<{W2}}  {'signal':<12}  1",
        f"{'norm':<{W1}}  {'rateParam':<10}  {neg_bin:<{W2}}  {'signal':<12}  1",
        f"",
        f"# ── Systematic uncertainties (lnN) ─────────────────────────────────",
        f"# Format: name  lnN  kappa_pos_signal  -  kappa_neg_signal  -",
        f"# kappa = N_syst / N_nom  (asymmetric: kappa_down/kappa_up)",
        f"# '-' = not applicable (dummy background has rate 1e-6, negligible)",
        f"",
    ]

    for base in syst_bases:
        k_pos_dn, k_pos_up, k_neg_dn, k_neg_up = kappas[base]
        pos_str = fmt_kappa(k_pos_dn, k_pos_up)
        neg_str = fmt_kappa(k_neg_dn, k_neg_up)
        lines.append(f"{base:<{W1}}  {'lnN':<10}  {pos_str:<{W2}}  {'-':<{W2}}  {neg_str:<{W2}}  {'-'}")

    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# Generated file content: AsymmetryModel.py and run_impacts.sh
# ──────────────────────────────────────────────────────────────────────────────

# The AsymmetryModel source to copy into the output folder (needs to be in
# the same directory as the datacards and on PYTHONPATH when calling Combine).
_ASYMMETRY_MODEL_SRC = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "AsymmetryModel.py"
)


def _make_run_impacts_sh(era: str) -> str:
    return textwrap.dedent(f"""\
        #!/bin/bash
        # run_impacts.sh
        # Full Combine impact-fit workflow for all charge-asymmetry datacards.
        #
        # Requirements
        # ------------
        #   CMSSW environment with both:
        #     - HiggsAnalysis/CombinedLimit  (Combine)
        #     - CombineHarvester              (combineTool.py + plotImpacts.py)
        #   Typical setup on lxplus:
        #     source /cvmfs/cms.cern.ch/cmsset_default.sh
        #     cd <CMSSW_BASE_DIR>; cmsenv; cd -
        #
        # Usage
        # -----
        #   cd Datacards/{era}
        #   bash run_impacts.sh                  # Asimov mode (default)
        #   ASIMOV="" bash run_impacts.sh         # real-data mode (obs. counts)
        #   N_CORES=8 bash run_impacts.sh         # more parallel jobs
        #
        # Outputs
        # -------
        #   impact_{{name}}.pdf   for each datacard

        set -e
        set -u

        # ── Config ────────────────────────────────────────────────────────────
        # -t -1  : use Asimov pseudo-data (MC prediction as observed data).
        # Remove this flag when fitting to real recorded data.
        ASIMOV="${{ASIMOV:--t -1}}"

        # Number of parallel jobs for the nuisance parameter scans.
        N_CORES="${{N_CORES:-4}}"

        # Common options passed to all Combine calls.
        COMMON_OPTS="$ASIMOV --rMin -1 --rMax 1 \\
          --cminDefaultMinimizerStrategy 0 \\
          --cminDefaultMinimizerTolerance 0.01"

        # ── Make AsymmetryModel importable ────────────────────────────────────
        export PYTHONPATH="${{PWD}}:${{PYTHONPATH:-}}"

        # ── Process each datacard ─────────────────────────────────────────────
        mapfile -t ALL_CARDS < <(ls *.txt 2>/dev/null || true)
        if [[ ${{#ALL_CARDS[@]}} -eq 0 ]]; then
            echo "No .txt datacards found in ${{PWD}}."
            exit 1
        fi
        echo "Found ${{#ALL_CARDS[@]}} datacard(s)."

        for CARD in "${{ALL_CARDS[@]}}"; do
            NAME="${{CARD%.txt}}"
            WS="${{NAME}}.root"

            # Parse human-readable label from filename, e.g. A_FB_UL2016preVFP_bin0
            LABEL=$(echo "$NAME" | sed 's/_bin/ bin /' | sed 's/_/ /g')

            echo ""
            echo "════════════════════════════════════════════════════════"
            echo "  $NAME"
            echo "════════════════════════════════════════════════════════"

            # 1. Convert datacard to RooFit workspace
            echo "  [1/4] text2workspace"
            text2workspace.py "$CARD" \\
                -P AsymmetryModel:asymmetryModel \\
                -m 0 \\
                -o "$WS" 2>&1 | tail -3

            # 2. Initial best-fit (find the MLE for A and all nuisances)
            echo "  [2/4] Initial fit"
            combineTool.py -M Impacts \\
                -d "$WS" -m 0 \\
                --doInitialFit \\
                $COMMON_OPTS \\
                -n "_${{NAME}}" 2>&1 | tail -5

            # 3. Scan each nuisance parameter (±1 σ, re-fit A each time)
            echo "  [3/4] Nuisance scans  (parallel=${{N_CORES}})"
            combineTool.py -M Impacts \\
                -d "$WS" -m 0 \\
                --doFits \\
                --parallel "${{N_CORES}}" \\
                $COMMON_OPTS \\
                -n "_${{NAME}}" 2>&1 | tail -5

            # 4. Collect scan results into a single JSON
            echo "  [4/4] Collect + plot"
            combineTool.py -M Impacts \\
                -d "$WS" -m 0 \\
                --output "impacts_${{NAME}}.json" \\
                -n "_${{NAME}}" 2>&1 | tail -3

            # 5. Render the PDF impact plot
            plotImpacts.py \\
                -i "impacts_${{NAME}}.json" \\
                -o "impact_${{NAME}}" \\
                --POI A \\
                --cms-label "${{LABEL}}"

            echo "  → impact_${{NAME}}.pdf"
        done

        echo ""
        echo "Done.  Impact PDFs: impact_*.pdf"
    """)


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Create Combine datacards for charge asymmetry impact fits."
    )
    parser.add_argument("--counts_file",    required=True,
                        help="Path to counts JSON from extractNs.py")
    parser.add_argument("--lumiXinfo_file", required=True,
                        help="Path to lumiXinfo JSON")
    parser.add_argument("--era",            required=True,
                        help="Era string, e.g. UL2016preVFP")
    parser.add_argument("--output_folder",  required=True,
                        help="Output folder for datacards (created if absent)")
    args = parser.parse_args()

    with open(args.counts_file) as f:
        counts = json.load(f)
    with open(args.lumiXinfo_file) as f:
        lumiXinfo = json.load(f)

    os.makedirs(args.output_folder, exist_ok=True)

    # ── Compute MC totals ──────────────────────────────────────────────────────
    print("Computing lumi-weighted MC totals...")
    nom, syst = compute_mc_totals(counts, lumiXinfo)
    data_nom  = compute_data_totals(counts)

    syst_bases = sorted(syst.keys())
    print(f"  {len(syst_bases)} systematic source(s): {syst_bases}")

    # ── Write AsymmetryModel.py ────────────────────────────────────────────────
    model_dst = os.path.join(args.output_folder, "AsymmetryModel.py")
    if os.path.exists(_ASYMMETRY_MODEL_SRC):
        shutil.copy2(_ASYMMETRY_MODEL_SRC, model_dst)
        print(f"Copied: {model_dst}")
    else:
        print(f"[WARNING] AsymmetryModel.py not found at {_ASYMMETRY_MODEL_SRC}; "
              f"skipping copy (place it manually in {args.output_folder}/)")

    # ── Write run_impacts.sh ───────────────────────────────────────────────────
    sh_path = os.path.join(args.output_folder, "run_impacts.sh")
    with open(sh_path, "w") as f:
        f.write(_make_run_impacts_sh(args.era))
    os.chmod(sh_path, 0o755)
    print(f"Wrote: {sh_path}")

    # ── Write datacards ────────────────────────────────────────────────────────
    n_written = 0
    n_skipped = 0

    for asym, (pos_key, neg_key) in ASYM_COUNT_KEYS.items():
        for i in range(N_BINS):
            mc_pos = float(nom[pos_key][i])
            mc_neg = float(nom[neg_key][i])
            mc_tot = mc_pos + mc_neg

            if mc_tot < MIN_YIELD:
                print(f"  [SKIP] {asym} bin{i}: mc_tot={mc_tot:.3g} < {MIN_YIELD}")
                n_skipped += 1
                continue

            obs_pos = float(data_nom[pos_key][i])
            obs_neg = float(data_nom[neg_key][i])
            if obs_pos == 0.0 and obs_neg == 0.0:
                # No real data collected yet: use MC nominal as pseudo-data.
                # This makes the fit find A_MC (the nominal MC asymmetry) with
                # correct statistical uncertainty σ_A ~ 1/sqrt(N_tot).
                obs_pos = mc_pos
                obs_neg = mc_neg

            # κ = N_syst / N_nom  for pos and neg bins separately
            kappas = {}
            for base in syst_bases:
                p_up = float(syst[base]["Up"][pos_key][i])
                p_dn = float(syst[base]["Down"][pos_key][i])
                n_up = float(syst[base]["Up"][neg_key][i])
                n_dn = float(syst[base]["Down"][neg_key][i])

                k_pos_up = p_up / mc_pos if mc_pos >= MIN_YIELD else 1.0
                k_pos_dn = p_dn / mc_pos if mc_pos >= MIN_YIELD else 1.0
                k_neg_up = n_up / mc_neg if mc_neg >= MIN_YIELD else 1.0
                k_neg_dn = n_dn / mc_neg if mc_neg >= MIN_YIELD else 1.0

                kappas[base] = (k_pos_dn, k_pos_up, k_neg_dn, k_neg_up)

            fname = f"{asym}_{args.era}_bin{i}.txt"
            fpath = os.path.join(args.output_folder, fname)
            write_datacard(
                path=fpath,
                asym=asym, era=args.era, bin_idx=i,
                obs_pos=obs_pos, obs_neg=obs_neg,
                mc_pos=mc_pos, mc_neg=mc_neg, mc_tot=mc_tot,
                kappas=kappas, syst_bases=syst_bases,
            )
            n_written += 1

    print(f"\n{n_written} datacard(s) written  ({n_skipped} skipped, mc_tot too small)")
    print(f"Output: {args.output_folder}/")
    print(f"\nNext steps:")
    print(f"  1. Copy {args.output_folder}/ to lxplus (where CMSSW + Combine are installed)")
    print(f"  2. source /cvmfs/cms.cern.ch/cmsset_default.sh && cd <CMSSW>; cmsenv; cd -")
    print(f"  3. cd {args.output_folder}/ && bash run_impacts.sh")
    print(f"  4. Impact plots: impact_A_FB_{args.era}_bin*.pdf  etc.")
    print(f"\nAsimov mode is ON by default (fits MC-predicted pseudo-data).")
    print(f"Set ASIMOV=\"\" in run_impacts.sh (or remove -t -1) for real data.")


if __name__ == "__main__":
    main()
