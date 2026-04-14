#!/usr/bin/env python3
"""
getObservables_gen.py

Compute gen-level observables for MC datasets using GenPart information.
Uses coffea's ProcessorABC + IterativeExecutor/FuturesExecutor pattern.
The `vector` package replaces ROOT.TLorentzVector for fully vectorized boosts.
Requires both a top (pdgId==6) and anti-top (pdgId==-6) with statusFlags bit 13
(isLastCopy) set. Events missing either are skipped.
Datasets in groups not starting with 'MC' (e.g. Data_mu) are skipped.

Usage:
    python getObservables_gen.py \\
        --era UL2016preVFP \\
        --json_file Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \\
        --output_folder Outputs/gen

Arguments:
    --era           Era string (used for output file naming).
    --json_file     Path to the dataFiles JSON {group: {dataset: {filepath: treename}}}.
    --output_folder Output folder (created if absent).
    --syst          Compute weight-based systematic variations (pileup, LHE scale,
                    PS weights, PDF).

Output:
    One coffea file per MC dataset in <output_folder>/:
        <era>_<dataset>.coffea  – {"gen": hist.Hist, "nEvents": np.int64, "nTotal": np.int64}

    The "gen" histogram has a leading "systematic" StrCategory axis:
        no --syst:  ["nominal"]
        --syst:     ["nominal",
                     "puUp",             "puDown",
                     "lheScaleMuRUp",    "lheScaleMuRDown",
                     "lheScaleMuFUp",    "lheScaleMuFDown",
                     "lheScaleMuRmuFUp", "lheScaleMuRmuFDown",
                     "psISRUp",          "psISRDown",
                     "psFSRUp",          "psFSRDown",
                     "lhePdfUp",         "lhePdfDown"]
                    nominal uses all theory/pileup nominal weights multiplied together.
                    Detector-level SFs (muonID, HLT, bTag, etc.) are NOT applied
                    at gen level.

Note:
    extractNs.py must slice the systematic axis to get counts, e.g.:
        h[{"systematic": "nominal"}]   for nominal
        h[{"systematic": "muonIDUp"}]  for a variation
"""

import argparse
import json
import os

import numpy as np
import awkward as ak
import vector
import hist
from coffea import processor
from coffea.nanoevents import BaseSchema
from coffea.analysis_tools import Weights
from coffea.util import save

vector.register_awkward()

# statusFlags bit 13 = isLastCopy
IS_LAST_COPY_BIT = 1 << 13

# LHEScaleWeight index mapping (3x3 grid; ordering: rows=muR 2→0.5, cols=muF 2→0.5;
# index = 3*(2-imuR) + (2-imuF)  where 0=0.5, 1=nominal, 2=2.0)
LHESCALE_IDX = {
    "muRUp":      1,
    "muRDown":    7,
    "muFUp":      3,
    "muFDown":    5,
    "muRmuFUp":   0,
    "muRmuFDown": 8,
}

PSWEIGHT_IDX = {
    "isrUp":   1,
    "isrDown": 0,
    "fsrUp":   3,
    "fsrDown": 2,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_ratio(varied: np.ndarray, nominal: np.ndarray) -> np.ndarray:
    """Return varied/nominal element-wise; assigns 1.0 where nominal == 0."""
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(nominal != 0.0, varied / nominal, 1.0).astype(np.float64)


def _compute_observables(top, atop) -> tuple:
    """
    Fully vectorized observables using the `vector` package.
    top / atop are vector.array objects built from (pt, eta, phi, mass).

    Returns (yt, ytbar, costheta, ttbar_mass) as plain numpy arrays.
    costheta = sign(y_ttbar) * pz_top / |p_top|  in the ttbar CM frame.
    """
    ttbar      = top + atop
    yt         = np.asarray(top.rapidity)
    ytbar      = np.asarray(atop.rapidity)
    ttbar_mass = np.asarray(ttbar.mass)
    top_cm     = top.boostCM_of_p4(ttbar)
    costheta   = (np.sign(np.asarray(ttbar.rapidity))
                  * np.asarray(top_cm.pz) / np.asarray(top_cm.p))
    return yt, ytbar, costheta, ttbar_mass


def _add_all_weights(
    weights,
    events,
    fields: list,
    n_total: int,
    detector_sfs: bool = True,
) -> None:
    """
    Register all systematic weight sources with a coffea Weights container.

    detector_sfs=True  : also add muonID, muonHLT, bTag, L1PreFiring, jetPUId.
    detector_sfs=False : skip those (gen-level; theory weights only).
    In both cases pileup, LHE scale, PS, and PDF weights are added.
    Missing branches (e.g. QCD, Data) fall back to weight = 1.
    """
    ones = np.ones(n_total, dtype=np.float64)

    def _f(name, fallback=None):
        """Return branch as float64 numpy array, or fallback if absent."""
        if name in fields:
            return ak.to_numpy(events[name]).astype(np.float64)
        return (fallback if fallback is not None else ones).copy()

    if detector_sfs:
        # --- Muon ID SF -------------------------------------------------
        weights.add("muonID",
                    _f("MuonIDWeight"),
                    weightUp=_f("MuonIDWeightUp"),
                    weightDown=_f("MuonIDWeightDown"))

        # --- Muon HLT SF (stat + syst combined in quadrature) -----------
        hlt_nom  = _f("MuonHLTWeight")
        hlt_stat = _f("MuonHLTWeightStat", np.zeros(n_total, dtype=np.float64))
        hlt_syst = _f("MuonHLTWeightSyst", np.zeros(n_total, dtype=np.float64))
        hlt_unc  = np.sqrt(hlt_stat**2 + hlt_syst**2)
        weights.add("muonHLT", hlt_nom,
                    weightUp=hlt_nom + hlt_unc,
                    weightDown=hlt_nom - hlt_unc)

        # --- b-Tagging SF -----------------------------------------------
        weights.add("bTag",
                    _f("bTaggingWeight"),
                    weightUp=_f("bTaggingWeightUp"),
                    weightDown=_f("bTaggingWeightDown"))

        # --- L1 PreFiring (2016/2017; branch present but ~1 for 2018) ---
        weights.add("l1PreFiring",
                    _f("L1PreFiringWeight_Nom"),
                    weightUp=_f("L1PreFiringWeight_Up"),
                    weightDown=_f("L1PreFiringWeight_Dn"))

        # --- Jet PU ID SF -----------------------------------------------
        weights.add("jetPUId",
                    _f("jetPUIdWeight"),
                    weightUp=_f("jetPUIdWeightUp"),
                    weightDown=_f("jetPUIdWeightDown"))

    # --- Pileup reweighting -------------------------------------------
    weights.add("pu",
                _f("puWeight"),
                weightUp=_f("puWeightUp"),
                weightDown=_f("puWeightDown"))

    # --- LHE Scale weights (muR / muF independent variations) ---------
    if "LHEScaleWeight" in fields:
        lhe_scale = ak.to_numpy(
            ak.fill_none(ak.pad_none(events["LHEScaleWeight"], 9, axis=1,
                                     clip=True), 1.0)
        ).astype(np.float64)
    else:
        lhe_scale = np.ones((n_total, 9), dtype=np.float64)

    weights.add("lheScaleMuR", ones,
                weightUp=lhe_scale[:, LHESCALE_IDX["muRUp"]],
                weightDown=lhe_scale[:, LHESCALE_IDX["muRDown"]])
    weights.add("lheScaleMuF", ones,
                weightUp=lhe_scale[:, LHESCALE_IDX["muFUp"]],
                weightDown=lhe_scale[:, LHESCALE_IDX["muFDown"]])
    weights.add("lheScaleMuRmuF", ones,
                weightUp=lhe_scale[:, LHESCALE_IDX["muRmuFUp"]],
                weightDown=lhe_scale[:, LHESCALE_IDX["muRmuFDown"]])

    # --- Parton Shower weights (ISR / FSR) ----------------------------
    if "PSWeight" in fields:
        ps_weights = ak.to_numpy(
            ak.fill_none(ak.pad_none(events["PSWeight"], 4, axis=1,
                                     clip=True), 1.0)
        ).astype(np.float64)
    else:
        ps_weights = np.ones((n_total, 4), dtype=np.float64)

    weights.add("psISR", ones,
                weightUp=ps_weights[:, PSWEIGHT_IDX["isrUp"]],
                weightDown=ps_weights[:, PSWEIGHT_IDX["isrDown"]])
    weights.add("psFSR", ones,
                weightUp=ps_weights[:, PSWEIGHT_IDX["fsrUp"]],
                weightDown=ps_weights[:, PSWEIGHT_IDX["fsrDown"]])

    # --- LHE PDF weights (NNPDF: std of replicas used as ±1σ envelope) -
    if "LHEPdfWeight" in fields:
        lhe_pdf = ak.to_numpy(events["LHEPdfWeight"]).astype(np.float64)
        pdf_unc = np.std(lhe_pdf, axis=1)
    else:
        pdf_unc = np.zeros(n_total, dtype=np.float64)

    weights.add("lhePdf", ones,
                weightUp=1.0 + pdf_unc,
                weightDown=np.maximum(1.0 - pdf_unc, 0.0))


def _new_histogram(syst_labels: list) -> hist.Hist:
    """Return a fresh histogram with a leading 'systematic' StrCategory axis."""
    return hist.Hist(
        hist.axis.StrCategory(syst_labels, name="systematic"),
        hist.axis.Regular(4, -2.5,  2.5,  name="yt",
                          label="Rapidity of top quark",
                          underflow=True, overflow=True),
        hist.axis.Regular(4, -2.5,  2.5,  name="ytbar",
                          label="Rapidity of anti-top quark",
                          underflow=True, overflow=True),
        hist.axis.Regular(2, -1,    1,    name="costheta",
                          label="cos(theta*) in ttbar CM frame",
                          underflow=True, overflow=True),
        hist.axis.Regular(2, -10,   10,   name="deltaAbsY",
                          label="|yt| - |ytbar|",
                          underflow=True, overflow=True),
        hist.axis.Regular(9,  300,  1200, name="ttbar_mass",
                          label="Invariant mass of ttbar system [GeV]",
                          underflow=True, overflow=True),
    )


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------

class GenObservablesProcessor(processor.ProcessorABC):
    """
    Coffea ProcessorABC for gen-level ttbar charge-asymmetry observables.

    Selects last-copy top (pdgId==6) and anti-top (pdgId==-6) from GenPart.
    Events missing either are dropped. When multiple candidates exist per event,
    the first is used.

    process() is called once per file chunk by the Runner. It returns:
        {dataset: {"gen": hist.Hist, "nEvents": np.int64, "nTotal": np.int64}}

    Weight design:
        Theory-level nominal weights (pileup, LHE scale) are multiplied together
        for the "nominal" fill. Detector SFs are NOT applied at gen level.
        Each Up/Down variation shifts one source while keeping all others at nominal.
        Missing branches fall back to weight = 1.
    """

    def __init__(self, era: str, output_folder: str, syst: bool = False):
        self._output_folder = output_folder
        self._era           = era
        self._syst          = syst
        self._syst_labels = (
            ["nominal",
             "puUp",             "puDown",
             "lheScaleMuRUp",    "lheScaleMuRDown",
             "lheScaleMuFUp",    "lheScaleMuFDown",
             "lheScaleMuRmuFUp", "lheScaleMuRmuFDown",
             "psISRUp",          "psISRDown",
             "psFSRUp",          "psFSRDown",
             "lhePdfUp",         "lhePdfDown"] if syst else ["nominal"]
        )

    def process(self, events):
        dataset = events.metadata["dataset"]
        n_total = len(events)

        # ---- Weight container --------------------------------------------
        # storeIndividual=True lets us call weights.weight(variation_name).
        weights = Weights(n_total, storeIndividual=True)
        if self._syst:
            _add_all_weights(weights, events, ak.fields(events), n_total,
                             detector_sfs=False)

        # ---- Gen-level particle selection --------------------------------
        idx = np.arange(n_total)

        is_last_copy = (events["GenPart_statusFlags"] & IS_LAST_COPY_BIT) != 0
        top_mask     = is_last_copy & (events["GenPart_pdgId"] ==  6)
        atop_mask    = is_last_copy & (events["GenPart_pdgId"] == -6)

        has_top  = ak.to_numpy(ak.sum(top_mask,  axis=1) >= 1)
        has_atop = ak.to_numpy(ak.sum(atop_mask, axis=1) >= 1)
        valid    = has_top & has_atop

        events    = events[valid]
        top_mask  = top_mask[valid]
        atop_mask = atop_mask[valid]
        idx       = idx[valid]

        # Take first last-copy top/anti-top per event
        def _first(branch, mask):
            return ak.to_numpy(
                ak.fill_none(ak.firsts(events[branch][mask]), 0.0)
            )

        top_pt    = _first("GenPart_pt",   top_mask)
        top_eta   = _first("GenPart_eta",  top_mask)
        top_phi   = _first("GenPart_phi",  top_mask)
        top_mass  = _first("GenPart_mass", top_mask)
        atop_pt   = _first("GenPart_pt",   atop_mask)
        atop_eta  = _first("GenPart_eta",  atop_mask)
        atop_phi  = _first("GenPart_phi",  atop_mask)
        atop_mass = _first("GenPart_mass", atop_mask)

        # ---- Observables (fully vectorized via `vector`) -----------------
        top  = vector.array({"pt": top_pt,  "eta": top_eta,
                              "phi": top_phi,  "mass": top_mass})
        atop = vector.array({"pt": atop_pt, "eta": atop_eta,
                              "phi": atop_phi, "mass": atop_mass})
        yt, ytbar, costheta, ttbar_mass = _compute_observables(top, atop)
        deltaAbsY = np.abs(yt) - np.abs(ytbar)

        fill_kw = dict(yt=yt, ytbar=ytbar, costheta=costheta,
                       deltaAbsY=deltaAbsY, ttbar_mass=ttbar_mass)

        # ---- Fill histogram ----------------------------------------------
        h = _new_histogram(self._syst_labels)
        h.fill(systematic="nominal", weight=weights.weight()[idx], **fill_kw)
        if self._syst:
            for var in sorted(weights.variations):
                h.fill(systematic=var, weight=weights.weight(var)[idx], **fill_kw)

        return {
            dataset: {
                "gen":     h,
                "nEvents": np.int64(len(idx)),
                "nTotal":  np.int64(n_total),
            }
        }

    def postprocess(self, accumulator):
        os.makedirs(self._output_folder, exist_ok=True)
        for dataset, out in accumulator.items():
            path = os.path.join(
                self._output_folder, f"{self._era}_{dataset}.coffea"
            )
            save(out, path)
            print(f"  [{dataset}] nTotal={out['nTotal']},  nEvents={out['nEvents']}")
            print(f"  -> Saved: {path}")
        return accumulator


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Gen-level observables for MC datasets using GenPart top quarks."
    )
    parser.add_argument("--era",           type=str, required=True,
                        help="Era string, e.g. UL2016preVFP")
    parser.add_argument("--json_file",     type=str, required=True,
                        help="Path to the dataFiles JSON")
    parser.add_argument("--output_folder", type=str, required=True,
                        help="Output folder (created if absent)")
    parser.add_argument(
        "--syst", action="store_true", default=False,
        help=("Compute weight-based systematic variations: pileup, LHE scale, "
              "PS weights, PDF."),
    )
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Number of worker processes (default: 1 = iterative, >1 = multicore).",
    )
    args = parser.parse_args()

    with open(args.json_file) as f:
        datasets = json.load(f)

    fileset = {}
    for group, group_datasets in datasets.items():
        if not group.startswith("MC"):
            print(f"[Skipping group '{group}']")
            continue
        for dataset_name, file_dict in group_datasets.items():
            fileset[dataset_name] = {
                "files":    file_dict,
                "metadata": {"group": group},
            }

    if not fileset:
        print("No MC datasets found. Exiting.")
        return

    print(f"Era: {args.era}  |  Gen-level selection (isLastCopy top + anti-top)")
    if args.syst:
        print("Systematics: muonIDUp, muonIDDown")
    print(f"Datasets ({len(fileset)}): {list(fileset.keys())}")

    if args.workers > 1:
        executor = processor.FuturesExecutor(workers=args.workers)
        print(f"Executor: FuturesExecutor ({args.workers} workers)")
    else:
        executor = processor.IterativeExecutor()
        print("Executor: IterativeExecutor (single-threaded)")

    run = processor.Runner(
        executor=executor,
        schema=BaseSchema,
        savemetrics=True,
    )

    out, metrics = run(
        fileset,
        processor_instance=GenObservablesProcessor(
            era=args.era,
            output_folder=args.output_folder,
            syst=args.syst,
        ),
    )

    print(f"\nDone. Processed {metrics['entries']} entries across {len(fileset)} dataset(s).")


if __name__ == "__main__":
    main()
