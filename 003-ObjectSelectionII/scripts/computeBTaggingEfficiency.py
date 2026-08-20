"""
computeBTaggingEfficiency.py

Computes per-flavor, per-dataset 2D efficiency maps for DeepJet b-tagging
in bins of (pT, |eta|), for all three official working points (Loose,
Medium, Tight), and saves them as ROOT files compatible with
coffea.lookup_tools.extractor -- matching exactly what bTaggingWeight.py
reads:
    Efficiency/FlavourB_Wp_pass_No / _BL / _BM / _BT   (hadronFlavour == 5)
    Efficiency/FlavourC_Wp_pass_No / _BL / _BM / _BT   (hadronFlavour == 4)
    Efficiency/FlavourL_Wp_pass_No / _BL / _BM / _BT   (hadronFlavour == 0)

The kinematic selection mirrors bTaggingWeight.py's per-jet cut exactly, so
the efficiency describes the same jet population the weight is applied to:
    pt > 25, |eta| < 2.4, jetId == 6, (puId > 0 or pt > 50)

WP thresholds (L/M/T) are read directly from the same correctionlib file
the weight module uses (the 'deepJet_wp_values' correction), so nothing is
hardcoded or guessed.

Output files are written to:
    <outputDir>/<era>/<sampleName>.root

where <sampleName> is the dataset identifier used as the channel key in
bTaggingWeight (e.g. "ttbar_SemiLeptonic").

Usage:
    python computeBTaggingEfficiency.py \\
        --fileList <fileset.json> \\
        --outputDir SFs/Efficiency \\
        --bTagSFFile SFs/UL2018_jet_Btagging.json \\
        [--sample <nWorkers>]

The fileset JSON must follow the coffea format produced by run_all.py with
metadata fields {"isData": false, "era": "<era>", "sample": "<sampleName>"}.
If the "sample" key is absent in metadata, the sample name is parsed from the
dataset key (format: {era}_{DataMC0}_{DataMC1}_{group}_{sample...}).
"""

import os
import sys
import json
import argparse
import logging

import numpy as np
import dask
import awkward as ak
import uproot
import hist
import correctionlib
from coffea.nanoevents import NanoAODSchema
from coffea import processor
from coffea.dataset_tools import apply_to_fileset, preprocess

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#  Binning for the efficiency maps                                             #
# --------------------------------------------------------------------------- #
# These edges match the binning already shipped in SFs/Efficiency/<era>/*.root.
PT_EDGES  = np.array([20., 30., 40., 70., 100., 140., 200., 300., 600., 1000.], dtype=float)
ETA_EDGES = np.array([0., 0.2, 0.7, 1.4, 2.0, 2.2, 2.4, 2.5],                   dtype=float)

# hadronFlavour code -> histogram-name prefix, matching bTaggingWeight.py
FLAVOURS = {"B": 5, "C": 4, "L": 0}
WP_LETTERS = {"BL": "L", "BM": "M", "BT": "T"}


def _sample_from_key(dataset_key: str, era: str) -> str:
    """Extract the short sample name from a coffea dataset key.

    Expected key format (produced by run_all.py):
        {era}_{DataMC0}_{DataMC1}_{group}_{sample_parts...}
    e.g. "UL2018_MC_mu_SingleTop_Tchannel" -> "Tchannel"

    Falls back to the full key if the format is unrecognised.
    """
    parts = dataset_key.split("_")
    if len(parts) > 4:
        return "_".join(parts[4:])
    return dataset_key


def _make_hist():
    return hist.Hist(
        hist.axis.Variable(PT_EDGES,  name="pt",  label="Jet p_{T} [GeV]"),
        hist.axis.Variable(ETA_EDGES, name="eta", label="|#eta|"),
        storage=hist.storage.Double(),
    )


# --------------------------------------------------------------------------- #
#  Coffea processor                                                            #
# --------------------------------------------------------------------------- #
class BTaggingEfficiencyProcessor(processor.ProcessorABC):
    """Fills, per hadron flavour, 2D (pT, |eta|) histograms for total jets
    and jets passing each DeepJet working point -- using the exact same
    kinematic selection as bTaggingWeight.py."""

    def __init__(self, wp_thresholds):
        self.wp_thresholds = wp_thresholds  # {"BL": val, "BM": val, "BT": val}

    def process(self, events):
        dataset = events.metadata["dataset"]
        era     = events.metadata["era"]
        sample  = events.metadata.get("sample", _sample_from_key(dataset, era))
        logger.info(f"Processing {dataset} (era={era}, sample={sample})")

        # Kinematic selection: matches bTaggingWeight.py's per-jet cut.
        jet_mask = (
            (events.Jet.pt > 25) &
            (abs(events.Jet.eta) < 2.4) &
            (events.Jet.jetId == 6) &
            ((events.Jet.puId > 0) | (events.Jet.pt > 50))
        )
        jets = events.Jet[jet_mask]

        # Materialise the dask arrays (called once per chunk)
        pt_all   = ak.to_numpy(ak.flatten(jets.pt).compute())
        eta_all  = ak.to_numpy(ak.flatten(abs(jets.eta)).compute())
        flav_all = ak.to_numpy(ak.flatten(jets.hadronFlavour).compute())
        disc_all = ak.to_numpy(ak.flatten(jets.btagDeepFlavB).compute())

        out = {"era": era, "sample": sample}
        for flavour_name, flavour_code in FLAVOURS.items():
            sel = flav_all == flavour_code
            pt_f, eta_f, disc_f = pt_all[sel], eta_all[sel], disc_all[sel]

            h_total = _make_hist()
            h_total.fill(pt=pt_f, eta=eta_f)
            out[f"h_{flavour_name}_No"] = h_total

            for wp_name in WP_LETTERS:
                pass_mask = disc_f > self.wp_thresholds[wp_name]
                h_pass = _make_hist()
                h_pass.fill(pt=pt_f[pass_mask], eta=eta_f[pass_mask])
                out[f"h_{flavour_name}_{wp_name}"] = h_pass

        return out

    def postprocess(self, accumulator):
        return accumulator


# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #
def _save_efficiency_root(outpath: str, hists: dict):
    """Write the per-flavor, per-WP histograms to a ROOT file."""
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with uproot.recreate(outpath) as rfile:
        for flavour_name in FLAVOURS:
            rfile[f"Efficiency/Flavour{flavour_name}_Wp_pass_No"] = hists[f"h_{flavour_name}_No"]
            for wp_name in WP_LETTERS:
                rfile[f"Efficiency/Flavour{flavour_name}_Wp_pass_{wp_name}"] = hists[f"h_{flavour_name}_{wp_name}"]
    logger.info(f"  Saved: {outpath}")


def _load_fileset(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def _load_wp_thresholds(bTagSFFile: str) -> dict:
    cset = correctionlib.CorrectionSet.from_file(bTagSFFile)
    wp_values = cset["deepJet_wp_values"]
    thresholds = {wp_name: wp_values.evaluate(letter) for wp_name, letter in WP_LETTERS.items()}
    logger.info(f"DeepJet WP thresholds from {bTagSFFile}: {thresholds}")
    return thresholds


# --------------------------------------------------------------------------- #
#  Main                                                                        #
# --------------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(
        description="Compute per-flavor DeepJet b-tagging efficiency maps and write ROOT files."
    )
    parser.add_argument(
        "--fileList", required=True,
        help="Coffea fileset JSON produced by run_all.py --prepareEfficiencyFileset."
    )
    parser.add_argument(
        "--outputDir", required=True,
        help=(
            "Base output directory for ROOT files. "
            "Files are written to <outputDir>/<era>/<sample>.root. "
            "Typically 'SFs/Efficiency' relative to NanoAODTools/."
        ),
    )
    parser.add_argument(
        "--bTagSFFile", required=True,
        help="correctionlib JSON providing 'deepJet_wp_values' for this era "
             "(the same file bTaggingWeight.py uses, e.g. SFs/UL2018_jet_Btagging.json)."
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Number of dask threads (default: 4)."
    )
    parser.add_argument(
        "--sample", action="store_true",
        help="Process only the first file per dataset (for quick testing)."
    )
    args = parser.parse_args()

    wp_thresholds = _load_wp_thresholds(args.bTagSFFile)

    fileset = _load_fileset(args.fileList)
    if not fileset:
        logger.error("Empty fileset — nothing to do.")
        sys.exit(1)

    logger.info(f"Loaded {len(fileset)} dataset(s) from {args.fileList}")

    if args.sample:
        trimmed = {}
        for k, v in fileset.items():
            first_file = dict(list(v["files"].items())[:1])
            trimmed[k] = dict(v, files=first_file)
        fileset = trimmed
        logger.info("--sample mode: using first file per dataset only.")

    available, _ = preprocess(fileset, step_size=50_000, skip_bad_files=True)

    to_compute, _ = apply_to_fileset(
        BTaggingEfficiencyProcessor(wp_thresholds),
        available,
        schemaclass=NanoAODSchema,
    )

    logger.info(f"Running with {args.workers} worker thread(s)…")
    (output,) = dask.compute(to_compute, scheduler="threads", num_workers=args.workers)

    for dataset_key, result in output.items():
        era    = result["era"]
        sample = result["sample"]
        outpath = os.path.join(args.outputDir, era, f"{sample}.root")

        hists = {k: v for k, v in result.items() if k.startswith("h_")}

        for flavour_name in FLAVOURS:
            n_total = int(hists[f"h_{flavour_name}_No"].values().sum())
            n_medium = int(hists[f"h_{flavour_name}_BM"].values().sum())
            eff_global = n_medium / n_total if n_total > 0 else float("nan")
            logger.info(
                f"  {dataset_key} [{flavour_name}]: {n_total} jets total, "
                f"{n_medium} passing Medium (global eff = {eff_global:.4f})"
            )

        _save_efficiency_root(outpath, hists)

    logger.info("Done.")


if __name__ == "__main__":
    main()
