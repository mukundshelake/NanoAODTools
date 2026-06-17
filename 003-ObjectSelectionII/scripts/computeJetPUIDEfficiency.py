"""
computeJetPUIDEfficiency.py

Computes per-dataset 2D efficiency maps for Jet PU ID (Loose WP)
in bins of (pT, |eta|) and saves them as ROOT files compatible with
coffea.lookup_tools.extractor.

Each output ROOT file contains two TH2D histograms under the directory
"Efficiency":
  - Efficiency/JetPUId_pass_No     : total jets passing kinematic cuts
                                     (12.5 < pT <= 50 GeV, |eta| < 5)
  - Efficiency/JetPUId_pass_Loose  : subset of those jets also passing
                                     the Loose PU ID working point (puId > 0)

The efficiency at (pT, |eta|) is computed by JetPUIDWeight.py as:
    eff = JetPUId_pass_Loose / JetPUId_pass_No

Output files are written to:
    <outputDir>/<era>/<sampleName>.root

where <sampleName> is the dataset identifier used as the channel key in
JetPUIDWeight (e.g. "ttbar_SemiLeptonic").

Usage:
    python computeJetPUIDEfficiency.py \\
        --fileList <fileset.json> \\
        --outputDir SFs/JetPUID/Efficiency \\
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
from pathlib import Path

import numpy as np
import dask
import awkward as ak
import uproot
import hist
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
# These edges match the typical CMS PU ID kinematic range:
#   - pT  : 12.5 – 50 GeV  (PU ID is applied only in this range)
#   - |eta|: 0   – 5.0
PT_EDGES  = np.array([12.5, 20., 25., 30., 35., 40., 45., 50.], dtype=float)
ETA_EDGES = np.array([0., 1.0, 2.0, 2.5, 3.0, 4.0, 5.0],       dtype=float)


def _sample_from_key(dataset_key: str, era: str) -> str:
    """Extract the short sample name from a coffea dataset key.

    Expected key format (produced by run_all.py):
        {era}_{DataMC0}_{DataMC1}_{group}_{sample_parts...}
    e.g. "UL2018_MC_mu_SemiLeptonic_ttbar_SemiLeptonic"
      -> "ttbar_SemiLeptonic"

    Falls back to the full key if the format is unrecognised.
    """
    parts = dataset_key.split("_")
    # era is always the first component (no underscores), DataMC is 2 components,
    # group is 1 component → sample starts at index 4
    if len(parts) > 4:
        return "_".join(parts[4:])
    return dataset_key


# --------------------------------------------------------------------------- #
#  Coffea processor                                                            #
# --------------------------------------------------------------------------- #
class JetPUIDEfficiencyProcessor(processor.ProcessorABC):
    """Fills 2D (pT, |eta|) histograms for total and Loose-passing jets."""

    def process(self, events):
        dataset = events.metadata["dataset"]
        era     = events.metadata["era"]
        sample  = events.metadata.get("sample", _sample_from_key(dataset, era))
        logger.info(f"Processing {dataset} (era={era}, sample={sample})")

        # Kinematic selection: PU ID range
        jet_mask = (
            (events.Jet.pt > 12.5) &
            (events.Jet.pt <= 50.0) &
            (ak.abs(events.Jet.eta) < 5.0)
        )
        jets = events.Jet[jet_mask]

        # Materialise the dask arrays (called once per chunk)
        pt_all   = ak.to_numpy(ak.flatten(jets.pt).compute())
        eta_all  = ak.to_numpy(ak.flatten(ak.abs(jets.eta)).compute())
        pass_L   = ak.to_numpy(ak.flatten(jets.puId > 0).compute())

        # Fill histograms
        # Axis 0 = pT  (ROOT x-axis) → second argument in evaluator(eta, pt)
        # Axis 1 = |eta| (ROOT y-axis) → first argument in evaluator(eta, pt)
        h_total = hist.Hist(
            hist.axis.Variable(PT_EDGES,  name="pt",  label="Jet p_{T} [GeV]"),
            hist.axis.Variable(ETA_EDGES, name="eta", label="|#eta|"),
            storage=hist.storage.Double(),
        )
        h_loose = hist.Hist(
            hist.axis.Variable(PT_EDGES,  name="pt",  label="Jet p_{T} [GeV]"),
            hist.axis.Variable(ETA_EDGES, name="eta", label="|#eta|"),
            storage=hist.storage.Double(),
        )
        h_total.fill(pt=pt_all,         eta=eta_all)
        h_loose.fill(pt=pt_all[pass_L], eta=eta_all[pass_L])

        return {
            "era":     era,
            "sample":  sample,
            "h_total": h_total,
            "h_loose": h_loose,
        }

    def postprocess(self, accumulator):
        return accumulator


# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #
def _save_efficiency_root(outpath: str, h_total: hist.Hist, h_loose: hist.Hist):
    """Write the two efficiency histograms to a ROOT file."""
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with uproot.recreate(outpath) as rfile:
        rfile["Efficiency/JetPUId_pass_No"]    = h_total
        rfile["Efficiency/JetPUId_pass_Loose"] = h_loose
    logger.info(f"  Saved: {outpath}")


def _load_fileset(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


# --------------------------------------------------------------------------- #
#  Main                                                                        #
# --------------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(
        description="Compute JetPUID Loose efficiency maps and write ROOT files."
    )
    parser.add_argument(
        "--fileList", required=True,
        help="Coffea fileset JSON produced by run_all.py --prepareFilesets."
    )
    parser.add_argument(
        "--outputDir", required=True,
        help=(
            "Base output directory for ROOT files. "
            "Files are written to <outputDir>/<era>/<sample>.root. "
            "Typically 'SFs/JetPUID/Efficiency' relative to NanoAODTools/."
        ),
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

    fileset = _load_fileset(args.fileList)
    if not fileset:
        logger.error("Empty fileset — nothing to do.")
        sys.exit(1)

    logger.info(f"Loaded {len(fileset)} dataset(s) from {args.fileList}")

    # Optionally restrict to one file per dataset
    if args.sample:
        trimmed = {}
        for k, v in fileset.items():
            first_file = dict(list(v["files"].items())[:1])
            trimmed[k] = dict(v, files=first_file)
        fileset = trimmed
        logger.info("--sample mode: using first file per dataset only.")

    # Preprocess (validate file list)
    available, _ = preprocess(fileset, step_size=50_000, skip_bad_files=True)

    # Build dask computation graph
    to_compute, _ = apply_to_fileset(
        JetPUIDEfficiencyProcessor(),
        available,
        schemaclass=NanoAODSchema,
    )

    # Execute
    logger.info(f"Running with {args.workers} worker thread(s)…")
    (output,) = dask.compute(to_compute, scheduler="threads", num_workers=args.workers)

    # Save one ROOT file per dataset
    for dataset_key, result in output.items():
        era    = result["era"]
        sample = result["sample"]
        outpath = os.path.join(args.outputDir, era, f"{sample}.root")

        h_total = result["h_total"]
        h_loose = result["h_loose"]

        n_total = int(h_total.values().sum())
        n_loose = int(h_loose.values().sum())
        eff_global = n_loose / n_total if n_total > 0 else float("nan")
        logger.info(
            f"  {dataset_key}: {n_total} jets total, {n_loose} passing Loose "
            f"(global eff = {eff_global:.4f})"
        )

        _save_efficiency_root(outpath, h_total, h_loose)

    logger.info("Done.")


if __name__ == "__main__":
    main()
