"""
computeDilutionFactor.py

Computes the dilution factor D = (Np - Nn) / (Np + Nn) for a qqbar-initiated
sample, where:
    Np = events where the incoming quark carries a larger momentum fraction
         than the incoming antiquark (x_q > x_qbar)
    Nn = events where the incoming antiquark carries a larger momentum
         fraction than the incoming quark (x_qbar > x_q)

Momentum fractions come from Generator_x1/Generator_x2 (Bjorken-x of the two
incoming partons). Which of the two partons is the quark and which is the
antiquark is determined from GenPart_pdgId: the two hard-process incoming
partons are the GenPart entries with GenPart_status == 21 (there are always
exactly two, one per beam). Sorting that pair by eta (descending) recovers
generator order: the +z / "beam 1" parton always matches Generator_id1 (and
therefore Generator_x1), the -z / "beam 2" parton always matches
Generator_id2 / Generator_x2 -- verified directly against Generator_id1/id2
on this sample.

Events where the initial state isn't qqbar (gg, qg, qq, qbar-qbar, ...) don't
have a well-defined "the quark" / "the antiquark", so they're excluded from
Np/Nn (counted separately as skipped).

This is an unweighted, per-event counting exercise (no Lumi*Xsec/Ngen or
SF weighting) over the 003-ObjectSelectionII skims -- object selection
already happened upstream in 003-ObjectSelectionI, and this chapter only
adds SF branches, so Generator_x1/x2 and GenPart_pdgId are unchanged
pass-through branches from NanoAOD.

Needs uproot/awkward (not part of the CMSSW env this repo otherwise uses,
e.g. `conda activate latestcoffea`).

Usage:
    python computeDilutionFactor.py \\
        --dataset-json outputs/midAugust/latest/UL2016preVFP/selectionII_midAugust_UL2016preVFP_datasets.json \\
        --data-mc MC_mu --group SemiLeptonic --dataset ttbar_SemiLeptonic
"""

import argparse
import json
import logging

import awkward as ak
import numpy as np
import uproot

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BRANCHES = ["GenPart_pdgId", "GenPart_status", "GenPart_eta", "Generator_x1", "Generator_x2"]


def get_file_list(dataset_json_path, data_mc, group, dataset):
    with open(dataset_json_path) as f:
        datasets = json.load(f)
    try:
        files = datasets[data_mc][group][dataset]
    except KeyError as e:
        raise KeyError(
            f"{data_mc}/{group}/{dataset} not found in {dataset_json_path}. "
            f"Available top-level keys: {list(datasets.keys())}"
        ) from e
    # files: {filepath: treename}
    return list(files.items())


def process_batch(arrays, counts):
    pdg = arrays["GenPart_pdgId"]
    status = arrays["GenPart_status"]
    eta = arrays["GenPart_eta"]
    x1 = arrays["Generator_x1"]
    x2 = arrays["Generator_x2"]

    incoming_mask = status == 21
    n_incoming = ak.sum(incoming_mask, axis=1)
    counts["n_events"] += len(pdg)

    good = n_incoming == 2
    n_bad = int(ak.sum(~good))
    if n_bad:
        logger.warning(f"  {n_bad} events without exactly 2 status==21 partons; skipping them.")
        counts["n_malformed"] += n_bad

    pdg = pdg[incoming_mask][good]
    eta = eta[incoming_mask][good]
    x1 = x1[good]
    x2 = x2[good]

    # Sort the pair by eta descending: +z ("beam 1"/x1 side) first, -z ("beam 2"/x2 side) second.
    order = ak.argsort(eta, axis=1, ascending=False)
    pdg = pdg[order]
    pdg_x1_side = pdg[:, 0]
    pdg_x2_side = pdg[:, 1]

    is_quark_x1 = (pdg_x1_side >= 1) & (pdg_x1_side <= 5)
    is_antiq_x1 = (pdg_x1_side >= -5) & (pdg_x1_side <= -1)
    is_quark_x2 = (pdg_x2_side >= 1) & (pdg_x2_side <= 5)
    is_antiq_x2 = (pdg_x2_side >= -5) & (pdg_x2_side <= -1)

    qqbar_x1q = is_quark_x1 & is_antiq_x2   # quark on x1 side, antiquark on x2 side
    qqbar_x2q = is_quark_x2 & is_antiq_x1   # quark on x2 side, antiquark on x1 side
    valid = qqbar_x1q | qqbar_x2q

    quark_x = ak.where(qqbar_x1q, x1, x2)
    antiquark_x = ak.where(qqbar_x1q, x2, x1)

    counts["n_valid"] += int(ak.sum(valid))
    counts["n_skipped_not_qqbar"] += int(ak.sum(~valid))
    counts["Np"] += int(ak.sum(valid & (quark_x > antiquark_x)))
    counts["Nn"] += int(ak.sum(valid & (quark_x < antiquark_x)))
    counts["n_tied"] += int(ak.sum(valid & (quark_x == antiquark_x)))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset-json", required=True,
                        help="selectionII_{tag}_{era}_datasets.json produced by run_all.py --generateDatasetJSON")
    parser.add_argument("--data-mc", default="MC_mu")
    parser.add_argument("--group", default="SemiLeptonic")
    parser.add_argument("--dataset", default="ttbar_SemiLeptonic")
    parser.add_argument("--step-size", type=int, default=200000, help="uproot.iterate step_size (entries per batch)")
    parser.add_argument("--max-files", type=int, default=None, help="Limit number of files (for quick tests)")
    args = parser.parse_args()

    files = get_file_list(args.dataset_json, args.data_mc, args.group, args.dataset)
    if args.max_files:
        files = files[:args.max_files]
    logger.info(f"{len(files)} files for {args.data_mc}/{args.group}/{args.dataset}")

    file_dict = {path: tree for path, tree in files}

    counts = {
        "n_events": 0,
        "n_malformed": 0,
        "n_valid": 0,
        "n_skipped_not_qqbar": 0,
        "n_tied": 0,
        "Np": 0,
        "Nn": 0,
    }

    for arrays in uproot.iterate(file_dict, expressions=BRANCHES, step_size=args.step_size):
        process_batch(arrays, counts)
        logger.info(f"  running: n_events={counts['n_events']}, Np={counts['Np']}, Nn={counts['Nn']}")

    Np, Nn = counts["Np"], counts["Nn"]
    N_valid = Np + Nn + counts["n_tied"]
    D = (Np - Nn) / (Np + Nn) if (Np + Nn) else float("nan")
    D_err = np.sqrt(max(1 - D**2, 0) / N_valid) if N_valid else float("nan")

    print("\n=== Dilution factor ===")
    print(f"Dataset:              {args.data_mc}/{args.group}/{args.dataset}")
    print(f"Total events read:    {counts['n_events']}")
    print(f"Malformed (skipped):  {counts['n_malformed']}")
    print(f"Non-qqbar (skipped):  {counts['n_skipped_not_qqbar']}")
    print(f"qqbar events:         {N_valid}  (tied x1==x2: {counts['n_tied']})")
    print(f"Np (quark harder):    {Np}")
    print(f"Nn (antiquark harder):{Nn}")
    print(f"D = (Np - Nn)/(Np + Nn) = {D:.6f} +/- {D_err:.6f}")


if __name__ == "__main__":
    main()
