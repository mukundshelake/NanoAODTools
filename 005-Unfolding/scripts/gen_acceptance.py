#!/usr/bin/env python3
"""Gen-level scan of the *unskimmed* NanoAOD, for the acceptance correction (issue #31).

Why this exists
---------------
Everything on local disk starts at the 002-Samples CRAB preselection, which
already requires a reconstructed muon + >=4 jets + >=2 b-tags + trigger + MET
filters. About 4.5% of generated ttbar events survive that, and the gen-level
information of the other ~95.5% exists nowhere we own -- only in the central
NANOAODSIM dataset on DAS.

The response matrix needs that missing piece as its inefficiency column. Without
it, TUnfold returns N_gen(i) * eff(i) rather than N_gen(i), and since eff depends
on |y_t| (forward tops throw the muon and jets out of |eta| < 2.4), that
efficiency shape sits directly on top of the asymmetry being measured.

What it produces
----------------
Per era, a pair of *fine* gen histograms over **all** generated events of the
signal dataset: (m_tt, y_t, y_tbar) in 3-D and (m_tt, delta|y|) in 2-D -- see
binning.FINE_* for the axes and for why both are needed. The
analysis binning is a cheap offline projection of that (binning.project_fine),
so changing the m_tt edges, the N+/N- definition or y0 never requires re-running
this scan. Output is a handful of MB per era.

Also recorded, for normalisation and cross-checks:
  - genEventCount / genEventSumw / genEventSumw2 summed over the Runs tree
  - the same fine histogram split by gen lepton flavour (e / mu / tau / other)
  - the number of events where the gen t or tbar could not be identified

Where it runs
-------------
lxplus (or any node with dasgoclient + a valid grid proxy + xrootd). It reads
only GenPart_{pdgId,statusFlags,pt,eta,phi,mass} and genWeight, so it is I/O
bound on the xrootd read, not on CPU.

    # once per session
    source /cvmfs/cms.cern.ch/cmsset_default.sh
    voms-proxy-init --voms cms -valid 192:00
    # a python with numpy + uproot + awkward, e.g.
    source /cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc13-opt/setup.sh

    # 1. resolve the dataset file list from DAS (uses 002-Samples/config.yaml)
    python scripts/gen_acceptance.py --era UL2016preVFP --build-filelist

    # 2. scan (resumable; re-running skips files already cached)
    python scripts/gen_acceptance.py --era UL2016preVFP --scan --workers 8

    # ... or split across HTCondor jobs, then merge
    python scripts/gen_acceptance.py --era UL2016preVFP --scan --shard 3/40
    python scripts/gen_acceptance.py --era UL2016preVFP --merge

Copy the resulting `gen_acceptance_{era}.npz` back to the analysis machine and
point `build_inputs.py --acceptance` at it.
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import binning  # noqa: E402  (single source of truth, see module docstring)
from kinematics import invariant_mass, rapidity  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
CHAPTER = Path(__file__).resolve().parents[1]

# Only what the gen-level quantities need. Keeping this list minimal is the
# whole reason the scan is affordable over xrootd.
GEN_BRANCHES = [
    "GenPart_pdgId",
    "GenPart_statusFlags",
    "GenPart_pt",
    "GenPart_eta",
    "GenPart_phi",
    "GenPart_mass",
]

# The event weight MUST match what the rest of the analysis applies, or the
# acceptance ratio is taken between two different normalisations. 003's
# LHEWeightSignProducer writes sign(LHEWeight_originalXWGTUP), with 0.0 for a
# zero weight and 1.0 when the branch is absent -- reproduced exactly in
# lhe_weight_sign() below. genWeight is read alongside only as a cross-check;
# genEventSumw in the Runs tree is its sum, which is a *different* quantity and
# must not be used to normalise a sign-weighted histogram (issue #41).
WEIGHT_BRANCHES = ["LHEWeight_originalXWGTUP", "genWeight"]

# GenPart_statusFlags bit positions (NanoAOD documentation)
BIT_IS_HARD_PROCESS = 7
BIT_IS_LAST_COPY = 13

LEPTON_CHANNELS = {11: "e", 13: "mu", 15: "tau"}
CHANNEL_ORDER = ["e", "mu", "tau", "other"]


# ---------------------------------------------------------------------------
# DAS
# ---------------------------------------------------------------------------

def load_das_query(era, dataset, group, datamc):
    """Pull the DAS dataset name out of 002-Samples/config.yaml.

    002 is the source of truth for which datasets exist; duplicating the
    dataset string here would be exactly the kind of drift issue #41 is about.
    """
    import yaml

    cfg_path = REPO_ROOT / "002-Samples" / "config.yaml"
    with open(cfg_path) as fh:
        cfg = yaml.safe_load(fh)

    try:
        return cfg["DASQueries"][era][datamc][group][dataset]
    except KeyError as exc:
        raise SystemExit(
            f"No DAS query for {era}/{datamc}/{group}/{dataset} in {cfg_path} "
            f"(missing key {exc})"
        )


def das_file_list(dataset_name):
    """`dasgoclient -query='file dataset=...'`, same pattern as 002/getFileList.py."""
    cmd = ["dasgoclient", "-query", f"file dataset={dataset_name}"]
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(
            "dasgoclient failed -- is a grid proxy active "
            "(voms-proxy-init --voms cms)?\n" + result.stderr
        )
    files = [line.strip() for line in result.stdout.splitlines() if line.strip().startswith("/store/")]
    if not files:
        raise SystemExit(f"DAS returned no files for {dataset_name}")
    return sorted(files)


# ---------------------------------------------------------------------------
# Per-file scan
# ---------------------------------------------------------------------------

def lhe_weight_sign(arrays, available, n):
    """Reproduce 003-ObjectSelectionII/scripts/modules/LHEWeightSign.py exactly.

    sign(LHEWeight_originalXWGTUP), with 0.0 for a zero weight and 1.0 when the
    branch is absent. Any divergence here silently rescales the acceptance
    denominator relative to the selected-event numerator.
    """
    import awkward as ak

    if "LHEWeight_originalXWGTUP" in available:
        w = ak.to_numpy(arrays["LHEWeight_originalXWGTUP"]).astype(np.float64)
        return np.sign(w)
    if "genWeight" in available:
        return np.sign(ak.to_numpy(arrays["genWeight"]).astype(np.float64))
    return np.ones(n, dtype=np.float64)


def cache_name(lfn):
    """Stable per-file cache key; LFNs are too long and slashy to use directly."""
    return hashlib.sha256(lfn.encode()).hexdigest()[:16] + ".npz"


def scan_file(lfn, redirector, cache_dir, retries=3, warnings=None):
    """Scan one NanoAOD file, returning its per-file payload (and caching it).

    Resumable by design: an existing cache entry is returned untouched, so a
    scan interrupted after 900 of 1000 files costs 100 files to finish.
    """
    import uproot
    import awkward as ak

    warnings = warnings if warnings is not None else []
    cache_path = Path(cache_dir) / cache_name(lfn)
    if cache_path.exists():
        with np.load(cache_path, allow_pickle=False) as data:
            return {k: data[k] for k in data.files}, True

    url = redirector.rstrip("/") + lfn
    last_error = None
    for attempt in range(retries):
        try:
            with uproot.open(url, timeout=600) as handle:
                tree = handle["Events"]
                tree_keys = set(tree.keys())
                runs = handle["Runs"].arrays(
                    ["genEventCount", "genEventSumw", "genEventSumw2"], library="np"
                )
                totals = np.array([
                    float(runs["genEventCount"].sum()),
                    float(runs["genEventSumw"].sum()),
                    float(runs["genEventSumw2"].sum()),
                ])

                available_weights = [b for b in WEIGHT_BRANCHES if b in tree_keys]
                if "LHEWeight_originalXWGTUP" not in available_weights:
                    warnings.append(
                        f"{lfn}: no LHEWeight_originalXWGTUP; "
                        + ("falling back to sign(genWeight)" if "genWeight" in available_weights
                           else "falling back to unit weights")
                    )

                hist, hist_dy = binning.new_fine()
                by_channel = {c: binning.new_fine() for c in CHANNEL_ORDER}
                n_total = 0
                n_no_top = 0
                sum_w = 0.0
                sum_gw = 0.0

                for arrays in tree.iterate(GEN_BRANCHES + available_weights,
                                           step_size="200 MB"):
                    n_total += len(arrays)

                    pdg = arrays["GenPart_pdgId"]
                    flags = arrays["GenPart_statusFlags"]
                    last_copy = (flags >> BIT_IS_LAST_COPY) & 1 == 1
                    hard = (flags >> BIT_IS_HARD_PROCESS) & 1 == 1

                    top = last_copy & (pdg == 6)
                    atop = last_copy & (pdg == -6)

                    def first_of(mask, field):
                        return ak.to_numpy(
                            ak.fill_none(ak.firsts(arrays[field][mask]), np.nan)
                        ).astype(np.float64)

                    t = [first_of(top, f) for f in
                         ("GenPart_pt", "GenPart_eta", "GenPart_phi", "GenPart_mass")]
                    a = [first_of(atop, f) for f in
                         ("GenPart_pt", "GenPart_eta", "GenPart_phi", "GenPart_mass")]

                    weight = lhe_weight_sign(arrays, available_weights, len(arrays))
                    sum_w += float(weight.sum())
                    if "genWeight" in available_weights:
                        sum_gw += float(
                            np.sign(ak.to_numpy(arrays["genWeight"]).astype(np.float64)).sum()
                        )

                    y_t = rapidity(t[0], t[1], t[3])
                    y_a = rapidity(a[0], a[1], a[3])
                    mtt = invariant_mass(t, a)

                    n_no_top += int(np.sum(~np.isfinite(t[0]) | ~np.isfinite(a[0])))

                    binning.fill_fine(hist, hist_dy, mtt, y_t, y_a, weight)

                    # Channel split: TTToSemiLeptonic has exactly one
                    # hard-process charged lepton per event. Recorded so the
                    # muon-channel subset can be cross-checked later without a
                    # second pass over the dataset.
                    lep = hard & (abs(pdg) >= 11) & (abs(pdg) <= 16) & (abs(pdg) % 2 == 1)
                    lep_pdg = ak.to_numpy(
                        ak.fill_none(ak.firsts(abs(pdg[lep])), 0)
                    ).astype(np.int64)
                    assigned = np.zeros(len(lep_pdg), dtype=bool)
                    for code, name in LEPTON_CHANNELS.items():
                        sel = lep_pdg == code
                        assigned |= sel
                        if sel.any():
                            binning.fill_fine(*by_channel[name], mtt[sel], y_t[sel],
                                              y_a[sel], weight[sel])
                    if (~assigned).any():
                        binning.fill_fine(*by_channel["other"], mtt[~assigned],
                                          y_t[~assigned], y_a[~assigned],
                                          weight[~assigned])

                payload = {
                    "hist": hist,
                    "hist_dy": hist_dy,
                    "totals": totals,
                    "counters": np.array([n_total, n_no_top, sum_w, sum_gw],
                                         dtype=np.float64),
                }
                for c in CHANNEL_ORDER:
                    payload[f"hist_{c}"], payload[f"hist_dy_{c}"] = by_channel[c]

                cache_path.parent.mkdir(parents=True, exist_ok=True)
                np.savez_compressed(cache_path, **payload)
                return payload, False

        except Exception as exc:  # noqa: BLE001 - xrootd failures are varied and transient
            last_error = exc
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))

    raise RuntimeError(f"{lfn}: failed after {retries} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def outdir_for(era, outdir):
    path = Path(outdir) / era
    path.mkdir(parents=True, exist_ok=True)
    return path


def do_build_filelist(args):
    dataset_name = load_das_query(args.era, args.dataset, args.group, args.datamc)
    print(f"Dataset: {dataset_name}")
    files = das_file_list(dataset_name)
    target = outdir_for(args.era, args.outdir) / "filelist.txt"
    target.write_text("\n".join(files) + "\n")
    print(f"Wrote {len(files)} files to {target}")


def read_filelist(args):
    path = outdir_for(args.era, args.outdir) / "filelist.txt"
    if not path.exists():
        raise SystemExit(f"{path} not found -- run --build-filelist first")
    files = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    if args.shard:
        index, total = (int(x) for x in args.shard.split("/"))
        if not 1 <= index <= total:
            raise SystemExit(f"--shard {args.shard}: index must be in 1..{total}")
        files = files[index - 1::total]
        print(f"Shard {index}/{total}: {len(files)} files")
    return files


def do_scan(args):
    files = read_filelist(args)
    cache_dir = outdir_for(args.era, args.outdir) / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    failures = []
    warnings = []
    done = 0
    started = time.time()

    def record(index, lfn):
        nonlocal done
        try:
            _, cached = scan_file(lfn, args.redirector, cache_dir, args.retries, warnings)
        except Exception as exc:  # noqa: BLE001
            failures.append((lfn, str(exc)))
            print(f"  [{index}/{len(files)}] FAILED {lfn}\n      {exc}")
            return
        done += 1
        tag = "cached" if cached else "read"
        rate = done / max(time.time() - started, 1e-9)
        print(f"  [{index}/{len(files)}] {tag:6s} {lfn.split('/')[-1]}  ({rate:.2f} files/s)")

    if args.workers > 1:
        from concurrent.futures import ThreadPoolExecutor
        # Thread pool, not process pool: the work is xrootd I/O, and threads
        # share the cache directory without any extra coordination.
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            list(pool.map(lambda pair: record(pair[0] + 1, pair[1]), enumerate(files)))
    else:
        for i, lfn in enumerate(files, 1):
            record(i, lfn)

    print(f"\nScanned {done}/{len(files)} files, {len(failures)} failures")
    if warnings:
        print(f"{len(warnings)} warnings; first few:")
        for line in warnings[:5]:
            print(f"  ! {line}")
    if failures:
        report = outdir_for(args.era, args.outdir) / "failures.json"
        report.write_text(json.dumps(dict(failures), indent=2))
        print(f"Failures written to {report} -- re-run --scan to retry them")
        return 1
    return 0


def do_merge(args):
    files = read_filelist(args)
    cache_dir = outdir_for(args.era, args.outdir) / "cache"

    hist, hist_dy = binning.new_fine()
    by_channel = {c: binning.new_fine() for c in CHANNEL_ORDER}
    totals = np.zeros(3, dtype=np.float64)
    counters = np.zeros(4, dtype=np.float64)

    missing = []
    for lfn in files:
        path = cache_dir / cache_name(lfn)
        if not path.exists():
            missing.append(lfn)
            continue
        with np.load(path, allow_pickle=False) as data:
            hist += data["hist"]
            hist_dy += data["hist_dy"]
            totals += data["totals"]
            counters += data["counters"]
            for c in CHANNEL_ORDER:
                by_channel[c][0] += data[f"hist_{c}"]
                by_channel[c][1] += data[f"hist_dy_{c}"]

    if missing:
        raise SystemExit(
            f"{len(missing)}/{len(files)} files have no cache entry; "
            "finish --scan before merging. First few:\n  " + "\n  ".join(missing[:5])
        )

    dataset_name = load_das_query(args.era, args.dataset, args.group, args.datamc)
    meta = {
        "era": args.era,
        "dataset": dataset_name,
        "n_files": len(files),
        "genEventCount": totals[0],
        "genEventSumw": totals[1],
        "genEventSumw2": totals[2],
        "n_events_read": counters[0],
        "n_events_missing_gen_top": counters[1],
        "sum_lhe_weight_sign": counters[2],
        "sum_gen_weight_sign": counters[3],
        "fine_mtt_edges": binning.FINE_MTT_EDGES.tolist(),
        "fine_y_edges": binning.FINE_Y_EDGES.tolist(),
        "fine_dy_edges": binning.FINE_DY_EDGES.tolist(),
        "generated_by": "005-Unfolding/scripts/gen_acceptance.py",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    out = outdir_for(args.era, args.outdir).parent / f"gen_acceptance_{args.era}.npz"
    per_channel = {}
    for c in CHANNEL_ORDER:
        per_channel[f"hist_{c}"], per_channel[f"hist_dy_{c}"] = by_channel[c]
    np.savez_compressed(out, hist=hist, hist_dy=hist_dy,
                        meta=json.dumps(meta), **per_channel)

    print(f"\n{'=' * 68}")
    print(f"Era                       : {args.era}")
    print(f"Files merged              : {len(files)}")
    print(f"Events read               : {counters[0]:,.0f}")
    print(f"genEventCount (Runs tree) : {totals[0]:,.0f}")
    print(f"genEventSumw  (Runs tree) : {totals[1]:,.1f}")
    print(f"sum sign(LHEWeight)       : {counters[2]:,.1f}   <- normalisation denominator")
    print(f"sum sign(genWeight)       : {counters[3]:,.1f}   (cross-check)")
    print(f"fine histogram total      : {hist.sum():,.1f}")
    print(f"events with no gen t/tbar : {counters[1]:,.0f}")
    if counters[0] != totals[0]:
        print("  NOTE: events read != genEventCount; the dataset was not fully scanned.")
    print()
    for c in CHANNEL_ORDER:
        total_c = by_channel[c][0].sum()
        frac = total_c / hist.sum() if hist.sum() else 0.0
        print(f"  channel {c:<6s} {total_c:>16,.1f}  ({frac:6.2%})")

    unrolled, unclassified = binning.project_fine(
        hist, hist_dy, np.array(args.gen_mtt_edges, dtype=float),
        scheme=args.scheme, y0=args.y0,
    )
    print(f"\nProjection onto gen binning ({args.scheme}, edges={args.gen_mtt_edges}):")
    for label, value in zip(binning.labels(np.array(args.gen_mtt_edges, dtype=float)), unrolled):
        print(f"  {label:<24s} {value:>14,.1f}")
    print(f"  {'outside binning':<24s} {unclassified:>14,.1f}")
    print(f"\nWrote {out}")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Gen-level acceptance scan over unskimmed NanoAOD (runs on lxplus)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--era", required=True,
                        choices=["UL2016preVFP", "UL2016postVFP", "UL2017", "UL2018"])
    parser.add_argument("--dataset", default="ttbar_SemiLeptonic",
                        help="dataset key inside 002-Samples/config.yaml DASQueries")
    parser.add_argument("--group", default="SemiLeptonic")
    parser.add_argument("--datamc", default="MC_mu")
    parser.add_argument("--outdir", default=str(CHAPTER / "outputs" / "gen_acceptance"))
    parser.add_argument("--redirector", default="root://cms-xrd-global.cern.ch/")
    parser.add_argument("--workers", type=int, default=4,
                        help="concurrent xrootd reads; the job is I/O bound")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--shard", default=None, metavar="I/N",
                        help="scan only shard I of N, for HTCondor splitting")
    parser.add_argument("--scheme", default="sign", choices=["sign", "threshold"],
                        help="classification used for the --merge summary only")
    parser.add_argument("--y0", type=float, default=1.2)
    parser.add_argument("--gen-mtt-edges", type=float, nargs="+",
                        default=[300, 450, 600, 750, 900, 1050, 1200],
                        help="used for the --merge summary only; the stored "
                             "histogram is binning-independent")

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--build-filelist", action="store_true")
    mode.add_argument("--scan", action="store_true")
    mode.add_argument("--merge", action="store_true")

    args = parser.parse_args()

    if args.build_filelist:
        return do_build_filelist(args)
    if args.scan:
        return do_scan(args)
    return do_merge(args)


if __name__ == "__main__":
    sys.exit(main() or 0)
