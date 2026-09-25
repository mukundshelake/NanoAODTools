#!/usr/bin/env python3
"""Tests for config.yaml and config.py.

The point of these is drift. 005's config carries Ngen/Xsec/Lumi copied from
004B-BDTVariables so the chapter is self-contained and hashable, which is only
safe if something checks the copies still match. Issue #41 exists partly
because the old scripts' docstrings carried --lumi 19520.0 / --xsec 365.5 /
--ngen 110787582 against the 19521.2 / 366.3 / 131106831 used everywhere else.

Run directly (`python scripts/test_config.py`) or under pytest.
"""

import glob
import os
import sys

import numpy as np
import yaml

import binning
import config as cfgmod

REPO = cfgmod.CHAPTER.parent
UPSTREAM = REPO / "004B-BDTVariables" / "config.yaml"


def _upstream():
    with open(UPSTREAM) as fh:
        return yaml.safe_load(fh)


def test_config_loads_and_hashes():
    cfg = cfgmod.load()
    assert len(cfg["config_hash"]) == 12
    assert cfgmod.load()["config_hash"] == cfg["config_hash"], "hash must be stable"


def test_ngen_and_xsec_match_004B():
    """Every normalisation number must equal 004B's, which produced the inputs."""
    if not UPSTREAM.exists():
        print("    (skipped: 004B config not found)")
        return
    cfg = cfgmod.load()
    up = _upstream()["NgenandXsec"]
    for era, eras in cfg["NgenandXsec"].items():
        for group, samples in eras["MC_mu"].items():
            for sample, entry in samples.items():
                ref = up[era]["MC_mu"][group][sample]
                assert entry["Ngen"] == ref["Ngen"], (
                    f"{era}/{sample}: Ngen {entry['Ngen']} != 004B {ref['Ngen']}")
                assert entry["Xsec"] == ref["Xsec"], (
                    f"{era}/{sample}: Xsec {entry['Xsec']} != 004B {ref['Xsec']}")


def test_lumi_matches_004B():
    if not UPSTREAM.exists():
        print("    (skipped: 004B config not found)")
        return
    cfg = cfgmod.load()
    up = _upstream()["DataLumiInfo"]
    for era, entry in cfg["DataLumiInfo"].items():
        assert entry["Lumi"] == up[era]["Lumi"], (
            f"{era}: Lumi {entry['Lumi']} != 004B {up[era]['Lumi']}")


def test_binning_is_representable_on_the_fine_grid():
    """Otherwise the gen-acceptance scan cannot be projected onto it exactly."""
    cfg = cfgmod.load()
    for edges in (cfgmod.gen_mtt_edges(cfg), cfgmod.reco_mtt_edges(cfg)):
        binning.check_edges_representable(edges, scheme=cfgmod.scheme(cfg),
                                          y0=cfgmod.y0(cfg))


def test_reco_edges_are_a_refinement_of_gen_edges():
    """Every gen boundary must also be a reco boundary, or migration between
    gen bins gets smeared by the reco binning itself."""
    cfg = cfgmod.load()
    gen = cfgmod.gen_mtt_edges(cfg)
    reco = cfgmod.reco_mtt_edges(cfg)
    missing = [e for e in gen if not np.any(np.isclose(reco, e))]
    assert not missing, f"gen edges {missing} are not reco edges"


def test_data_weights_exclude_l1_prefiring():
    """L1 prefiring corrects simulation for an effect data already contains."""
    cfg = cfgmod.load()
    offenders = [w for w in cfg["weights"]["Data"] if "L1PreFiring" in w]
    assert not offenders, f"weights.Data must not contain {offenders}"


def test_every_systematic_nominal_is_actually_applied():
    """A variation of a weight that is never applied nominally is meaningless."""
    cfg = cfgmod.load()
    applied = set(cfg["weights"]["MC"])
    for source, spec in cfg["systematics"].items():
        assert spec["nominal"] in applied, (
            f"systematic {source!r} varies {spec['nominal']!r}, which is not in "
            f"weights.MC {sorted(applied)}")


def test_signal_ngen_is_the_sum_of_signed_weights():
    """Ngen must be sum(sign(LHEWeight)), not the raw generated count.

    Since LHEWeightSign is applied as an event weight, the normalisation
    denominator has to be the matching sum of signed weights. Verified against
    the Runs trees of the skim, which carry the ORIGINAL dataset counts:
    sum(genEventSumw) / |genWeight| must equal the configured Ngen, while
    sum(genEventCount) -- the raw count -- must not.

    Slow (reads ~117 Runs trees); skips if the skim is not on local disk.
    """
    import uproot

    cfg = cfgmod.load()
    era = "UL2016preVFP"
    sample = cfg["Signal"]
    files = sorted(glob.glob(str(cfgmod.input_dir(cfg, era, "MC_mu", sample) / "*.root")))
    if not files:
        print("    (skipped: BDTScore skim not on local disk)")
        return

    count = sumw = 0.0
    for path in files:
        with uproot.open(path) as handle:
            runs = handle["Runs"].arrays(["genEventCount", "genEventSumw"], library="np")
            count += float(runs["genEventCount"].sum())
            sumw += float(runs["genEventSumw"].sum())

    with uproot.open(files[0]) as handle:
        weights = handle["Events"].arrays(["LHEWeight_originalXWGTUP"], library="np")
        magnitudes = np.unique(np.abs(weights["LHEWeight_originalXWGTUP"]))
    assert len(magnitudes) == 1, f"|LHEWeight| is not constant: {magnitudes[:5]}"

    implied = sumw / magnitudes[0]
    _, configured = cfgmod.sample_norm(cfg, era, sample)
    print(f"    raw count {count:,.0f} | signed-weight sum {implied:,.0f} "
          f"| configured {configured:,.0f}")
    assert abs(implied - configured) / configured < 1e-6, (
        f"Ngen {configured:,.0f} != sum of signed weights {implied:,.0f}")
    assert abs(count - configured) / configured > 1e-4, (
        "Ngen equals the RAW generated count; it must be the signed-weight sum")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
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
