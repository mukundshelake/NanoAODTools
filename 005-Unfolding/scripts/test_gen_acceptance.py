#!/usr/bin/env python3
"""Tests for gen_acceptance.py.

Both tests here are regressions for bugs found by the lxplus session when it
first ran the scan for real -- neither could have been caught by the local
validation, which reads local files with an empty redirector and never
exercised --merge across more than one cached file.
"""

import argparse
import json
import tempfile
from pathlib import Path

import numpy as np

import binning
import gen_acceptance as g


def test_build_url_uses_a_double_slash_for_xrootd():
    """xrootd needs root://host//store/... -- a single slash makes the path
    relative and every open fails with '[3010] Opening relative path ...
    is disallowed'."""
    for redirector in ("root://cms-xrd-global.cern.ch/",
                       "root://cms-xrd-global.cern.ch",
                       "root://cms-xrd-global.cern.ch///"):
        url = g.build_url(redirector, "/store/mc/x.root")
        assert url == "root://cms-xrd-global.cern.ch//store/mc/x.root", url


def test_build_url_leaves_local_paths_alone():
    # The local-file path is how this script is validated against the skim;
    # it must not acquire a leading double slash.
    assert g.build_url("", "/mnt/disk1/a.root") == "/mnt/disk1/a.root"


def _write_cache(cache_dir, lfn, weight, channel="mu"):
    """Write a synthetic per-file cache entry shaped like scan_file's payload."""
    hist, hist_dy = binning.new_fine()
    binning.fill_fine(hist, hist_dy, np.array([500.0]), np.array([0.4]),
                      np.array([-0.2]), np.array([weight]))
    payload = {
        "hist": hist,
        "hist_dy": hist_dy,
        "totals": np.array([10.0, 3000.0, 900.0]),
        "counters": np.array([10.0, 0.0, weight, weight]),
    }
    for c in g.CHANNEL_ORDER:
        ch, ch_dy = binning.new_fine()
        if c == channel:
            binning.fill_fine(ch, ch_dy, np.array([500.0]), np.array([0.4]),
                              np.array([-0.2]), np.array([weight]))
        payload[f"hist_{c}"], payload[f"hist_dy_{c}"] = ch, ch_dy
    cache_dir.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(cache_dir / g.cache_name(lfn), **payload)


def test_merge_accumulates_across_cached_files():
    """--merge over more than one file.

    binning.new_fine() returns a tuple, so the merge loop's
    `by_channel[c][0] += ...` raised TypeError on the item assignment -- the
    in-place add on the array succeeded, then the tuple refused the
    write-back. Only reachable with at least one cache entry present.
    """
    with tempfile.TemporaryDirectory() as tmp:
        outdir = Path(tmp)
        era = "UL2016preVFP"
        era_dir = outdir / era
        era_dir.mkdir(parents=True)
        lfns = ["/store/mc/a.root", "/store/mc/b.root"]
        (era_dir / "filelist.txt").write_text("\n".join(lfns) + "\n")
        for lfn, w in zip(lfns, (1.0, 3.0)):
            _write_cache(era_dir / "cache", lfn, w)

        args = argparse.Namespace(
            era=era, outdir=str(outdir), dataset="ttbar_SemiLeptonic",
            group="SemiLeptonic", datamc="MC_mu", shard=None,
            scheme="sign", y0=1.2,
            gen_mtt_edges=[300, 450, 600, 750, 900, 1050, 1200],
        )
        assert g.do_merge(args) == 0

        merged = outdir / f"gen_acceptance_{era}.npz"
        assert merged.exists(), "merge wrote no output"
        with np.load(merged, allow_pickle=False) as data:
            assert np.isclose(data["hist"].sum(), 4.0), data["hist"].sum()
            assert np.isclose(data["hist_dy"].sum(), 4.0)
            # the mu channel took both entries, the others none
            assert np.isclose(data["hist_mu"].sum(), 4.0)
            assert np.isclose(data["hist_e"].sum(), 0.0)
            assert np.isclose(sum(data[f"hist_{c}"].sum() for c in g.CHANNEL_ORDER),
                              data["hist"].sum()), "channel split must sum to the total"
            meta = json.loads(str(data["meta"]))
            assert meta["genEventCount"] == 20.0
            assert meta["n_files"] == 2


def test_merge_refuses_to_run_with_missing_cache_entries():
    """A partial scan must not silently produce a short-normalised result."""
    with tempfile.TemporaryDirectory() as tmp:
        outdir = Path(tmp)
        era = "UL2016preVFP"
        era_dir = outdir / era
        era_dir.mkdir(parents=True)
        lfns = ["/store/mc/a.root", "/store/mc/b.root"]
        (era_dir / "filelist.txt").write_text("\n".join(lfns) + "\n")
        _write_cache(era_dir / "cache", lfns[0], 1.0)   # only one of two

        args = argparse.Namespace(
            era=era, outdir=str(outdir), dataset="ttbar_SemiLeptonic",
            group="SemiLeptonic", datamc="MC_mu", shard=None,
            scheme="sign", y0=1.2,
            gen_mtt_edges=[300, 450, 600, 750, 900, 1050, 1200],
        )
        try:
            g.do_merge(args)
        except SystemExit:
            return
        raise AssertionError("merge should refuse when cache entries are missing")


def test_shard_is_one_indexed():
    """--shard 0/N is rejected; the lxplus session hit this first time out."""
    with tempfile.TemporaryDirectory() as tmp:
        outdir = Path(tmp)
        era = "UL2016preVFP"
        (outdir / era).mkdir(parents=True)
        (outdir / era / "filelist.txt").write_text(
            "\n".join(f"/store/mc/{i}.root" for i in range(117)) + "\n")
        base = dict(era=era, outdir=str(outdir))
        try:
            g.read_filelist(argparse.Namespace(shard="0/58", **base))
        except SystemExit:
            pass
        else:
            raise AssertionError("--shard 0/58 should be rejected")
        # 117 files over 58 shards: shard 1 gets files 0 and 58 and 116 -> 3
        got = g.read_filelist(argparse.Namespace(shard="1/58", **base))
        assert len(got) == 3, f"expected 3 files in shard 1/58, got {len(got)}"
        # every file lands in exactly one shard
        seen = []
        for i in range(1, 59):
            seen += g.read_filelist(argparse.Namespace(shard=f"{i}/58", **base))
        assert len(seen) == 117 and len(set(seen)) == 117


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
