#!/usr/bin/env python3
"""Tests for binning.py, the single source of truth for the 005 binning.

Run directly (`python scripts/test_binning.py`) or under pytest.

The property that matters most here is *exactness of the fine-histogram
projection*. gen_acceptance.py stores fine histograms so that the analysis
binning can be changed without re-running a many-hour lxplus scan; that is only
legitimate if projecting the fine histogram gives bit-for-bit the same answer as
binning the events directly. An early version stored only (m_tt, y_t, y_tbar)
and projected sign(delta|y|) from it, which misassigned every event whose
|y_t| and |y_tbar| landed in the same 0.1-wide bin -- 0.25% of the sample,
against an A_C of order 0.001. test_sign_projection_is_exact guards that.
"""

import numpy as np

import binning as b


def _sample(n=200_000, seed=0, y_scale=1.1):
    rng = np.random.default_rng(seed)
    return (
        rng.uniform(200, 2200, n),
        rng.normal(0, y_scale, n),
        rng.normal(0, y_scale, n),
        rng.choice([1.0, -1.0], n, p=[0.96, 0.04]),
    )


EDGES = np.array([300, 450, 600, 750, 900, 1050, 1200], dtype=float)


def _direct(mtt, yt, ytbar, w, scheme):
    plus, minus = b.classify(yt, ytbar, scheme=scheme)
    bins = b.unrolled_bin(mtt, plus, minus, EDGES)
    out = np.zeros(b.n_unrolled_bins(EDGES))
    ok = bins >= 0
    np.add.at(out, bins[ok], w[ok])
    return out


def test_zero_and_y0_lie_exactly_on_edges():
    # sign(delta|y|) is the observable, so a fine bin straddling delta|y| = 0
    # would mix N+ and N- with no way to unmix them.
    assert np.any(b.FINE_DY_EDGES == 0.0)
    assert np.any(b.FINE_Y_EDGES == 0.0)
    assert np.any(b.FINE_Y_EDGES == 1.2)   # default y0 for the threshold scheme


def test_fill_conserves_total():
    mtt, yt, ytbar, w = _sample()
    hist, hist_dy = b.new_fine()
    b.fill_fine(hist, hist_dy, mtt, yt, ytbar, w)
    assert np.isclose(hist.sum(), w.sum())
    assert np.isclose(hist_dy.sum(), w.sum())


def test_non_finite_inputs_are_counted_not_dropped():
    # A failed reconstruction must stay countable rather than vanish or be
    # silently classified as a central top (issue #42).
    hist, hist_dy = b.new_fine()
    b.fill_fine(hist, hist_dy,
                np.array([np.nan, 500.0, np.inf]),
                np.array([0.5, 0.5, 0.5]),
                np.array([0.1, 0.1, np.nan]),
                np.array([1.0, 1.0, 1.0]))
    assert hist.sum() == 3.0
    assert hist_dy.sum() == 3.0


def test_projection_conserves_total():
    mtt, yt, ytbar, w = _sample()
    hist, hist_dy = b.new_fine()
    b.fill_fine(hist, hist_dy, mtt, yt, ytbar, w)
    for scheme in ("sign", "threshold"):
        unrolled, outside = b.project_fine(hist, hist_dy, EDGES, scheme=scheme)
        assert np.isclose(unrolled.sum() + outside, w.sum())


def test_sign_projection_is_exact():
    # y_scale kept well inside the +-5 fine axis so nothing lands in the
    # rapidity overflow, which project_fine deliberately leaves unclassified.
    mtt, yt, ytbar, w = _sample(y_scale=0.9)
    hist, hist_dy = b.new_fine()
    b.fill_fine(hist, hist_dy, mtt, yt, ytbar, w)
    unrolled, _ = b.project_fine(hist, hist_dy, EDGES, scheme="sign")
    assert np.array_equal(unrolled, _direct(mtt, yt, ytbar, w, "sign"))


def test_threshold_projection_is_exact():
    mtt, yt, ytbar, w = _sample(y_scale=0.9)
    hist, hist_dy = b.new_fine()
    b.fill_fine(hist, hist_dy, mtt, yt, ytbar, w)
    unrolled, _ = b.project_fine(hist, hist_dy, EDGES, scheme="threshold", y0=1.2)
    assert np.array_equal(unrolled, _direct(mtt, yt, ytbar, w, "threshold"))


def test_rapidity_overflow_is_unclassified_not_misassigned():
    # |y| > 5 is beyond the fine axis. Such an event must be reported as
    # outside the binning, never folded into an edge bin.
    hist, hist_dy = b.new_fine()
    b.fill_fine(hist, hist_dy, np.array([500.0]), np.array([7.0]),
                np.array([0.1]), np.array([1.0]))
    unrolled, outside = b.project_fine(hist, hist_dy, EDGES, scheme="sign")
    assert unrolled.sum() == 0.0
    assert outside == 1.0


def test_off_grid_binning_is_rejected():
    hist, hist_dy = b.new_fine()
    for edges, kwargs in (
        (np.array([300.0, 460.0, 600.0]), {}),                       # 460 not on 25 GeV grid
        (EDGES, dict(scheme="threshold", y0=1.25)),                  # y0 not on 0.1 grid
    ):
        try:
            b.project_fine(hist, hist_dy, edges, **kwargs)
        except ValueError:
            continue
        raise AssertionError(f"expected ValueError for {edges}, {kwargs}")


def test_unrolled_bin_round_trip():
    mtt, yt, ytbar, _ = _sample(n=10_000)
    plus, minus = b.classify(yt, ytbar, scheme="sign")
    bins = b.unrolled_bin(mtt, plus, minus, EDGES)
    ok = bins >= 0
    n_mtt = len(EDGES) - 1
    # The N+ half is the first n_mtt entries, and the m_tt index recovered from
    # the unrolled bin must match the m_tt the event was binned from.
    assert np.array_equal(b.is_Nplus_bin(bins[ok], EDGES), bins[ok] < n_mtt)
    recovered = b.mtt_bin_of_unrolled(bins[ok], EDGES)
    expected = np.searchsorted(EDGES, mtt[ok], side="right") - 1
    assert np.array_equal(recovered, expected)


def test_classify_rejects_unknown_scheme():
    try:
        b.classify(np.array([0.1]), np.array([0.2]), scheme="nonsense")
    except ValueError:
        return
    raise AssertionError("expected ValueError for an unknown scheme")


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
