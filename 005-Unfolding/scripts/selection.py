"""THE reco selection for 005-Unfolding. Defined once, applied everywhere.

Issue #32 exists because `response_matrix.py` cut on `Pgof > 0` while
`make_histograms.py` did not, so 2.09% of the measured spectrum was counted as
a miss in the matrix while still being present in the data it was unfolding.
Any mask used to fill a histogram, a response matrix or a systematic variation
must come from `reco_mask()` below -- not from an inline comparison.

It also applies the physical sanity requirements of issue #42. The kinematic
fit in 004A is unbounded and returns m_tt up to 101,632 GeV; `_fill_failure()`
writes -1 sentinels for the top four-vectors, which the naive rapidity formula
turns into an innocuous-looking y = 0.757. Everything removed here is counted
and reported rather than dropped silently.
"""

import numpy as np


class SelectionReport:
    """Per-cut counts, so what the selection removes is visible, not implied."""

    def __init__(self, total):
        self.total = int(total)
        self.cuts = []

    def add(self, name, mask_before, mask_after):
        removed = int(mask_before.sum() - mask_after.sum())
        self.cuts.append((name, removed, int(mask_after.sum())))

    def __str__(self):
        width = max((len(c[0]) for c in self.cuts), default=10)
        lines = [f"  {'input':<{width}} {self.total:>12,}"]
        for name, removed, remaining in self.cuts:
            frac = removed / self.total if self.total else 0.0
            lines.append(f"  {name:<{width}} {remaining:>12,}   (-{removed:,}, {frac:.3%})")
        kept = self.cuts[-1][2] if self.cuts else self.total
        lines.append(f"  {'kept':<{width}} {kept:>12,}   ({kept / max(self.total, 1):.3%})")
        return "\n".join(lines)


def reco_mask(df, cfg, report=True):
    """Boolean mask of events whose reconstruction is usable.

    `df` must carry: Pgof, chi2_status, mtt_reco, yt_lab, ytbar_lab, and
    bdt_score when a BDT cut is configured.

    Returns (mask, SelectionReport).
    """
    sel = cfg["selection"]
    n = len(df)
    mask = np.ones(n, dtype=bool)
    rep = SelectionReport(n)

    def step(name, condition):
        nonlocal mask
        before = mask
        mask = mask & condition
        rep.add(name, before, mask)

    # Quality of the kinematic fit.
    step("Pgof", df["Pgof"].values > sel["pgof_min"])

    reject = sel.get("reject_chi2_status") or []
    if reject:
        if "chi2_status" in df:
            step(f"chi2_status not in {list(reject)}",
                 ~np.isin(df["chi2_status"].values, reject))
        else:
            raise KeyError(
                "config selection.reject_chi2_status is set but the input has no "
                "chi2_status column; re-extract with scripts/extract.py"
            )

    # Physical sanity. Non-finite rapidities are a failed fit, not a central top.
    yt = df["yt_lab"].values
    ytbar = df["ytbar_lab"].values
    mtt = df["mtt_reco"].values
    step("finite kinematics", np.isfinite(yt) & np.isfinite(ytbar) & np.isfinite(mtt))
    step(f"|y| < {sel['abs_y_max']}",
         (np.abs(yt) < sel["abs_y_max"]) & (np.abs(ytbar) < sel["abs_y_max"]))
    step(f"m_tt < {sel['mtt_reco_max']} GeV", mtt < sel["mtt_reco_max"])

    if sel.get("bdt_cut") is not None:
        if "bdt_score" not in df:
            raise KeyError(
                "config selection.bdt_cut is set but the input has no bdt_score "
                "column; re-extract with scripts/extract.py"
            )
        step(f"BDT > {sel['bdt_cut']}", df["bdt_score"].values > sel["bdt_cut"])

    return (mask, rep) if report else mask


def assert_no_nan_reaches_histogram(values, weights, label):
    """Guard at the fill site: nothing non-finite may be histogrammed.

    reco_mask() already removes these, so tripping this means a caller built a
    fill array without applying the selection -- exactly the class of mistake
    issue #32 is about.
    """
    bad = ~np.isfinite(values) | ~np.isfinite(weights)
    if bad.any():
        raise ValueError(
            f"{label}: {bad.sum()} non-finite entries reached the histogram fill. "
            "Apply selection.reco_mask() before filling."
        )
