"""Single source of truth for the 005-Unfolding binning.

Every script in this chapter -- the extractor, the response matrix builder, the
measured histogram, the validation suite and the lxplus gen-acceptance scan --
imports its binning from here. Nothing below may be re-implemented elsewhere;
the Pgof drift between `response_matrix.py` and `make_histograms.py` (issue #32)
is exactly what happens when it is.

Deliberately pure numpy: no ROOT, no pandas, no yaml at import time, so this
module also imports cleanly inside a bare lxplus/CMSSW python where
`gen_acceptance.py` runs.

Two classification schemes are supported:

  "sign"       delta|y| = |y_t| - |y_tbar|;  N+ = delta|y| > 0, N- = delta|y| < 0.
               Every event is classified. This is the definition theory
               predictions are quoted against (issue #34).

  "threshold"  N+ = |y_t| > y0 AND |y_tbar| < y0
               N- = |y_tbar| > y0 AND |y_t| < y0
               Events with both tops forward or both central are unclassified.
               Enhances the per-event asymmetry at a large cost in statistics;
               kept so the old result can be reproduced and compared.

Unrolled bin numbering is the same in both schemes and in both gen and reco:

    bin = i            for N+ in m_tt bin i
    bin = n_mtt + i    for N- in m_tt bin i
    bin = UNCLASSIFIED for events outside the m_tt range or (threshold scheme)
                       in neither rapidity class
"""

import numpy as np

# Sentinel for "this event does not belong in any unrolled bin". Callers must
# test `== UNCLASSIFIED` (or `< 0`) explicitly rather than letting it index.
UNCLASSIFIED = -1

# ---------------------------------------------------------------------------
# Fine gen-level axes used by gen_acceptance.py
#
# The lxplus scan is expensive (~131M events per era, read over xrootd) and we
# do not want to repeat it every time an m_tt edge or the N+/N- definition
# moves. So it stores a fine 3-D histogram of (m_tt, y_t, y_tbar) and every
# analysis binning is derived from it offline by project_fine().
#
# Signed rapidities are kept (not |y|) so that both schemes above, and any
# future observable built from y_t and y_tbar separately, remain derivable.
# ---------------------------------------------------------------------------
def _symmetric_edges(half_width, step):
    """Edges from -half_width to +half_width that contain **exactly** 0.0.

    Built by mirroring a one-sided arange rather than stepping across zero, so
    no floating-point drift can put the boundary at 1e-16 instead of 0. That
    matters: sign(delta|y|) is the observable, so a cell straddling zero would
    mix N+ and N- irrecoverably.
    """
    pos = np.round(np.arange(0.0, half_width + 1e-9, step), 10)
    return np.concatenate((-pos[:0:-1], pos))


FINE_MTT_EDGES = np.round(np.arange(250.0, 2000.0 + 1e-9, 25.0), 10)   # 70 bins
FINE_Y_EDGES = _symmetric_edges(5.0, 0.1)                              # 100 bins
FINE_DY_EDGES = _symmetric_edges(5.0, 0.1)                             # 100 bins

# Fine histograms carry explicit under/overflow, so the stored array shape is
# (nbins+1) per axis and no event is ever silently dropped.
#
# Two histograms are stored, not one, because neither alone is exact for both
# classification schemes:
#
#   FINE_SHAPE     (m_tt, y_t, y_tbar) -- exact for scheme="threshold", whose
#                  boundary is |y| = y0 and therefore lands on a y edge.
#   FINE_DY_SHAPE  (m_tt, delta|y|)    -- exact for scheme="sign", whose
#                  boundary is delta|y| = 0 and therefore lands on a dy edge.
#
# Projecting "sign" out of the 3-D histogram instead would misassign every
# event whose |y_t| and |y_tbar| fall in the same 0.1-wide bin: measured at
# ~0.25% of the sample, which dwarfs an A_C of order 0.001.
FINE_SHAPE = (len(FINE_MTT_EDGES) + 1, len(FINE_Y_EDGES) + 1, len(FINE_Y_EDGES) + 1)
FINE_DY_SHAPE = (len(FINE_MTT_EDGES) + 1, len(FINE_DY_EDGES) + 1)


def check_edges_representable(edges, scheme="sign", y0=1.2):
    """Raise unless an analysis binning can be projected out of the fine axes exactly.

    Every analysis m_tt edge must coincide with a fine m_tt edge, and (threshold
    scheme) y0 must coincide with a fine y edge. Otherwise project_fine() would
    split a fine bin and silently return approximate contents.
    """
    edges = np.asarray(edges, dtype=np.float64)
    interior = edges[(edges > FINE_MTT_EDGES[0]) & (edges < FINE_MTT_EDGES[-1])]
    off_grid = [e for e in interior if not np.any(np.isclose(FINE_MTT_EDGES, e))]
    if off_grid:
        raise ValueError(
            f"m_tt edges {off_grid} do not lie on the fine grid "
            f"({FINE_MTT_EDGES[0]}..{FINE_MTT_EDGES[-1]} step 25)"
        )
    if scheme == "threshold" and not np.any(np.isclose(FINE_Y_EDGES, y0)):
        raise ValueError(f"y0={y0} does not lie on the fine rapidity grid (step 0.1)")


def delta_abs_y(y_top, y_antitop):
    """delta|y| = |y_t| - |y_tbar|, the charge-asymmetry observable."""
    return np.abs(y_top) - np.abs(y_antitop)


def classify(y_top, y_antitop, scheme="sign", y0=1.2):
    """Return (is_Nplus, is_Nminus) boolean masks.

    Events that are neither (only possible for scheme="threshold", or where a
    rapidity is not finite) are False in both.
    """
    y_top = np.asarray(y_top, dtype=np.float64)
    y_antitop = np.asarray(y_antitop, dtype=np.float64)
    finite = np.isfinite(y_top) & np.isfinite(y_antitop)

    if scheme == "sign":
        dy = delta_abs_y(y_top, y_antitop)
        return finite & (dy > 0), finite & (dy < 0)

    if scheme == "threshold":
        top_fwd = np.abs(y_top) > y0
        atop_fwd = np.abs(y_antitop) > y0
        return finite & top_fwd & ~atop_fwd, finite & atop_fwd & ~top_fwd

    raise ValueError(f"unknown classification scheme {scheme!r}; expected 'sign' or 'threshold'")


def unrolled_bin(mtt, is_Nplus, is_Nminus, edges):
    """Map (m_tt, N+/N- class) onto the unrolled axis.

    Out-of-range m_tt and unclassified events return UNCLASSIFIED. Note this
    deliberately does *not* fold overflow into the edge bins: callers decide
    how to book those (see build_inputs.py, which counts them explicitly).
    """
    mtt = np.asarray(mtt, dtype=np.float64)
    edges = np.asarray(edges, dtype=np.float64)
    n = len(edges) - 1

    result = np.full(len(mtt), UNCLASSIFIED, dtype=np.int64)
    in_range = np.isfinite(mtt) & (mtt >= edges[0]) & (mtt < edges[-1])
    idx = np.searchsorted(edges, mtt, side="right") - 1

    result[in_range & is_Nplus] = idx[in_range & is_Nplus]
    result[in_range & is_Nminus] = n + idx[in_range & is_Nminus]
    return result


def n_unrolled_bins(edges):
    return 2 * (len(edges) - 1)


def labels(edges):
    """Bin labels for the unrolled axis, in TLatex form for ROOT axis labels."""
    out = []
    for sign in ("N_{+}", "N_{-}"):
        for i in range(len(edges) - 1):
            out.append(f"{sign}({int(edges[i])}-{int(edges[i + 1])})")
    return out


def mtt_bin_of_unrolled(bin_index, edges):
    """Inverse of the unrolling: which m_tt bin an unrolled index belongs to."""
    n = len(edges) - 1
    return np.asarray(bin_index) % n


def is_Nplus_bin(bin_index, edges):
    """True for the N+ half of the unrolled axis."""
    n = len(edges) - 1
    return np.asarray(bin_index) < n


# ---------------------------------------------------------------------------
# Fine-histogram helpers
# ---------------------------------------------------------------------------

def new_fine():
    """Allocate the pair of fine histograms gen_acceptance.py accumulates into."""
    return (np.zeros(FINE_SHAPE, dtype=np.float64),
            np.zeros(FINE_DY_SHAPE, dtype=np.float64))


def fill_fine(hist, hist_dy, mtt, y_top, y_antitop, weights):
    """Accumulate into the fine (m_tt, y_t, y_tbar) and (m_tt, delta|y|) histograms.

    Both are filled in place from the same events, so their totals agree and
    each equals sum(weights) exactly -- that total is the normalisation
    denominator the acceptance correction needs.

    Non-finite inputs are routed to underflow rather than dropped, again so
    that the total is conserved and anything pathological stays countable
    (issue #42).
    """
    mtt = np.asarray(mtt, dtype=np.float64)
    y_top = np.asarray(y_top, dtype=np.float64)
    y_antitop = np.asarray(y_antitop, dtype=np.float64)
    weights = np.asarray(weights, dtype=np.float64)

    bad = ~(np.isfinite(mtt) & np.isfinite(y_top) & np.isfinite(y_antitop))
    mtt_s = np.where(bad, -np.inf, mtt)
    y_t = np.where(bad, -np.inf, y_top)
    y_a = np.where(bad, -np.inf, y_antitop)
    dy = np.where(bad, -np.inf, delta_abs_y(y_top, y_antitop))

    im = np.searchsorted(FINE_MTT_EDGES, mtt_s, side="right")
    it = np.searchsorted(FINE_Y_EDGES, y_t, side="right")
    ia = np.searchsorted(FINE_Y_EDGES, y_a, side="right")
    np.add.at(hist.reshape(-1),
              np.ravel_multi_index((im, it, ia), FINE_SHAPE), weights)

    idy = np.searchsorted(FINE_DY_EDGES, dy, side="right")
    np.add.at(hist_dy.reshape(-1),
              np.ravel_multi_index((im, idy), FINE_DY_SHAPE), weights)

    return hist, hist_dy


def _mtt_centres():
    return np.concatenate(([-np.inf],
                           0.5 * (FINE_MTT_EDGES[:-1] + FINE_MTT_EDGES[1:]),
                           [np.inf]))


def project_fine(hist, hist_dy, edges, scheme="sign", y0=1.2):
    """Project the fine histograms onto the unrolled analysis axis.

    Dispatches to whichever stored histogram represents `scheme` exactly:
    "sign" reads the (m_tt, delta|y|) histogram, whose axis has an edge at
    delta|y| = 0; "threshold" reads the 3-D histogram, whose y axis has an edge
    at |y| = y0. See the FINE_SHAPE comment for why this matters.

    Returns (unrolled, unclassified_total) with

        unrolled.sum() + unclassified_total == hist.sum()

    by construction, which is what makes this safe as an acceptance denominator.
    """
    check_edges_representable(edges, scheme=scheme, y0=y0)
    edges = np.asarray(edges, dtype=np.float64)
    mtt_c = _mtt_centres()

    if scheme == "sign":
        hist_dy = np.asarray(hist_dy, dtype=np.float64)
        if hist_dy.shape != FINE_DY_SHAPE:
            raise ValueError(f"dy histogram has shape {hist_dy.shape}, expected {FINE_DY_SHAPE}")
        dy_c = np.concatenate(([-np.inf],
                               0.5 * (FINE_DY_EDGES[:-1] + FINE_DY_EDGES[1:]),
                               [np.inf]))
        M, D = np.meshgrid(mtt_c, dy_c, indexing="ij")
        M, D, W = M.ravel(), D.ravel(), hist_dy.ravel()
        # +-inf centres are the dy under/overflow: |delta|y|| > 5 is unphysical
        # for a pair of tops and must stay unclassified rather than be counted.
        plus = np.isfinite(D) & (D > 0)
        minus = np.isfinite(D) & (D < 0)
        total = hist_dy.sum()
    elif scheme == "threshold":
        hist = np.asarray(hist, dtype=np.float64)
        if hist.shape != FINE_SHAPE:
            raise ValueError(f"fine histogram has shape {hist.shape}, expected {FINE_SHAPE}")
        y_c = np.concatenate(([-np.inf],
                              0.5 * (FINE_Y_EDGES[:-1] + FINE_Y_EDGES[1:]),
                              [np.inf]))
        M, T, A = np.meshgrid(mtt_c, y_c, y_c, indexing="ij")
        M, T, A, W = M.ravel(), T.ravel(), A.ravel(), hist.ravel()
        plus, minus = classify(T, A, scheme="threshold", y0=y0)
        total = hist.sum()
    else:
        raise ValueError(f"unknown classification scheme {scheme!r}")

    bins = unrolled_bin(M, plus, minus, edges)
    out = np.zeros(n_unrolled_bins(edges), dtype=np.float64)
    good = bins >= 0
    np.add.at(out, bins[good], W[good])
    return out, float(total - out.sum())
