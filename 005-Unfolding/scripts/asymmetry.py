"""Charge asymmetry from an unfolded spectrum, with correct error propagation.

    A_C = (N_+ - N_-) / (N_+ + N_-)

The unfolded bins are strongly correlated -- rho_avg came out at 0.98 on
UL2016preVFP -- so treating them as independent misstates sigma(A_C), and not
in a predictable direction: the N_+ and N_- halves of the unrolled axis are
positively correlated, which *cancels* in the difference and would make an
independent-bin estimate too large, while correlations within a half do the
opposite. The only defensible route is the full covariance (issue #39).

For A = (a - b)/(a + b),

    dA/da =  2b/(a+b)^2        dA/db = -2a/(a+b)^2

so with J the Jacobian of the whole A vector with respect to the unfolded
vector x,  cov(A) = J C J^T.  Building J as a matrix rather than propagating
bin by bin also gives the correlations *between* m_tt bins of A_C, which the
inclusive number needs.

Pure numpy, no ROOT, so the propagation can be tested against Monte Carlo
sampling directly (see test_asymmetry.py).
"""

import numpy as np

import binning


def asymmetry(n_plus, n_minus):
    """(N+ - N-)/(N+ + N-), nan where the denominator vanishes."""
    n_plus = np.asarray(n_plus, dtype=np.float64)
    n_minus = np.asarray(n_minus, dtype=np.float64)
    total = n_plus + n_minus
    with np.errstate(invalid="ignore", divide="ignore"):
        out = (n_plus - n_minus) / total
    return np.where(total != 0, out, np.nan)


def jacobian(x, edges):
    """Jacobian of the A_C vector with respect to the unfolded vector x.

    Returns (values, J) where `values` has length n_mtt + 1 -- one A_C per
    m_tt bin, then the inclusive A_C -- and J has shape
    (n_mtt + 1, len(x)), with J[k, j] = dA_k / dx_j.

    x must be ordered as binning.unrolled_bin produces it: the N+ half first,
    then the N- half, both in increasing m_tt.
    """
    x = np.asarray(x, dtype=np.float64)
    n_mtt = len(edges) - 1
    expected = binning.n_unrolled_bins(edges)
    if len(x) != expected:
        raise ValueError(f"x has {len(x)} bins, expected {expected} for these edges")

    values = np.empty(n_mtt + 1, dtype=np.float64)
    J = np.zeros((n_mtt + 1, len(x)), dtype=np.float64)

    for k in range(n_mtt):
        a, b = x[k], x[n_mtt + k]
        total = a + b
        if total == 0:
            values[k] = np.nan
            continue
        values[k] = (a - b) / total
        J[k, k] = 2.0 * b / total ** 2
        J[k, n_mtt + k] = -2.0 * a / total ** 2

    # Inclusive: sum each half first, then the same formula. Its Jacobian is
    # NOT the average of the per-bin ones -- it is the derivative of the ratio
    # of sums, which is why the inclusive error needs the cross-bin
    # correlations that a per-bin treatment throws away.
    a_total = x[:n_mtt].sum()
    b_total = x[n_mtt:].sum()
    total = a_total + b_total
    if total == 0:
        values[n_mtt] = np.nan
    else:
        values[n_mtt] = (a_total - b_total) / total
        J[n_mtt, :n_mtt] = 2.0 * b_total / total ** 2
        J[n_mtt, n_mtt:] = -2.0 * a_total / total ** 2

    return values, J


def propagate(x, covariance, edges):
    """A_C per m_tt bin plus inclusive, with the full propagated covariance.

    Returns (values, cov_A) where both cover n_mtt + 1 entries, the last being
    the inclusive asymmetry. sigma is sqrt(diag(cov_A)).
    """
    covariance = np.asarray(covariance, dtype=np.float64)
    values, J = jacobian(x, edges)
    if covariance.shape != (len(x), len(x)):
        raise ValueError(
            f"covariance has shape {covariance.shape}, expected "
            f"{(len(x), len(x))} to match the unfolded vector"
        )
    return values, J @ covariance @ J.T


def propagate_shift(x, shift, edges):
    """Change in A_C induced by a shift `dx` of the unfolded vector.

    Used for the per-source systematic breakdown: TUnfold gives the shift in
    the unfolded spectrum for each source (GetDeltaSysSource), and the
    corresponding shift in A_C is J dx to first order. Linear propagation is
    appropriate here because the shifts are small relative to the bin contents.
    """
    _, J = jacobian(x, edges)
    return J @ np.asarray(shift, dtype=np.float64)


def errors(cov_A):
    """Per-entry uncertainties from a propagated covariance."""
    return np.sqrt(np.clip(np.diag(np.asarray(cov_A, dtype=np.float64)), 0.0, None))


def labels(edges):
    """Row labels matching the A_C vector: per m_tt bin, then 'inclusive'."""
    edges = np.asarray(edges)
    out = [f"{int(edges[i])}-{int(edges[i + 1])}" for i in range(len(edges) - 1)]
    return out + ["inclusive"]


def format_table(values, cov_stat, cov_total, edges, systematics=None):
    """Human-readable A_C table with stat and stat+syst uncertainties.

    `systematics` maps a source name to its A_C shift vector, as produced by
    propagate_shift().
    """
    stat = errors(cov_stat)
    total = errors(cov_total)
    syst = np.sqrt(np.clip(total ** 2 - stat ** 2, 0.0, None))
    names = labels(edges)

    width = max(len(n) for n in names)
    lines = [
        f"  {'m_tt [GeV]':<{width}} {'A_C':>10} {'stat':>10} {'syst':>10} {'total':>10}",
        f"  {'-' * (width + 44)}",
    ]
    for i, name in enumerate(names):
        if i == len(names) - 1:
            lines.append(f"  {'-' * (width + 44)}")
        lines.append(f"  {name:<{width}} {values[i]:>+10.5f} {stat[i]:>10.5f} "
                     f"{syst[i]:>10.5f} {total[i]:>10.5f}")

    if systematics:
        lines.append("")
        lines.append(f"  per-source breakdown on the inclusive A_C:")
        for source, shift in sorted(systematics.items(),
                                    key=lambda kv: -abs(kv[1][-1])):
            lines.append(f"    {source:<{width}} {shift[-1]:>+10.5f}")
    return "\n".join(lines)
