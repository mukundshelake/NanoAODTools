#!/usr/bin/env python3
"""
Measurement-level evaluation for the A_C-sensitivity study (Appendix-B, scripts/asym).

Everything is in the *signed* frame of a real analysis:
    s      = sign(Delta|y|_true),  Delta|y| = |y_t| - |y_tbar|
    reco Delta|y| = muCharge * rec_dabsy_lh       (mu+ => leptonic top is the top)
    ML observable O = muCharge * h'(x)            (h' learned on charge-blind inputs, target s' = q*s)

Two frameworks for what "A_C" is allowed to do (see README):
    T1 (model-independent, like 005): event weights 1 + alpha * s           -> A_C = A0 + alpha
    T2 (deviation carried by qqbar):  event weights 1 + alpha * s * 1qqbar  -> A_C = A0 + alpha * f_qq

For an observable binned into b, the Fisher information per event on alpha is
    I = sum_b p_b * m_b^2,   m_b = mean of the morph target t in bin b (t = s or s*1qq),
with the finite-sample bias m_b^2 -> m_b^2 - var_b / n_b removed. Then
    sigma(A_C) * sqrt(N) = 1/sqrt(I)            (T1)
                         = f_qq / sqrt(I)       (T2)
which is the statistical precision of a binned template fit in that observable.
"""

import numpy as np


def quantile_bins(x, n):
    """Bin index by quantiles of x (ties-safe)."""
    edges = np.unique(np.quantile(x, np.linspace(0, 1, n + 1)[1:-1]))
    return np.searchsorted(edges, x, side='right')


def info(bins, t, w=None):
    """Bias-corrected sum_b p_b m_b^2 for integer bin labels and target t (optionally event weights w)."""
    b = np.unique(bins, return_inverse=True)[1]
    if w is None:
        w = np.ones_like(t, dtype=np.float64)
    n = np.bincount(b, weights=w)
    s1 = np.bincount(b, weights=w * t)
    s2 = np.bincount(b, weights=w * t * t)
    ok = n > 1
    m = s1[ok] / n[ok]
    var = s2[ok] / n[ok] - m**2
    neff = n[ok]
    return float(np.sum(n[ok] / w.sum() * (m**2 - var / (neff - 1))))


def info_with_error(bins, t, n_boot=20, seed=0):
    rng = np.random.default_rng(seed)
    val = info(bins, t)
    boots = [info(bins, t, rng.poisson(1.0, len(t)).astype(np.float64)) for _ in range(n_boot)]
    return val, float(np.std(boots))


def template_fit_bias(bins, t_template, injections):
    """Linearised one-parameter template fit, weights 1 + alpha * t_template, to injected
    weight variations dN_ev (dict name -> per-event array; true dA_C is the array's mean of s*dN/s...).
    Returns alpha_hat per injection: sum_b T_b dN_b / N_b / sum_b T_b^2 / N_b."""
    b = np.unique(bins, return_inverse=True)[1]
    N = np.bincount(b).astype(np.float64)
    T = np.bincount(b, weights=t_template)
    den = np.sum(T**2 / N)
    out = {}
    for name, dn in injections.items():
        D = np.bincount(b, weights=dn)
        out[name] = float(np.sum(T * D / N) / den)
    return out
