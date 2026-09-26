#!/usr/bin/env python3
"""Tests for asymmetry.py.

The one that matters is test_propagation_matches_monte_carlo: it checks the
analytic covariance propagation against actually sampling from the covariance
and measuring the spread of A_C. That validates the result rather than
re-deriving my own algebra, and it is the test that would catch a wrong sign
in the Jacobian or a dropped correlation term.

test_correlation_changes_the_error is the one with physics content: it shows
that assuming independent bins is not a small approximation here.
"""

import numpy as np

import asymmetry as a
import binning

EDGES = np.array([300, 450, 600, 750, 900, 1050, 1200], dtype=float)
N_MTT = len(EDGES) - 1


def _spectrum(seed=0, asym=0.01, scale=50_000.0):
    """A plausible unfolded vector: falling m_tt, small injected asymmetry."""
    rng = np.random.default_rng(seed)
    shape = scale * np.exp(-np.arange(N_MTT) / 1.5) + 100.0
    n_plus = shape * (1 + asym)
    n_minus = shape * (1 - asym)
    return np.concatenate([n_plus, n_minus]), rng


def _covariance(x, rng, rho_within=0.6, rho_across=0.9, rel=0.02):
    """Covariance with strong positive correlation, like a real unfolded result."""
    n = len(x)
    sigma = rel * x
    corr = np.full((n, n), rho_within)
    half = n // 2
    # N+ and N- halves correlate even more strongly with each other, which is
    # what makes the naive independent-bin error badly wrong for a difference.
    corr[:half, half:] = rho_across
    corr[half:, :half] = rho_across
    np.fill_diagonal(corr, 1.0)
    # Project to the nearest positive-definite matrix so sampling is valid.
    eigenvalues, vectors = np.linalg.eigh(corr)
    corr = vectors @ np.diag(np.clip(eigenvalues, 1e-9, None)) @ vectors.T
    d = np.sqrt(np.diag(corr))
    corr = corr / np.outer(d, d)
    return np.outer(sigma, sigma) * corr


def test_symmetric_spectrum_has_zero_asymmetry():
    x = np.concatenate([np.full(N_MTT, 1000.0), np.full(N_MTT, 1000.0)])
    values, _ = a.jacobian(x, EDGES)
    assert np.allclose(values, 0.0)


def test_asymmetry_matches_definition():
    x, _ = _spectrum(asym=0.05)
    values, _ = a.jacobian(x, EDGES)
    expected = a.asymmetry(x[:N_MTT], x[N_MTT:])
    assert np.allclose(values[:N_MTT], expected)
    assert np.isclose(values[N_MTT],
                      (x[:N_MTT].sum() - x[N_MTT:].sum()) / x.sum())


def test_jacobian_matches_numerical_derivative():
    x, _ = _spectrum(asym=0.03)
    values, J = a.jacobian(x, EDGES)
    eps = 1e-4
    for j in range(len(x)):
        shifted = x.copy()
        shifted[j] += eps * x[j]
        moved, _ = a.jacobian(shifted, EDGES)
        numerical = (moved - values) / (eps * x[j])
        assert np.allclose(numerical, J[:, j], atol=1e-6), f"column {j}"


def test_propagation_matches_monte_carlo():
    """Analytic sigma(A_C) must match the spread of sampled spectra."""
    x, rng = _spectrum(asym=0.01)
    cov = _covariance(x, rng)
    values, cov_A = a.propagate(x, cov, EDGES)
    analytic = a.errors(cov_A)

    draws = rng.multivariate_normal(x, cov, size=200_000)
    sampled = np.empty((len(draws), N_MTT + 1))
    sampled[:, :N_MTT] = a.asymmetry(draws[:, :N_MTT], draws[:, N_MTT:])
    sampled[:, N_MTT] = a.asymmetry(draws[:, :N_MTT].sum(axis=1),
                                    draws[:, N_MTT:].sum(axis=1))
    empirical = sampled.std(axis=0)

    print(f"    analytic  {np.array2string(analytic, precision=5)}")
    print(f"    sampled   {np.array2string(empirical, precision=5)}")
    assert np.allclose(analytic, empirical, rtol=0.05), (
        "propagated sigma disagrees with Monte Carlo by more than 5%")
    assert np.allclose(values[:N_MTT], sampled[:, :N_MTT].mean(axis=0), atol=2e-4)


def test_correlation_changes_the_error():
    """Assuming independent bins is not a small approximation here."""
    x, rng = _spectrum(asym=0.01)
    cov = _covariance(x, rng)
    _, cov_A = a.propagate(x, cov, EDGES)
    _, cov_A_diag = a.propagate(x, np.diag(np.diag(cov)), EDGES)
    full, naive = a.errors(cov_A), a.errors(cov_A_diag)
    ratio = full[-1] / naive[-1]
    print(f"    inclusive sigma: full cov {full[-1]:.5f}, "
          f"diagonal-only {naive[-1]:.5f}, ratio {ratio:.3f}")
    assert not np.isclose(ratio, 1.0, atol=0.05), (
        "correlations should change the inclusive error appreciably")


def test_inclusive_is_not_the_mean_of_the_bins():
    """A ratio of sums is not the mean of ratios; guards a tempting shortcut."""
    x, _ = _spectrum(asym=0.02)
    x[0] *= 5.0   # make one bin dominate
    values, _ = a.jacobian(x, EDGES)
    assert not np.isclose(values[-1], values[:N_MTT].mean(), atol=1e-4)


def test_shift_propagation_is_first_order_consistent():
    x, _ = _spectrum(asym=0.01)
    shift = 0.001 * x
    delta = a.propagate_shift(x, shift, EDGES)
    exact, _ = a.jacobian(x + shift, EDGES)
    base, _ = a.jacobian(x, EDGES)
    assert np.allclose(delta, exact - base, atol=1e-7)


def test_empty_bins_give_nan_not_zero():
    x = np.zeros(binning.n_unrolled_bins(EDGES))
    x[1] = x[N_MTT + 1] = 500.0
    values, _ = a.jacobian(x, EDGES)
    assert np.isnan(values[0]), "an empty m_tt bin must be nan, not a fake zero"
    assert np.isclose(values[1], 0.0)


def test_covariance_shape_is_checked():
    x, _ = _spectrum()
    try:
        a.propagate(x, np.eye(len(x) - 1), EDGES)
    except ValueError:
        return
    raise AssertionError("expected ValueError on a mismatched covariance")


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
