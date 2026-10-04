#!/usr/bin/env python3
"""
Unfolding-like (differential) sensitivity for the A_C study (Appendix-B, scripts/asym).

005 does not fit one asymmetry parameter: it unfolds A_C in gen m_tt bins and sums them, so a
mass-dependent asymmetry is not a bias there, at the price of variance. The faithful
statistical comparison is therefore a Fisher matrix with one free asymmetry per gen m_tt bin j
(morph weights 1 + sum_j alpha_j * s * 1_j, model-independent T1 framework):

    F_jk = sum_b p_b (m_jb m_kb - cov_jk,b / (n_b - 1)),   m_jb = mean of s*1_j in reco bin b
    inclusive A_C = sum_j f_j A_j  ->  sigma(A_C) * sqrt(N) = sqrt(f^T F^-1 f),  f_j = N_j / N

A reco binning that cannot separate gen m_tt bins makes F ill-conditioned and sigma large,
exactly as an unfolding would. Reco binnings compared (all with 005's reco m_tt edges):
    005 (004A fit):  reco m_tt(fit) x sign(Delta|y| fit)        -- the current measurement
    005 (chi2):      reco m_tt(chi2) x sign(Delta|y| chi2)
    m_tt x O deciles: reco m_tt(chi2) x signed ML observable q*h_all in K quantile bins

Usage (latestcoffea or torchml):
    python scripts/asym/asym_differential.py --asymDir <asym/<hash>> [--asymDir <.../deepsets>] \
        --featureDir <appendixB/<tag>/<hash>/<era>> --config asym_config.yaml
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).parent))
from asym_eval import quantile_bins  # noqa: E402

GEN_MTT_EDGES = [450, 600, 750, 900, 1050, 1200]   # 005 gen edges [300..1200]; <450 and >=1200 merged in


def fisher_sigma(bins, s, gen_bin, n_gen, weights=None):
    b = np.unique(bins, return_inverse=True)[1]
    nb = b.max() + 1
    w = np.ones(len(s)) if weights is None else weights
    N = np.bincount(b, weights=w, minlength=nb)
    T = np.stack([np.bincount(b, weights=w * s * (gen_bin == j), minlength=nb) for j in range(n_gen)])
    Q = np.stack([np.bincount(b, weights=w * (gen_bin == j), minlength=nb) for j in range(n_gen)])  # (s 1_j)^2 = 1_j
    ok = N > 1
    m = T[:, ok] / N[ok]
    p = N[ok] / w.sum()
    # unbiased m_j m_k: subtract within-bin covariance / (n-1); cov_jk = delta_jk E[1_j] - m_j m_k
    F = np.einsum('b,jb,kb->jk', p, m, m)
    cov_diag = Q[:, ok] / N[ok] - m**2
    F -= np.diag(np.sum(p * cov_diag / (N[ok] - 1), axis=1))
    f = np.array([np.sum(w * (gen_bin == j)) for j in range(n_gen)]) / w.sum()
    cond = float(np.linalg.cond(F))
    return float(np.sqrt(f @ np.linalg.solve(F, f))), cond


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--asymDir', nargs='+', required=True, help='dirs with test_observables.parquet')
    ap.add_argument('--featureDir', required=True)
    ap.add_argument('--config', required=True)
    ap.add_argument('--targets', nargs='+', default=['MC_mu/SemiLeptonic/ttbar_SemiLeptonic'])
    ap.add_argument('--deciles', type=int, default=10)
    ap.add_argument('--bootstrap', type=int, default=10)
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config))

    files = []
    for t in args.targets:
        files += sorted((Path(args.featureDir) / t).glob('*.parquet'))
    have = set(pd.read_parquet(files[0]).columns)
    cols = [c for c in ['event', 'fold', 'tt_m', 'rec_dabsy_lh', 'kf_tt_m', 'kf_dabsy_lh', 'gen_tt_m'] if c in have]
    feat = pd.concat([pd.read_parquet(f, columns=cols) for f in files], ignore_index=True)
    feat = feat[feat.fold.isin(cfg['Split']['test'])]
    feat = feat[~feat.event.duplicated(keep=False)]

    for d in args.asymDir:
        d = Path(d)
        o = pd.read_parquet(d / 'test_observables.parquet')
        o = o[~o.event.duplicated(keep=False)].drop(columns=['gen_tt_m']).merge(feat, on='event', how='inner')
        q, s = o.muCharge.to_numpy(), o.s.to_numpy().astype(np.float64)
        gen_bin = np.digitize(o.gen_tt_m.to_numpy(), GEN_MTT_EDGES)
        n_gen = len(GEN_MTT_EDGES) + 1
        e = cfg['RecoMttEdges']
        mb = np.digitize(o.tt_m.to_numpy(), e)
        obs = {'005 (chi2): mtt x sign': mb * 2 + (q * o.rec_dabsy_lh.to_numpy() > 0)}
        if 'kf_dabsy_lh' in o:
            obs['005 (004A fit): mtt x sign'] = (np.digitize(o.kf_tt_m.to_numpy(), e) * 2 +
                                                (q * o.kf_dabsy_lh.to_numpy() > 0))
        K = args.deciles
        obs[f'mtt x sign x |dy| {K // 2} bins (chi2)'] = mb * K + quantile_bins(q * o.rec_dabsy_lh.to_numpy(), K)
        obs[f'mtt x ML h_all {K} bins'] = mb * K + quantile_bins(q * o.h_all.to_numpy(), K)
        obs[f'mtt x ML h_all {2 * K} bins'] = mb * 2 * K + quantile_bins(q * o.h_all.to_numpy(), 2 * K)
        obs['ML h_all 100 bins (no mtt)'] = quantile_bins(q * o.h_all.to_numpy(), 100)
        rng = np.random.default_rng(0)
        res = {'n_test': int(len(o)), 'gen_mtt_edges': GEN_MTT_EDGES, 'observables': {}}
        base = None
        for name, b in obs.items():
            sig, cond = fisher_sigma(b, s, gen_bin, n_gen)
            boots = [fisher_sigma(b, s, gen_bin, n_gen, rng.poisson(1.0, len(s)).astype(float))[0]
                     for _ in range(args.bootstrap)]
            res['observables'][name] = {'sigma_sqrtN': sig, 'sigma_sqrtN_err': float(np.std(boots)),
                                        'cond_F': cond, 'n_bins': int(len(np.unique(b)))}
        ref = res['observables'].get('005 (004A fit): mtt x sign', res['observables']['005 (chi2): mtt x sign'])
        for name, r in res['observables'].items():
            r['sigma_rel_to_005_current'] = r['sigma_sqrtN'] / ref['sigma_sqrtN']
            print(f"{d.name:14s} {name:34s} sigma*sqrtN {r['sigma_sqrtN']:.3f} +- {r['sigma_sqrtN_err']:.3f}  "
                  f"rel {r['sigma_rel_to_005_current']:.3f}  cond(F) {r['cond_F']:.0f}")
        json.dump(res, open(d / 'differential.json', 'w'), indent=1)


if __name__ == '__main__':
    main()
