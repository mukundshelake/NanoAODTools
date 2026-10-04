#!/usr/bin/env python3
"""
A_C-sensitivity study, XGBoost version (Appendix-B). Run in latestcoffea via
run_all.py --asymStudy, or directly. See asym_eval.py for the statistical framework.

Outputs (--outDir): summary.json, sensitivity.png, observable_by_class.png, model_xgb.json,
test_observables.parquet (event, q, s, prodType, gen columns, h_all, h_qq).
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).parent))
from asym_eval import info_with_error, quantile_bins, template_fit_bias  # noqa: E402

META = ['event', 'fold', 'prodType', 'y', 'muCharge', 'gen_delta_absy', 'gen_tt_m', 'gen_tt_absy', 'tt_m']


def load(feature_dir, targets, cols):
    files = []
    for t in targets:
        files += sorted((Path(feature_dir) / t).glob('*.parquet'))
    have = set(pd.read_parquet(files[0]).columns)
    cols = [c for c in cols if c in have]
    df = pd.concat([pd.read_parquet(f, columns=cols) for f in files], ignore_index=True)
    df = df[(df.prodType >= 0) & np.isfinite(df.gen_delta_absy) & (df.gen_delta_absy != 0)].reset_index(drop=True)
    df['s'] = np.sign(df.gen_delta_absy).astype(np.int8)
    df['sp'] = (df.s * df.muCharge).astype(np.int8)            # s' = q * s
    df['cls'] = (2 * (1 - df.y) + (df.sp < 0)).astype(np.int8)  # 0 qq+, 1 qq-, 2 other+, 3 other-
    return df, have


def observables(te, cfg, h_all=None, h_qq=None, qq_score=None, have=()):
    q = te.muCharge.to_numpy()
    obs = {}
    dy = q * te.rec_dabsy_lh.to_numpy()
    sgn = (dy > 0).astype(int)
    mtt_bin = np.digitize(te.tt_m.to_numpy(), cfg['RecoMttEdges'])
    obs['sign(dy) chi2'] = sgn
    obs['005: mtt x sign (chi2)'] = mtt_bin * 2 + sgn
    obs['dy fine (chi2)'] = quantile_bins(dy, cfg['FineDabsyBins'])
    if 'kf_dabsy_lh' in have:
        dyk = q * te.kf_dabsy_lh.to_numpy()
        sk = (dyk > 0).astype(int)
        obs['sign(dy) 004A fit'] = sk
        obs['005: mtt x sign (004A fit)'] = np.digitize(te.kf_tt_m.to_numpy(), cfg['RecoMttEdges']) * 2 + sk
    if qq_score is not None:
        obs['005 (chi2) x qq-score quartile'] = (mtt_bin * 2 + sgn) * 4 + quantile_bins(qq_score, 4)
    if h_all is not None:
        obs['ML h_all'] = quantile_bins(q * h_all, cfg['MLBins'])
        obs['ML h_qq'] = quantile_bins(q * h_qq, cfg['MLBins'])
    return obs


def evaluate(te, obs, cfg, out):
    s = te.s.to_numpy().astype(np.float64)
    qq = (te.y.to_numpy() == 1).astype(np.float64)
    f_qq = qq.mean()
    res = {'n_test': int(len(te)), 'f_qqbar': f_qq, 'observables': {}}
    inj = {
        'qq_uniform':      s * qq,
        'qq_mtt_rising':   s * qq * te.gen_tt_m.to_numpy() / te.gen_tt_m[qq > 0].mean(),
        'qq_absy_rising':  s * qq * te.gen_tt_absy.to_numpy() / te.gen_tt_absy[qq > 0].mean(),
        'all_channels':    s,
    }
    true_dA = {k: float(np.mean(s * v)) for k, v in inj.items()}
    for name, b in obs.items():
        I1, e1 = info_with_error(b, s, cfg['Bootstrap'])
        I2, e2 = info_with_error(b, s * qq, cfg['Bootstrap'])
        a1 = template_fit_bias(b, s, inj)
        a2 = template_fit_bias(b, s * qq, inj)
        res['observables'][name] = {
            'n_bins': int(len(np.unique(b))),
            'T1': {'I': I1, 'I_err': e1, 'sigma_sqrtN': 1 / np.sqrt(I1),
                   'fit_over_true': {k: a1[k] / true_dA[k] for k in inj}},
            'T2': {'I': I2, 'I_err': e2, 'sigma_sqrtN': f_qq / np.sqrt(I2),
                   'fit_over_true': {k: a2[k] * f_qq / true_dA[k] for k in inj}},
        }
        logging.info(f"{name:34s} T1 sigma*sqrtN {1/np.sqrt(I1):7.3f} (I {I1:.4f}+-{e1:.4f})   "
                     f"T2 sigma*sqrtN {f_qq/np.sqrt(I2):7.3f} (I {I2:.6f}+-{e2:.6f})")
    base = res['observables']['005: mtt x sign (chi2)']
    for fw in ('T1', 'T2'):
        for name, r in res['observables'].items():
            r[fw]['sigma_rel_to_005chi2'] = r[fw]['sigma_sqrtN'] / base[fw]['sigma_sqrtN']
    json.dump(res, open(out / 'summary.json', 'w'), indent=1)

    names = list(res['observables'])
    fig, axes = plt.subplots(1, 2, figsize=(12, 0.45 * len(names) + 1.5), sharey=True)
    for ax, fw, title in zip(axes, ('T1', 'T2'),
                             ('T1: model-independent (all events)', 'T2: deviation carried by qqbar')):
        v = [res['observables'][n][fw]['sigma_rel_to_005chi2'] for n in names]
        ax.barh(names, v, color=['#888'] * len(names))
        for i, x in enumerate(v):
            ax.text(x, i, f' {x:.3f}', va='center', fontsize=8)
        ax.axvline(1, color='k', lw=0.8)
        ax.set_xlabel('expected sigma(A_C) / sigma(005 binning, chi2 reco)')
        ax.set_title(title, fontsize=10)
        ax.set_xlim(0, max(v) * 1.15)
    axes[0].invert_yaxis()
    fig.tight_layout()
    fig.savefig(out / 'sensitivity.png', dpi=130)
    plt.close(fig)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--featureDir', required=True)
    ap.add_argument('--config', required=True)
    ap.add_argument('--outDir', required=True)
    ap.add_argument('--targets', nargs='+', required=True)
    ap.add_argument('--qqScores', default=None, help='test_scores.parquet with the qqbar classifier score')
    ap.add_argument('--qqScoreColumn', default=None, help='override asym_config QQScoreColumn')
    ap.add_argument('--sample', action='store_true')
    args = ap.parse_args()
    out = Path(args.outDir)
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler(out / 'asym_study.log'), logging.StreamHandler(sys.stdout)])
    cfg = yaml.safe_load(open(args.config))
    if args.qqScoreColumn:
        cfg['QQScoreColumn'] = args.qqScoreColumn
    feats_req = cfg['SymmetricFeatures'] + cfg['AsymFeatures'] + cfg['OptionalFeatures']
    df, have = load(args.featureDir, args.targets, sorted(set(META + feats_req + ['kf_tt_m'])))
    feats = [f for f in feats_req if f in have]
    logging.info(f'{len(df)} events, {len(feats)} features, qqbar {df.y.mean():.4f}')
    if args.sample:
        df = df.sample(n=min(len(df), 600_000), random_state=1).reset_index(drop=True)
    sp = cfg['Split']
    tr, va, te = (df[df.fold.isin(sp[k])].reset_index(drop=True) for k in ('train', 'val', 'test'))
    del df

    import xgboost as xgb
    t0 = time.time()
    m = xgb.XGBClassifier(objective='multi:softprob', num_class=4, eval_metric='mlogloss', **cfg['XGB'])
    m.fit(tr[feats].to_numpy(np.float32), tr.cls, eval_set=[(va[feats].to_numpy(np.float32), va.cls)],
          verbose=200)
    logging.info(f'xgb: {m.best_iteration} trees, {time.time()-t0:.0f}s')
    m.save_model(out / 'model_xgb.json')
    P = m.predict_proba(te[feats].to_numpy(np.float32))
    h_all = P[:, 0] + P[:, 2] - P[:, 1] - P[:, 3]
    h_qq = P[:, 0] - P[:, 1]

    qq_score = None
    if args.qqScores:
        sc = pd.read_parquet(args.qqScores, columns=['event', cfg['QQScoreColumn']])
        sc = sc[~sc.event.duplicated(keep=False)]
        j = te[['event']].merge(sc, on='event', how='left')
        if j[cfg['QQScoreColumn']].notna().all():
            qq_score = j[cfg['QQScoreColumn']].to_numpy()
        else:
            logging.info(f"qq-score join incomplete ({j[cfg['QQScoreColumn']].isna().sum()} missing); skipping")

    obs = observables(te, cfg, h_all, h_qq, qq_score, have)
    res = evaluate(te, obs, cfg, out)
    res['xgb_best_iteration'] = int(m.best_iteration)
    res['features'] = feats
    json.dump(res, open(out / 'summary.json', 'w'), indent=1)

    o = te[['event', 'muCharge', 's', 'y', 'prodType', 'gen_tt_m', 'gen_delta_absy']].copy()
    o['h_all'], o['h_qq'] = h_all.astype(np.float32), h_qq.astype(np.float32)
    o.to_parquet(out / 'test_observables.parquet', index=False)

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    q = te.muCharge.to_numpy()
    for ax, h, lab in zip(axes, (h_all, h_qq), ('O_all = q h_all', 'O_qq = q h_qq')):
        O = q * h
        bins = np.linspace(np.quantile(O, 0.001), np.quantile(O, 0.999), 60)
        for sv, pr, c, ls in [(1, 1, '#d62728', '-'), (-1, 1, '#d62728', ':'), (1, 0, '#2ca02c', '-'),
                              (-1, 0, '#2ca02c', ':')]:
            sel = (te.s.to_numpy() == sv) & ((te.y.to_numpy() == 1) == (pr == 1))
            ax.hist(O[sel], bins=bins, density=True, histtype='step', color=c, ls=ls,
                    label=f"{'qqbar' if pr else 'gg+qg'} s={'+' if sv > 0 else '-'}")
        ax.set_yscale('log')
        ax.set_xlabel(lab)
        ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out / 'observable_by_class.png', dpi=130)
    plt.close(fig)
    logging.info('Done.')


if __name__ == '__main__':
    main()
