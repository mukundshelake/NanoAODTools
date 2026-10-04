#!/usr/bin/env python3
"""
Train and evaluate qqbar-vs-(gg,qg) classifiers on the Appendix-B feature parquet.

For every (model, featureSet) in training_config.yaml Runs:
  * fit on the train folds (early stopping / model choice on val)
  * evaluate on the test folds only
Writes into --outDir:
  summary.json                    AUCs, best params, purity tables, sculpting check, per-mtt AUC
  single_feature_auc.csv          test AUC of every feature on its own
  roc.png, roc_zoom.png           all runs overlaid
  {run}/score_by_prodType.png     test score per production channel + train overlay (overtraining)
  {run}/purity.png                qqbar purity / efficiency vs kept fraction
  {run}/auc_vs_mtt.png            AUC in bins of gen m_tt
  {run}/importance.csv (+png)     xgb gain + permutation importance (val subset)
  {run}/model.pkl                 {'model', 'preproc', 'features'}
  test_scores.parquet             event, fold, prodType, gen columns, one score column per run
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import joblib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml
from scipy.stats import ks_2samp
from sklearn.metrics import roc_auc_score, roc_curve

PROD_NAMES = {1: 'qqbar', 2: 'qg', 0: 'gg'}
PROD_COLORS = {1: '#d62728', 2: '#1f77b4', 0: '#2ca02c'}


def flatten(lst):
    out = []
    for x in lst:
        out.extend(flatten(x) if isinstance(x, list) else [x])
    return out


def load(feature_dir, targets, columns, sample_rows=None):
    files = []
    for t in targets:
        files += sorted((Path(feature_dir) / t).glob('*.parquet'))
    if not files:
        raise SystemExit(f'No parquet under {feature_dir} for {targets}')
    logging.info(f'Reading {len(files)} parquet files')
    if sample_rows:
        files = files[:max(1, len(files) // 20)]
    df = pd.concat([pd.read_parquet(f, columns=columns) for f in files], ignore_index=True)
    df = df[df.prodType >= 0].reset_index(drop=True)
    return df


def weighted_auc(y, s, w):
    """Mann-Whitney AUC with arbitrary-sign weights (ties count 1/2); sklearn rejects w < 0."""
    o = np.argsort(s, kind='mergesort')
    s, y, w = s[o], y[o], w[o]
    _, start = np.unique(s, return_index=True)
    grp = np.repeat(np.arange(len(start)), np.diff(np.append(start, len(s))))
    wn = np.bincount(grp, weights=w * (y == 0))
    wp = np.bincount(grp, weights=w * (y == 1))
    below = np.cumsum(wn) - wn
    return float(np.sum(wp * (below + 0.5 * wn)) / (wp.sum() * wn.sum()))


def class_weights(y):
    pos = y.sum()
    neg = len(y) - pos
    return np.where(y == 1, 0.5 * len(y) / pos, 0.5 * len(y) / neg)


# ---------------------------------------------------------------------------
def fit_xgb(cfg, Xtr, ytr, Xva, yva):
    import xgboost as xgb
    wtr, wva = class_weights(ytr), class_weights(yva)
    grid = cfg['grid']
    keys = list(grid)
    points = [dict(zip(keys, v)) for v in np.array(np.meshgrid(*[grid[k] for k in keys]), dtype=object)
              .T.reshape(-1, len(keys))]
    best = None
    trials = []
    for pt in points:
        pt = {k: (int(v) if k == 'max_depth' else float(v)) for k, v in pt.items()}
        t0 = time.time()
        m = xgb.XGBClassifier(**cfg['fixed'], **pt)
        m.fit(Xtr, ytr, sample_weight=wtr, eval_set=[(Xva, yva)], sample_weight_eval_set=[wva], verbose=False)
        auc = roc_auc_score(yva, m.predict_proba(Xva)[:, 1])
        trials.append({**pt, 'val_auc': auc, 'best_iteration': int(m.best_iteration),
                       'seconds': round(time.time() - t0, 1)})
        logging.info(f'  xgb {pt}: val AUC {auc:.4f}, {m.best_iteration} trees, {time.time()-t0:.0f}s')
        if best is None or auc > best[0]:
            best = (auc, m, pt)
    return best[1], None, {'grid': trials, 'best': best[2]}


def fit_mlp(cfg, Xtr, ytr, Xva, yva):
    from sklearn.neural_network import MLPClassifier
    from sklearn.preprocessing import QuantileTransformer
    n = min(len(ytr), cfg['max_train_rows'])
    rng = np.random.default_rng(42)
    idx = rng.choice(len(ytr), n, replace=False) if n < len(ytr) else np.arange(len(ytr))
    pre = QuantileTransformer(output_distribution='normal', n_quantiles=1000, subsample=1_000_000,
                              random_state=42)
    Xs = np.nan_to_num(pre.fit_transform(Xtr[idx]))
    ys = ytr[idx]
    m = MLPClassifier(hidden_layer_sizes=tuple(cfg['hidden_layer_sizes']), activation=cfg['activation'],
                      alpha=cfg['alpha'], batch_size=cfg['batch_size'],
                      learning_rate_init=cfg['learning_rate_init'], max_iter=cfg['max_iter'],
                      early_stopping=cfg['early_stopping'], validation_fraction=cfg['validation_fraction'],
                      n_iter_no_change=cfg['n_iter_no_change'], random_state=cfg['random_state'])
    t0 = time.time()
    m.fit(Xs, ys, sample_weight=class_weights(ys))
    auc = roc_auc_score(yva, m.predict_proba(np.nan_to_num(pre.transform(Xva)))[:, 1])
    logging.info(f'  mlp: {m.n_iter_} epochs, val AUC {auc:.4f}, {time.time()-t0:.0f}s')
    return m, pre, {'epochs': int(m.n_iter_), 'val_auc': auc, 'train_rows': int(n),
                    'seconds': round(time.time() - t0, 1)}


def predict(model, pre, X):
    if pre is not None:
        X = np.nan_to_num(pre.transform(X))
    return model.predict_proba(X)[:, 1]


# ---------------------------------------------------------------------------
def purity_table(score, y, prod, fracs):
    order = np.argsort(-score)
    rows = []
    npos = y.sum()
    for f in fracs:
        k = max(1, int(round(f * len(score))))
        sel = order[:k]
        r = {'keep_fraction': f, 'n': int(k), 'qqbar_purity': float(y[sel].mean()),
             'qqbar_efficiency': float(y[sel].sum() / npos), 'threshold': float(score[order[k - 1]])}
        for p, name in PROD_NAMES.items():
            r[f'frac_{name}'] = float(np.mean(prod[sel] == p))
        rows.append(r)
    return rows


def sculpting_check(score, prod, dabsy, nq=4):
    """Gen A_C = <sign(Delta|y|)> per score quartile, overall and per channel.
    The features are charge- and z-symmetric, so the score cannot see sign(Delta|y|)
    directly. A trend inside one channel is still allowed: the true qqbar A_C grows with
    m_tt and |y_tt|, which the score correlates with. gg must stay at zero in every
    quartile (it has no charge asymmetry); a non-zero gg value would be a real bias."""
    edges = np.quantile(score, np.linspace(0, 1, nq + 1))
    q = np.clip(np.searchsorted(edges, score, side='right') - 1, 0, nq - 1)
    ok = np.isfinite(dabsy)
    out = {}
    for name, m in [('all', np.ones_like(ok))] + [(n, prod == p) for p, n in PROD_NAMES.items()]:
        rows = []
        for i in range(nq):
            s = ok & m & (q == i)
            nn = int(s.sum())
            ac = float(np.mean(np.sign(dabsy[s]))) if nn else float('nan')
            rows.append({'quartile': i, 'n': nn, 'A_C': ac,
                         'err': float(np.sqrt(max(1 - ac**2, 0) / nn)) if nn else float('nan')})
        out[name] = rows
    return out


def auc_in_bins(score, y, var, bins):
    rows = []
    for lo, hi in zip(bins[:-1], bins[1:]):
        s = (var >= lo) & (var < hi)
        if s.sum() > 100 and 0 < y[s].sum() < s.sum():
            rows.append({'lo': lo, 'hi': hi, 'n': int(s.sum()), 'qqbar_frac': float(y[s].mean()),
                         'auc': float(roc_auc_score(y[s], score[s]))})
    return rows


def plot_scores(run_dir, name, s_te, prod_te, s_tr, prod_tr):
    fig, ax = plt.subplots(figsize=(7, 5))
    bins = np.linspace(0, 1, 51)
    ks = {}
    for p, pn in PROD_NAMES.items():
        h, _ = np.histogram(s_te[prod_te == p], bins=bins, density=True)
        ax.step(bins[:-1], h, where='post', color=PROD_COLORS[p], label=f'{pn} test')
        htr, _ = np.histogram(s_tr[prod_tr == p], bins=bins, density=True)
        ax.plot(0.5 * (bins[1:] + bins[:-1]), htr, '.', color=PROD_COLORS[p], ms=3, label=f'{pn} train')
        ks[pn] = float(ks_2samp(s_te[prod_te == p], s_tr[prod_tr == p]).pvalue)
    ax.set_xlabel('classifier score P(qqbar)')
    ax.set_ylabel('normalised')
    ax.set_yscale('log')
    ax.set_title(f'{name}   KS p: ' + ', '.join(f'{k} {v:.2g}' for k, v in ks.items()), fontsize=9)
    ax.legend(fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(run_dir / 'score_by_prodType.png', dpi=130)
    plt.close(fig)
    return ks


def plot_purity(run_dir, name, rows):
    f = [r['keep_fraction'] for r in rows]
    fig, ax = plt.subplots(figsize=(6, 4.5))
    ax.plot(f, [r['qqbar_purity'] for r in rows], 'o-', label='qqbar purity')
    ax.plot(f, [r['qqbar_efficiency'] for r in rows], 's--', label='qqbar efficiency')
    ax.set_xscale('log')
    ax.set_xlabel('fraction of events kept (highest score)')
    ax.set_title(name)
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(run_dir / 'purity.png', dpi=130)
    plt.close(fig)


def plot_auc_mtt(run_dir, name, rows):
    if not rows:
        return
    x = [0.5 * (r['lo'] + r['hi']) for r in rows]
    fig, ax = plt.subplots(figsize=(6, 4.5))
    ax.errorbar(x, [r['auc'] for r in rows], xerr=[0.5 * (r['hi'] - r['lo']) for r in rows], fmt='o')
    ax2 = ax.twinx()
    ax2.plot(x, [r['qqbar_frac'] for r in rows], 'x:', color='gray')
    ax2.set_ylabel('qqbar fraction', color='gray')
    ax.set_xlabel('gen m_tt [GeV]')
    ax.set_ylabel('test AUC')
    ax.set_title(name)
    fig.tight_layout()
    fig.savefig(run_dir / 'auc_vs_mtt.png', dpi=130)
    plt.close(fig)


def importance(model, pre, feats, Xva, yva, nrows, run_dir, is_xgb):
    rng = np.random.default_rng(1)
    idx = rng.choice(len(yva), min(nrows, len(yva)), replace=False)
    X, y = Xva[idx].copy(), yva[idx]
    base = roc_auc_score(y, predict(model, pre, X))
    rows = []
    gain = model.get_booster().get_score(importance_type='gain') if is_xgb else {}
    for i, f in enumerate(feats):
        Xp = X.copy()
        Xp[:, i] = Xp[rng.permutation(len(y)), i]
        rows.append({'feature': f, 'perm_auc_drop': base - roc_auc_score(y, predict(model, pre, Xp)),
                     'gain': gain.get(f, gain.get(f'f{i}', 0.0))})
    df = pd.DataFrame(rows).sort_values('perm_auc_drop', ascending=False)
    df.to_csv(run_dir / 'importance.csv', index=False)
    top = df.head(25)[::-1]
    fig, ax = plt.subplots(figsize=(6, 7))
    ax.barh(top.feature, top.perm_auc_drop)
    ax.set_xlabel('val AUC drop when permuted')
    fig.tight_layout()
    fig.savefig(run_dir / 'importance.png', dpi=130)
    plt.close(fig)
    return df.head(15).to_dict('records')


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--featureDir', required=True)
    ap.add_argument('--trainingConfig', required=True)
    ap.add_argument('--outDir', required=True)
    ap.add_argument('--era', required=True)
    ap.add_argument('--targets', nargs='+', required=True)
    ap.add_argument('--sample', action='store_true')
    args = ap.parse_args()

    out = Path(args.outDir)
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler(out / 'train.log'), logging.StreamHandler(sys.stdout)])
    cfg = yaml.safe_load(open(args.trainingConfig))
    fsets = {k: flatten(v) for k, v in cfg['FeatureSets'].items()}
    feats_all = sorted({f for r in cfg['Runs'] for f in fsets[r['features']]})
    meta = ['event', 'fold', 'prodType', 'y', 'genWeight', 'gen_tt_m', 'gen_delta_absy']
    reco_cols = fsets.get('reco', [])
    df = load(args.featureDir, args.targets, sorted(set(meta + feats_all + reco_cols)),
              sample_rows=cfg['Evaluation']['sample_rows'] if args.sample else None)
    sp = cfg['Split']
    parts = {k: df[df.fold.isin(sp[k])].reset_index(drop=True) for k in ('train', 'val', 'test')}
    if args.sample:
        n = cfg['Evaluation']['sample_rows']
        parts = {k: v.iloc[:n] for k, v in parts.items()}
    for k, v in parts.items():
        logging.info(f'{k}: {len(v)} events, qqbar fraction {v.y.mean():.4f}')
    tr, va, te = parts['train'], parts['val'], parts['test']
    ytr, yva, yte = tr.y.values, va.y.values, te.y.values

    summary = {'era': args.era, 'n': {k: len(v) for k, v in parts.items()},
               'qqbar_fraction': {k: float(v.y.mean()) for k, v in parts.items()},
               'prodType_fraction_test': {PROD_NAMES[p]: float(np.mean(te.prodType == p)) for p in PROD_NAMES},
               'runs': {}}

    # single-feature AUCs on test (orientation-free)
    sf = []
    for f in sorted(set(feats_all)):
        x = te[f].fillna(-999).values
        a = roc_auc_score(yte, x)
        sf.append({'feature': f, 'auc': max(a, 1 - a), 'direction': '+' if a >= 0.5 else '-'})
    pd.DataFrame(sf).sort_values('auc', ascending=False).to_csv(out / 'single_feature_auc.csv', index=False)

    scores = te[['event', 'fold', 'prodType', 'y', 'genWeight', 'gen_tt_m', 'gen_delta_absy']].copy()
    roc_data = {}
    for run in cfg['Runs']:
        name = f"{run['model']}_{run['features']}"
        feats = fsets[run['features']]
        rd = out / name
        rd.mkdir(exist_ok=True)
        logging.info(f'=== {name}: {len(feats)} features')
        Xtr, Xva, Xte = (p[feats].to_numpy(np.float32) for p in (tr, va, te))
        fit = fit_xgb if run['model'] == 'xgb' else fit_mlp
        model, pre, info = fit(cfg['Models'][run['model']], Xtr, ytr, Xva, yva)
        s_tr = predict(model, pre, Xtr[:2_000_000])
        s_va = predict(model, pre, Xva)
        s_te = predict(model, pre, Xte)
        res = {'features': feats, 'fit': info,
               'auc_train': float(roc_auc_score(ytr[:2_000_000], s_tr)),
               'auc_val': float(roc_auc_score(yva, s_va)),
               'auc_test': float(roc_auc_score(yte, s_te)),
               'auc_test_genWeightSign': weighted_auc(yte, s_te, np.sign(te.genWeight.values))}
        # bootstrap-free AUC stat error (Hanley-McNeil)
        a, n1, n0 = res['auc_test'], yte.sum(), len(yte) - yte.sum()
        q1, q2 = a / (2 - a), 2 * a * a / (1 + a)
        res['auc_test_err'] = float(np.sqrt((a * (1 - a) + (n1 - 1) * (q1 - a * a) + (n0 - 1) * (q2 - a * a)) / (n1 * n0)))
        res['auc_test_one_vs_one'] = {
            f'qqbar_vs_{PROD_NAMES[p]}': float(roc_auc_score(yte[(te.prodType == 1) | (te.prodType == p)],
                                                             s_te[(te.prodType == 1) | (te.prodType == p)]))
            for p in (0, 2)}
        res['purity'] = purity_table(s_te, yte, te.prodType.values, cfg['Evaluation']['keep_fractions'])
        res['auc_vs_gen_mtt'] = auc_in_bins(s_te, yte, te.gen_tt_m.values, cfg['Evaluation']['mtt_bins'])
        res['sculpting'] = sculpting_check(s_te, te.prodType.values, te.gen_delta_absy.values)
        res['ks_pvalue_train_vs_test'] = plot_scores(rd, name, s_te, te.prodType.values,
                                                     s_tr, tr.prodType.values[:2_000_000])
        plot_purity(rd, name, res['purity'])
        plot_auc_mtt(rd, name, res['auc_vs_gen_mtt'])
        res['importance_top15'] = importance(model, pre, feats, Xva, yva, cfg['Evaluation']['permutation_rows'],
                                             rd, run['model'] == 'xgb')
        joblib.dump({'model': model, 'preproc': pre, 'features': feats}, rd / 'model.pkl')
        json.dump(res, open(rd / 'result.json', 'w'), indent=1)
        logging.info(f"  {name}: AUC train {res['auc_train']:.4f} val {res['auc_val']:.4f} "
                     f"test {res['auc_test']:.4f} +- {res['auc_test_err']:.4f}")
        summary['runs'][name] = {k: v for k, v in res.items() if k != 'features'}
        scores[f'score_{name}'] = s_te.astype(np.float32)
        roc_data[name] = (roc_curve(yte, s_te), res['auc_test'])

    scores.to_parquet(out / 'test_scores.parquet', index=False)
    json.dump(summary, open(out / 'summary.json', 'w'), indent=1)

    for zoom in (False, True):
        fig, ax = plt.subplots(figsize=(6, 6))
        for name, ((fpr, tpr, _), auc) in roc_data.items():
            ax.plot(fpr, tpr, label=f'{name} (AUC {auc:.4f})', ls=':' if 'oracle' in name else '-')
        ax.plot([0, 1], [0, 1], 'k--', lw=0.8)
        ax.set_xlabel('false-positive rate (gg + qg kept)')
        ax.set_ylabel('true-positive rate (qqbar kept)')
        if zoom:
            ax.set_xlim(0, 0.2)
            ax.set_ylim(0, 0.6)
        ax.legend(fontsize=8)
        ax.set_title(f'{args.era}: qqbar vs gg+qg (test folds)')
        fig.tight_layout()
        fig.savefig(out / ('roc_zoom.png' if zoom else 'roc.png'), dpi=130)
        plt.close(fig)
    logging.info('Done.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
