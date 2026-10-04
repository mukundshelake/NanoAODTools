#!/usr/bin/env python3
"""
A_C-sensitivity study, DeepSets version (Appendix-B). Runs in the torchml env.

Same 4-class target as asym_study.py (qqbar/other x s'=+/-, s' = muCharge * sign(Delta|y|_true)),
trained with an unweighted cross-entropy so the softmax is a calibrated estimate of the class
probabilities. Inputs: per-object arrays (charge-blind, canonically oriented; the muon and
neutrino flags identify the leptonic side) + the symmetric and lepton/hadron-side flat features.
Evaluation is shared with asym_study.py (asym_eval / evaluate()).
"""

import argparse
import json
import logging
import math
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import yaml

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
from asym_study import META, evaluate, load, observables  # noqa: E402
from train_torch import OBJ_FIELDS, DeepSets, object_tensor  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--featureDir', required=True)
    ap.add_argument('--config', required=True)
    ap.add_argument('--outDir', required=True)
    ap.add_argument('--targets', nargs='+', required=True)
    ap.add_argument('--qqScores', default=None)
    ap.add_argument('--qqScoreColumn', default=None)
    ap.add_argument('--sample', action='store_true')
    args = ap.parse_args()
    out = Path(args.outDir)
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler(out / 'asym_torch.log'), logging.StreamHandler(sys.stdout)])
    cfg = yaml.safe_load(open(args.config))
    if args.qqScoreColumn:
        cfg['QQScoreColumn'] = args.qqScoreColumn
    tc = cfg['Torch']
    torch.set_num_threads(tc['threads'])
    torch.manual_seed(42)
    n_obj = tc['NObjects']
    obj_cols = [f'obj{i}_{k}' for i in range(n_obj) for k in OBJ_FIELDS]
    feats_req = cfg['SymmetricFeatures'] + cfg['AsymFeatures'] + cfg['OptionalFeatures']
    df, have = load(args.featureDir, args.targets, sorted(set(META + feats_req + obj_cols + ['kf_tt_m'])))
    feats = [f for f in feats_req if f in have]
    if args.sample:
        df = df.sample(n=min(len(df), 600_000), random_state=1).reset_index(drop=True)
    sp = cfg['Split']
    parts = {k: df[df.fold.isin(sp[k])].reset_index(drop=True) for k in ('train', 'val', 'test')}
    del df
    logging.info({k: len(v) for k, v in parts.items()})

    from sklearn.preprocessing import QuantileTransformer
    qt = QuantileTransformer(output_distribution='normal', n_quantiles=1000, subsample=1_000_000, random_state=42)
    qt.fit(parts['train'][feats].to_numpy(np.float32))

    def T(p):
        x, p4, mask = object_tensor(p, n_obj, use_qgl=False)
        flat = np.nan_to_num(qt.transform(p[feats].to_numpy(np.float32))).astype(np.float32)
        return {'flat': torch.from_numpy(flat), 'x': torch.from_numpy(x), 'p4': torch.from_numpy(p4),
                'mask': torch.from_numpy(mask), 'c': torch.from_numpy(p.cls.to_numpy(np.int64))}
    TT = {}
    for k in ('train', 'val', 'test'):
        TT[k] = T(parts[k])
        parts[k] = parts[k].drop(columns=obj_cols)

    mcfg = {**tc['model'], 'n_out': 4}
    model = DeepSets(TT['train']['flat'].shape[1], TT['train']['x'].shape[2], mcfg)
    lossf = nn.CrossEntropyLoss()
    opt = torch.optim.AdamW(model.parameters(), lr=tc['lr'], weight_decay=tc['weight_decay'])
    ntr = len(TT['train']['c'])
    spe = math.ceil(ntr / tc['batch_size'])
    sched = torch.optim.lr_scheduler.OneCycleLR(opt, max_lr=tc['lr'], total_steps=tc['epochs'] * spe, pct_start=0.1)

    @torch.no_grad()
    def predict(D, bs=16384):
        model.eval()
        out_ = []
        for s in range(0, len(D['c']), bs):
            sl = slice(s, s + bs)
            out_.append(torch.softmax(model(D['flat'][sl], D['x'][sl], D['p4'][sl], D['mask'][sl]), -1))
        return torch.cat(out_).numpy()

    best, best_state, bad, hist = 1e9, None, 0, []
    g = torch.Generator().manual_seed(42)
    for ep in range(tc['epochs']):
        model.train()
        t0 = time.time()
        perm = torch.randperm(ntr, generator=g)
        tot = 0.0
        for s in range(0, ntr, tc['batch_size']):
            b = perm[s:s + tc['batch_size']]
            D = TT['train']
            opt.zero_grad(set_to_none=True)
            loss = lossf(model(D['flat'][b], D['x'][b], D['p4'][b], D['mask'][b]), D['c'][b])
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            sched.step()
            tot += loss.item() * len(b)
        Pv = predict(TT['val'])
        vl = float(-np.mean(np.log(np.clip(Pv[np.arange(len(Pv)), TT['val']['c'].numpy()], 1e-9, 1))))
        hist.append({'epoch': ep, 'train_loss': tot / ntr, 'val_loss': vl, 'seconds': round(time.time() - t0, 1)})
        logging.info(f'  epoch {ep}: train {tot/ntr:.5f} val {vl:.5f} ({time.time()-t0:.0f}s)')
        if vl < best - 1e-6:
            best, bad = vl, 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            bad += 1
            if bad >= tc['patience']:
                break
    model.load_state_dict(best_state)
    torch.save(model.state_dict(), out / 'model_deepsets.pt')

    P = predict(TT['test'])
    te = parts['test']
    h_all = P[:, 0] + P[:, 2] - P[:, 1] - P[:, 3]
    h_qq = P[:, 0] - P[:, 1]
    qq_score = None
    if args.qqScores:
        sc = pd.read_parquet(args.qqScores, columns=['event', cfg['QQScoreColumn']])
        sc = sc[~sc.event.duplicated(keep=False)]
        j = te[['event']].merge(sc, on='event', how='left')
        if j[cfg['QQScoreColumn']].notna().all():
            qq_score = j[cfg['QQScoreColumn']].to_numpy()
    obs = observables(te, cfg, h_all, h_qq, qq_score, have)
    # also carry the XGBoost observables of the same asym run (one directory up), for one table
    xo = out.parent / 'test_observables.parquet'
    if xo.exists():
        x = te[['event']].merge(pd.read_parquet(xo, columns=['event', 'h_all', 'h_qq']), on='event', how='left')
        if x.h_all.notna().all():
            q = te.muCharge.to_numpy()
            from asym_eval import quantile_bins
            obs['XGB h_all'] = quantile_bins(q * x.h_all.to_numpy(), cfg['MLBins'])
            obs['XGB h_qq'] = quantile_bins(q * x.h_qq.to_numpy(), cfg['MLBins'])
    res = evaluate(te, obs, cfg, out)
    res['history'] = hist
    res['features'] = feats
    json.dump(res, open(out / 'summary.json', 'w'), indent=1)
    o = te[['event', 'muCharge', 's', 'y', 'prodType', 'gen_tt_m', 'gen_delta_absy']].copy()
    o['h_all'], o['h_qq'] = h_all.astype(np.float32), h_qq.astype(np.float32)
    o.to_parquet(out / 'test_observables.parquet', index=False)
    logging.info('Done.')


if __name__ == '__main__':
    main()
