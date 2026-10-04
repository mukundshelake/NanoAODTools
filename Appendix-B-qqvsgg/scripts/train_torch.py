#!/usr/bin/env python3
"""
PyTorch models for the qqbar-vs-(gg,qg) task (Appendix-B). Runs in the `torchml` env
(CPU torch); run_all.py --trainTorch launches it with torch_config.yaml TorchPython.

Same parquet, same event%10 split and the same evaluation as train.py (its helpers are
imported), so numbers are directly comparable with the XGBoost runs.

Architectures (torch_config.yaml Runs):
  mlp       flat 'reco' features (QuantileTransformer -> N(0,1))
  deepsets  per-object phi-net, masked sum+mean pooling, + flat features -> rho-net
  part      Particle-Transformer-lite: object tokens, attention biased by pairwise
            features (ln dR, ln m_ij, ln kT, ln z), class-attention pooling, + flat features

Objects (features.py ObjectArrays): muon, neutrino, up to NJets jets, canonically oriented
(system along +z, phi relative to the muon, leading jet dphi >= 0). No charge anywhere.
"""

import argparse
import json
import logging
import math
import sys
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import yaml
from sklearn.metrics import roc_auc_score, roc_curve

sys.path.insert(0, str(Path(__file__).parent))
from train import (PROD_NAMES, auc_in_bins, flatten, plot_auc_mtt, plot_purity, plot_scores,  # noqa: E402
                   purity_table, sculpting_check, weighted_auc)

OBJ_FIELDS = ['pt', 'eta', 'dphi', 'mass', 'btag', 'qgl', 'isMu', 'isNu']


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------
def object_tensor(df, n_obj, use_qgl):
    """(N, n_obj, F) per-object inputs, (N, n_obj, 4) four-vectors, (N, n_obj) mask."""
    g = {k: np.stack([df[f'obj{i}_{k}'].to_numpy(np.float32) for i in range(n_obj)], axis=1) for k in OBJ_FIELDS}
    mask = g['pt'] > 0
    isjet = (~(g['isMu'] > 0) & ~(g['isNu'] > 0) & mask).astype(np.float32)
    feats = [np.log1p(g['pt']), g['eta'], np.sin(g['dphi']), np.cos(g['dphi']), np.log1p(g['mass']),
             g['btag'], g['isMu'], g['isNu'], isjet]
    if use_qgl:
        feats.append(np.where(isjet > 0, g['qgl'], 0.0))
    x = np.stack(feats, axis=-1).astype(np.float32)
    x[~mask] = 0.0
    px, py = g['pt'] * np.cos(g['dphi']), g['pt'] * np.sin(g['dphi'])
    pz = g['pt'] * np.sinh(g['eta'])
    e = np.sqrt(px**2 + py**2 + pz**2 + g['mass']**2)
    p4 = np.stack([px, py, pz, e], axis=-1).astype(np.float32)
    return x, p4, mask


def pair_features(p4, mask):
    """ParT interaction features (B, 4, L, L): ln dR, ln kT, ln z, ln m^2."""
    px, py, pz, e = p4.unbind(-1)
    pt = torch.sqrt(px**2 + py**2).clamp_min(1e-6)
    rap = 0.5 * torch.log(((e + pz).clamp_min(1e-6)) / ((e - pz).clamp_min(1e-6)))
    phi = torch.atan2(py, px)
    dy = rap[:, :, None] - rap[:, None, :]
    dphi = (phi[:, :, None] - phi[:, None, :] + math.pi) % (2 * math.pi) - math.pi
    dr = torch.sqrt(dy**2 + dphi**2).clamp_min(1e-6)
    ptmin = torch.minimum(pt[:, :, None], pt[:, None, :])
    kt = ptmin * dr
    z = ptmin / (pt[:, :, None] + pt[:, None, :])
    s = p4[:, :, None, :] + p4[:, None, :, :]
    m2 = (s[..., 3]**2 - s[..., 0]**2 - s[..., 1]**2 - s[..., 2]**2).clamp_min(1e-6)
    u = torch.stack([torch.log(dr), torch.log(kt.clamp_min(1e-6)), torch.log(z.clamp_min(1e-6)), torch.log(m2)], 1)
    pm = (mask[:, :, None] & mask[:, None, :]).unsqueeze(1)
    return torch.where(pm, u, torch.zeros_like(u))


# ---------------------------------------------------------------------------
# models
# ---------------------------------------------------------------------------
def mlp(sizes, dropout=0.0, last_act=False):
    layers = []
    for i in range(len(sizes) - 1):
        layers.append(nn.Linear(sizes[i], sizes[i + 1]))
        if i < len(sizes) - 2 or last_act:
            layers += [nn.LayerNorm(sizes[i + 1]), nn.SiLU()]
            if dropout:
                layers.append(nn.Dropout(dropout))
    return nn.Sequential(*layers)


class FlatMLP(nn.Module):
    def __init__(self, n_flat, cfg):
        super().__init__()
        self.net = mlp([n_flat] + cfg['hidden'] + [1], cfg.get('dropout', 0.0))

    def forward(self, flat, x=None, p4=None, mask=None):
        return self.net(flat).squeeze(-1)


class DeepSets(nn.Module):
    def __init__(self, n_flat, n_obj_feat, cfg):
        super().__init__()
        self.phi = mlp([n_obj_feat] + cfg['phi'], cfg.get('dropout', 0.0), last_act=True)
        d = cfg['phi'][-1]
        self.use_flat = cfg.get('use_flat', True)
        self.n_out = cfg.get('n_out', 1)
        self.rho = mlp([2 * d + (n_flat if self.use_flat else 0)] + cfg['rho'] + [self.n_out], cfg.get('dropout', 0.0))

    def forward(self, flat, x, p4, mask):
        h = self.phi(x) * mask.unsqueeze(-1)
        n = mask.sum(1, keepdim=True).clamp_min(1)
        z = [h.sum(1) / 10.0, h.sum(1) / n]
        if self.use_flat:
            z.append(flat)
        o = self.rho(torch.cat(z, -1))
        return o.squeeze(-1) if self.n_out == 1 else o


class Block(nn.Module):
    def __init__(self, d, heads, ff, dropout):
        super().__init__()
        self.ln1, self.ln2 = nn.LayerNorm(d), nn.LayerNorm(d)
        self.att = nn.MultiheadAttention(d, heads, dropout=dropout, batch_first=True)
        self.ff = nn.Sequential(nn.Linear(d, ff), nn.SiLU(), nn.Dropout(dropout), nn.Linear(ff, d))

    def forward(self, x, bias):
        h = self.ln1(x)
        x = x + self.att(h, h, h, attn_mask=bias, need_weights=False)[0]
        return x + self.ff(self.ln2(x))


class ClassBlock(nn.Module):
    """Class-attention: a CLS token attends to the object tokens."""
    def __init__(self, d, heads, ff, dropout):
        super().__init__()
        self.ln1, self.ln2 = nn.LayerNorm(d), nn.LayerNorm(d)
        self.att = nn.MultiheadAttention(d, heads, dropout=dropout, batch_first=True)
        self.ff = nn.Sequential(nn.Linear(d, ff), nn.SiLU(), nn.Linear(ff, d))

    def forward(self, cls, x, key_pad):
        h = self.ln1(torch.cat([cls, x], 1))
        pad = torch.cat([torch.zeros_like(key_pad[:, :1]), key_pad], 1)
        cls = cls + self.att(h[:, :1], h, h, key_padding_mask=pad, need_weights=False)[0]
        return cls + self.ff(self.ln2(cls))


class ParTLite(nn.Module):
    def __init__(self, n_flat, n_obj_feat, cfg):
        super().__init__()
        d, H = cfg['dim'], cfg['heads']
        self.H = H
        self.embed = mlp([n_obj_feat, d, d], cfg.get('dropout', 0.0), last_act=True)
        self.use_pair = cfg.get('pair_features', True)
        if self.use_pair:
            self.pair = nn.Sequential(nn.Conv2d(4, 32, 1), nn.SiLU(), nn.Conv2d(32, 32, 1), nn.SiLU(),
                                      nn.Conv2d(32, H * cfg['layers'], 1))
        self.blocks = nn.ModuleList([Block(d, H, cfg['ff'], cfg.get('dropout', 0.0)) for _ in range(cfg['layers'])])
        self.cls = nn.Parameter(torch.zeros(1, 1, d))
        self.cblocks = nn.ModuleList([ClassBlock(d, H, cfg['ff'], 0.0) for _ in range(cfg.get('cls_layers', 2))])
        self.use_flat = cfg.get('use_flat', True)
        self.head = mlp([d + (n_flat if self.use_flat else 0)] + cfg['head'] + [1], cfg.get('dropout', 0.0))

    def forward(self, flat, x, p4, mask):
        B, L, _ = x.shape
        h = self.embed(x)
        neg = torch.zeros(B, 1, L, L, device=x.device)
        neg = neg.masked_fill(~mask[:, None, None, :], float('-inf'))
        if self.use_pair:
            pb = self.pair(pair_features(p4, mask))  # (B, H*layers, L, L)
        for i, blk in enumerate(self.blocks):
            bias = neg.expand(B, self.H, L, L)
            if self.use_pair:
                bias = bias + pb[:, i * self.H:(i + 1) * self.H]
            h = blk(h, bias.reshape(B * self.H, L, L))
        cls = self.cls.expand(B, 1, -1)
        for cb in self.cblocks:
            cls = cb(cls, h, ~mask)
        z = cls[:, 0]
        if self.use_flat:
            z = torch.cat([z, flat], -1)
        return self.head(z).squeeze(-1)


# ---------------------------------------------------------------------------
@torch.no_grad()
def infer(model, T, idx=None, bs=16384):
    model.eval()
    n = len(T['y']) if idx is None else len(idx)
    out = np.empty(n, dtype=np.float32)
    for s in range(0, n, bs):
        sl = slice(s, s + bs) if idx is None else idx[s:s + bs]
        out[s:s + bs] = torch.sigmoid(model(T['flat'][sl], T['x'][sl], T['p4'][sl], T['mask'][sl])).numpy()
    return out


def fit(model, Ttr, Tva, cfg, log_prefix):
    opt = torch.optim.AdamW(model.parameters(), lr=cfg['lr'], weight_decay=cfg.get('weight_decay', 1e-4))
    y = Ttr['y']
    pos = float(y.sum())
    lossf = nn.BCEWithLogitsLoss(pos_weight=torch.tensor((len(y) - pos) / pos))
    steps_per_epoch = math.ceil(len(y) / cfg['batch_size'])
    sched = torch.optim.lr_scheduler.OneCycleLR(opt, max_lr=cfg['lr'], total_steps=cfg['epochs'] * steps_per_epoch,
                                                pct_start=0.1)
    best, best_state, bad, hist = -1, None, 0, []
    g = torch.Generator().manual_seed(42)
    for ep in range(cfg['epochs']):
        model.train()
        t0 = time.time()
        perm = torch.randperm(len(y), generator=g)
        tot = 0.0
        for s in range(0, len(y), cfg['batch_size']):
            b = perm[s:s + cfg['batch_size']]
            opt.zero_grad(set_to_none=True)
            loss = lossf(model(Ttr['flat'][b], Ttr['x'][b], Ttr['p4'][b], Ttr['mask'][b]), Ttr['yf'][b])
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            sched.step()
            tot += loss.item() * len(b)
        auc = roc_auc_score(Tva['y'].numpy(), infer(model, Tva))
        hist.append({'epoch': ep, 'loss': tot / len(y), 'val_auc': auc, 'seconds': round(time.time() - t0, 1)})
        logging.info(f'  {log_prefix} epoch {ep}: loss {tot/len(y):.4f} val AUC {auc:.4f} ({time.time()-t0:.0f}s)')
        if auc > best + 1e-5:
            best, bad = auc, 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            bad += 1
            if bad >= cfg.get('patience', 3):
                logging.info(f'  {log_prefix}: early stop')
                break
    model.load_state_dict(best_state)
    return hist


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--featureDir', required=True)
    ap.add_argument('--config', required=True)
    ap.add_argument('--outDir', required=True)
    ap.add_argument('--era', required=True)
    ap.add_argument('--targets', nargs='+', required=True)
    ap.add_argument('--sample', action='store_true')
    ap.add_argument('--runs', nargs='*', default=None, help='subset of run names')
    args = ap.parse_args()

    out = Path(args.outDir)
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler(out / 'train_torch.log'), logging.StreamHandler(sys.stdout)])
    cfg = yaml.safe_load(open(args.config))
    torch.set_num_threads(cfg.get('threads', 20))
    torch.manual_seed(42)
    flat_feats = flatten(cfg['FlatFeatures'])
    n_obj = cfg['NObjects']
    obj_cols = [f'obj{i}_{k}' for i in range(n_obj) for k in OBJ_FIELDS]
    meta = ['event', 'fold', 'prodType', 'y', 'genWeight', 'gen_tt_m', 'gen_delta_absy']

    files = []
    for t in args.targets:
        files += sorted((Path(args.featureDir) / t).glob('*.parquet'))
    if args.sample:
        files = files[::40]
    logging.info(f'Reading {len(files)} parquet files')
    df = pd.concat([pd.read_parquet(f, columns=meta + flat_feats + obj_cols) for f in files], ignore_index=True)
    df = df[df.prodType >= 0].reset_index(drop=True)

    sp = cfg['Split']
    parts = {k: df[df.fold.isin(sp[k])].reset_index(drop=True) for k in ('train', 'val', 'test')}
    del df
    mx = cfg.get('max_train_rows')
    if mx and len(parts['train']) > mx:
        parts['train'] = parts['train'].sample(n=mx, random_state=42).reset_index(drop=True)
    for k, v in parts.items():
        logging.info(f'{k}: {len(v)} events, qqbar fraction {v.y.mean():.4f}')

    from sklearn.preprocessing import QuantileTransformer
    qt = QuantileTransformer(output_distribution='normal', n_quantiles=1000, subsample=1_000_000, random_state=42)
    qt.fit(parts['train'][flat_feats].to_numpy(np.float32))

    def tensors(p):
        # Built once for all runs; the qgl channel is the last object feature and is
        # sliced off for runs without qgl. Object columns are dropped from the frame after.
        x, p4, mask = object_tensor(p, n_obj, use_qgl=True)
        flat = np.nan_to_num(qt.transform(p[flat_feats].to_numpy(np.float32))).astype(np.float32)
        return {'flat': torch.from_numpy(flat), 'x_all': torch.from_numpy(x), 'p4': torch.from_numpy(p4),
                'mask': torch.from_numpy(mask), 'y': torch.from_numpy(p.y.to_numpy(np.int64)),
                'yf': torch.from_numpy(p.y.to_numpy(np.float32))}

    TT = {}
    for k in ('train', 'val', 'test'):
        TT[k] = tensors(parts[k])
        parts[k] = parts[k].drop(columns=obj_cols + flat_feats)
    logging.info('Tensors built')

    te = parts['test']
    yte = te.y.to_numpy()
    summary_path = out / 'summary.json'
    summary = json.load(open(summary_path)) if summary_path.exists() else {'era': args.era, 'runs': {}}
    summary['n'] = {k: len(v) for k, v in parts.items()}
    scores_path = out / 'test_scores.parquet'
    scores = (pd.read_parquet(scores_path) if scores_path.exists()
              else te[['event', 'fold', 'prodType', 'y', 'genWeight', 'gen_tt_m', 'gen_delta_absy']].copy())

    for run in cfg['Runs']:
        name = run['name']
        if args.runs and name not in args.runs:
            continue
        rd = out / name
        rd.mkdir(exist_ok=True)
        mcfg = {**cfg['Models'][run['model']], **run.get('override', {})}
        use_qgl = run.get('qgl', False)
        logging.info(f'=== {name}: model {run["model"]}, qgl {use_qgl}, {mcfg}')
        T = {k: {**v, 'x': v['x_all'] if use_qgl else v['x_all'][..., :-1]} for k, v in TT.items()}
        n_flat, n_of = T['train']['flat'].shape[1], T['train']['x'].shape[2]
        model = {'mlp': lambda: FlatMLP(n_flat, mcfg), 'deepsets': lambda: DeepSets(n_flat, n_of, mcfg),
                 'part': lambda: ParTLite(n_flat, n_of, mcfg)}[run['model']]()
        logging.info(f'  parameters: {sum(p.numel() for p in model.parameters()):,}')
        t0 = time.time()
        Ttr = T['train']
        mx_run = run.get('max_train_rows')
        if mx_run and mx_run < len(Ttr['y']):
            sub = torch.randperm(len(Ttr['y']), generator=torch.Generator().manual_seed(7))[:mx_run]
            Ttr = {k: v[sub] for k, v in Ttr.items() if k != 'x_all'}
            logging.info(f'  training on a {mx_run}-event subsample of the train split')
        hist = fit(model, Ttr, T['val'], {**cfg['Training'], **mcfg.get('training', {})}, name)
        tr_idx = torch.arange(min(2_000_000, len(T['train']['y'])))
        s_tr = infer(model, T['train'], tr_idx)
        s_va, s_te = infer(model, T['val']), infer(model, T['test'])
        ytr = parts['train'].y.to_numpy()[:len(s_tr)]
        res = {'model': run['model'], 'qgl': use_qgl, 'config': mcfg, 'history': hist,
               'train_seconds': round(time.time() - t0, 1),
               'auc_train': float(roc_auc_score(ytr, s_tr)),
               'auc_val': float(roc_auc_score(T['val']['y'].numpy(), s_va)),
               'auc_test': float(roc_auc_score(yte, s_te)),
               'auc_test_genWeightSign': weighted_auc(yte, s_te, np.sign(te.genWeight.to_numpy()))}
        res['auc_test_one_vs_one'] = {
            f'qqbar_vs_{PROD_NAMES[p]}': float(roc_auc_score(yte[(te.prodType == 1) | (te.prodType == p)],
                                                             s_te[((te.prodType == 1) | (te.prodType == p)).values]))
            for p in (0, 2)}
        ev = cfg['Evaluation']
        res['purity'] = purity_table(s_te, yte, te.prodType.to_numpy(), ev['keep_fractions'])
        res['auc_vs_gen_mtt'] = auc_in_bins(s_te, yte, te.gen_tt_m.to_numpy(), ev['mtt_bins'])
        res['sculpting'] = sculpting_check(s_te, te.prodType.to_numpy(), te.gen_delta_absy.to_numpy())
        res['ks_pvalue_train_vs_test'] = plot_scores(rd, name, s_te, te.prodType.to_numpy(), s_tr,
                                                     parts['train'].prodType.to_numpy()[:len(s_tr)])
        plot_purity(rd, name, res['purity'])
        plot_auc_mtt(rd, name, res['auc_vs_gen_mtt'])
        torch.save(model.state_dict(), rd / 'model.pt')
        joblib.dump({'flat_features': flat_feats, 'quantile': qt, 'n_obj': n_obj, 'qgl': use_qgl,
                     'model': run['model'], 'config': mcfg}, rd / 'preproc.pkl')
        json.dump(res, open(rd / 'result.json', 'w'), indent=1)
        logging.info(f"  {name}: AUC train {res['auc_train']:.4f} val {res['auc_val']:.4f} test {res['auc_test']:.4f}")
        summary['runs'][name] = {k: v for k, v in res.items() if k not in ('config',)}
        scores[f'score_{name}'] = s_te
        json.dump(summary, open(summary_path, 'w'), indent=1)
        scores.to_parquet(scores_path, index=False)
        del T

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    for zoom in (False, True):
        fig, ax = plt.subplots(figsize=(6, 6))
        for c in [c for c in scores.columns if c.startswith('score_')]:
            fpr, tpr, _ = roc_curve(scores.y, scores[c])
            ax.plot(fpr, tpr, label=f"{c[6:]} (AUC {roc_auc_score(scores.y, scores[c]):.4f})")
        ax.plot([0, 1], [0, 1], 'k--', lw=0.8)
        if zoom:
            ax.set_xlim(0, 0.2)
            ax.set_ylim(0, 0.6)
        ax.set_xlabel('false-positive rate (gg + qg kept)')
        ax.set_ylabel('true-positive rate (qqbar kept)')
        ax.legend(fontsize=8)
        ax.set_title(f'{args.era}: torch models (test folds)')
        fig.tight_layout()
        fig.savefig(out / ('roc_zoom.png' if zoom else 'roc.png'), dpi=130)
        plt.close(fig)
    logging.info('Done.')


if __name__ == '__main__':
    sys.exit(main())
