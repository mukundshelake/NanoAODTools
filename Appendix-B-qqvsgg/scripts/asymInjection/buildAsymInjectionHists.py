#!/usr/bin/env python3
"""
Gen-level N+ / N- vs m_tt from selectionII ttbar, plus unit asymmetry-injection templates
(Appendix-B, scripts/asymInjection). Run in latestcoffea:

    python scripts/asymInjection/buildAsymInjectionHists.py --tag earlySeptember_corrected

Definitions (gen level, last-copy tops as in Appendix-A):
    Delta|y| = |y_t| - |y_tbar|,  s = sign(Delta|y|),  N+ = sum w (s > 0),  N- = sum w (s < 0)
    w = product of the 003-III weightList branches * Lumi * Xsec / Ngen   (003-III units)

Injection templates. For event i in fine gen m_tt bin j (25 GeV grid, with flow bins):
    uniform    delta_i = c_u * (s_i - A_j)
    qqbar      delta_i = c_q * 1qq_i * (s_i - A^qq_j)
    mttRising  delta_i = c_m * r_j * (s_i - A_j),   r_j = <m_tt>_j / <m_tt>
A_j (A^qq_j) is the weighted gen asymmetry of all (qqbar) events in fine bin j, so every
template has sum_i w_i delta_i = 0 in every fine bin: adding it moves events between N+ and
N- without changing N+ + N-. dN+ (dN-) = sum of w_i delta_i over events with s > 0 (s < 0).
c is set so that adding (dN+, dN-) shifts the inclusive A_C by exactly UnitDeltaAC; the
relation is linear, so  N+-(dA) = N+- + (dA / UnitDeltaAC) * dN+-.
Per-event equivalent: weight w_i * (1 + (dA / UnitDeltaAC) * delta_i).

Outputs in outputs/{tag}/injection/{hash}/{era}/:
    {tag}_{era}_MC_mu_SemiLeptonic_regionA_asymInjectionHists.coffea
        {"{era}_MC_mu_SemiLeptonic": {name: boost_histogram.Histogram}}  (003-III layout)
    same histograms as TH1D in a .root file, and summary.json with every closure check.
Histogram names: Nplus_<binning>, Nminus_<binning>, dNplus_<model>_<binning>, dNminus_<model>_<binning>
with <binning> in {mttGen005, mttGenFine} and <model> in {uniform, qqbar, mttRising}.
"""

import argparse
import json
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import awkward as ak
import boost_histogram as bh
import numpy as np
import uproot
import yaml

CHAPTER = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(CHAPTER / 'scripts'))
import utils  # noqa: E402
from features import gen_columns  # noqa: E402


def read_file(args):
    path, weights, region, norm = args
    t = uproot.open(path)['Events']
    br = ['ABCD_region', 'Generator_id1', 'Generator_id2', 'Generator_x1', 'Generator_x2', 'Generator_weight',
          'GenPart_pdgId', 'GenPart_statusFlags', 'GenPart_pt', 'GenPart_eta', 'GenPart_phi', 'GenPart_mass']
    present = [w for w in weights if w in t.keys()]
    a = t.arrays(br + present)
    a = a[a.ABCD_region == region]
    w = np.full(len(a), norm, dtype=np.float64)
    for b in present:
        w = w * ak.to_numpy(a[b]).astype(np.float64)
    g = gen_columns(a)
    return {'w': w, 'mtt': g['gen_tt_m'].astype(np.float64), 'dabsy': g['gen_delta_absy'].astype(np.float64),
            'qq': (g['prodType'] == 1), 'missing_weights': sorted(set(weights) - set(present))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tag', default='earlySeptember_corrected')
    ap.add_argument('--config', default=str(CHAPTER / 'injection_config.yaml'))
    ap.add_argument('--sample', action='store_true', help='first 5 files only (no closure vs 003-III)')
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config))
    cfg_hash = utils.compute_config_hash(args.config)
    storage = utils.resolve_storage_path(utils.load_config(CHAPTER / 'config.yaml'))
    unit = cfg['UnitDeltaAC']

    for era in cfg['Eras']:
        out = CHAPTER / 'outputs' / args.tag / 'injection' / cfg_hash / era
        out.mkdir(parents=True, exist_ok=True)
        shutil.copy2(args.config, out.parent / 'injection_config.yaml')
        summary = {'era': era, 'config_hash': cfg_hash, 'unitDeltaAC': unit, 'datasets': {}}
        result = {}
        for ds in cfg['Datasets']:
            DataMC, group, dataset = ds.split('/')
            base = Path(storage) / 'selectionII' / cfg['SelectionII']['tag'] / cfg['SelectionII']['hash'] / era / ds
            files = sorted(base.glob('*.root'))
            if args.sample:
                files = files[:5]
            n = cfg['Normalisation'][era]
            norm = n['Lumi'] * n[dataset]['Xsec'] / n[dataset]['Ngen']
            print(f'{era} {ds}: {len(files)} files, Lumi*Xsec/Ngen = {norm:.6g}', flush=True)
            with ProcessPoolExecutor(cfg['Workers']) as ex:
                parts = list(ex.map(read_file, [(str(f), cfg['WeightList'], cfg['Region'], norm) for f in files]))
            missing = sorted({m for p in parts for m in p['missing_weights']})
            w = np.concatenate([p['w'] for p in parts])
            mtt = np.concatenate([p['mtt'] for p in parts])
            dabsy = np.concatenate([p['dabsy'] for p in parts])
            qq = np.concatenate([p['qq'] for p in parts]).astype(np.float64)
            n_all, w_all = len(w), float(w.sum())
            ok = np.isfinite(mtt) & np.isfinite(dabsy) & (dabsy != 0)
            w, mtt, dabsy, qq = w[ok], mtt[ok], dabsy[ok], qq[ok]
            s = np.sign(dabsy)

            fb = cfg['Binning']['mttGenFine']
            fine_edges = np.arange(fb['start'], fb['stop'] + 1e-9, fb['step'])
            axes = {'mttGen005': bh.axis.Variable(cfg['Binning']['mttGen005'], metadata='gen m_tt [GeV]'),
                    'mttGenFine': bh.axis.Variable(fine_edges, metadata='gen m_tt [GeV]')}
            # fine-bin index including flow: 0 = underflow, ..., len(edges) = overflow
            j = np.digitize(mtt, fine_edges)
            nb = len(fine_edges) + 1

            def per_bin(weights):
                return np.bincount(j, weights=weights, minlength=nb)

            W, Ws = per_bin(w), per_bin(w * s)
            Wq, Wqs = per_bin(w * qq), per_bin(w * qq * s)
            A = np.divide(Ws, W, out=np.zeros(nb), where=W != 0)
            Aq = np.divide(Wqs, Wq, out=np.zeros(nb), where=Wq != 0)
            m_bin = np.divide(per_bin(w * mtt), W, out=np.zeros(nb), where=W != 0)
            r = m_bin / (np.sum(w * mtt) / np.sum(w))
            raw = {'uniform': s - A[j], 'qqbar': qq * (s - Aq[j]), 'mttRising': r[j] * (s - A[j])}
            A_incl = float(np.sum(w * s) / np.sum(w))

            hists, checks = {}, {}
            for bname, ax in axes.items():
                for sign_name, sel in (('Nplus', s > 0), ('Nminus', s < 0)):
                    h = bh.Histogram(ax, storage=bh.storage.Double())
                    h.fill(mtt[sel], weight=w[sel])
                    hists[f'{sign_name}_{bname}'] = h
            for model in cfg['Models']:
                d = raw[model]
                # inclusive dA for coefficient 1: (sum w d s) / sum w  (yields unchanged, so dA = d(N+ - N-)/N)
                dA1 = float(np.sum(w * d * s) / np.sum(w))
                c = unit / dA1
                delta = c * d
                for bname, ax in axes.items():
                    for sign_name, sel in (('dNplus', s > 0), ('dNminus', s < 0)):
                        h = bh.Histogram(ax, storage=bh.storage.Double())
                        h.fill(mtt[sel], weight=(w * delta)[sel])
                        hists[f'{sign_name}_{model}_{bname}'] = h
                # closure checks on the fine binning (flow included)
                dNp, dNm = per_bin(w * delta * (s > 0)), per_bin(w * delta * (s < 0))
                Np, Nm = per_bin(w * (s > 0)), per_bin(w * (s < 0))
                yield_change = float(np.max(np.abs(dNp + dNm)))
                A_new = (np.sum(Np + dNp) - np.sum(Nm + dNm)) / np.sum(Np + Nm)
                # largest dA (in units of UnitDeltaAC) keeping every bin and every event weight >= 0
                with np.errstate(divide='ignore', invalid='ignore'):
                    kb = np.concatenate([np.where(dNp < 0, -Np / dNp, np.inf), np.where(dNm < 0, -Nm / dNm, np.inf)])
                    ke_pos = np.min(np.where(delta < 0, -1 / delta, np.inf))
                    ke_neg = np.min(np.where(delta > 0, 1 / delta, np.inf))
                # per-bin dA on the 005 binning
                h005p, h005m = hists[f'Nplus_mttGen005'], hists[f'Nminus_mttGen005']
                t005p, t005m = hists[f'dNplus_{model}_mttGen005'], hists[f'dNminus_{model}_mttGen005']
                N5p, N5m = h005p.values(flow=True), h005m.values(flow=True)
                D5p, D5m = t005p.values(flow=True), t005m.values(flow=True)
                with np.errstate(divide='ignore', invalid='ignore'):
                    A5 = (N5p - N5m) / (N5p + N5m)
                    dA5 = ((D5p - D5m) / (N5p + N5m))
                checks[model] = {
                    'coefficient_c': c,
                    'inclusive_dA_from_template': float(A_new - A_incl),
                    'max_abs_fine_bin_yield_change': yield_change,
                    'max_scale_all_bins_nonnegative': float(np.min(kb)) * unit,
                    'max_positive_dA_all_event_weights_nonnegative': float(ke_pos) * unit,
                    'max_negative_dA_all_event_weights_nonnegative': -float(ke_neg) * unit,
                    'mttGen005_bins_with_flow': {
                        'edges': ['<300'] + [str(e) for e in cfg['Binning']['mttGen005']] + ['overflow'],
                        'A_nominal': [None if not np.isfinite(x) else float(x) for x in A5],
                        'dA_per_unit_template': [None if not np.isfinite(x) else float(x) for x in dA5],
                    },
                }
                print(f"  {model:10s} c={c:.4g}  inclusive dA {A_new - A_incl:+.6f}  max|fine-bin yield change| "
                      f"{yield_change:.2e}  dA per 005 bin " +
                      ' '.join('—' if not np.isfinite(x) else f'{x:+.4f}' for x in dA5), flush=True)

            total = float(w.sum())
            ref = cfg['ReferenceYield'].get(era)
            summary['datasets'][ds] = {
                'files': len(files), 'events_regionA': n_all, 'weighted_yield_all': w_all,
                'events_used': int(ok.sum()), 'events_dropped_no_gen_sign': int(n_all - ok.sum()),
                'weighted_yield_used': total, 'missing_weight_branches': missing,
                'reference_003III_yield': None if args.sample else ref,
                'closure_vs_003III': None if (args.sample or ref is None) else w_all / ref,
                'A_C_inclusive_gen': A_incl, 'f_qqbar_weighted': float(np.sum(w * qq) / total),
                'N_plus': float(np.sum(w * (s > 0))), 'N_minus': float(np.sum(w * (s < 0))),
                'templates': checks,
            }
            print(f"  yield {w_all:.2f} (003-III {ref}) ratio {summary['datasets'][ds]['closure_vs_003III']}, "
                  f"A_C(gen) {A_incl:+.5f}, missing weights {missing}", flush=True)
            result[f'{era}_{DataMC}_{group}'] = hists

        from coffea.util import save
        stem = f'{args.tag}_{era}_MC_mu_SemiLeptonic_regionA_asymInjectionHists'
        save(result, out / f'{stem}.coffea')
        with uproot.recreate(out / f'{stem}.root') as f:
            for key, hs in result.items():
                for name, h in hs.items():
                    f[f'{key}/{name}'] = h
        json.dump(summary, open(out / 'summary.json', 'w'), indent=1)
        print(f'Wrote {out}', flush=True)


if __name__ == '__main__':
    main()
