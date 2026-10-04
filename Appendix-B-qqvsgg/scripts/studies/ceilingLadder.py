#!/usr/bin/env python3
"""Truth-level ceiling ladder (Appendix-B, 2026-10-02): XGBoost on progressively more parton-level
information, every 6th UL2018 file, region A. Joins the c9087777f281 feature parquet with GenPart/LHEPart
from the selectionII skims. Run in latestcoffea: python scripts/studies/ceilingLadder.py"""
import sys, glob, numpy as np, awkward as ak, uproot, pandas as pd, xgboost as xgb
from concurrent.futures import ProcessPoolExecutor
sys.path.insert(0, '/home/mukund/Projects/PhysicsTools/NanoAODTools/Appendix-B-qqvsgg/scripts')
import features as F
FEAT = '/mnt/disk3/nfs/mukund/DataFiles/appendixB/earlySeptember_corrected/c9087777f281/UL2018/MC_mu/SemiLeptonic/ttbar_SemiLeptonic/'
SEL = '/mnt/disk3/nfs/mukund/DataFiles/selectionII/earlySeptember_corrected/37f710b13012/UL2018/MC_mu/SemiLeptonic/ttbar_SemiLeptonic/'

def one(pq):
    reco = pd.read_parquet(pq)
    root = SEL + pq.split('/')[-1].replace('.parquet', '.root')
    a = uproot.open(root)['Events'].arrays(['event', 'ABCD_region', 'LHEPart_pt', 'LHEPart_eta', 'LHEPart_phi',
        'LHEPart_mass', 'LHEPart_pdgId', 'GenPart_pdgId', 'GenPart_statusFlags', 'GenPart_pt', 'GenPart_eta',
        'GenPart_phi', 'GenPart_mass', 'Generator_x1', 'Generator_x2'])
    a = a[a.ABCD_region == 0]
    a = a[np.isin(ak.to_numpy(a.event), reco.event.values)]
    pid, last = a.GenPart_pdgId, (a.GenPart_statusFlags & (1 << 13)) != 0
    def top(sign):
        i = ak.to_numpy(ak.fill_none(ak.firsts(ak.local_index(pid)[(pid == sign * 6) & last]), 0))
        return F.p4(*[ak.to_numpy(ak.firsts(a[f'GenPart_{c}'][ak.singletons(ak.Array(i))])) for c in ('pt', 'eta', 'phi', 'mass')])
    t, tb = top(1), top(-1)
    tt = F.add(t, tb)
    ts = F.boost(t, tt)
    cth = ts[2] / np.sqrt(ts[0]**2 + ts[1]**2 + ts[2]**2)
    x1, x2 = ak.to_numpy(a.Generator_x1), ak.to_numpy(a.Generator_x2)
    ep = lambda c: ak.to_numpy(a[f'LHEPart_{c}'][:, 2])
    out = pd.DataFrame({'event': ak.to_numpy(a.event).astype(np.uint64),
        'g_tt_m': F.mass(tt), 'g_tt_pt': F.pt(tt), 'g_tt_absy': np.abs(F.rapidity(tt)),
        'g_xmax': np.maximum(x1, x2), 'g_xmin': np.minimum(x1, x2), 'g_abscos': np.abs(cth),
        'g_top_ptmax': np.maximum(F.pt(t), F.pt(tb)),
        'lhe_x_pt': ep('pt'), 'lhe_x_abseta': np.abs(ep('eta')), 'lhe_x_isglu': (ep('pdgId') == 21).astype(int)})
    return reco.merge(out, on='event')

files = sorted(glob.glob(FEAT + '*.parquet'))[::6]
with ProcessPoolExecutor(15) as ex:
    df = pd.concat(list(ex.map(one, files)), ignore_index=True)
print('events', len(df), 'qqbar', df.y.mean())
print('extra parton is gluon, by prodType:', df.groupby('prodType').lhe_x_isglu.mean().round(3).to_dict())

reco = [c for c in pd.read_parquet(files[0]).columns if c not in
        ('run','luminosityBlock','event','fold','muCharge','prodType','y','gen_x1','gen_x2','genWeight',
         'gen_tt_m','gen_tt_pt','gen_tt_absy','gen_delta_absy','extra1_qgl','wjets_qgl_min')]
ladder = {
 'A  tt m,pT,|y| (previous oracle)': ['g_tt_m','g_tt_pt','g_tt_absy'],
 'B  + x1/x2, |cos theta*|, top pT': ['g_tt_m','g_tt_pt','g_tt_absy','g_xmax','g_xmin','g_abscos','g_top_ptmax'],
 'C  + extra parton pT, |eta|':      ['g_tt_m','g_tt_pt','g_tt_absy','g_xmax','g_xmin','g_abscos','g_top_ptmax','lhe_x_pt','lhe_x_abseta'],
 'D  + extra parton flavour (full parton level)': ['g_tt_m','g_tt_pt','g_tt_absy','g_xmax','g_xmin','g_abscos','g_top_ptmax','lhe_x_pt','lhe_x_abseta','lhe_x_isglu'],
 'R  reco (xgb_reco features, same subset)': reco,
}
from sklearn.metrics import roc_auc_score
tr, te = df[df.fold >= 3], df[df.fold < 3]
w = np.where(tr.y == 1, 0.5/tr.y.mean(), 0.5/(1-tr.y.mean()))
for name, cols in ladder.items():
    m = xgb.XGBClassifier(n_estimators=2000, max_depth=6, learning_rate=0.1, subsample=0.8, tree_method='hist',
                          n_jobs=20, early_stopping_rounds=30, eval_metric='auc')
    va = tr.fold == 3
    m.fit(tr[~va][cols], tr[~va].y, sample_weight=w[~va.values], eval_set=[(tr[va][cols], tr[va].y)], verbose=False)
    s = m.predict_proba(te[cols])[:, 1]
    vs = {p: roc_auc_score(te.y[(te.prodType==1)|(te.prodType==p)], s[((te.prodType==1)|(te.prodType==p)).values]) for p in (0,2)}
    print(f'{name:48s} AUC {roc_auc_score(te.y, s):.4f}   vs gg {vs[0]:.3f}  vs qg {vs[2]:.3f}', flush=True)
