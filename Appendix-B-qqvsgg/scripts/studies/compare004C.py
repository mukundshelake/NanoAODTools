#!/usr/bin/env python3
"""
Like-for-like comparison with 004C (Appendix-B, UL2016preVFP).

Joins 004C's per-event BDTScore (bdtScore/{tag}/{hash}/{era}, ttbar_SemiLeptonic) onto the
Appendix-B test folds (test_scores.parquet of a preVFP training run), so both classifiers are
scored on exactly the same region-A events with the same metrics.

Caveat: 004C used a random 70/30 split whose membership was not saved with event IDs, so
about 70 % of these events were in 004C's training set. 004C's number here is therefore
slightly optimistic -- the comparison is conservative in 004C's favour.

Run in latestcoffea:
  python scripts/studies/compare004C.py --testScores <.../training/<hash>/test_scores.parquet> \
      --bdtScoreDir /mnt/disk3/nfs/mukund/DataFiles/bdtScore/earlySeptember_corrected/386b4160d6fa/UL2016preVFP/MC_mu/SemiLeptonic/ttbar_SemiLeptonic \
      --out <dir>/compare004C.json
"""

import argparse
import glob
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import uproot
from sklearn.metrics import roc_auc_score

sys.path.insert(0, str(Path(__file__).parent.parent))
from train import PROD_NAMES, purity_table  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--testScores', required=True)
    ap.add_argument('--bdtScoreDir', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()

    ts = pd.read_parquet(args.testScores)

    def read(f):
        return uproot.open(f)['Events'].arrays(['event', 'ABCD_region', 'BDTScore'], library='pd')

    files = sorted(glob.glob(f'{args.bdtScoreDir}/*.root'))
    with ThreadPoolExecutor(10) as ex:
        b = pd.concat(list(ex.map(read, files)), ignore_index=True)
    b = b[b.ABCD_region == 0]
    dup_b = b.event.duplicated(keep=False)
    dup_t = ts.event.duplicated(keep=False)
    b, ts = b[~dup_b], ts[~dup_t]
    m = ts.merge(b[['event', 'BDTScore']], on='event', how='inner')
    res = {'n_test_events': int(len(ts)), 'n_matched': int(len(m)), 'n_dropped_duplicate_event_ids':
           int(dup_b.sum() + dup_t.sum()), 'auc': {}, 'one_vs_one': {}, 'purity': {}}
    cols = ['BDTScore'] + [c for c in m.columns if c.startswith('score_')]
    y = m.y.to_numpy()
    for c in cols:
        s = m[c].to_numpy()
        name = '004C_BDT' if c == 'BDTScore' else c[6:]
        res['auc'][name] = float(roc_auc_score(y, s))
        res['one_vs_one'][name] = {f'qqbar_vs_{PROD_NAMES[p]}': float(roc_auc_score(
            y[(m.prodType == 1) | (m.prodType == p)], s[((m.prodType == 1) | (m.prodType == p)).values]))
            for p in (0, 2)}
        res['purity'][name] = purity_table(s, y, m.prodType.to_numpy(), [0.5, 0.2, 0.1, 0.05, 0.02])
    json.dump(res, open(args.out, 'w'), indent=1)
    print(f"matched {res['n_matched']}/{res['n_test_events']} test events")
    for k, v in res['auc'].items():
        top10 = [r for r in res['purity'][k] if r['keep_fraction'] == 0.1][0]
        print(f'  {k:22s} AUC {v:.4f}  qqbar purity top10% {top10["qqbar_purity"]:.3f}')


if __name__ == '__main__':
    main()
