#!/usr/bin/env python3
"""
Parallel feature extraction for Appendix-B: one selectionII skim -> one parquet file.

Input is a task-list JSON written by run_all.py --extractFeatures (one dict per file:
era, DataMC, file, outFile). Each output is written to <outFile>.tmp and renamed only
on success, so a parquet that exists is always complete (unlike ROOT skims, whose
output file appears at worker start). Per-file stats go to <outFile>.json.
"""

import argparse
import json
import os
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))


def work(task, cfg):
    import features  # import inside the worker (fresh numpy/uproot state per process)
    t0 = time.time()
    out = task['outFile']
    try:
        df, stats = features.compute_features(task['file'], cfg, task['era'], is_mc=task['DataMC'].startswith('MC'))
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        tmp = out + '.tmp'
        df.to_parquet(tmp, index=False)
        os.replace(tmp, out)
        stats.update(file=task['file'], seconds=round(time.time() - t0, 2))
        with open(out.replace('.parquet', '.json'), 'w') as f:
            json.dump(stats, f, indent=1)
        return task['file'], stats, None
    except Exception:
        return task['file'], None, traceback.format_exc()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--taskJSON', required=True)
    p.add_argument('--config', required=True)
    p.add_argument('--workers', type=int, default=15)
    p.add_argument('--sample', action='store_true', help='only the first task')
    args = p.parse_args()

    cfg = yaml.safe_load(open(args.config))
    tasks = json.load(open(args.taskJSON))
    if args.sample:
        tasks = tasks[:1]
    print(f'{len(tasks)} files, {args.workers} workers', flush=True)

    n_done, n_fail, tot = 0, 0, {'n_in': 0, 'n_out': 0, 'njet_mismatch': 0, 'nbjet_mismatch': 0}
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(work, t, cfg) for t in tasks]
        for fu in as_completed(futs):
            fname, stats, err = fu.result()
            if err:
                n_fail += 1
                print(f'FAILED {fname}\n{err}', flush=True)
                continue
            n_done += 1
            for k in tot:
                tot[k] += stats.get(k, 0)
            if n_done % 20 == 0 or n_done == len(tasks):
                el = time.time() - t0
                print(f'[{n_done}/{len(tasks)}] {el/60:.1f} min, events in/out {tot["n_in"]}/{tot["n_out"]}', flush=True)
    print(f'DONE ok={n_done} failed={n_fail} totals={tot}', flush=True)
    if tot['njet_mismatch'] or tot['nbjet_mismatch']:
        print('WARNING: selected-jet definition does not reproduce sel_nJet/sel_nbjet', flush=True)
    return 1 if n_fail else 0


if __name__ == '__main__':
    sys.exit(main())
