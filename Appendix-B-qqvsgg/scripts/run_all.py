#!/usr/bin/env python3
"""
Master script for Appendix-B: ML separation of qqbar-initiated ttbar from gg/qg.

Steps (run in this order, any subset per invocation):
    [0] --generateSelectionIIDatasetJSON  scan the 003-ObjectSelectionII output on disk
    [1] --extractFeatures                 selectionII skim -> per-file parquet (features.py)
                                          {STORAGE}/appendixB/{tag}/{hash}/{era}/...
    [2] --train                           train/evaluate the models of training_config.yaml
                                          outputs/{tag}/{hash}/{era}/training/{trainingHash}/
    [3] --trainTorch                      PyTorch models of torch_config.yaml (torchml env)
                                          outputs/{tag}/{hash}/{era}/training_torch/{torchHash}/

config.yaml (extraction) and training_config.yaml (models) are hashed separately, so
retuning a model never re-extracts features.

Usage (latestcoffea env, not cmsenv):
    python scripts/run_all.py --tag earlySeptember_corrected --filter UL2018 \\
        --generateSelectionIIDatasetJSON --selectionIITag earlySeptember_corrected --selectionIIHash 37f710b13012 \\
        --extractFeatures --train
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import utils


def matches_filter(filters, era, data_mc=None, group=None, dataset=None):
    """era[/DataMC[/group[/dataset]]] filters, '*' wildcard, OR-ed (as in the other chapters)."""
    if not filters:
        return True
    for f in filters:
        parts = f.split('/')
        if parts[0] not in ('*', era):
            continue
        if data_mc is not None and len(parts) >= 2 and parts[1] not in ('*', data_mc):
            continue
        if group is not None and len(parts) >= 3 and parts[2] not in ('*', group):
            continue
        if dataset is not None and len(parts) >= 4 and parts[3] not in ('*', dataset):
            continue
        return True
    return False


def run_cmd(cmd):
    print(f"    Running command: {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd).returncode == 0


def main():
    parser = argparse.ArgumentParser(description='Appendix-B: qqbar vs gg/qg classifier')
    parser.add_argument('-t', '--tag', default='Dump')
    parser.add_argument('--force', action='store_true', help='Re-extract files whose parquet already exists')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER')
    parser.add_argument('--generateSelectionIIDatasetJSON', action='store_true',
                        help='[0] Scan {STORAGE}/selectionII/{selectionIITag}/{selectionIIHash}/{era}')
    parser.add_argument('--selectionIITag', default=None)
    parser.add_argument('--selectionIIHash', default=None)
    parser.add_argument('--inputStage', choices=['selectionII', 'reconstruction'], default='selectionII',
                        help='[0] storage stage to read: selectionII (003-II) or reconstruction (004A output, '
                             'adds the kf_* kinematic-fit features). Use a distinct --tag per stage.')
    parser.add_argument('--extractFeatures', action='store_true', help='[1] per-file feature parquet')
    parser.add_argument('--train', action='store_true', help='[2] train + evaluate (train.py)')
    parser.add_argument('--trainingConfig', default=None, help='[2] default: training_config.yaml')
    parser.add_argument('--trainTorch', action='store_true',
                        help='[3] PyTorch models (train_torch.py, torch_config.yaml) in the TorchPython env')
    parser.add_argument('--asymStudy', action='store_true',
                        help='[4] A_C-sensitivity study (scripts/asym/asym_study.py, asym_config.yaml)')
    parser.add_argument('--qqScores', default=None, help='[4] test_scores.parquet with the qqbar score')
    parser.add_argument('--asymTorch', action='store_true',
                        help='[5] DeepSets version of the A_C study (asym_torch.py) in the torchml env, '
                             'into asym/{asymHash}/deepsets/')
    parser.add_argument('--qqScoreColumn', default=None, help='[4] score column in --qqScores')
    parser.add_argument('--torchRuns', nargs='*', default=None, help='[3] subset of torch_config Runs by name')
    parser.add_argument('--sample', action='store_true', help='[1] first file only; [2] small subsample')
    parser.add_argument('--workers', type=int, default=None, help='default: config Workers')
    parser.add_argument('--printHash', action='store_true')
    args = parser.parse_args()

    base_dir = Path(__file__).parent.parent
    scripts_dir = base_dir / 'scripts'
    config_path = base_dir / 'config.yaml'
    train_cfg_path = Path(args.trainingConfig) if args.trainingConfig else base_dir / 'training_config.yaml'
    outputs_base = base_dir / 'outputs' / args.tag
    inputs_folder = base_dir / 'inputs'

    config = utils.load_config(config_path)
    output_dir, config_hash, is_new = utils.create_output_directory(outputs_base, config_path, inputs_folder)
    print(f"Output directory: {output_dir} ({'new' if is_new else 'existing'})")
    latest = outputs_base / 'latest'
    if latest.exists() or latest.is_symlink():
        latest.unlink()
    latest.symlink_to(output_dir.name)
    storageBase = utils.resolve_storage_path(config)
    if args.printHash:
        print(f"Config hash: {config_hash}  training hash: {utils.compute_config_hash(train_cfg_path)}")
        return 0
    workers = args.workers or config.get('Workers', 15)

    eras = [e for e in config['Eras'] if matches_filter(args.filter, e)]
    targets = [tuple(t.split('/')) for t in config['TargetDatasets']]

    def feature_dir(era):
        return Path(storageBase) / 'appendixB' / args.tag / config_hash / era

    # [0]
    if args.generateSelectionIIDatasetJSON:
        if not args.selectionIITag or not args.selectionIIHash:
            print('Error: [0] needs --selectionIITag and --selectionIIHash')
            return 1
        for era in eras:
            name = f'selectionII_{era}_datasets.json'
            if not run_cmd([sys.executable, str(scripts_dir / 'generateDatasetJSON.py'),
                            '--outputDirectory', str(inputs_folder), '--outputFileName', name,
                            '--baseDirectory', os.path.join(storageBase, args.inputStage, args.selectionIITag,
                                                            args.selectionIIHash, era)]):
                return 1
            (output_dir / 'inputs').mkdir(parents=True, exist_ok=True)
            shutil.copy2(inputs_folder / name, output_dir / 'inputs' / name)
            with open(output_dir / 'inputs' / f'selectionII_{era}_source.json', 'w') as f:
                json.dump({'inputStage': args.inputStage, 'inputTag': args.selectionIITag,
                           'inputHash': args.selectionIIHash}, f)

    # [1]
    if args.extractFeatures:
        for era in eras:
            ds_json = output_dir / 'inputs' / f'selectionII_{era}_datasets.json'
            if not ds_json.exists():
                print(f'  Warning: {ds_json} missing, run [0] first. Skipping {era}.')
                continue
            datasets = json.load(open(ds_json))
            tasks, skipped = [], 0
            for DataMC, group, dataset in targets:
                if not matches_filter(args.filter, era, DataMC, group, dataset):
                    continue
                files = datasets.get(DataMC, {}).get(group, {}).get(dataset)
                if not files:
                    print(f'  Warning: {DataMC}/{group}/{dataset} not in {ds_json.name}')
                    continue
                for fp in sorted(files):
                    out = feature_dir(era) / DataMC / group / dataset / Path(fp).name.replace('.root', '.parquet')
                    if out.exists() and not args.force:
                        skipped += 1
                        continue
                    tasks.append({'era': era, 'DataMC': DataMC, 'group': group, 'dataset': dataset,
                                  'file': fp, 'outFile': str(out)})
            task_json = output_dir / era / f'extract_tasks_{era}.json'
            task_json.parent.mkdir(parents=True, exist_ok=True)
            json.dump(tasks, open(task_json, 'w'), indent=1)
            print(f'  {era}: {len(tasks)} files to extract ({skipped} already done)')
            if tasks:
                cmd = [sys.executable, str(scripts_dir / 'extractFeatures.py'), '--taskJSON', str(task_json),
                       '--config', str(config_path), '--workers', str(workers)]
                if args.sample:
                    cmd.append('--sample')
                if not run_cmd(cmd):
                    return 1

    # [2]
    if args.train:
        train_hash = utils.compute_config_hash(train_cfg_path)
        for era in eras:
            out = output_dir / era / 'training' / train_hash
            out.mkdir(parents=True, exist_ok=True)
            shutil.copy2(train_cfg_path, out / 'training_config.yaml')
            cmd = [sys.executable, str(scripts_dir / 'train.py'), '--featureDir', str(feature_dir(era)),
                   '--trainingConfig', str(train_cfg_path), '--outDir', str(out), '--era', era,
                   '--targets', *config['TargetDatasets']]
            if args.sample:
                cmd.append('--sample')
            if not run_cmd(cmd):
                return 1

    # [3]
    if args.trainTorch:
        tcfg_path = base_dir / 'torch_config.yaml'
        tcfg = utils.load_config(tcfg_path)
        torch_hash = utils.compute_config_hash(tcfg_path)
        for era in eras:
            out = output_dir / era / 'training_torch' / torch_hash
            out.mkdir(parents=True, exist_ok=True)
            shutil.copy2(tcfg_path, out / 'torch_config.yaml')
            cmd = [tcfg['TorchPython'], str(scripts_dir / 'train_torch.py'), '--featureDir', str(feature_dir(era)),
                   '--config', str(tcfg_path), '--outDir', str(out), '--era', era,
                   '--targets', *config['TargetDatasets']]
            if args.torchRuns:
                cmd += ['--runs', *args.torchRuns]
            if args.sample:
                cmd.append('--sample')
            if not run_cmd(cmd):
                return 1

    # [4]
    if args.asymStudy:
        acfg_path = base_dir / 'asym_config.yaml'
        asym_hash = utils.compute_config_hash(acfg_path)
        for era in eras:
            out = output_dir / era / 'asym' / asym_hash
            out.mkdir(parents=True, exist_ok=True)
            shutil.copy2(acfg_path, out / 'asym_config.yaml')
            cmd = [sys.executable, str(scripts_dir / 'asym' / 'asym_study.py'), '--featureDir', str(feature_dir(era)),
                   '--config', str(acfg_path), '--outDir', str(out), '--targets', *config['TargetDatasets']]
            if args.qqScores:
                cmd += ['--qqScores', args.qqScores]
            if args.qqScoreColumn:
                cmd += ['--qqScoreColumn', args.qqScoreColumn]
            if args.sample:
                cmd.append('--sample')
            if not run_cmd(cmd):
                return 1

    # [5]
    if args.asymTorch:
        acfg_path = base_dir / 'asym_config.yaml'
        acfg = utils.load_config(acfg_path)
        asym_hash = utils.compute_config_hash(acfg_path)
        for era in eras:
            out = output_dir / era / 'asym' / asym_hash / 'deepsets'
            out.mkdir(parents=True, exist_ok=True)
            cmd = [acfg['Torch']['TorchPython'], str(scripts_dir / 'asym' / 'asym_torch.py'),
                   '--featureDir', str(feature_dir(era)), '--config', str(acfg_path), '--outDir', str(out),
                   '--targets', *config['TargetDatasets']]
            if args.qqScores:
                cmd += ['--qqScores', args.qqScores]
            if args.qqScoreColumn:
                cmd += ['--qqScoreColumn', args.qqScoreColumn]
            if args.sample:
                cmd.append('--sample')
            if not run_cmd(cmd):
                return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
