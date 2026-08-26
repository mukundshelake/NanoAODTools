# 004C-BDTTraining — Parquet Extraction & BDT Training

Takes the `BDTVariables` ROOT files from 004B-BDT, extracts the BDT feature
branches plus the `y` training target into parquet files (one, or more if a
dataset is too bulky, per dataset), and then trains an XGBoost classifier per
era that distinguishes qqbar-initiated (`y==1`) from gg-initiated (`y==2`)
`ttbar_SemiLeptonic` production.

Unlike 002/003-I/003-II/004A/004B, this stage is pure-python (`uproot` /
`awkward` / `pyarrow`): it does not run the NanoAODTools `PostProcessor` and
needs no CMSSW environment, so there is no CRAB submission path and no
`modules/` folder here. Run it in the `latestcoffea` conda env (see
`environment.yml` at the repo root), same as 005-Unfolding's `getParquet.py`,
not inside `cmsenv`.

## Inputs

- `BDTVariables_{tag}_{era}_datasets.json` from 004B-BDT, fetched into
  `inputs/` via `--fetchFromPreviousChapter --previousHash <hash>`.

## What it does

### Extraction (`scripts/extractParquet.py`, run via `run_all.py`)

For each dataset, reads each of its `*_BDTVars.root` files in full via
`uproot`, accumulating chunks across files (rather than resetting a chunk
boundary at every file the way `uproot.iterate(..., step_size=...)` would —
that would fragment a dataset made of many small files into one tiny part
per file), and flushes to a `{dataset}_part{N}.parquet` file once the
accumulated row count crosses `MaxEventsPerParquet` from `config.yaml`.
Columns pulled out:

- The 17 BDT/event-shape branches listed under `BDTVariables` in
  `config.yaml` (`JetHT`, `pTSum`, `FW1`/`FW2`/`FW3`, `AL`, the six
  sphericity-tensor elements, `S`/`P`/`A`, `p2in`/`p2out`).
- `AdditionalFeatures`: `sel_nJet`, `sel_nbjet` — jet and b-tagged-jet
  multiplicity, written far upstream by `003-ObjectSelectionI`'s
  `SelectedObjectsProducer` and carried through untouched (every stage in
  this pipeline runs with `branchsel=None`).
- `TargetBranch` (`y`): the truth-level hard-scattering classification
  filled by `BDTvariableModule` in 004B-BDT (`1`=qqbar, `2`=gg, `3`=qg,
  `4`=qq' diff. flavour, `5`=qq same flavour, `0`=undefined/data).

So a dataset that fits in one accumulated chunk gets a single `_part0.parquet`,
and a large one is split across several parts, without ever holding the
whole dataset in memory at once.

### Training (`scripts/trainBDT.py`, run via `run_all.py --trainBDT`)

Trains **one XGBoost model per era** (not a single model pooled across
eras) on the nominal `MC_mu/SemiLeptonic/ttbar_SemiLeptonic` parquet output
of a *specific, pinned* extraction run (see `--parquetHash` below) — the
`MC_alt` generator-tune systematic variants
(`ttbar_SemiLeptonic_Gluonmove`/`_QCDinspired`/`_TuneCP5up`/`_TuneCP5down`/`_erdON`)
are deliberately excluded from training.

Steps, configured entirely by `training_config.yaml` (see that file for the
full schema): keep only events with `y in {1, 2}` and map `1`(qqbar)`->0`,
`2`(gg)`->1` (everything else — `y in {0,3,4,5}` — is dropped, *not* folded
into a "background" class); balance the two classes (downsampling by
default, or `scale_pos_weight` to keep all events); `train_test_split`;
median/mean imputation (median for the two integer jet-count features);
`GridSearchCV` over an XGBoost hyperparameter grid; built-in (gain) and
permutation feature importance; and, if `FeatureSelection.select_features`
is set, a retrain on just the top-N most important features, with a
full-vs-reduced AUC comparison. This mirrors the structure of an old,
now-deleted ad-hoc training script (recoverable from git history at
`git show f7fd8f5:004B-BDT/scripts/old_ignore/BDT.py`) which used
`sklearn.GradientBoostingClassifier` and — a bug this rewrite fixes —
trained qqbar-vs-*everything-else* rather than strictly qqbar-vs-gg.

## Outputs

- Parquet files: `{STORAGE}/BDTParquet/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/{dataset}_part{N}.parquet`
- `Parquet_{tag}_{era}_datasets.json` (via `--generateDatasetJSON`) — same
  nested `DataMC -> group -> dataset -> {filepath: row_count}` shape as the
  other chapters' dataset JSONs.
- Training artifacts, per era:
  `outputs/{tag}/{parquetHash}/{era}/bdt/{training_hash}/` — model
  (`bdt_model.pkl`, a `joblib`-pickled `{'model', 'imputer', 'features'}`
  dict), `best_params.json`, `scores.csv`, `feature_importance.csv`,
  `permutation_importance.csv`, `feature_importance_comparison.png`,
  `roc_curve.png`, a `training_config.yaml` snapshot, `trainBDT_{era}.log`,
  `run_manifest.json`, and — if feature selection is enabled —
  `reduced_model_params.json`, `bdt_model_reduced.pkl`, `scores_reduced.csv`.
  Nested under the **extraction** run's `{parquetHash}` (not a new top-level
  hash) because a training run is only meaningful relative to a specific
  parquet extraction; `training_hash` (from `training_config.yaml`, hashed
  independently of `config.yaml`) is what actually versions the `bdt/`
  subdirectory, so tuning the grid search never forces parquet re-extraction.

## Running it

```
run_all.py --fetchFromPreviousChapter --previousHash <hash>
run_all.py --generateProcessListJSON
run_all.py --writeBashScript
scripts/run_all_{tag}.sh
run_all.py --generateDatasetJSON
```

`--filter`, `--force`, `--sample`, `--workers` work as in the other
chapters, except `--sample` and `--force`/existing-output checks operate at
dataset granularity here (one task = one dataset's full file list), not per
file: `--sample` runs only the first dataset of each era, using only that
dataset's first ROOT file.

### Training

Once `--generateDatasetJSON` has produced `Parquet_{tag}_{era}_datasets.json`
for a given extraction hash:

```
run_all.py --trainBDT --parquetHash <extraction-hash>
```

`--filter <era>` scopes to specific eras (training's dataset selection
itself comes from `training_config.yaml`'s `TrainingSample`, not `--filter`
— there's only one training sample). `--sample` caps the rows read per era
to a few thousand for a fast mechanical smoke pass; `--force` retrains even
if `best_params.json` already exists for this `training_hash`;
`--trainWriteBashScript` writes `scripts/train_all_{tag}.sh` instead of
running directly. `run_all.py --trainBDT --printHash` prints both the
extraction `config_hash` and the `training_hash` without running anything.

Needs `xgboost` in the `latestcoffea` env (declared in `environment.yml` at
the repo root).

## Storage

`STORAGE` in `config.yaml` is a dict keyed by machine (matched as a
substring of `socket.gethostname()`), resolved in
`utils.resolve_storage_path()`, same as every other chapter.
