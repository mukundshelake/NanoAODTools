# 003-ObjectSelectionIII — Data/MC Validation Plots

A validation spur off the main pipeline: builds weighted histograms from the
`selectionII` output and produces CMS-style Data/MC comparison plots. Nothing here feeds
forward into 004A-Reconstruction (that reads `selectionII` directly).

## Inputs

- `{tag}_{DataMC}_{group}_{dataset}_{era}_fileset.json` (coffea filesets from
  003-ObjectSelectionII's `--prepareFileset` step) — the only thing this chapter actually
  consumes from 003-ObjectSelectionII. Fetched into `inputs/` via
  `--fetchFromPreviousChapter --previousHash <hash>`, one file per era/DataMC/group/dataset
  (the filename already encodes all four, so they sit flat in `inputs/` like every other
  chapter's fetched files — no per-dataset subdirectories needed).
- `histDetails` / `weightList` / `plotSettings` in `config.yaml` define which variables to
  histogram, which weight branches to multiply together, and how MC groups are
  labelled/colored/stacked.

## What it does

1. `--buildSelectionHists` → `buildSelectionHists.py` (coffea + dask): for each dataset,
   builds the product of `weightList` branches and fills a `boost_histogram` per entry in
   `histDetails`, lazily via `dask_histogram`.
2. `--aggregrateGroupHists` → sums per-dataset histograms into per-group histograms,
   scaled by `Lumi * Xsec / Ngen` (MC) or 1 (Data).
3. `--makeplots` → `rootHists.py` converts the aggregated `boost_histogram`s to ROOT
   `TH1F`s, stacks MC groups per `plotSettings` order/color, and draws a CMS-style
   Data/MC panel with a ratio subplot. `createPlotsPDF.py` then bundles everything plus
   the config into one PDF report.

## Outputs

Per dataset: `{tag}_{DataMC}_{group}_{dataset}_{era}_selectionHists.coffea`
Per group: `{tag}_{era}_{DataMC}_{group}_selectionHists.coffea`
Per era: `plots/{era}/{histName}.{png,pdf,C}`, `plots/{era}/rootHists.root`,
`{tag}_{hash}_report.pdf`

## Running it

```
run_all.py --fetchFromPreviousChapter --previousHash <hash>
run_all.py --buildSelectionHists
run_all.py --aggregrateGroupHists
run_all.py --makeplots
```

`--filter` works as in the other chapters.

## Storage

Same per-machine `STORAGE` dict / `utils.resolve_storage_path()` pattern as the other
chapters, though this chapter reads/writes only under `outputs/`, not `STORAGE` directly.
