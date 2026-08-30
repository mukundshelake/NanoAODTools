# 003-ObjectSelectionIII — Data/MC Validation Plots

A validation spur off the main pipeline: builds weighted histograms from the
`selectionII` output and produces CMS-style Data/MC comparison plots. Nothing here feeds
forward into 004A-Reconstruction (that reads `selectionII` directly).

## Inputs

- `inputs/{DataMC}_{group}_{dataset}_{era}_fileset.json` — built by `run_all.py
  --generateSelectionIIDatasetJSON --selectionIITag <tag> --selectionIIHash <hash>`, which
  scans `{STORAGE}/selectionII/{selectionIITag}/{selectionIIHash}/{era}` on disk fresh via
  `scripts/generateDatasetJSON.py` (the same health-checked scan 003-ObjectSelectionII itself
  uses) and wraps each dataset's file list into a coffea fileset (`files` + `metadata`) --
  the same wrapping 003-ObjectSelectionII's own `--prepareFileset` does, done here instead of
  fetching that chapter's already-built copy, so the recorded paths always reflect where the
  files actually are right now rather than a copied-once snapshot that can go stale (mirrors
  003-ObjectSelectionI's `--generatePreselectionDatasetJSON` and 003-ObjectSelectionII's
  `--generateSelectionIDatasetJSON`). One file per era/DataMC/group/dataset (the filename
  already encodes all four, so they sit flat in `inputs/` like every other chapter's inputs
  — no per-dataset subdirectories needed).
- `histDetails` / `weightList` / `plotSettings` in `config.yaml` define which variables to
  histogram, which weight branches to multiply together, and how MC groups are
  labelled/colored/stacked.
- `inputs/SFs/{era}_abcdScaleFactor.root` — the `ABCD_transferFactor_R` TH2 map, fetched
  directly from 003-ObjectSelectionII's own output via `run_all.py --fetchABCDScaleFactor`
  (reuses `--selectionIITag`/`--selectionIIHash`). Only needed for `--regionFilter 1` (see
  "ABCD / data-driven QCD template" below) — not required for the nominal region-A plots.

## What it does

1. `--buildSelectionHists` → `buildSelectionHists.py` (coffea + dask): for each dataset,
   builds the product of `weightList` branches, scoped to one `ABCD_region` code via
   `--regionFilter` (default 0 = A, the nominal signal region), and fills a
   `boost_histogram` per entry in `histDetails`, lazily via `dask_histogram`.
2. `--aggregrateGroupHists` → sums per-dataset histograms into per-group histograms,
   scaled by `Lumi * Xsec / Ngen` (MC) or 1 (Data). Also scoped by `--regionFilter`.
3. `--makeplots` → `rootHists.py` converts the aggregated `boost_histogram`s to ROOT
   `TH1F`s, stacks MC groups per `plotSettings` order/color, and draws a CMS-style
   Data/MC panel with a ratio subplot. `createPlotsPDF.py` then bundles everything plus
   the config into one PDF report.

### ABCD / data-driven QCD template

The nominal plots (`--regionFilter 0`, region A) use plain MC for the `QCD` group like any
other background. `--buildQCDTemplate` replaces that MC-only QCD estimate with a
data-driven one, using the ABCD method (see 003-ObjectSelectionI's `SelectedObjectsProducer`
for the region tagging, and 003-ObjectSelectionII's `computeABCDScaleFactor.py` for how the
transfer factor `R = N_C/N_D` is measured):

1. `--buildSelectionHists --regionFilter 1` scopes histograms to region B (loose isolation,
   high MET) and additionally folds the ABCD transfer factor `R` into each event's weight —
   looked up live from `inputs/SFs/{era}_abcdScaleFactor.root` by that event's own
   `SelMuon_pt`/`|SelMuon_eta|` (`--abcdScaleFactorFile`, passed automatically by
   `run_all.py` for this region). Applied to **both** Data and MC region-B events.
2. `--aggregrateGroupHists --regionFilter 1` aggregates those R-weighted region-B
   histograms per group, same as any other region.
3. `--buildQCDTemplate` computes, per `histDetails` variable, `max(region-B Data −
   Σ non-QCD MC_mu groups, 0)` — both sides already R-weighted from step 1, so this
   background-subtracted region-B shape is already the properly-normalized region-A QCD
   prediction (`N_A_pred = R * N_B`), no separate overall-normalization step needed.
   Floors negative bins at 0 (reported). Writes `{tag}_{era}_QCDTemplate_selectionHists.coffea`.
4. `--makeplots` picks this file up automatically (via `rootHists.py`'s
   `get_qcd_template_path()`) and substitutes it for the `QCD` group's stack entry, falling
   back to plain QCD MC with a warning if the template hasn't been built for that era.

Region B is used as the shape source (not region D) because it shares region A's high-MET
requirement — differing only in isolation — so its shape in every `histDetails` variable is
expected to track region A's shape much more closely than region D's (low-MET) would. `R`
then corrects for the isolation-efficiency mismatch between "loose" (B) and "tight" (A).

## Outputs

Per dataset: `{tag}_{DataMC}_{group}_{dataset}_{era}_region{A,B,C,D}_selectionHists.coffea`
Per group: `{tag}_{era}_{DataMC}_{group}_region{A,B,C,D}_selectionHists.coffea`
Per era: `{tag}_{era}_QCDTemplate_selectionHists.coffea` (via `--buildQCDTemplate`),
`plots/{era}/{histName}.{png,pdf,C}`, `plots/{era}/rootHists.root`, `{tag}_{hash}_report.pdf`

## Running it

```
run_all.py --generateSelectionIIDatasetJSON --selectionIITag <tag> --selectionIIHash <hash>
run_all.py --fetchABCDScaleFactor --selectionIITag <tag> --selectionIIHash <hash>
run_all.py --buildSelectionHists --regionFilter 0
run_all.py --aggregrateGroupHists --regionFilter 0
run_all.py --buildSelectionHists --regionFilter 1
run_all.py --aggregrateGroupHists --regionFilter 1
run_all.py --buildQCDTemplate
run_all.py --makeplots
```

`--filter` works as in the other chapters. `--regionFilter 2`/`3` (C/D) are also available
for `--buildSelectionHists`/`--aggregrateGroupHists` if needed for debugging, but the main
pipeline above only ever needs regions A and B.

## Storage

Same per-machine `STORAGE` dict / `utils.resolve_storage_path()` pattern as the other
chapters. Used directly by `--generateSelectionIIDatasetJSON` to resolve
`{STORAGE}/selectionII/{selectionIITag}/{selectionIIHash}/{era}`; every other step reads/
writes only under `outputs/`.
