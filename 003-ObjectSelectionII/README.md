# 003-ObjectSelectionII — Scale-Factor Weights

Takes the `selectionI` skims and adds per-event MC scale-factor branches. No new
kinematic cuts are applied — object selection already happened in 003-ObjectSelectionI.

## Inputs

- `selectionI_{tag}_{era}_datasets.json` and `{era}_goldenJSON.json` (unused here — no
  cuts — but kept for parity/pass-through to later chapters), fetched from
  003-ObjectSelectionI into `inputs/` via `--fetchFromPreviousChapter --previousHash <hash>`.
- `SFs/` (repo root, one level up) — correctionlib JSON files for muon ID/HLT, jet PU ID,
  and b-tagging, plus PU-ID/b-tag efficiency ROOT files. Snapshotted into
  `outputs/{tag}/{hash}/SFs/` by `utils.create_output_directory` so the worker script's
  relative `SFs/...` paths resolve after it `chdir`s into the run folder.

## What it does

Runs NanoAOD `PostProcessor` (no `cut=`) with, for MC only, these modules from
`scripts/modules/`:

| Module | Adds |
|---|---|
| `lheWeightSign` | Sign of `LHEWeight_originalXWGTUP` |
| `muonID` | Tight muon ID SF (correctionlib) on `SelMuon` |
| `muonHLT` | HLT/isolation SF (correctionlib) on `SelMuon` |
| `jetPUID` | Per-jet PU-ID SF for 12.5 < pT ≤ 50 GeV jets |
| `bTagging` | Per-jet DeepJet b-tag SF (product method) |

Data gets no modules (`ModuleList.Data: []`).

## Outputs

- Skim ROOT files: `{STORAGE}/selectionII/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/*_Skim.root`
- `selectionII_{tag}_{era}_datasets.json` (via `--generateDatasetJSON`) — input for 003-ObjectSelectionIII.
- Coffea filesets (via `--prepareFileset`) — also input for 003-ObjectSelectionIII's histogramming step.

## Running it

```
run_all.py --fetchFromPreviousChapter --previousHash <hash>
run_all.py --generateProcessListJSON
run_all.py --writeBashScript
scripts/run_all_{tag}.sh
run_all.py --generateDatasetJSON
run_all.py --prepareFileset
```

`--filter ERA[/DataMC[/group[/dataset]]]` and `--force` work the same way as in 003-I.

## Storage

`STORAGE` in `config.yaml` is a dict keyed by machine (matched as a substring of
`socket.gethostname()`), resolved in `utils.resolve_storage_path()`. Add a new machine by
adding a `key: path` entry.
