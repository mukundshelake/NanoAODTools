# 003-ObjectSelectionII — Scale-Factor Weights

Takes the `selectionI` skims and adds per-event MC scale-factor branches. No new
kinematic cuts are applied — object selection already happened in 003-ObjectSelectionI.

## Inputs

- `inputs/selectionI_earlyApril_{era}_datasets.json` — file lists from 003-ObjectSelectionI.
- `inputs/{era}_goldenJSON.json` — unused here (no cuts), kept for parity with other chapters.
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

Same four-step pattern as 003-ObjectSelectionI:
`--generateProcessListJSON` → `--writeBashScript` → run the generated
`scripts/run_all_{tag}.sh` → `--generateDatasetJSON` (+ `--prepareFileset`).

`--filter ERA[/DataMC[/group[/dataset]]]` and `--force` work the same way as in 003-I.

## Storage

`STORAGE` in `config.yaml` is a dict keyed by machine (matched as a substring of
`socket.gethostname()`), resolved in `utils.resolve_storage_path()`. Add a new machine by
adding a `key: path` entry.
