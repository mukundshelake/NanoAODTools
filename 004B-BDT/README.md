# 004B-BDT — Event-Shape / BDT Input Variables

Takes the `reconstruction` skims and computes 17 event-shape and Fox-Wolfram-moment
variables per event, for later use as BDT training/inference input. This chapter only
computes the variable branches — it does not train a classifier.

## Inputs

- `reconstruction_{tag}_{era}_datasets.json` from 004A-Reconstruction, fetched into
  `inputs/` via `--fetchFromPreviousChapter --previousHash <hash>`.

## What it does

`scripts/modules/BDTvariableModule.py` (`BDTvariableProducer`), run via
`runBDTVariables.py`, no config needed. Per event, from the `Jet` collection and `MET`:

- `JetHT`, `pTSum`
- Fox-Wolfram moments `FW1`/`FW2`/`FW3`, longitudinal alignment `AL`
- Sphericity tensor elements `Sxx`/`Syy`/`Sxy`/`Sxz`/`Syz`/`Szz`
- Derived shape variables: sphericity `S`, planarity `P`, alignment `A`, `p2in`, `p2out`

It also fills the truth-level hard-scattering classification from `GenPart` (MC only,
absorbed from the old standalone `yCalculator` module — same event loop, one less
full-tree pass):

- `y`: `1`=qqbar, `2`=gg, `3`=qg, `4`=qq' (different flavour), `5`=qq (same flavour),
  `0`=undefined/data. This is the training label the BDT is meant to be trained against.
- `qDir`: `+1`/`-1` = incoming quark direction for the qqbar case, else `0`.

## Outputs

- ROOT files: `{STORAGE}/BDTVariables/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/*_BDTVars.root`
- `BDTVariables_{tag}_{era}_datasets.json` (via `--generateDatasetJSON`) — input for
  005-Unfolding / 006-Results / 007-Systematics.
- `--buildBDTVariableHists` / `--aggregateBDTVariableHists` / `--makeBDTVariablePlots` —
  same coffea-histogram-then-ROOT-plot pattern as 003-ObjectSelectionIII, applied to the
  17 BDT variables instead of selection kinematics.

## Running it

```
run_all.py --fetchFromPreviousChapter --previousHash <hash>
run_all.py --generateProcessListJSON
run_all.py --writeBashScript
scripts/run_all_{tag}.sh
run_all.py --generateDatasetJSON
```

`--filter`, `--force`, `--sample`, `--workers` work as in the other chapters.

### CRAB alternative to Step 2/3 (lxplus only)

Same pattern as 003-ObjectSelectionI/II and 004A-Reconstruction's CRAB support:

```
source scripts/crab/getcrabReady.sh
run_all.py --submitBDTJobs [--sample]
run_all.py --checkCrabStatus [--resubmitFailedCrabJobs] [--removeSubmitFailedCrabJobs]
run_all.py --generateDatasetJSON
```

`scripts/crab/submit_bdt_flexible.py` builds `Data.userInputFiles` from
`reconstruction_{tag}_{era}_datasets.json` the same way (reconstruction output isn't
DBS-registered either), and `scripts/crab/crab_script_bdt.py` resolves each LFN to
`root://eosuser.cern.ch/...` directly, bypassing `crabhelper.inputFiles()` for the same
reason as 003-I. No golden JSON is shipped or needed here (neither a cut string nor
golden-JSON filtering is re-applied), and unlike 004A-Reconstruction's worker script, no
LCG `sys.path` fix is needed either -- `BDTvariableModule` only imports `numpy`, which is
part of the stock CMSSW python environment.

## Storage

`STORAGE` in `config.yaml` is a dict keyed by machine (matched as a substring of
`socket.gethostname()`), resolved in `utils.resolve_storage_path()`.
