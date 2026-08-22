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
| `bTagging` | Per-jet DeepJet b-tag SF (product method) |

(`jetPUID` also exists in `scripts/modules/` and `config.yaml` but isn't in `ModuleList.MC`
currently — not run.)

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

### CRAB alternative to Step 2/3 (lxplus only)

Same pattern as 003-ObjectSelectionI's CRAB support:

```
source scripts/crab/getcrabReady.sh
run_all.py --submitSelectionJobs [--sample]
run_all.py --checkCrabStatus [--resubmitFailedCrabJobs] [--removeSubmitFailedCrabJobs]
run_all.py --generateDatasetJSON
```

`scripts/crab/submit_selectionII_flexible.py` builds `Data.userInputFiles` from
`selectionI_{tag}_{era}_datasets.json` the same way (selectionI output isn't DBS-registered
either), and `scripts/crab/crab_script_selectionII.py` resolves each LFN to
`root://eosuser.cern.ch/...` directly, bypassing `crabhelper.inputFiles()` for the same
reason as 003-I.

The one real difference from 003-I: this stage's modules need real SF files
(`SFs/UL{era}_mu_ID.json`, `_mu_HLT.json`, `_jet_Btagging.json`, plus a per-dataset
b-tagging efficiency ROOT file, `SFs/Efficiency/{era}/{dataset}.root`) — CRAB flattens
`JobType.inputFiles` into the sandbox root, so `crab_script_selectionII.py` recreates the
expected `SFs/...` relative layout by moving the flat-shipped files into place before
instantiating any module. Data jobs (`ModuleList.Data: []`) ship no SF files at all and
just run `PostProcessor` with an empty module list, mirroring the local path exactly.

Another real difference from 003-I: `correctionlib`/`coffea`/`awkward` (needed by
`muonID`/`muonHLT`/`bTagging`) are **not** part of the stock CMSSW python environment —
on lxplus they only import because they happen to be pip-installed under the user's own
AFS home (`~/.local/...`), which a grid worker node has no access to at all. Verified
this directly on a bare CVMFS-only VM: a plain `cmsenv` there cannot import any of the
three. `crab_script_selectionII.py` instead prepends the CVMFS-hosted LCG software
stack's site-packages to `sys.path` (currently `LCG_105/x86_64-el9-gcc12-opt`, matched to
CMSSW_13_3_0's `el9_amd64_gcc12`; bump this if the CMSSW release changes) — deliberately
just `sys.path`, not a full `source .../setup.sh`, since that would risk swapping out
CMSSW's own ROOT build. Also verified on the same VM: ROOT's Cling C++ interpreter needs
`glibc-devel`/`gcc`/`gcc-c++` present on the worker OS to even initialize (segfaults
otherwise) — a minimal grid worker image should have these already, but this bit a bare
cloud VM used for testing.

## Storage

`STORAGE` in `config.yaml` is a dict keyed by machine (matched as a substring of
`socket.gethostname()`), resolved in `utils.resolve_storage_path()`. Add a new machine by
adding a `key: path` entry.
