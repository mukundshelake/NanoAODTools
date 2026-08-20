# 003-ObjectSelectionII — Scale-Factor Weights

Takes the `selectionI` skims and adds per-event MC scale-factor branches. No new
kinematic cuts are applied — object selection already happened in 003-ObjectSelectionI.

## Inputs

- `selectionI_{tag}_{era}_datasets.json` and `{era}_goldenJSON.json` (unused here — no
  cuts — but kept for parity/pass-through to later chapters), fetched from
  003-ObjectSelectionI into `inputs/` via `--fetchFromPreviousChapter --previousHash <hash>`.
- `inputs/SFs/UL{era}_mu_ID.json` / `_mu_HLT.json` / `_jet_jmar.json.gz` / `_jet_Btagging.json`
  — correctionlib JSON files for muon ID/HLT, jet PU ID, and b-tagging, obtained from the
  relevant CMS POGs (MUO/JME/BTV) via `run_all.py --fetchSFFiles` — see "Fetching the
  correctionlib SF files" below. Like the selectionI dataset JSON above, these are a
  chapter-local input: re-synced into `outputs/{tag}/{hash}/inputs/` on every `run_all.py`
  invocation, so the worker script's relative `inputs/SFs/...` paths resolve after it
  `chdir`s into the run folder.
- `SFs/` (repo root, one level up) — PU-ID/b-tag efficiency ROOT files, generated in-repo
  from the selectionI skims (see "Regenerating the efficiency maps" below) rather than
  fetched from outside. Unlike `inputs/SFs/`, this is shared across config hashes/tags —
  re-synced into `outputs/{tag}/{hash}/SFs/` by `utils.create_output_directory` on every
  invocation, so a freshly (re)computed efficiency map is always picked up without needing
  a new config hash.

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
run_all.py --fetchSFFiles
run_all.py --generateProcessListJSON
run_all.py --writeBashScript --runBashScript
run_all.py --generateDatasetJSON
run_all.py --prepareFileset
```

`--writeBashScript` and `--runBashScript` can be combined in one invocation (write then
run, as above) or split across two (e.g. to inspect `scripts/run_all_{tag}.sh` before
running it, or to hand it off to a different environment).

`--filter ERA[/DataMC[/group[/dataset]]]` and `--force` work the same way as in 003-I.

### Fetching the correctionlib SF files

`run_all.py --fetchSFFiles` pulls the muon ID/HLT, jet PU ID, and b-tagging correctionlib
files from `SFSource` (config.yaml, hostname-resolved exactly like `STORAGE` — see Storage
below) into `inputs/SFs/`. Currently only `lxplus` is configured (CVMFS's
`jsonpog-integration` mount); add another machine's entry once you've confirmed where it
can reach that tree (or any other source) from.

On any machine *not* in `SFSource` (no local CVMFS mount), it falls back to
`SFSourceSSHRelay` — a plain SSH host/alias (default: `lxplus.cern.ch`) — and relays every
file through `ssh <relay> cat <path>` instead of local filesystem I/O. This runs fully
interactively: stdin/stderr stay attached to your terminal, so a password/2FA prompt on
first connection works normally — only the file's own bytes (stdout) are captured. All the
fetches in one `--fetchSFFiles` run share a single multiplexed SSH connection (`ssh -O
ControlMaster=auto/ControlPersist`, keyed by `~/.ssh/cm-sf-<user>@<host>:<port>`), so
you're prompted once per session, not once per file.

Idempotent: a file already present in `inputs/SFs/` is left alone, so re-running
`--fetchSFFiles` costs nothing once fetched. Pass `--force` to refetch everything
regardless (e.g. after a POG updates a file upstream).

`muon_Z.json.gz` gets fetched once and written to *both* `UL{era}_mu_ID.json` and
`UL{era}_mu_HLT.json` — it's the same upstream file containing every muon correction, just
duplicated under the two names this repo's config already expects. `jmar.json.gz` is kept
gzipped (matching what's already in the repo); `btagging.json.gz` is decompressed, same as
`muon_Z.json.gz`.

### Regenerating the efficiency maps

`bTagging` and `jetPUID` both need a per-(pT, |eta|) MC efficiency map (see Inputs above)
computed from the selectionI skims themselves, before their weight modules can run.
Needed once per config hash the first time (or whenever the underlying MC samples
change) — run right after `--fetchFromPreviousChapter`:

```
run_all.py --fetchFromPreviousChapter --previousHash <hash>
run_all.py --prepareEfficiencyFileset
run_all.py --computeBTaggingEfficiency
run_all.py --computeJetPUIDEfficiency
```

`--prepareEfficiencyFileset` builds a per-era, MC-only coffea fileset from the selectionI
dataset JSON (not this chapter's own output — the maps are an input the weight modules
need, so they can't depend on selectionII having already run). The two `--compute*`
flags then run `scripts/computeBTaggingEfficiency.py` / `scripts/computeJetPUIDEfficiency.py`
against that fileset, writing ROOT files into the shared, repo-root `SFs/Efficiency/` and
`SFs/JetPUID/Efficiency/` respectively — picked up by every subsequent run regardless of
tag/hash (see Inputs above: the `SFs/` snapshot is re-synced into `outputs/{tag}/{hash}/SFs/`
on every `run_all.py` invocation, not just the first).

`jetPUID` is not currently in `ModuleList.MC` (see What it does) — computing its
efficiency map doesn't re-enable it by itself; that's a separate, deliberate edit to
`config.yaml`.

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
(`inputs/SFs/UL{era}_mu_ID.json`, `_mu_HLT.json`, `_jet_Btagging.json` — run
`--fetchSFFiles` on lxplus before submitting, since `submit_selectionII_flexible.py`
reads them straight off disk at submission time to build `JobType.inputFiles` — plus a
per-dataset b-tagging efficiency ROOT file, `SFs/Efficiency/{era}/{dataset}.root`) — CRAB
flattens `JobType.inputFiles` into the sandbox root, so `crab_script_selectionII.py`
recreates the expected `inputs/SFs/...` and `SFs/Efficiency/...` relative layouts by
moving the flat-shipped files into place before instantiating any module. Data jobs
(`ModuleList.Data: []`) ship no SF files at all and just run `PostProcessor` with an empty
module list, mirroring the local path exactly.

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

`SFSource` works the same way (`utils.resolve_sf_source_path()`), for `--fetchSFFiles` —
see "Fetching the correctionlib SF files" above.
