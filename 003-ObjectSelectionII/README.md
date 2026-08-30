# 003-ObjectSelectionII — Scale-Factor Weights

Takes the `selectionI` skims and adds per-event MC scale-factor branches (muon ID/HLT,
b-tagging). No new kinematic cuts are applied — object selection already happened in
003-ObjectSelectionI.

This chapter is also where the data-driven QCD ABCD transfer factor `R = N_C/N_D` gets
computed (`--computeABCDScaleFactor`, after the SF-weight skims exist) — see "ABCD scale
factor" below. It's computed here rather than in 003-ObjectSelectionI because the
background subtraction underneath it needs the muon ID/HLT and b-tagging SFs this chapter
writes; it's computed as an offline step reading this chapter's own already-produced
output, not as a per-event module, so 003-ObjectSelectionIII looks `R` up live when it
needs it rather than this chapter baking it into a skim branch.

## Inputs

- `inputs/selectionI_{era}_datasets.json` — built by `run_all.py --generateSelectionIDatasetJSON
  --selectionITag <tag> --selectionIHash <hash>`, which scans
  `{STORAGE}/selectionI/{selectionITag}/{selectionIHash}/{era}` on disk fresh via
  `scripts/generateDatasetJSON.py` (the same health-checked scan 003-ObjectSelectionI itself
  uses), so the recorded paths always reflect where the files actually are right now rather
  than a copied-once snapshot that can go stale (mirrors 003-ObjectSelectionI's own
  `--generatePreselectionDatasetJSON`).
- `inputs/{era}_goldenJSON.json` (unused here — no cuts — but kept for parity/pass-through to
  later chapters) — downloaded directly from the CMS URLs in `config.yaml`'s `golden_json_urls`
  via `run_all.py --downloadGoldenJSONs`, independent of any particular 003-ObjectSelectionI run.
- `inputs/SFs/UL{era}_mu_ID.json` / `_mu_HLT.json` / `_jet_jmar.json.gz` / `_jet_Btagging.json`
  — correctionlib JSON files for muon ID/HLT, jet PU ID, and b-tagging, obtained from the
  relevant CMS POGs (MUO/JME/BTV) via `run_all.py --fetchSFFiles` — see "Fetching the
  correctionlib SF files" below. Like the selectionI dataset JSON above, these are a
  chapter-local input: re-synced into `outputs/{tag}/{hash}/inputs/` on every `run_all.py`
  invocation, so the worker script's relative `inputs/SFs/...` paths resolve after it
  `chdir`s into the run folder.
- `inputs/SFs/Efficiency/{era}/*.root` and `inputs/SFs/JetPUID/Efficiency/{era}/*.root` — PU-ID/
  b-tag efficiency ROOT files, generated in-repo from the selectionI skims (see "Regenerating
  the efficiency maps" below) rather than fetched from outside. Chapter-local, same as every
  other entry under `inputs/SFs/` above: copied into this run's own
  `outputs/{tag}/{hash}/inputs/SFs/...` snapshot right after being computed.

## What it does

Runs NanoAOD `PostProcessor` (no `cut=`) with these modules from `scripts/modules/`:

| Module | Adds | MC | Data |
|---|---|---|---|
| `lheWeightSign` | Sign of `LHEWeight_originalXWGTUP` | ✓ | |
| `muonID` | Tight muon ID SF (correctionlib) on `SelMuon` | ✓ | |
| `muonHLT` | HLT/isolation SF (correctionlib) on `SelMuon` | ✓ | |
| `bTagging` | Per-jet DeepJet b-tag SF (product method) | ✓ | |

(`jetPUID` also exists in `scripts/modules/` and `config.yaml` but isn't in `ModuleList.MC`
currently — not run.)

`ModuleList.Data` is empty — Data jobs still run through `PostProcessor` (for parity with
the local path and to produce a `selectionII` skim at all), just with no modules attached;
every module here is MC-only.

## ABCD scale factor

After the SF-weight skims above exist, `--computeABCDScaleFactor` runs
`scripts/computeABCDScaleFactor.py` on `selectionII_{tag}_{era}_datasets.json` (from
`--generateDatasetJSON`): computes the data-driven QCD transfer factor, binned in
`(SelMuon_pt, |SelMuon_eta|)`. Per bin: `N_qcd_X = max(N_data_X - N_bkg_X, 0)` for each
region `X ∈ {B, C, D}` (raw Data count under `--dataDataMC`/`--dataGroup`, default
`Data_mu`/`SingleMuon`, minus the full-weight sum of every `--mcDataMC` group except
`--qcdGroup`, default `MC_mu` excluding `QCD`), floored at 0 (floors are reported). The
non-QCD MC background sum uses the **same full per-event weight** as the rest of the
analysis — `Lumi*Xsec/Ngen` times every branch this chapter's own SF modules just wrote
(`muonIDWeight`, `muonHLTWeight`, `bTagWeight`, `L1PreFiringWeight_Nom`, `lheWeightSign`) —
unlike the version of this script that used to live in 003-ObjectSelectionI, which could
only use `Lumi*Xsec/Ngen*sign(LHEWeight)` since none of those SF branches existed yet at
that stage. Writes the transfer factor `R = N_qcd_C / N_qcd_D` as an `ABCD_transferFactor_R`
TH2 to `abcdScaleFactor_{era}.root`, plus `abcdScaleFactor_{era}_report.json`, both under
`outputs/{tag}/{hash}/{era}/`.

There is no per-event branch or module that writes `R` onto a skim in this chapter — doing
so would need `R` as an input to the very PostProcessor pass that produces the SF branches
`R`'s own background subtraction depends on. Instead, 003-ObjectSelectionIII fetches
`abcdScaleFactor_{era}.root` directly from this chapter's output and looks `R` up live, per
event, only when building region-B histograms (see that chapter's README).

## Outputs

- Skim ROOT files: `{STORAGE}/selectionII/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/*_Skim.root`
- `selectionII_{tag}_{era}_datasets.json` (via `--generateDatasetJSON`) — input for 003-ObjectSelectionIII.
- `abcdScaleFactor_{era}.root` / `abcdScaleFactor_{era}_report.json` (via `--computeABCDScaleFactor`)
  — the ABCD transfer factor `R`, fetched directly by 003-ObjectSelectionIII.
- Coffea filesets (via `--prepareFileset`) — also input for 003-ObjectSelectionIII's histogramming step.

## Running it

```
run_all.py --generateSelectionIDatasetJSON --selectionITag <tag> --selectionIHash <hash>
run_all.py --downloadGoldenJSONs
run_all.py --fetchSFFiles
run_all.py --generateProcessListJSON
run_all.py --writeBashScript --runBashScript
run_all.py --generateDatasetJSON
run_all.py --computeABCDScaleFactor
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
change) — run right after `--generateSelectionIDatasetJSON`:

```
run_all.py --generateSelectionIDatasetJSON --selectionITag <tag> --selectionIHash <hash>
run_all.py --prepareEfficiencyFileset
run_all.py --computeBTaggingEfficiency
run_all.py --computeJetPUIDEfficiency
```

`--prepareEfficiencyFileset` builds a per-era, MC-only coffea fileset from the selectionI
dataset JSON (not this chapter's own output — the maps are an input the weight modules
need, so they can't depend on selectionII having already run). The two `--compute*`
flags then run `scripts/computeBTaggingEfficiency.py` / `scripts/computeJetPUIDEfficiency.py`
against that fileset, writing ROOT files into `inputs/SFs/Efficiency/` and
`inputs/SFs/JetPUID/Efficiency/` respectively, and copying the freshly computed per-era
files into this run's own `outputs/{tag}/{hash}/inputs/SFs/...` snapshot right away (same
dual-write as every other fetch/compute step in this chapter).

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
`selectionI_{era}_datasets.json` the same way (selectionI output isn't DBS-registered
either), and `scripts/crab/crab_script_selectionII.py` resolves each LFN to
`root://eosuser.cern.ch/...` directly, bypassing `crabhelper.inputFiles()` for the same
reason as 003-I.

The one real difference from 003-I: this stage's modules need real SF files
(`inputs/SFs/UL{era}_mu_ID.json`, `_mu_HLT.json`, `_jet_Btagging.json` — run
`--fetchSFFiles` on lxplus before submitting, since `submit_selectionII_flexible.py`
reads them straight off disk at submission time to build `JobType.inputFiles` — plus a
per-dataset b-tagging efficiency ROOT file, `inputs/SFs/Efficiency/{era}/{dataset}.root`,
from `--computeBTaggingEfficiency`) — CRAB flattens `JobType.inputFiles` into the sandbox
root, so `crab_script_selectionII.py` recreates the expected `inputs/SFs/...` relative
layout by moving the flat-shipped files into place before instantiating any module. Both
branches build `module_names` from `config["ModuleList"]["Data" if is_data else "MC"]`, so
Data jobs still run through `PostProcessor` even though `ModuleList.Data` is currently
empty.

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
