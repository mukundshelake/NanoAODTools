# 003-ObjectSelectionI — Object Selection & Flat Branch Production

This chapter takes the pre-selected NanoAOD ROOT files produced by the preselection stage (`002-Samples`) and applies a full physics object selection. It runs an event-level cut string to reject events that cannot form the signal topology, then identifies the selected objects (muon and four jets) and writes their kinematics as flat branches into output skim files.

---

## Expected Inputs

### 1. `inputs/` folder (tracked in-repo)

| File | Description |
|---|---|
| `preselection_{era}_datasets.json` | File-path lists for each era pointing to the preselection-stage ROOT files on disk. Organised as `{DataMC → group → dataset → {filepath: "Events"}}`. These are the direct inputs to the event loop. |
| `{era}_goldenJSON.json` | CMS Golden JSON for that era, used by NanoAOD's `PostProcessor` to certify data runs. Not applied to MC. |

Both files are generated directly by this chapter's own `run_all.py`, not fetched from
002-Samples' output -- there is no cross-chapter dependency here:

```
run_all.py --generatePreselectionDatasetJSON --preselectionTag <002-Samples tag> --preselectionHash <002-Samples config hash>
run_all.py --downloadGoldenJSONs
```

`--generatePreselectionDatasetJSON` scans `{STORAGE}/preselection/{preselectionTag}/{preselectionHash}/{era}`
on disk via `scripts/generateDatasetJSON.py` (the same health-checked scan 002-Samples itself
uses for its own `--generatePreselectionDatasetJSON`) and writes `preselection_{era}_datasets.json`
straight into `inputs/`. Because it's a fresh scan of whatever files are actually reachable right
now, the recorded paths can't go stale the way a copied-once JSON can -- the previous fetch-based
approach (`--fetchFromPreviousChapter`) copied a JSON 002-Samples had generated at some earlier
point, and nothing caught it if those paths later moved or became unreachable from a different
machine. `--downloadGoldenJSONs` downloads `{era}_goldenJSON.json` directly from the CMS URLs in
`config.yaml`'s `golden_json_urls` -- independent of any particular 002-Samples run, since the
golden JSON's content only depends on era. Both steps also copy their output into this run's own
`outputs/{tag}/{hash}/inputs/` snapshot (so the snapshot stays complete even though it's normally
taken once, at the start of the invocation, before these steps run).

### 2. `config.yaml`

The single source of truth for the entire chapter. Key sections:

| Section | Purpose |
|---|---|
| `STORAGE` | Dict mapping a machine-identifying key to the root path on disk for that machine, e.g. `{cms2: "/mnt/disk2/mukund/DataFiles", lxplus: "/eos/user/m/mshelake/DataFiles/"}`. Resolved at runtime by `utils.resolve_storage_path()`, which matches the key as a substring of `socket.gethostname()` (not an exact match) |
| `LFN_Base` | `/store/...` base path CRAB writes selectionI output under, on EOS. Only used by `--submitSelectionJobs` |
| `golden_json_urls` | Golden JSON URL per era, used by `--downloadGoldenJSONs` |
| `SelectionCuts` | Era-dependent event-level cut strings passed directly to `PostProcessor(cut=...)`. Includes muon, jet, b-jet, HLT, and MET flag requirements |
| `ModuleList` | Which analysis modules run on MC vs Data (`selectedObjects` for both) |
| `Modules.selectedObjects` | Per-era kinematic thresholds and output branch name prefixes for the object-selection module |
| `DataLumiInfo` | Integrated luminosity (pb⁻¹) and uncertainty per era, for downstream normalisation |
| `NgenandXsec` | Number of generated events and cross-section (pb) for every MC dataset in every era, also for downstream normalisation |
| `ABCDVariables` | Variable/binning definitions for `--plotABCDVariables` (muon isolation, MET) |

### 3. Preselection ROOT files on disk

Located under `{STORAGE}/{002-Samples config hash}/{era}/...` (no `preselection/{tag}/` prefix,
unlike other chapters' output paths -- see 002-Samples' README for why: these files are
consumed via CRAB's `Data.userInputFiles`, which CRABServer hard-caps at 255 chars per
entry, and every extra prefix character comes out of that budget). These are the
NanoAOD-format ROOT files (tree name `Events`) produced by the previous chapter
(002-Samples), hash-versioned by that chapter's own config hash. Paths are listed
explicitly in `inputs/preselection_{era}_datasets.json`.

---

## What It Does

### Event-level selection

For each era a combined cut string is assembled from `SelectionCuts` in `config.yaml` and passed to NanoAOD's `PostProcessor`. All four conditions must be satisfied simultaneously:

| Cut | UL2016preVFP | UL2017 | UL2018 |
|---|---|---|---|
| **Muon** | exactly 1 tight muon, pT > 26 GeV, \|η\| < 2.4, PFRelIso04 ≤ 0.2 | pT > 29 GeV | pT > 27 GeV |
| **Jets** | ≥ 4 jets with pT > 25 GeV, \|η\| < 2.4, jetId == 6, PU-ID pass | same | same |
| **b-jets** | ≥ 2 DeepFlavour b-tagged jets (WP: 0.2598 / 0.2489 / 0.3040 / 0.2783 per era) | | |
| **HLT** | `HLT_IsoMu24 \|\| HLT_IsoTkMu24` | `HLT_IsoMu27` | `HLT_IsoMu24` |
| **MET flags** | standard CMS 2016–2018 noise filters (`Flag_goodVertices`, halo, HBHE, ECAL, BadPFMuon, eeBadSc) | same | same |

The muon isolation upper bound (0.2) is deliberately looser than the "tight" muon
definition used everywhere else (0.06, matching CMS convention and this module's own
`abcdRegion.isoTightMax` -- see below): 0.2 is the isolation ceiling 002-Samples' own
preselection stage already enforces, so it's the widest bound that admits any events at
all. Loosening the event-level cut only this far (rather than removing the isolation
requirement) lets *anti-isolated* events -- needed as an ABCD sideband -- survive into the
skim, while still excluding pathological/non-prompt outliers beyond what preselection kept.

For **data**, the CMS Golden JSON is also applied via `PostProcessor(jsonInput=...)`.

Events with zero entries after the cut string are detected cheaply before spawning PostProcessor (pre-check using `TTree.GetEntries(cut)`) and skipped to avoid a ROOT segfault.

### Object identification (`SelectedObjectsProducer`)

Implemented in `scripts/modules/SelectedObjects.py`. For each event that passes the cut string, the module:

1. **Muon selection** — iterates over the `Muon` collection, applies per-variable `lohi` (range) and `value` (equality) cuts from `config.yaml`, and picks the highest-pT muon that passes all cuts.

2. **Jet selection** — iterates over the `Jet` collection, applies pT > 25 GeV, \|η\| < 2.4, jetId == 6, and the PU-ID criterion (`pT > 50 OR puId > 0`). Sorts surviving jets by pT descending.

3. **b-jet / light-jet assignment** — from the sorted jet list, greedily picks the two highest-pT b-tagged jets (DeepFlavour score above the era-specific threshold) as the leading and subleading b-jets. The two highest-pT jets not in that pair become the leading and subleading light jets. This avoids misidentification when b-tagged jets happen to be the 5th or 6th by pT.

4. **Branch writing** — writes flat scalar branches for each identified object. If an object is absent (e.g. fewer than 2 b-jets found), its `_pt` branch is set to the sentinel value `−1.0` and all other fields to zero / −1.

5. **ABCD region tagging** — tags each event with its region in the muon-isolation × MET plane, for a CMS-standard QCD-multijet background estimate. This step only *tags*; it never rejects an event. It reuses the exact muon object selected in step 1 rather than re-deriving it or reading it from another module: NanoAODTools builds each `Event` strictly from the *input* tree (see `postprocessing/framework/eventloop.py` -- `Event(inputTree, entry)`), so a separate module further down `ModuleList` could not read a branch this module writes via `self.out.fillBranch()` -- that branch only exists on the output tree buffer. Region definitions (config keys under `abcdRegion`, era-independent values currently: `isoTightMax: 0.06`, `metBranch: "MET_pt"`, `metThreshold: 30.0`):

   | Region | Isolation | MET | Role |
   |---|---|---|---|
   | A | tight (≤ `isoTightMax`) | high (≥ `metThreshold`) | signal region |
   | B | loose | high | |
   | C | tight | low | |
   | D | loose | low | QCD-enriched template region |

   The actual QCD yield estimate (`N_A ≈ N_B·N_C/N_D`, computed from Data with non-QCD MC subtracted from B/C/D) is **not** computed by this chapter -- this stage only produces the per-event tag needed for it.

---

## Outputs

### Skim ROOT files (primary output)

Written to:
```
{STORAGE}/selectionI/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/{originalName}_Skim.root
```

Each skim retains **all** original NanoAOD branches plus the following new flat branches:

| Branch prefix | Object | Fields written |
|---|---|---|
| `SelMuon` | Selected leading muon | `_pt`, `_eta`, `_phi`, `_mass`, `_pfRelIso04_all` (F); `_charge` (I); `_tightId` (O) |
| `leadingbJet` | Highest-pT b-tagged jet | `_pt`, `_eta`, `_phi`, `_mass`, `_btagDeepFlavB` (F); `_jetId`, `_puId` (I); `_hadronFlavour` (I, MC only) |
| `subleadingbJet` | Second b-tagged jet | same fields |
| `leadingJet` | Highest-pT light jet | same fields |
| `subleadingJet` | Second light jet | same fields |
| `ABCD` | Event's ABCD-plane region tag (not an object -- see "ABCD region tagging" above) | `_isTightIso`, `_isHighMET` (O); `_region` (I: 0=A, 1=B, 2=C, 3=D, -1=undefined) |

Sentinel value for a missing object: `*_pt = -1.0`, all other fields = 0 / −1. `ABCD_region = -1`
(undefined) when no muon was selected, since isolation is meaningless without one;
`ABCD_isHighMET` is still always meaningful (MET doesn't depend on the muon).

### Provenance files (under `outputs/`)

`run_all.py` creates a hash-based sub-directory from the SHA-256 of `config.yaml` (first 12 hex chars):

```
outputs/{tag}/
    latest -> {config_hash}                # symlink to the most recent run for this tag
    {config_hash}/
        config.yaml                            # snapshot of config at run time
        inputs/                                # copy of the inputs/ folder
        {era}/
            {tag}_{era}_processListJSON.json   # task list fed to runSelection.py
            {DataMC}/{group}/
                {tag}_{era}_{DataMC}_{group}.log
```

### Post-run dataset JSON (optional)

After the skim files are written, `--generateDatasetJSON` scans the output storage directory and produces `selectionI_{tag}_{era}_datasets.json` listing all healthy skim ROOT files. This is the input format expected by the next chapter.

### Output verification (optional)

`--verifyOutput` runs `scripts/verifyOutput.py` on `selectionI_{tag}_{era}_datasets.json` (from `--generateDatasetJSON`). Unlike 002-Samples' `verifyOutput.py`, which checks every branch against a curated `branch_selection.keep` allowlist, this stage's skims keep *all* original NanoAOD branches untouched -- there's no drop list to check against. Instead this script is scoped to exactly the branches `SelectedObjectsProducer` creates: it confirms every expected `SelMuon_*`/`leading[b]Jet_*`/`subleading[b]Jet_*`/`sel_nJet`/`sel_nbjet`/`ABCD_*` branch is present (era- and Data/MC-aware, via `config.yaml`'s `Modules.selectedObjects.branchNames`), computes min/max/mean/stddev for those branches only, and checks cross-branch invariants that must always hold given the module's deterministic algorithm -- object-selection invariants (e.g. `sel_nbjet <= sel_nJet`, and each `leading/subleading` slot is filled if and only if the object count says it should be) and ABCD-tagging invariants (e.g. `ABCD_region` is only ever one of `{-1,0,1,2,3}`, `ABCD_isTightIso`/`ABCD_isHighMET` match a direct recomputation from `SelMuon_pfRelIso04_all`/`MET_pt` against `abcdRegion.isoTightMax`/`metThreshold`, and `ABCD_region`'s code matches those two flags) -- any violation there is a real bug, not noise. It also reports each object's sentinel (`*_pt == -1`) rate and the per-dataset A/B/C/D region split as diagnostics, since `SelectionCuts` already guarantees enough muons/jets/b-jets before this module runs, so a healthy skim should show ~0% sentinel rate. Writes a JSON report per era.

### ABCD-plane variable distributions (exploratory)

`--plotABCDVariables` runs `scripts/plotABCDVariables.py` on `selectionI_{tag}_{era}_datasets.json` (from `--generateDatasetJSON`), as a first look at candidate ABCD-method discriminating variables (muon isolation, MET) ahead of picking any region-boundary values -- see `config.yaml`'s `ABCDVariables` block for the variable/binning definitions. For each era it overlays normalized (unit-area) shape distributions across `--abcdGroups` (default: `SemiLeptonic` and `QCD` under `--abcdDataMC`, default `MC_mu`). Since 003-ObjectSelectionII's SF weights don't exist yet at this stage (and these skims have no `genWeight` branch), each dataset is combined into its group total using only `sign(LHEWeight_originalXWGTUP) * Lumi*Xsec/Ngen` -- the same sign convention as 003-ObjectSelectionII's `LHEWeightSignProducer`, and the same per-dataset `Lumi*Xsec/Ngen` scalar-weight convention 003-ObjectSelectionIII's `--aggregrateGroupHists` uses when merging a group's datasets (e.g. QCD's several pT-binned samples) into one. This is a shape-comparison tool only, not an ABCD yield estimate. Writes `{var}_{era}.png`/`.pdf` and a `abcdVariables_{era}.root` (raw weighted + normalized histograms) per era to `outputs/{tag}/{hash}/{era}/abcdPlots/`.

Note: with the current `SelectionCuts` (`Muon_pfRelIso04_all <= 0.06`), the muon-isolation shape is only visible up to that cut -- the anti-isolated tail needed for the eventual ABCD sideband regions isn't in these skims yet. The MET shape, by contrast, is unconstrained by any existing cut and is fully visible now.

### ABCD closure test (exploratory)

`--abcdClosureTest` runs `scripts/abcdClosureTest.py` on `selectionI_{tag}_{era}_datasets.json`
(from `--generateDatasetJSON`): the standard sanity check for whether muon isolation and MET
are independent enough, within a target process, for the ABCD method to be valid. Sums the
`ABCD_region` branch `SelectedObjectsProducer` already wrote (no re-derivation from
`SelMuon_pfRelIso04_all`/`MET_pt` needed) across `--groups` under `--dataMC` (default: `QCD`
under `MC_mu` -- the actual ABCD estimate target) and checks `N_A ≈ N_B·N_C/N_D`; a large
deviation means the two variables are correlated within that process and the estimate would
be biased. Datasets within a group are combined with the same `Lumi*Xsec/Ngen`-weighted,
`sign(LHEWeight_originalXWGTUP)`-corrected convention as `--plotABCDVariables`. Writes
`abcdClosureTest_{era}_report.json` to `outputs/{tag}/{hash}/{era}/`.

Note: the actual data-driven QCD transfer factor `R = N_C/N_D` is **not** computed in this
chapter -- it needs the muon ID/HLT and b-tagging scale factors that only exist starting in
003-ObjectSelectionII, so `computeABCDScaleFactor.py` lives there instead (reading
`selectionII` skims, once the SF branches exist) -- see that chapter's README.

---

## Flow of the Chapter

```
run_all.py --generatePreselectionDatasetJSON --preselectionTag <tag> --preselectionHash <hash>
run_all.py --downloadGoldenJSONs                              ← Step 0 (see "Expected Inputs" above)
        │
        ▼
config.yaml
inputs/preselection_{era}_datasets.json
inputs/{era}_goldenJSON.json
        │
        ▼
┌─────────────────────────────────────┐
│  run_all.py --generateProcessListJSON│  ← Step 1
│                                     │
│  Reads preselection dataset JSONs.  │
│  Builds per-era task lists, one     │
│  entry per input ROOT file, each    │
│  carrying: era, DataMC, group,      │
│  dataset, outputDir, cut string,    │
│  golden JSON path, module configs.  │
│                                     │
│  Writes: outputs/{tag}/{hash}/{era}/│
│    {tag}_{era}_processListJSON.json │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  run_all.py --writeBashScript       │  ← Step 2
│                                     │
│  Generates run_all_{tag}.sh with    │
│  one runSelection.py command per    │
│  era/DataMC/group, each piped to a  │
│  log file. The bash script is       │
│  what you actually execute.         │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  run_all_{tag}.sh                   │  ← Step 3 (execute the generated script)
│  → runSelection.py (per group)      │
│                                     │
│  For each task in the process list: │
│  • Pre-check: count events passing  │
│    cut string. Skip if 0.           │
│  • Instantiate SelectedObjects-     │
│    Producer with era config.        │
│  • Run NanoAOD PostProcessor:       │
│    - Apply cut string               │
│    - Apply golden JSON (data only)  │
│    - Run SelectedObjectsProducer    │
│      → fills SelMuon_*, *Jet_*      │
│    - Write *_Skim.root              │
│  Pool of N workers (default 15),    │
│  maxtasksperchild=1 for ROOT safety │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  run_all.py --generateDatasetJSON   │  ← Step 4 (optional, after all skims done)
│                                     │
│  Scans {STORAGE}/selectionI/{tag}/  │
│  Validates each ROOT file           │
│  (zombie check, >0 events).         │
│  Writes selectionI_{tag}_{era}_     │
│    datasets.json  ← input for the   │
│    next chapter.                    │
└─────────────────────────────────────┘
```

### Convenience scripts

| Script | Purpose |
|---|---|
| `run_all_{tag}.sh` | Auto-generated bash script (written to `scripts/`) with explicit `runSelection.py` calls per group; executed in step 3 |
| `notebooks/configBuilder.ipynb` | Interactive helper for building or inspecting `config.yaml` |

### CRAB alternative to Step 2/3 (lxplus only)

Steps 1 and 4 are unchanged. Instead of `--writeBashScript` + running the generated script
locally, the same object selection can be submitted to CRAB — useful since `runSelection.py`
is the slow, time-consuming step:

```
source scripts/crab/getcrabReady.sh          # cmsenv + CRAB env + voms-proxy-init
run_all.py --submitSelectionJobs
run_all.py --checkCrabStatus [--resubmitFailedCrabJobs] [--removeSubmitFailedCrabJobs]
run_all.py --generateDatasetJSON             # same step 4, unchanged
```

Unlike 002-Samples' preselection CRAB job, the input here (preselection skims already on
EOS) is not a DBS-registered dataset, so `scripts/crab/submit_selection_flexible.py` uses
`Data.userInputFiles` — an LFN list built directly from `preselection_{era}_datasets.json`
(the same file `--generateProcessListJSON` reads locally) via `utils.lfn_path_for_local_file()`.
Since there's no DBS lumi mask in that mode, golden-JSON filtering is applied the same way
the local path does it: `jsonInput=<shipped golden JSON file>`, passed directly to
`PostProcessor` inside `scripts/crab/crab_script_selection.py` (which runs the same
`SelectedObjectsProducer`, shipped as a CRAB input file since it isn't part of the
installed NanoAODTools package).

CRAB output is written to `{LFN_Base}/selectionI/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}`
— deliberately the same layout as the local `{STORAGE}/selectionI/...` path (`LFN_Base` in
`config.yaml` and `STORAGE.lxplus` point at the same physical EOS area). This means
`--generateDatasetJSON` can scan CRAB's output in place on lxplus, no manual copy step
needed, unlike the 002→003-I preselection handoff.

This is purely additive: it doesn't change `runSelection.py`, the local Pool-based path, or
anything else described above.

### Idempotency

The pipeline is idempotent. `runSelection.py` checks whether `{basename}_Skim.root` already exists before dispatching a task. Re-running the script resumes from where it left off. Use `--force` to override this and reprocess all files.

### Filtering

Both `run_all.py` and `runSelection.py` accept `--filter ERA[/DataMC[/group[/dataset]]]` with `*` as a wildcard at any level, allowing partial re-runs (e.g. `--filter UL2018/MC_mu/SemiLeptonic`).

---

## Directory Structure

```
003-ObjectSelectionI/
├── config.yaml                        # Central config (selection cuts, modules, lumi/xsec)
├── inputs/
│   ├── preselection_{era}_datasets.json   # Input file lists (from preselection stage)
│   └── {era}_goldenJSON.json              # CMS Golden JSON per era
├── outputs/
│   └── {tag}/
│       ├── latest -> {config_hash}    # symlink to the most recent run
│       └── {config_hash}/             # Hash-versioned run directory
│           ├── config.yaml
│           ├── inputs/
│           └── {era}/
│               ├── {tag}_{era}_processListJSON.json
│               └── {DataMC}/{group}/{tag}_{era}_{DataMC}_{group}.log
├── scripts/
│   ├── run_all.py                     # Master orchestration script
│   ├── run_all_{tag}.sh               # Auto-generated execution script (per tag)
│   ├── runSelection.py                # Worker: runs PostProcessor + modules in parallel
│   ├── generateDatasetJSON.py         # Scan a ROOT-file dir, build a dataset JSON -- used for
│   │                                  #   both the pre-run preselection scan and the post-run
│   │                                  #   selectionI skim scan
│   ├── downloadGoldenJsons.py         # Pre-run: download {era}_goldenJSON.json from CMS URLs
│   ├── verifyOutput.py                # Post-run: verify selectionI skim branches/invariants
│   ├── plotABCDVariables.py           # Exploratory: ABCD-plane variable shape distributions
│   ├── abcdClosureTest.py             # Exploratory: N_A ~= N_B*N_C/N_D closure test
│   ├── utils.py                       # Config hashing, output directory management
│   ├── crab/                          # CRAB alternative to local runSelection.py execution
│   └── modules/
│       └── SelectedObjects.py         # NanoAOD module: object ID + flat branch writing
└── notebooks/
    └── configBuilder.ipynb            # Config inspection / generation helper
```
