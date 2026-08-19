# 002-Samples — Dataset Discovery, Golden JSONs & CRAB Preselection

This chapter is the entry point of the analysis: it discovers datasets on DAS, downloads
golden JSONs and luminosity info, submits the CRAB preselection jobs, and produces
`preselection_{era}_datasets.json`, the input 003-ObjectSelectionI expects.

It spans two environments, driven by the same `scripts/run_all.py` in both:

- **lxplus** — DAS querying, golden JSON download, `brilcalc` luminosity info, and CRAB
  job submission/monitoring. CRAB output ROOT files land on EOS under
  `{LFN_Base}/preselection/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/` —
  deliberately the same layout as `{STORAGE}/preselection/{tag}/{config_hash}/{era}/...`,
  since `STORAGE.lxplus` and `LFN_Base` point at the same physical EOS area. So on lxplus
  itself, no copy step is needed: `--generatePreselectionDatasetJSON` can scan CRAB's
  output in place.
- **wherever the rest of the analysis runs** (e.g. `cms2`) — if that's a *different*
  machine without direct EOS access, copy/stage the CRAB output down to local disk under
  the same `{STORAGE}/preselection/{tag}/{config_hash}/{era}/...` path first, then run
  `--generatePreselectionDatasetJSON` there.

Cloning this repo onto lxplus and running `scripts/run_all.py` there (with CRAB and
`voms-proxy-init` set up) is expected to just work — no lxplus-only scripts live outside
this chapter's `scripts/` folder.

## config.yaml

| Section | Purpose |
|---|---|
| `LFN_Base` | `/store/...` base path CRAB writes preselection output under, on EOS |
| `STORAGE` | Dict mapping a machine-identifying key (matched as a substring of the hostname) to a local-disk root. Resolved by `utils.resolve_storage_path()`. Only used by `--generatePreselectionDatasetJSON` |
| `golden_json_urls` | Golden JSON URL per era |
| `DASQueries` | `{era: {DataMC: {group: {dataset: DAS_query_string}}}}` — the source of truth for which datasets exist |
| `Pre-SelectionCuts` | Era-dependent loose cut strings applied by the CRAB preselection job (`crab_script_preselection.py`) |
| `branch_selection` | `keep`/`drop` lists used to build `keep_and_drop.txt` for the preselection `PostProcessor` |
| `btag_threshold` | Per-era DeepFlavour WP, referenced by `Pre-SelectionCuts` via YAML anchors |

## Workflow

`-t/--tag` defaults to `Dump` (like every other chapter) if omitted. `--printHash` prints
the config hash and exits before any step runs, useful for checking what a run would be
versioned under.

### 1. lxplus — DAS discovery, golden JSONs, lumi info

```bash
python scripts/run_all.py -t earlyApril --getFileList
python scripts/run_all.py -t earlyApril --getDatasetInfo
python scripts/run_all.py -t earlyApril --downloadGoldenJSONs
python scripts/run_all.py -t earlyApril --getLumiInformation      # needs brilcalc env sourced
python scripts/run_all.py -t earlyApril --aggregrateDatasetInfo
python scripts/run_all.py -t earlyApril --getFileInfo             # per-file run-lumi info, slow
python scripts/run_all.py -t earlyApril --generateRunLumiFiles
python scripts/run_all.py -t earlyApril --generateDASDatasetJSON  # DAS_{era}_dataset.json, feeds CRAB
```

All of these are idempotent (skip already-fetched files) and support
`--filter ERA[/DataMC[/group[/dataset]]]` and `--force`.

### 2. lxplus — CRAB submission & monitoring

```bash
source /cvmfs/cms.cern.ch/crab3/crab.sh
voms-proxy-init --voms cms -valid 192:00

python scripts/run_all.py -t earlyApril --submitPreSelectionJobs
python scripts/run_all.py -t earlyApril --checkCrabStatus [--resubmitFailedCrabJobs] [--removeSubmitFailedCrabJobs]
```

`--submitPreSelectionJobs` builds one CRAB config per dataset
(`scripts/crab/submit_preselection_flexible.py`) using `scripts/crab/PSet.py`,
`crab_preselection.sh`, and `crab_script_preselection.py` as the job payload; the worker
node applies `Pre-SelectionCuts` and `branch_selection` from `config.yaml` (sent along as
a CRAB input file) via NanoAODTools' `PostProcessor`. Output lands on EOS under
`{LFN_Base}/preselection/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}`.

`scripts/crab/getcrabReady.sh` is a convenience one-liner for `cmsenv` + CRAB env +
`voms-proxy-init` + a status/resubmit pass — edit the hardcoded CMSSW path at the top for
your own release area before using it.

### 3. Build the dataset JSON

Once CRAB jobs finish:

```bash
python scripts/run_all.py -t earlyApril --generatePreselectionDatasetJSON
```

This runs `scripts/generateDatasetJSON.py` (the same local-disk-scan script used by every
later chapter) against `{STORAGE}/preselection/{tag}/{config_hash}/{era}`, validating each
ROOT file and writing `outputs/{tag}/{hash}/{era}/preselection_{era}_datasets.json`.
`003-ObjectSelectionI --fetchFromPreviousChapter --previousHash <hash>` pulls this straight
from there — no manual copy needed.

If you're running this step on a machine that isn't lxplus and doesn't have EOS mounted
(so `STORAGE` for that machine can't point at the CRAB output directly), copy/xrdcp the
EOS output down to `{STORAGE}/preselection/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/`
on that machine first (this copy step is manual — not automated here).

### 4. Status check

```bash
python scripts/run_all.py -t earlyApril --getStatus
```

Cross-references DAS expectations, `getFileInfo` coverage, the DAS dataset JSON, number
of CRAB jobs submitted, and number of ROOT files actually present on EOS
(`utils.eos_path_from_lfn_base(LFN_Base)`), at the era / DataMC / group / dataset level.

## Directory Structure

```
002-Samples/
├── config.yaml
├── outputs/
│   └── {tag}/
│       ├── latest -> {config_hash}
│       └── {config_hash}/
│           ├── config.yaml
│           └── {era}/
│               ├── {DataMC}/{group}/{dataset}/*_file_list.json, *_dataset_info.json, file_{id}/...
│               ├── {era}_goldenJSON.json
│               ├── {era}_lumi_info.csv
│               ├── {era}_aggregated_dataset_info.json
│               ├── DAS_{era}_dataset.json
│               └── preselection_{era}_datasets.json   # -> 003-ObjectSelectionI/inputs/
└── scripts/
    ├── run_all.py                        # Master orchestration script (lxplus + local steps)
    ├── utils.py                          # Config hashing, output dir mgmt, STORAGE/EOS path resolution
    ├── getFileList.py                    # [lxplus] DAS file list per dataset
    ├── getDatasetInfo.py                 # [lxplus] DAS nevents/nfiles per dataset
    ├── getFileInfo.py                    # [lxplus] DAS per-file run-lumi info
    ├── generateRunLumiFiles.py           # brilcalc-friendly run-lumi JSON per file
    ├── downloadGoldenJsons.py            # Golden JSON download
    ├── getLumiInformation.py             # [lxplus] brilcalc luminosity report
    ├── generateDatasetJSON.py            # Local-disk scan -> preselection_{era}_datasets.json
    └── crab/
        ├── submit_preselection_flexible.py   # [lxplus][CRAB] build + submit CRAB configs
        ├── checkStatus.py                    # [lxplus][CRAB] status / resubmit / cleanup
        ├── crab_preselection.sh              # CRAB scriptExe: runs the worker script, renames output
        ├── crab_script_preselection.py       # CRAB worker payload: PostProcessor + cuts + branch selection
        ├── PSet.py                           # Fake PSet CRAB/local-test require
        └── getcrabReady.sh                   # Convenience: cmsenv + CRAB env + voms-proxy-init
```

## References
* Lumi recommendations: `https://twiki.cern.ch/twiki/bin/viewauth/CMS/LumiRecommendationsRun2`
* Golden JSON files (and parent folders for other eras):
    * 2016: `https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions16/13TeV/Legacy_2016/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt`
    * 2017: `https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt`
    * 2018: `https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions18/13TeV/Legacy_2018/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt`
