# HTCondor preselection submission (parallel to CRAB)

An alternative to the CRAB path (`scripts/crab/`) for the preselection stage,
using CERN's `lxbatch/eossubmit` HTCondor schedds instead of grid submission.
Both paths read the same `DAS_{era}_dataset.json` (from
`--generateDASDatasetJSON`) and both are driven from `run_all.py` — pick
whichever fits a given dataset/era better; nothing requires committing to one
path for an entire campaign.

## Why a second path at all

CRAB is grid-wide (many sites) and heavyweight per dataset (one task object,
`crab status`/`resubmit` semantics, myproxy delegation, 255-char LFN limits).
Condor via `lxbatch/eossubmit` runs only at CERN but is much lighter per job
and needs no grid proxy delegation dance — useful when CRAB's per-site
scheduling or task overhead isn't buying anything (e.g. everything's already
readable via xrootd from CERN) or when CRAB-side issues (site failures,
myproxy) are blocking a specific dataset.

## Key difference from CRAB: output layout

CRAB output lands in its own auto-nested tree and needs
`--generateCrabDatasetJSON` + `--consolidateCrabOutput` to reach the flat
layout `{STORAGE}/preselection/{tag}/{config_hash}/{era}/{DataMC}/{group}/
{dataset}/{filename}` that `--generatePreselectionDatasetJSON` expects.
Condor jobs write **directly** into that flat layout — there's no condor
equivalent of those two steps. Just run `--submitCondorJobs`, wait for
`--checkCondorStatus` to show jobs done, then `--generatePreselectionDatasetJSON`.

## The eossubmit constraints (confirmed via live test jobs, Sep 2026)

- Standard (non-eossubmit) schedds reject `/eos` paths outright. Load the
  module before submitting: `module load lxbatch/eossubmit`.
- Everything the job touches must be reachable on EOS. This implementation
  sidesteps condor's file-transfer mechanism entirely (no
  `transfer_input_files` for per-job data) — the wrapper script and worker
  script just read/write absolute `/eos/...` paths directly at runtime, since
  EOS is a normal mounted filesystem on lxbatch worker nodes (confirmed: EOS
  read/write, CVMFS, and AFS-read are all reachable from a worker node
  launched via eossubmit).
- Local `/tmp` on an lxplus **login node** is not visible to the condor
  schedd (it runs on a separate node, e.g. `bigbird103.cern.ch`) — never put
  submission files there.
- `log = ...` may only vary by `$(Cluster)` across a cluster, not per-job.
  `output =`/`error =` **can** vary per-job (confirmed live with a
  queue-from-file `$(job_id)` variable across 3 jobs — each got its own
  distinct `.out`/`.err` with no collisions).
- The same login-node-vs-access-point split bites `x509userproxy` too, not
  just job files: pointing it at the default proxy location
  (`/tmp/x509up_u<uid>`) makes every job hold immediately with `Transfer
  input files failure ... reading from file /tmp/x509up_u<uid>: No such
  file or directory` (confirmed live) — the eossubmit schedd's access point
  is a separate node (e.g. `bigbird103`) with no view of the login node's
  `/tmp`. `submit_preselection_condor.py` copies the proxy onto EOS under
  `--work-area` before submitting and points `x509userproxy` there instead.

## Grid proxy (xrootd auth)

Input files are read via the xrootd global redirector
(`root://cms-xrd-global.cern.ch/`), which requires grid auth against the
site that actually holds each file. Without a proxy, `TFile::Open` loops
through redirects and fails with `[FATAL] Redirect limit has been reached`
(confirmed live). `submit_preselection_condor.py` sets `x509userproxy` in
the submit file so HTCondor forwards the proxy to the job and sets
`X509_USER_PROXY` there — see the eossubmit-constraints section above for
the local-`/tmp` gotcha this runs into.

## CMSSW environment on the worker

Confirmed live: `source /cvmfs/cms.cern.ch/cmsset_default.sh`, then `cd
<EOS-hosted CMSSW_BASE>/src && cmsenv` works from an eossubmit worker node,
resolving `CMSSW_BASE=/eos/home-m/mshelake/Analysis/CMSSW_13_3_0` and
`SCRAM_ARCH=el9_amd64_gcc12` correctly, with `PostProcessor` importable
afterward. No AFS-based release area needed.

## Files

- `submit_preselection_condor.py` — reads a (possibly `run_all.py`-filtered)
  `DAS_{era}_dataset.json`, chunks each dataset's files into jobs
  (`--files-per-job`, default 1, matching CRAB's `Data.unitsPerJob=1`), and
  submits the whole era as one `queue ... from <manifest>` condor cluster.
  Writes `jobs.txt` (per-job manifest), `preselection.sub`, and
  `cluster_ids.txt` under `--work-area`.
- `condor_preselection.sh` — the condor executable. Sets up CMSSW via cvmfs,
  runs the worker script, copies its `*_Skim.root` output to
  `{out_dir}/{job_id}.root`.
- `condor_script_preselection.py` — the actual preselection logic (cuts +
  branch selection from `config.yaml`, same as CRAB's
  `crab_script_preselection.py`). Takes its file list as a JSON array of LFNs
  rather than via CRAB's `crabhelper.inputFiles()`/`runsAndLumis()` (there's
  no CRAB-generated `PSet.py` under condor); for Data jobs it passes the
  *entire* golden JSON as `jsonInput` rather than a job-scoped lumi slice —
  correct, just without CRAB's per-job lumi-mask narrowing.
- `checkCondorStatus.py` — cross-checks `jobs.txt` against actual output
  files on EOS (done) and `condor_q` (idle/running/held) to classify every
  job; `--resubmitHeld` (`condor_release`) and `--resubmitMissing`
  (re-`condor_submit` a fresh cluster for jobs with no output and not
  queued) are both supported.

## run_all.py flags

`--submitCondorJobs`, `--checkCondorStatus`, `--resubmitHeldCondorJobs`,
`--resubmitMissingCondorJobs`, `--condorFilesPerJob` (default 1),
`--condorJobFlavour` (default `workday`). Same `--tag`/`--filter`/`--sample`
conventions as the rest of `run_all.py`.

```bash
module load lxbatch/eossubmit   # once per shell, before any condor step
python3 scripts/run_all.py --tag earlySep --filter UL2018/MC_mu --sample \
    --submitCondorJobs
python3 scripts/run_all.py --tag earlySep --filter UL2018/MC_mu \
    --checkCondorStatus
```
