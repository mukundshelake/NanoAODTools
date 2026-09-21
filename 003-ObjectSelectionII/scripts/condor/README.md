# HTCondor submission (003/004)

A parallel alternative to each chapter's CRAB path. Both write the same
output layout, so everything downstream works unchanged either way.

| Chapter | Submit | Worker | Status |
|---|---|---|---|
| 003-ObjectSelectionI | `submit_selection_condor.py` | `condor_script_selection.py` | `checkCondorStatus.py` |
| 003-ObjectSelectionII | `submit_selectionII_condor.py` | `condor_script_selectionII.py` | `checkCondorStatus.py` |
| 004A-Reconstruction | `submit_reconstruction_condor.py` | `condor_script_reconstruction.py` | `checkCondorStatus.py` |

## Prerequisites

```bash
module load lxbatch/eossubmit     # once per shell, before any condor step
cd <CMSSW_13_3_0/src> && cmsenv && cd -
```

`--output-dir` and `--work-area` must both be `/eos/...` paths. The
eossubmit schedd's access point is a different node from the lxplus login
node and cannot see its local `/tmp`; the submit scripts refuse non-EOS
paths rather than letting jobs fail later.

No grid proxy is needed, unlike 002-Samples' condor path. That stage reads
DBS-registered inputs through the xrootd global redirector, which needs grid
auth against whichever site holds each file. These stages read private skims
that are already on EOS.

## Typical use

```bash
# Dry run first -- prints the job breakdown, submits nothing
python3 scripts/condor/submit_selection_condor.py --era UL2017 \
    --dataset-json inputs/preselection_UL2017_datasets.json \
    --golden-json inputs/UL2017_goldenJSON.json \
    --output-dir  ${STORAGE}/selectionI/${TAG}/${HASH}/UL2017 \
    --work-area   ${STORAGE}/condor_work_selectionI/${TAG}/${HASH}/UL2017

# Then --submit. --sample restricts each dataset to its first job.
```

Status and resubmission:

```bash
python3 scripts/condor/checkCondorStatus.py -d <work-area>
python3 scripts/condor/checkCondorStatus.py -d <work-area> --resubmitHeld
python3 scripts/condor/checkCondorStatus.py -d <work-area> --resubmitMissing
```

`--resubmitMissing` refuses to act when more than 25 jobs *and* more than 40%
of a work area are missing at once: "missing" only means "no output file
present", which is indistinguishable from output that was deliberately
deleted after being verified and transferred elsewhere. Pass
`--forceResubmitMissing` if the jobs genuinely never ran.

Use `condor_q -global` for any manual queue inspection. eossubmit spreads
submissions across ~40 schedds, so a plain `condor_q <id>` checks only
whichever schedd the calling shell defaults to and can report "0 jobs" for a
perfectly alive cluster.

## Design notes

**Everything is passed by absolute EOS path; nothing is condor-transferred.**
EOS is a normal mounted filesystem on lxbatch worker nodes, so jobs read
inputs and write outputs directly. This sidesteps eossubmit's restrictions
on `transfer_input_files` and on per-job output/error filenames in one move.

**`transfer_output_files = ""` is load-bearing.** With
`should_transfer_files = YES` and no explicit list, HTCondor auto-transfers
every new file in the job's scratch back to wherever `condor_submit` ran.
The wrapper already copies its own output to `$OUT_DIR`, so without this
every completed job's skim lands a second time in the submitting directory.
002-Samples hit exactly this: 2714 stray files, 234G.

**The worker is copied into scratch and run from there**, not invoked by its
absolute path. Python puts `sys.path[0]` at the *script's* directory, so
running it in place makes the flat module copies invisible to it (confirmed
live: `ModuleNotFoundError: No module named 'SelectedObjects'`).

**correctionlib/scipy come from the LCG stack on CVMFS**, prepended to
`sys.path` — they are not in stock CMSSW, and the user-local pip install they
resolve to on lxplus is invisible to a worker node. A `sys.path` prepend
rather than a full `source setup.sh`, which would risk swapping out CMSSW's
own ROOT build.

**Module lists come from `config.yaml`**, not from a hardcoded list in the
worker, and an unknown name raises. Hardcoding is what let 003-ObjectSelectionI's
CRAB path silently stop matching its local path when `metXYCorr` was added.

**A missing input file fails the job** rather than being treated as "no
events passed". Both outcomes produce no output file, but only one of them
is correct to ignore.
