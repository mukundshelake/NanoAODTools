# ============================================================
# condor/README.md
# ============================================================
# HTCondor submission guide for lxplus
# ============================================================

# HTCondor Workflow on lxplus

This directory contains everything needed to run the NanoAODTools pipeline on
CERN's **lxplus HTCondor** cluster.  It is designed to work alongside the same
`main_new.py` that runs interactively on TIFR — the only difference between the
two machines is the `.env` file.

---

## Prerequisites

### 1. Copy / sync data to EOS (one-time)

The input NanoAOD files that currently live on the TIFR NFS must be accessible
from lxplus.  Copy them to EOS before submitting jobs:

```bash
# From TIFR (ehep154)
rsync -av --progress \
    /nfs/home/common/RUN2_UL/Tree_crab/ \
    your-cern-username@lxplus.cern.ch:/eos/user/m/mshelake/RUN2_UL/Tree_crab/
```

> Only do this once.  EOS is accessible from all lxplus workers without extra setup.

### 2. Clone / copy the repo to AFS

```bash
# On lxplus
git clone https://github.com/<your-org>/NanoAODTools.git \
    /afs/cern.ch/work/m/mshelake/NanoAODTools
```

Or use `rsync` from TIFR to copy the whole working directory.

### 3. Create `.env` on lxplus

Copy `.env.lxplus.example` to `.env` inside the repo and **do not commit it**:

```bash
cp .env.lxplus.example .env
# Edit to set your actual EOS paths, Telegram token, etc.
```

The key variables for lxplus:

```
REPO_ROOT=/afs/cern.ch/work/m/mshelake/NanoAODTools
INPUT_STORAGE=/eos/user/m/mshelake/RUN2_UL/Tree_crab
OUTPUT_STORAGE=/eos/user/m/mshelake/skimmed_Run2
```

### 4. Convert existing JSONs to relative paths (once)

If you are re-using JSON files originally created on TIFR, convert their
absolute paths to relative paths portable across machines:

```bash
cd /path/to/NanoAODTools
python3 scripts/migrate_json_paths.py
```

A backup is written to `Datasets/backup_absolute/` automatically.

### 5. Set up the conda environment on lxplus (once)

The `job_wrapper.sh` first tries to activate the `latestcoffea` conda env.
Create it from the repo's `environment.yml`:

```bash
# On lxplus
conda env create -f environment.yml -n latestcoffea
```

Alternatively, `job_wrapper.sh` falls back to LCG_106 from CVMFS if conda is
not available.

---

## Running jobs

### Step 1 — Generate submit files

```bash
cd /afs/cern.ch/work/m/mshelake/NanoAODTools

# All stages, all eras
python3 condor/prepare_condor.py --outputTag midNov

# Or limit to one stage / era for testing
python3 condor/prepare_condor.py --outputTag midNov --stage selection --era UL2018
```

This creates:
- `condor/jobs/submit_<stage>_<era>.sub`  — one per (stage, era) combination
- `condor/jobs/keys_<stage>_<era>.txt`    — list of dataset keys for that job set
- `condor/submit_all.sh`                  — master script to submit everything

### Step 2 — Submit

```bash
# Submit all at once
bash condor/submit_all.sh

# Or submit selectively
condor_submit condor/jobs/submit_selection_UL2018.sub
```

### Step 3 — Monitor

```bash
condor_q                     # see your jobs
condor_q -better-analyze     # diagnose held/idle jobs
condor_q -format "%s\n" Args # see which keys are running
```

Log files are in `condor/logs/`.

### Step 4 — Harvest output JSONs

Once all jobs for a stage are done, regenerate the dataFiles JSON index:

```bash
# Regenerate JSON for one stage/era
python3 main_new.py -t midNov --stage selection --era UL2018 --harvest-only

# Or all stages / eras
python3 main_new.py -t midNov --harvest-only
```

Then proceed to the next stage.

---

## Job flavours and runtimes

| Flavour       | Max runtime | Typical use                     |
|---------------|-------------|----------------------------------|
| `espresso`    | 20 min      | Quick tests                      |
| `microcentury`| 1 h         | Small datasets                   |
| `longlunch`   | 2 h         | Medium datasets                  |
| `workday`     | 8 h         | Normal production (default)      |
| `tomorrow`    | 1 day       | Large datasets (QCD, DY)         |

Pass `--job-flavour tomorrow` to `prepare_condor.py` for large samples.

---

## Tips

- **EOS quota**: check with `eos quota /eos/user/m/mshelake/`
- **Failed jobs**: check `condor/logs/` for stderr; re-submit single keys with
  `condor_submit` and `condor/jobs/submit_<stage>_<era>.sub` after fixing the issue.
- **Sample test**: add `--sample` to the main_new.py arguments in
  `job_wrapper.sh` for a quick 1-file test before full submission.
