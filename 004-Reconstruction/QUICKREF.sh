#!/bin/bash
# Quick Reference: 004-Reconstruction Data/MC Plotting Workflow

# ============================================
# MAIN WORKFLOW
# ============================================

# Generate all plots (reads eras, tag, variables from config.yaml)
python scripts/run_all.py

# Override eras from config (process multiple eras)
python scripts/run_all.py --eras UL2017,UL2018
python scripts/run_all.py --eras UL2016preVFP,UL2016postVFP,UL2017,UL2018

# Override tag
python scripts/run_all.py --tag midNov

# Force regenerate with same config
python scripts/run_all.py --force

# Plot specific variables only (faster iteration)
python scripts/run_all.py --variables Top_lep_pt,Top_had_mass,Chi2

# Tag a run for easy reference
python scripts/run_all.py --tag-run baseline
python scripts/run_all.py --tag-run paper_v1
python scripts/run_all.py --tag-run systematic_test

# ============================================
# CONFIGURATION EXAMPLES
# ============================================

# config.yaml - Single era
analysis:
  eras:
    - "UL2017"
  tag: "midNov"

# config.yaml - Multiple eras (will process all)
analysis:
  eras:
    - "UL2016preVFP"
    - "UL2016postVFP"
    - "UL2017"
    - "UL2018"
  tag: "midNov"

# Template pattern for sample info (automatically selects correct file per era)
inputs:
  sample_info_template: "configs/{tag}_{era}_reco_lumiXinfo.json"

# ============================================
# STEP-BY-STEP WORKFLOW
# ============================================

# Step 1: Generate histograms only
python scripts/RecoDataMCHist.py -e UL2017 -t midNov

# Step 2: Generate plots from existing .coffea file
python scripts/RecoHistPlotter.py outputs/midNov_UL2017_reco.coffea

# With custom output directory
python scripts/RecoHistPlotter.py outputs/midNov_UL2017_reco.coffea --output-dir plots/

# ============================================
# INSPECT OUTPUTS
# ============================================

# View latest run outputs
ls -lh outputs/latest/

# View plots from latest run
ls outputs/latest/plots/

# View specific hash-based output
ls outputs/a1b2c3d4e5f6/

# View tagged runs
ls outputs/tags/

# ============================================
# RUN HISTORY
# ============================================

# View all runs
cat run_history.txt

# View last 20 lines
tail -20 run_history.txt

# Find runs for specific era
grep "Era: UL2017" run_history.txt

# ============================================
# CONFIGURATION
# ============================================

# Edit configuration
nano config.yaml

# View current config
cat config.yaml

# Test config hash
python scripts/utils.py

# ============================================
# CHI2 STATUS CODES
# ============================================

# 0 = Success (reconstruction converged)
# 1 = Jet selection failed (need exactly 2 b-jets and 2 light jets)
# 2 = Neutrino solution failed (negative discriminant)
# 3 = Kinematic fit failed (using pre-fit values)

# Plot without chi2_status filter (include all events)
python scripts/run_all.py --no-filter

# ============================================
# RECONSTRUCTION VARIABLES (13 branches)
# ============================================

# Leptonic top 4-vector:
#   Top_lep_pt, Top_lep_eta, Top_lep_phi, Top_lep_mass

# Hadronic top 4-vector:
#   Top_had_pt, Top_had_eta, Top_had_phi, Top_had_mass

# Fit quality:
#   Chi2_prefit (before kinematic fit)
#   Chi2 (after kinematic fit)
#   Pgof (probability of goodness-of-fit)
#   chi2_status (reconstruction status code)

# ============================================
# TROUBLESHOOTING
# ============================================

# Check if dataFiles.json exists
ls data/Datasets/*_reco_*_dataFiles.json

# Verify sample_info.json
cat configs/sample_info.json | python -m json.tool

# Test histogram generation with debug logging
python scripts/RecoDataMCHist.py -e UL2017 -t midNov --sample

# Check ROOT file branches
python -c "import uproot; print(uproot.open('path/to/file.root:Events').keys())"

# ============================================
# CLEANUP
# ============================================

# Remove specific hash output
rm -rf outputs/a1b2c3d4e5f6/

# Remove all outputs (careful!)
# rm -rf outputs/*/

# Remove all plots
rm -rf outputs/*/plots/

# ============================================
# EXAMPLE WORKFLOWS
# ============================================

# Full workflow for paper
python scripts/run_all.py --era UL2017 --tag midNov --tag-run paper_v1

# Quick test with subset
python scripts/run_all.py --sample --variables Top_lep_pt,Chi2

# Multiple eras (run sequentially)
for era in UL2016preVFP UL2016postVFP UL2017 UL2018; do
    python scripts/run_all.py --era $era --tag midNov
done

# Generate plots only from existing coffea files
python scripts/run_all.py --skip-histograms

# Generate histograms without plotting
python scripts/run_all.py --skip-plots
