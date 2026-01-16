#!/bin/bash
# Quick Reference: 002-Samples Workflow

# ============================================
# MAIN WORKFLOW
# ============================================

# Generate all outputs (downloads golden JSONs automatically)
python scripts/run_all.py

# Force regenerate (refresh data, same config)
python scripts/run_all.py --force

# Tag a run (for easy reference)
python scripts/run_all.py --tag baseline
python scripts/run_all.py --tag paper_v1

# ============================================
# GOLDEN JSON MANAGEMENT
# ============================================

# Check status of golden JSON files
python scripts/download_golden_jsons.py --check

# Download all (skip existing)
python scripts/download_golden_jsons.py

# Force re-download all files
python scripts/download_golden_jsons.py --force

# ============================================
# INSPECT OUTPUTS
# ============================================

# View latest run
cat outputs/latest/goldenJSONs.json | jq .

# View specific output type
cat outputs/latest/dataSamples.json | jq .data

# View with metadata only
cat outputs/latest/goldenJSONs.json | jq .metadata

# Compare two runs
diff <(jq . outputs/3eb98df0f001/goldenJSONs.json) \
     <(jq . outputs/3971d8c2f5ad/goldenJSONs.json)

# ============================================
# RUN HISTORY
# ============================================

# Show all runs
cat run_history.txt | jq .

# Recent 5 runs
tail -5 run_history.txt | jq .

# Find runs by user
cat run_history.txt | jq 'select(.user=="mukund")'

# Count runs per config
cat run_history.txt | jq -r .config_hash | sort | uniq -c

# ============================================
# STATUS & MONITORING
# ============================================

# Quick status check
bash scripts/status.sh

# Config hash
python -c "import sys; sys.path.insert(0, 'scripts'); import utils; print(utils.compute_config_hash('config.yaml'))"

# List all output directories
ls -1 outputs/ | grep -E '^[0-9a-f]{12}$'

# Show downloaded golden JSON files
ls -lh data/golden_jsons/

# ============================================
# CONFIGURATION
# ============================================

# Edit config
vim config.yaml

# Verify config syntax
python -c "import sys; sys.path.insert(0, 'scripts'); import utils; import json; print(json.dumps(utils.load_config('config.yaml'), indent=2))" | head -20

# ============================================
# CLEANUP & MAINTENANCE
# ============================================

# Remove old runs (keep last N runs)
# Note: Adjust find command as needed
ls -1t outputs/ | grep -E '^[0-9a-f]{12}$' | tail -n +6 | while read d; do rm -rf outputs/$d; done

# Clean golden JSON files (will be re-downloaded)
rm data/golden_jsons/*.txt data/golden_jsons/*.json

# Show space usage
du -sh outputs/ data/

# ============================================
# TROUBLESHOOTING
# ============================================

# Check git status
git status

# Show uncommitted changes
git diff config.yaml

# Revert config to last commit
git checkout config.yaml

# View git commit history
git log --oneline -10

# ============================================
# USEFUL PATHS
# ============================================

# Current directory
pwd

# Config file
cat config.yaml

# Latest outputs
outputs/latest/

# Tagged outputs
outputs/tags/baseline/
outputs/tags/paper_v1/

# Run history
run_history.txt
