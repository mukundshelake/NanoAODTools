# 004-Reconstruction

This folder contains the ttbar reconstruction module and Data/MC plotting workflow for reconstructed variables.

## Overview

The reconstruction workflow consists of two main components:

1. **RecoModule.py** - Physics module that performs kinematic reconstruction of ttbar events
2. **Data/MC Plotting** - Complete workflow to create stacked Data/MC distributions for reconstruction variables

## Structure

```
004-Reconstruction/
├── config.yaml              # Main configuration (git-tracked)
├── QUICKREF.sh             # Quick reference commands
├── run_history.txt         # Log of all plotting runs (git-tracked)
├── configs/
│   └── sample_info.json    # Cross-sections, luminosities, category mappings
├── scripts/
│   ├── RecoDataMCHist.py   # Generate histograms from reconstructed ROOT files
│   ├── RecoHistPlotter.py  # Create Data/MC plots with ratio panels
│   ├── run_all.py          # Master orchestration script
│   └── utils.py            # Utility functions (hashing, logging, etc.)
├── data/
│   └── Datasets/           # Input file lists (*_reco_*_dataFiles.json)
├── outputs/
│   ├── a1b2c3d4e5f6/       # Hash-based output directory
│   │   ├── config.yaml     # Config snapshot for reproducibility
│   │   ├── *.coffea        # Histogram files
│   │   └── plots/          # Generated PNG plots
│   ├── latest -> a1b2c3d4e5f6/  # Symlink to most recent run
│   └── tags/               # Named bookmarks (baseline, paper_v1, etc.)
└── plots/                  # Additional plots output (optional)
```

## Reconstruction Branches

The [RecoModule.py](../python/postprocessing/modules/custom/RecoModule.py) adds 13 branches to each ROOT file:

### Top Quark 4-vectors

**Leptonic top:**
- `Top_lep_pt` - Transverse momentum (GeV)
- `Top_lep_eta` - Pseudorapidity
- `Top_lep_phi` - Azimuthal angle
- `Top_lep_mass` - Invariant mass (GeV)

**Hadronic top:**
- `Top_had_pt` - Transverse momentum (GeV)
- `Top_had_eta` - Pseudorapidity
- `Top_had_phi` - Azimuthal angle
- `Top_had_mass` - Invariant mass (GeV)

### Fit Quality Variables

- `Chi2_prefit` - χ² before kinematic fitting
- `Chi2` - χ² after kinematic fitting
- `Pgof` - Probability of goodness-of-fit: exp(-0.5 × χ²)
- `chi2_status` - Reconstruction status code:
  - `0`: Success (reconstruction converged)
  - `1`: Jet selection failed (need exactly 2 b-jets and 2 light jets)
  - `2`: Neutrino solution failed (negative discriminant)
  - `3`: Kinematic fit failed (using pre-fit values)

## Data/MC Plotting Workflow

### Prerequisites

1. **Reconstructed ROOT files** with branches from RecoModule.py
2. **Input file lists** in `data/Datasets/` with pattern: `{tag}_reco_{era}_dataFiles.json`
3. **Python environment** with required packages:
   ```bash
   pip install coffea hist dask awkward uproot pyyaml
   # Also requires ROOT with PyROOT support
   ```

### Quick Start

```bash
# 1. Edit configuration (set era, tag, variables)
nano config.yaml

# 2. Generate all plots
python scripts/run_all.py

# 3. View outputs
ls outputs/latest/plots/
```

### Configuration

Edit [config.yaml](config.yaml) to specify:

- **Eras**: Single era or list of eras to process (e.g., `["UL2017"]` or `["UL2016preVFP", "UL2016postVFP", "UL2017", "UL2018"]`)
- **Tag**: Identifier for input files (e.g., `midNov`)
- **Variables**: List of variables to plot, or `"all"` for all 13 branches
- **Chi2 filter**: Whether to apply `chi2_status == 0` filter (default: true)

Example config.yaml:
```yaml
analysis:
  eras:
    - "UL2017"
    - "UL2018"
  tag: "midNov"
  apply_chi2_filter: true

inputs:
  sample_info_template: "configs/{tag}_{era}_reco_lumiXinfo.json"

variables:
  - "Top_lep_pt"
  - "Top_lep_mass"
  - "Top_had_pt"
  - "Top_had_mass"
  - "Chi2"
  - "chi2_status"
```

### Sample Info Configuration

Era-specific configuration files in `configs/` directory follow the naming pattern `{tag}_{era}_reco_lumiXinfo.json`:

- **cross_sections**: Process cross-sections in pb
- **generated_events**: N_gen per dataset (update with actual values)
- **category_map**: Groups datasets into physics categories
- **Luminosity**: Integrated luminosity for the era (pb⁻¹)
- **luminosity_uncertainty**: Fractional uncertainty for the era
- **era**: Display string for the era

Example: `midNov_UL2017_reco_lumiXinfo.json`

The config file uses a template pattern that automatically selects the correct file for each era:
```yaml
inputs:
  sample_info_template: "configs/{tag}_{era}_reco_lumiXinfo.json"
```

### Workflow Commands

#### Basic eras from config
python scripts/run_all.py --eras UL2017,UL2018

# Override tag
python scripts/run_all.py
```bash
# Generate all plots (reads from config.yaml)
python scripts/run_all.py

# Override config settings
python scripts/run_all.py --era UL2017 --tag midNov

# Plot specific variables only
python scripts/run_all.py --variables Top_lep_pt,Top_had_mass,Chi2
```

#### Advanced Options

```bash
# Force regeneration with same config
python scripts/run_all.py --force

# Tag a run for easy reference
python scripts/run_all.py --tag-run baseline

# Skip histogram generation (plot existing .coffea)
python scripts/run_all.py --skip-histograms

# Skip plotting (only generate histograms)
python scripts/run_all.py --skip-plots

# Include failed reconstructions (no chi2_status filter)
python scripts/run_all.py --no-filter
```

#### Step-by-Step Workflow

```bash
# Step 1: Generate histograms manually
cd scripts/
python RecoDataMCHist.py -e UL2017 -t midNov

# Step 2: Create plots from .coffea file
python RecoHistPlotter.py ../outputs/midNov_UL2017_reco.coffea
```

### Output Organization

The workflow uses hash-based directories for reproducibility (following 002-Samples pattern):

```bash
outputs/
├── a1b2c3d4e5f6/           # Hash of config.yaml
│   ├── config.yaml         # Config snapshot
│   ├── midNov_UL2017_reco.coffea   # Format: {tag}_{era}_reco.coffea
│   └── plots/
│       ├── Top_lep_pt.png
│       ├── Top_had_mass.png
│       └── ...
├── latest -> a1b2c3d4e5f6/ # Always points to latest run
└── tags/
    ├── baseline -> ../a1b2c3d4e5f6/
    └── paper_v1 -> ../f6e5d4c3b2a1/
```

**Key features:**
- Same config → same hash → reuses directory
- Different config → new hash → new directory
- `latest/` symlink always points to most recent run
- Named tags for important runs (baseline, paper versions, etc.)
- Each directory contains its config snapshot for reproducibility

### Inspecting Outputs

```bash
# View latest run
ls outputs/latest/

# View plots from latest run
ls outputs/latest/plots/

# View specific hash-based output
ls outputs/a1b2c3d4e5f6/

# View tagged runs
ls outputs/tags/

# View run history
cat run_history.txt
tail -20 run_history.txt
grep "Era: UL2017" run_history.txt
```

### Plot Features

Each plot includes:
- **Stacked MC backgrounds** (ordered: ttbar, single top, diboson, W+jets, Drell-Yan, QCD)
- **Data overlay** with error bars
- **Ratio panel** (Data/MC) with statistical + luminosity uncertainties
- **CMS Preliminary** label
- **Luminosity and era** information
- **Channel label** (μ + jets)
- **Log scale** Y-axis for better visibility

### Troubleshooting

**Problem: dataFiles.json not found**
```bash
# Check if reconstruction output files are indexed
ls data/Datasets/*_reco_*_dataFiles.json

# Should see files like:
# data/Datasets/midNov_reco_UL2017_dataFiles.json
```

**Problem: BaseSchema cannot access branches**
```python
# Check if branches exist in ROOT file
import uproot
tree = uproot.open('path/to/file.root:Events')
print(tree.keys())
# Should see: Top_lep_pt, Top_had_pt, Chi2, etc.
```

**Problem: Empty histograms**
```bash
# Check chi2_status distribution first
python scripts/run_all.py --no-filter --variables chi2_status

# If most events have chi2_status != 0, reconstruction may have failed
```

**Problem: Missing weights**
```bash
# Run with debug logging
python scripts/RecoDataMCHist.py -e UL2017 -t midNov --sample

# Check log for warnings about missing weight branches
```

### Quick Reference

See [QUICKREF.sh](QUICKREF.sh) for comprehensive command examples and patterns.

### Dependencies

**Python packages:**
```bash
pip install coffea hist dask dask-awkward awkward uproot pyyaml numpy
```

**ROOT with PyROOT:**
- Install from https://root.cern/install/
- Or use conda: `conda install -c conda-forge root`

### Notes

- **Chi2 filtering**: By default, only events with `chi2_status == 0` are plotted (successful reconstructions). Use `--no-filter` to include all events.
- **Sample mode**: Use `--sample` flag during development to test with smaller dataset
- **Variable selection**: Plotting all 13 variables can be slow; use `--variables` to plot subset
- **Generated events**: Update `generated_events` in sample_info.json with actual N_gen from your datasets for correct MC scaling

### See Also

- [002-Samples](../002-Samples/) - Similar workflow structure for data/MC sample tables
- [RecoModule.py](../python/postprocessing/modules/custom/RecoModule.py) - Reconstruction implementation
- [DataMCHist.py](../../../Coffea_Analysis/src/Scripts/DataMC/DataMCHist.py) - Original histogram script (pre-reconstruction)
