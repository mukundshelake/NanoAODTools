# 002-Samples

This folder contains configuration, scripts, and outputs to populate the Samples chapter (002) in the Analysis Note.

## Structure

```
002-Samples/
├── config.yaml              # Main configuration (git-tracked)
├── run_history.txt          # Log of all runs (git-tracked)
├── scripts/
│   ├── utils.py            # Shared utilities (hashing, provenance)
│   └── run_all.py          # Master script to generate all outputs
├── outputs/
│   ├── a1b2c3d4e5f6/       # Hash-based output directory
│   │   ├── config.yaml     # Config snapshot for reproducibility
│   │   ├── dataSamples.json
│   │   ├── goldenJSONs.json
│   │   ├── MCSamples.json
│   │   └── SystSamples.json
│   ├── latest -> a1b2c3d4e5f6/  # Symlink to most recent run
│   ├── placeholder/        # Initial templates (git-tracked)
│   └── tags/               # Named bookmarks (baseline, paper_v1, etc.)
└── .gitignore
```

## Workflow

### 1. Edit Configuration

Modify `config.yaml` with your analysis parameters:
- Data/MC paths
- Luminosities
- Golden JSON files
- Cross-sections
- Output specifications

### 2. Generate Outputs

Run the master script:
```bash
python scripts/run_all.py
```

This will:
- Compute hash of `config.yaml` (e.g., `a1b2c3d4e5f6`)
- Create `outputs/a1b2c3d4e5f6/` directory
- Copy `config.yaml` to that directory
- Generate all JSON outputs with provenance metadata
- Update `outputs/latest` symlink
- Log the run in `run_history.txt`

**Important**: If you run with identical config, it will reuse the same directory (no duplicates).

### 3. Optional: Tag Important Runs

Create named bookmarks for key configurations:
```bash
python scripts/run_all.py --tag baseline
python scripts/run_all.py --tag paper_v1
python scripts/run_all.py --tag systematic_JES_up
```

Access tagged runs via `outputs/tags/baseline/`, etc.

### 4. Force Regeneration

If you need to regenerate outputs with the same config:
```bash
python scripts/run_all.py --force
```

### 5. Use Outputs in Analysis Note

Reference the latest outputs:
```bash
# View latest results
cat outputs/latest/dataSamples.json

# Use in LaTeX generation scripts
python generate_latex_tables.py --input outputs/latest/
```

## Expected Outputs

Each output directory contains JSON files for tables in `AnalysisNote/002-Samples/`:

- **dataSamples.json** → populates Table `tab:datasamples` in `dataSamples.tex`
- **goldenJSONs.json** → populates Table `tab:goldenjson` in `goldenJSONs.tex`
- **MCSamples.json** → populates Table `tab:mcsamples` in `MCSamples.tex`
- **SystSamples.json** → populates Table `tab:systsamples` in `SystSamples.tex`

## Output JSON Format

Each JSON includes:
- `table_id`: LaTeX table label
- `caption`: Table description
- `data`: Structured data for table rows
- `metadata`: Provenance tracking
  - `status`: `placeholder` | `generated` | `validated`
  - `version`: Schema version
  - `provenance`: Config hash, git commit, timestamp, user

## Querying Run History

```bash
# View all runs
cat run_history.txt | jq .

# Show recent 5 runs
tail -5 run_history.txt | jq .

# Find runs with specific config hash
grep "a1b2c3d4e5f6" run_history.txt | jq .

# Compare two configs
diff outputs/a1b2c3d4e5f6/config.yaml outputs/f7g8h9i0j1k2/config.yaml
```

## Status Definitions

- `placeholder`: Template JSON with example structure, no real data
- `generated`: JSON populated by analysis scripts with real data
- `validated`: Human-reviewed and approved for publication

## References
* The Lumi recommendations are mentioned here: `https://twiki.cern.ch/twiki/bin/viewauth/CMS/LumiRecommendationsRun2`

* The golden json files can be found here (and in it's parent folders)
    * 2016: `https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions16/13TeV/Legacy_2016/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt`
    * 2017: `https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt`
    * 2018: `https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions18/13TeV/Legacy_2018/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt`