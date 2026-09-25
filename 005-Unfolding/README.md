# 005-Unfolding — TUnfold measurement of the t̄t charge asymmetry

Takes the BDTScore trees from 004C, builds a response matrix and a measured
spectrum in (m_tt, N±), and unfolds to the gen level to extract A_C.

```
A_C = (N₊ − N₋) / (N₊ + N₋)      with  Δ|y| = |y_t| − |y_t̄|,  N₊ = N(Δ|y| > 0)
```

## Environment

**`latestcoffea`** is the only environment that can run this chapter end to end
— it is the one with ROOT *and* pandas, uproot, awkward and yaml together.

TUnfold is **not** part of ROOT's default build (the `unfold` CMake option is
OFF), so it is vendored and must be built once:

```bash
conda activate latestcoffea      # must be activated, not just on PATH
../external/TUnfold_V17.9/build.sh
```

The activation requirement is real: `rootcling` finds the C system headers
through `CONDA_BUILD_SYSROOT`, which only `activate-root.sh` exports. Scripts
load it through `scripts/tunfold_env.py`, never `gSystem.Load` directly.

## Layout

| file | role |
|---|---|
| `config.yaml` | the only place binning, selection, weights and normalisation are defined |
| `scripts/config.py` | config loading, `{STORAGE}/unfolding/{tag}/{hash}/{era}/` paths, provenance |
| `scripts/binning.py` | **the** N±/m_tt binning, both classification schemes, the fine-histogram projection |
| `scripts/kinematics.py` | rapidity, invariant mass, the t/t̄ charge convention, muon selection |
| `scripts/selection.py` | **the** reco selection, with per-cut counts |
| `scripts/extract.py` | BDTScore ROOT → parquet (signal / background / data) |
| `scripts/build_inputs.py` | parquet → unrolled histograms + response matrix |
| `scripts/unfold.py` | TUnfold, covariance, figures |
| `scripts/plots.py` | figures, shared with the validation suite |
| `scripts/gen_acceptance.py` | **lxplus only** — gen scan of the unskimmed NanoAOD for the acceptance correction |
| `scripts/test_*.py` | run them; `python scripts/test_binning.py` etc. |

Binning, selection and kinematics are each defined exactly once. That is not
tidiness: `Pgof` drifting between the old `response_matrix.py` and
`make_histograms.py` left 2.09% of the measured spectrum counted as a miss in
the matrix while still present in the data being unfolded (issue #32).

## Workflow

```bash
conda activate latestcoffea

python scripts/extract.py      --era UL2016preVFP --mode signal     --tag midNov
python scripts/extract.py      --era UL2016preVFP --mode background --tag midNov
python scripts/extract.py      --era UL2016preVFP --mode data       --tag midNov
python scripts/build_inputs.py --era UL2016preVFP --tag midNov
python scripts/unfold.py       --era UL2016preVFP --tag midNov
```

Outputs land under `{STORAGE}/unfolding/{tag}/{config_hash}/{era}/`. The hash is
of `config.yaml`, so changing a cut or a binning cannot overwrite earlier
results. Parquets are **not** committed to the repo.

## The N₊/N₋ definition

`config.yaml: binning.scheme` selects between:

- **`sign`** (default) — `N₊ = N(Δ|y| > 0)`, `N₋ = N(Δ|y| < 0)`. Every event is
  classified, and this is the quantity theory predictions are quoted against.
- **`threshold`** — `N₊ = |y_t| > y₀ ∧ |y_t̄| < y₀`. Enhances the per-event
  asymmetry but discards every event with both tops forward or both central.

The difference is not marginal. On UL2016preVFP signal, response-matrix
categories:

| | `threshold` (y₀ = 1.2) | `sign` |
|---|---|---|
| hits | 14.77% | **96.95%** |
| misses | 20.51% | 2.12% |
| fakes | 10.94% | 0.55% |
| neither (discarded) | 53.78% | **0.37%** |

`threshold` is kept only so the earlier result can be reproduced.

## Normalisation

`lumi_scale = Xsec × Lumi / Ngen`, all three from `config.yaml`.

**`Ngen` is the sum of signed generator weights, not the raw generated count.**
`LHEWeightSign` is applied as an event weight, so the denominator must match.
Verified from the Runs trees of the UL2016preVFP signal skim, which carry the
original dataset counts:

```
sum genEventCount = 132,178,000          <- raw, NOT the right denominator
sum genEventSumw  = 39,772,305,735.1
|genWeight|       = 303.358 (constant for this powheg sample)
sum genEventSumw / |genWeight| = 131,106,832  ==  configured Ngen 131,106,831
```

`scripts/test_config.py` re-checks this, and pins every Ngen/Xsec/Lumi against
`004B-BDTVariables/config.yaml` so the copies cannot drift.

## Known limitations

These are tracked as issues; none of them is hidden in the code.

- **No acceptance correction (#31).** Everything on disk starts at the 002
  preselection, which already requires a reconstructed muon, ≥4 jets and ≥2
  b-tags. The gen-level shape of the ~95.5% of events that fail it exists only
  in the central NanoAOD. Without `--acceptance`, `build_inputs.py` labels its
  output the gen spectrum of **selected events**, which is not a parton-level
  A_C. `scripts/gen_acceptance.py` produces the missing piece but must be run on
  lxplus.
- **Response-matrix bookkeeping (#36).** No inefficiency column; all fakes land
  in a single gen underflow bin; no m_tt overflow handling.
- **Regularisation crosses the N₊/N₋ boundary (#35).** `kRegModeCurvature` runs
  over consecutive unrolled indices, so it smooths between the highest-m_tt N₊
  bin and the lowest-m_tt N₋ bin.
- **A_C is not computed from the unfolded vector (#39).** The unfolded bins are
  strongly correlated (ρ̄ ≈ 0.98), so the error must go through the covariance.
- **The closure test is trivial (#38).** Pseudo-data and response matrix come
  from the same events, so the pulls are identically zero and nothing is tested
  beyond wiring.
- **Backgrounds and data are extracted but not used (#37).**
- **Missing scale-factor branches upstream (#44).** 428,538 background events
  sit in files lacking `LHEWeightSign`/`bTaggingWeight`/`MuonHLT/IDWeight`;
  `WJetsToLNu_0J` loses 100% of its events. Those files are skipped and the loss
  is written to `skipped_files_background.json`, never silently defaulted.
- **Reconstruction dilution (#40).** D ≈ 0.21, so σ(A_C) is amplified ~4.7×.
  This, not the unfolding, is what limits the measurement.
