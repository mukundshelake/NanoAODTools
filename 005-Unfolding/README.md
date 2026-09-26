# 005-Unfolding — TUnfold measurement of the t̄t charge asymmetry

Takes the BDTScore trees from 004C, builds a response matrix and a measured
spectrum in (m_tt, N±), and unfolds to the gen level to extract A_C.

```
A_C = (N₊ − N₋) / (N₊ + N₋)      with  Δ|y| = |y_t| − |y_t̄|,  N₊ = N(Δ|y| > 0)
```

## Campaign

Input is **`earlySeptember_corrected`** / 004C **training** hash
**`386b4160d6fa`**, **UL2016preVFP only**.

```
{STORAGE}/bdtScore/earlySeptember_corrected/386b4160d6fa/{era}/{DataMC}/{group}/{sample}
```

That is 004C's `bdtScore` stage — the 004B files with a `BDTScore` branch added.
`InputHash` is therefore the *training* hash, which is what that stage is keyed
by, not 004B's `1022d663f7d9`.

The chapter previously read `BDTScore/midNov` off `/mnt/disk1/skimmed_Run2`, the
older pre-hash pipeline. That was a mistake worth recording, because the
difference is not cosmetic — the reconstruction there is materially worse:

| | midNov | earlySeptember_corrected |
|---|---|---|
| dilution D | 0.213 | **0.407** |
| charge assignment | 85.1% | 83.5% |
| median ΔR(Top_lep, gen) | 0.704 | **0.541** |
| σ(A_C) inclusive, closure | 0.00352 | **0.00170** |
| response matrix condition number | 90.6 | **11.8** |

Two naming traps in this campaign:

- **Weight branches are lowercase-initial** — `lheWeightSign`, `muonIDWeight`,
  `bTagWeight`, `muonHLTWeight`. midNov used `LHEWeightSign`, `MuonIDWeight`,
  `bTaggingWeight`, `MuonHLTWeight`. Do not mix them; probing for the wrong
  spelling silently finds nothing.
- **`muonHLTWeightStat`/`Syst` and `muonIsoWeightStat`/`Syst` are absolute
  uncertainties** (~0.07%), not varied weights like `muonIDWeightUp`. The
  config's `systematics` block has a separate `absolute:` form for them.

`ttbar_mass` is **computed** from the two fitted tops rather than read — this
campaign has no such branch, and computing it keeps reco and gen m_tt on the
same footing.

**`selection.bdt_cut` is `null`, and that is a measured conclusion.** With the
acceptance correction in place a BDT cut is strictly counterproductive:

| bdt_cut | signal | bkg/sig | A_C | total err |
|---|---|---|---|---|
| **null** | 193,675 | 22.0% | +0.00395 | **0.00204** |
| 0.3 | 157,310 | 22.8% | +0.00395 | 0.00214 |
| 0.4 | 120,998 | 23.4% | +0.00395 | 0.00228 |
| 0.5 | 75,831 | 23.2% | +0.00395 | 0.00266 |

A_C does not move and the uncertainty rises monotonically, because we unfold to
the **full generated spectrum**, so the parton-level answer is the same whatever
reco subset we keep — the cut only removes events. A scan on the gen asymmetry
of *selected* events does show A_C rising 2.5× with the cut, and that rise is
exactly what the acceptance correction undoes. Nor does the cut help the
background: 004C separates qqbar from gg t̄t, not signal from background.

A cut would pay off only if the measurement were *defined* in the BDT-selected
phase space, which is not a quantity theory predicts.

Other eras are deliberately out of scope: postVFP stops at 004A, UL2017's 004A
was built on the **uncorrected** 003 chain and must not be combined with
preVFP, and UL2018 has no 004A.

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

## ABCD region — easy to miss

The 002 preselection keeps `Muon_pfRelIso04_all <= 0.2` while the ABCD split is
at 0.06, so **the skim contains all four ABCD regions** and 005 must pick one.
`ABCD_region` is 0 = A (tight isolation, high mT(W), the nominal signal region),
1 = B, 2 = C, 3 = D. `config.yaml: selection.abcd_region: 0`.

Without that cut only ~56% of t̄t and ~48% of data events belong in the
measurement, and essentially all the QCD contamination is included.

## Data and backgrounds

`build_inputs.py` writes, alongside the closure histograms:

| histogram | content |
|---|---|
| `h_data_measured` | real data, region A, Poisson errors |
| `h_bkg_{group}` | each non-QCD MC group, lumi-scaled |
| `h_qcd_data_driven` | QCD from the ABCD transfer factor |

QCD comes from data by default, not from the QCD MC: `R(pT, |η|)` from
003-ObjectSelectionII gives `N_qcd_A = R · (data_B − non-QCD MC_B)`. Region B
shares region A's high-mT(W) requirement so it has the right shape.

Yields on UL2016preVFP:

```
data                227,499        FullyLeptonic   21,643
signal (ttbar)      193,675        SingleTop       16,977
backgrounds          45,850        WJets            3,394
prediction          239,525        QCD (data)       3,242
data / prediction     0.950        DrellYan           566
                                   Diboson             29
```

**Unfolding real data requires `--unblind`.** The default unfolds the MC closure
spectrum. Extracting A_C from data is an unblinding step and should be a
conscious decision rather than the result of running the default command.
Physics backgrounds are subtracted only in that mode — the closure pseudo-data
is pure signal, so subtracting them there would remove events that were never in
it. Fakes are subtracted in both, being part of the signal MC.

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

## The N₊/N₋ definition — an open decision (#34)

`config.yaml: binning.scheme` selects between:

- **`sign`** (current default) — `N₊ = N(Δ|y| > 0)`, `N₋ = N(Δ|y| < 0)`. Every
  event is classified, and this is the quantity theory predictions and
  published CMS results are quoted against.
- **`threshold`** — `N₊ = |y_t| > y₀ ∧ |y_t̄| < y₀`, and vice versa. Events with
  both tops forward or both central are unclassified.

**The trade-off is not the one you would guess**, so it is written out here
rather than assumed. Measured on the UL2016preVFP signal, with the corrected
charge convention:

| | `threshold` (y₀ = 1.2) | `sign` |
|---|---|---|
| response-matrix hits | 14.77% | **96.95%** |
| misses | 20.51% | 2.12% |
| fakes | 10.94% | 0.55% |
| neither (discarded) | 53.78% | **0.37%** |
| usable yield | 77,201 | **292,329** |
| dilution D | **0.508** | 0.216 |
| A_C(gen) | **+0.00260** | +0.00156 |
| σ(A_C) ≈ 1/(D√N) | **0.0071** | 0.0085 |
| **σ(A_C) / A_C** | **2.73** | 5.45 |

The threshold estimator throws away 73% of the events and is still **2.0×
better in relative precision**, because the y₀ requirement more than doubles
the dilution — 0.508 against 0.216. Issue #34's parenthetical that it enhances
the per-event asymmetry "at the cost of statistics" is empirically backwards:
on these numbers it is a net statistical *gain*.

What still argues for `sign`:

- it is the definition theory predicts and other experiments publish, so the
  result is directly comparable without a dedicated calculation;
- its response matrix is 97% diagonal rather than 15%, so the unfolding is
  barely an inversion at all;
- correcting a 54%-discarded sample leans much harder on the MC modelling of
  the discarded class, which is a systematic cost this table does not price.

The default is `sign` pending that decision. Whichever is chosen, the
"neither" class needs explicit treatment in the response matrix rather than
being merged into the fakes — that part is #36.

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

## Uncertainty breakdown

`unfold.py` reports each component separately rather than as stat/syst:

```
  m_tt        A_C      data stat  MC stat (A)  systematics  unaccounted    total
  inclusive  +0.00395    0.00120     0.00119      0.00016      0.00006   0.00170
```

**The response matrix's MC statistical error is as large as the data
statistical error**, and roughly 10× the genuine systematics. An earlier version
printed a single "syst" column computed as `sqrt(total² − stat²)`, which was
~99% MC statistics — a reader would have concluded the measurement was
systematics limited when it is limited by the size of the MC sample (#45).

The `unaccounted` column is whatever the listed components do not explain. It
should stay small; if it grows, a component is missing from the breakdown rather
than the total being wrong.

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
- **Reconstruction dilution (#40).** D ≈ 0.41 here, so σ(A_C) is amplified
  ~2.4×. Still the dominant limitation, but half the problem it looked like on
  midNov. A study of widening the hadronic-W light-jet permutation set
  (`004A-Reconstruction/scripts/studies/lightjet_permutation_gain.py`) puts the
  remaining gain at 1.3–1.4×, not the 2.6× midNov suggested.
- **The split-closure pull is not unit-calibrated.** Over 8 splits its deviation
  was mean −0.13σ (no bias) with spread 1.54 rather than 1.0, so some variance
  from splitting the MC between matrix and pseudo-data is still unpropagated.
