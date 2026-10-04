# Appendix-B — qq̄ vs (gg, qg) classifier for semileptonic tt̄

An independent ML study: separate **qq̄-initiated** tt̄ production from **gg- and
qg-initiated** production using reco-level quantities only. It reads the
**selectionII** (003-ObjectSelectionII) skims directly, so it needs neither 004A
reconstruction nor 004B variables. If it beats 004C's BDT, it is a candidate replacement.

Run it in the `latestcoffea` conda env (pure python: uproot / awkward / xgboost /
scikit-learn), not inside `cmsenv`.

## Labels

From `Generator_id1/id2`, with the same convention as Appendix-A's `Gen_prodType`:

| prodType | initial state | class |
|---|---|---|
| 1 | q q̄ (same flavour) | **signal** (y = 1) |
| 2 | q g or q̄ g | background |
| 0 | g g | background |

UL2018 check (1.42M events, every 20th file): there are only gg, qg, q̄g and qq̄.
There are **no** qq / q̄q̄ / qq′ events, and every qq̄ pair has the same flavour
(79446/79446). `LHEPart` incoming partons agree event by event. In region A, qq̄ is
**5.4 %**, qg 24.9 %, gg 69.6 % — the same in all four ABCD regions to ±0.3 %.

## Selection

- Era UL2018, campaign `earlySeptember_corrected`, selectionII hash `37f710b13012`.
- Nominal `MC_mu/SemiLeptonic/ttbar_SemiLeptonic` only (never MC_alt).
- **Region A** only (`ABCD_region == 0`: tight iso, high mTW).
- Selected jets reproduce 003-I `SelectedObjects._passes_jet_cuts` (pT > 25, |η| < 2.4,
  jetId = 6, puId > 0 below 50 GeV; DeepJet medium WP per era). Extraction checks
  this against `sel_nJet` / `sel_nbjet`: 0 mismatches on all 15.2M UL2018 events.

## Features (`scripts/features.py`)

**Design rule:** every feature is symmetric under charge conjugation and under z → −z.
- The muon charge is never used.
- Rapidities enter as |y|.
- The two tops enter as max/min, never as (t, t̄).

So the classifier cannot learn sign(Δ|y|), and it cannot sculpt the charge asymmetry
that the score is meant to help measure.

| group | features | physics handle |
|---|---|---|
| global | n_jets, n_bjets, μ pT/|η|, MET, ν |pz| (W-mass constraint), HT, centrality | — |
| full system (μ+ν+all jets) | mass, pT, |y|, |pz|, x_max / x_min = (M_T/√s) e^{±|y|} | valence-quark boost; x asymmetry |
| event shape | sphericity, aplanarity | radiation pattern |
| χ² tt̄ reco | log χ², m(t_had), m(t_lep), m(W_had), m_tt, pT_tt, |y_tt|, |pz_tt|, x_max/min, max/min |y_t|, |Δy|, max/min pT_t | m_tt, boost, recoil |
| angles (tt̄ frame) | |cos θ*| of the top w.r.t. the beam, cos φ(ℓ, d), helicity c_kk | production angle, spin correlation |
| extra radiation | HT of non-tt̄ jets, leading extra jet pT/|η| | gluons radiate ~C_A/C_F = 9/4 more |
| jets | pT, |η|, DeepJet of the 6 leading jets | — |
| q/g (separate set) | qgl of the leading extra jet, min qgl of the W jets | quark vs gluon ISR |

The χ² reconstruction is only a feature builder:
- b candidates are the two leading b-tagged jets
- W candidates are every pair of the 4 leading non-b jets
- σ(t_had, t_lep, W) = 35, 30, 20 GeV

Truth columns are stored for labels and diagnostics only, never as inputs:
- `prodType`, `y`
- `gen_x1/x2`
- `gen_tt_m/pt/absy`
- `gen_delta_absy`
- `genWeight`

## Training (`scripts/train.py`, `training_config.yaml`)

- **Split:** by `event % 10` — test {0,1,2} = 30 %, val {3} = 10 %, train {4..9} = 60 %.
  This is reproducible, and a score can later be applied without train/test leakage.
- **Weighting:** unweighted (one nominal sample, about 0.4 % negative genWeight).
  Class imbalance is handled by per-class weights normalised to equal sums. The test AUC
  is also reported with sign(genWeight) weights.
- **Runs:**
  - `xgb_reco` — XGBoost on the kinematic features.
  - `xgb_reco_qgl` — adds qgl. Jet_qgl is mis-modelled in data, so this must be checked
    on data before use.
  - `xgb_gen_oracle` — true m_tt, pT_tt, |y_tt|. A reference for how much the tt̄ system
    alone carries.
  - `mlp_reco` — sklearn MLP (256-128-64) on quantile-normalised inputs. There is no
    torch in `latestcoffea`.
- XGBoost: a grid over max_depth {4,6,8}, each point early-stopped on val, choosing the
  best val AUC.

**Outputs** (`outputs/{tag}/{hash}/{era}/training/{trainingHash}/`):
- `summary.json`
- `roc.png`, `roc_zoom.png`
- `single_feature_auc.csv`
- `test_scores.parquet`
- per run:
  - `score_by_prodType.png` — test vs train with KS p-values, as an overtraining check
  - `purity.png` + table — qq̄ purity and efficiency vs the kept fraction
  - `auc_vs_mtt.png`
  - `importance.csv` — xgb gain + permutation AUC drop on val
  - `model.pkl`
  - the sculpting check — gen A_C per score quartile, overall and per channel.
    gg must stay at 0. A rising qq̄ A_C is expected physics, since A_C grows with m_tt
    and |y_tt|.

## Running

```bash
conda activate latestcoffea
cd Appendix-B-qqvsgg
python scripts/run_all.py --tag earlySeptember_corrected --filter UL2018 \
    --generateSelectionIIDatasetJSON --selectionIITag earlySeptember_corrected --selectionIIHash 37f710b13012 \
    --extractFeatures --train          # add --sample for a quick test
```

Further steps:

```bash
# [3] PyTorch qqbar classifiers (torchml env, launched by run_all)
python scripts/run_all.py --tag earlySeptember_corrected --filter UL2018 --trainTorch
# [4] A_C-sensitivity study, XGBoost (latestcoffea); --qqScores adds the 005 x qq-score baseline
python scripts/run_all.py --tag earlySeptember_corrected --filter UL2018 --asymStudy \
    --qqScores outputs/earlySeptember_corrected/62defcd4ad5a/UL2018/training_torch/24872f4cf47f/test_scores.parquet
# [5] the same with DeepSets (torchml env) -> asym/{asymHash}/deepsets/
python scripts/run_all.py --tag earlySeptember_corrected --filter UL2018 --asymTorch --qqScores <same>
# unfolding-like (per gen m_tt bin) sensitivity for any asym output dir
python scripts/asym/asym_differential.py --asymDir <asym dir> [<asym dir>/deepsets] \
    --featureDir /mnt/disk3/nfs/mukund/DataFiles/appendixB/earlySeptember_corrected/e627d72c38ed/UL2018 \
    --config asym_config.yaml
# preVFP from the 004A output (adds the kf_* features)
python scripts/run_all.py --tag earlySeptember_corrected --filter UL2016preVFP --inputStage reconstruction \
    --generateSelectionIIDatasetJSON --selectionIITag earlySeptember_corrected --selectionIIHash 802fbf296122 \
    --extractFeatures
```

- Features go to `{STORAGE}/appendixB/{tag}/{configHash}/{era}/...`, one parquet per
  selectionII file. Each file is written atomically: `.tmp` and then renamed, so a file
  that exists is complete.
- `config.yaml` (extraction) and `training_config.yaml` (models) are hashed separately.
- UL2018 extraction: 391 files in 5.2 min with 15 workers.

## Results — UL2018, region A (2026-10-02)

- Extraction hash `c9087777f281`, training hash `4764a22f2367`.
- 15,225,592 events: train 9.14M / val 1.52M / test 4.57M; qq̄ 5.56 %.
- Output: `outputs/earlySeptember_corrected/c9087777f281/UL2018/training/4764a22f2367/`.

| run | test AUC | qq̄ vs gg | qq̄ vs qg | train AUC | qq̄ purity, top 10 % | top 2 % |
|---|---|---|---|---|---|---|
| **xgb_reco** (depth 4, 1207 trees) | **0.7134 ± 0.0006** | 0.697 | 0.758 | 0.723 | 15.3 % (eff 28 %) | 22.9 % |
| xgb_reco_qgl | 0.7151 ± 0.0006 | 0.699 | 0.761 | 0.724 | 15.4 % | 23.0 % |
| mlp_reco | 0.7087 ± 0.0006 | — | — | 0.713 | — | — |
| xgb_gen_oracle (true m, pT, \|y\| of tt̄) | 0.7119 ± 0.0006 | 0.694 | 0.761 | 0.712 | 18.0 % (eff 32 %) | 30.3 % |
| *004C BDT (preVFP, all ABCD regions, 004B shapes)* | *0.7013* | | | | | |

Weighting the test set by sign(genWeight) changes no AUC by more than 0.0006.

1. **The reco classifier beats 004C** by +0.012 in AUC, needs no 004A reconstruction, and
   its result does not depend on tree depth: max_depth 4/6/8 agree to 0.0007.
   - The comparison is not exact: 004C was preVFP and all regions.
   - A fair comparison needs this chapter run on preVFP, or 004C's score evaluated on
     region A.
2. **The information, not the model, sets the limit.**
   - Every XGBoost depth gives the same AUC, and the MLP is slightly worse.
   - The reco model beats the gen-tt̄ oracle in AUC, because it also sees radiation and
     angles.
   - The oracle wins clearly at low false-positive rate (see `roc_zoom.png`): 30 % vs 23 %
     purity in the top 2 %.
   - **The high-purity tail is limited by tt̄ resolution** (m_tt, |y_tt|). Better tt̄
     reconstruction is the most promising improvement.
3. **The strongest inputs are about recoil and boost, not angles.** Ranked by permutation
   importance:
   1. `sys_xmin`, the smaller x of the full system
   2. `sys_pt`, the recoil
   3. `tt_pt`
   4. `top_pt_max`
   5. `centrality`
   6. `sys_absy`
   7. `m_thad`

   Spin correlation (`cos_phi_ld`, `c_kk`) does not reach the top 15.
4. **qgl adds only +0.0017.** That is not worth depending on a variable that is
   mis-modelled in data.
5. **AUC rises with m_tt:** 0.69 (300–450 GeV) → 0.70 → 0.74 → 0.77 → 0.79 (> 1 TeV).
6. **Sculpting check (gen A_C per score quartile, xgb_reco):**
   - gg: −0.0014, −0.0014, −0.0005, −0.0007 (±0.0011), consistent with 0.
   - qq̄: −0.019 → +0.013. This is physics: the charge-blind gen oracle shows a larger
     version of the same trend (−0.042 → +0.024).
   - Inclusive: 0.0011 → 0.0032 from the lowest to the highest quartile.
7. **Mild overtraining** in the qq̄ class: the KS test of train vs test scores gives
   p = 5e-11 for qq̄, against 0.41 for qg and 0.23 for gg. Train AUC is 0.723 vs test
   0.713. Val = test (0.7139 / 0.7134), so the reported numbers are unbiased. Raising
   `min_child_weight` / `reg_lambda` may recover some AUC.

### What is the ceiling? (`scripts/studies/ceilingLadder.py`)

The same XGBoost trained on progressively more **truth** information. It used
2.6M region-A events (every 6th file) with the same fold split.

| what the model sees | AUC | qq̄ vs gg | qq̄ vs qg |
|---|---|---|---|
| reco (`xgb_reco` features, same subset) | 0.710 | 0.694 | 0.754 |
| A: true tt̄ m, pT, \|y\| | 0.712 | 0.695 | 0.762 |
| B: + true x₁/x₂, \|cos θ*\| (tt̄ frame), max top pT | 0.766 | 0.740 | 0.840 |
| C: + extra-parton pT, \|η\| (LHEPart[2]) | 0.783 | 0.761 | 0.847 |
| D: + extra-parton flavour (full parton level) | **0.827** | 0.765 | **1.000** |

- **0.71 is a reco limit, not a physics limit.** With perfect parton kinematics the
  ceiling is 0.78, or 0.83 with the extra parton's flavour.
- **POWHEG hvq always writes exactly one extra LHE parton.** It is a gluon in 100 % of gg
  and 99.9 % of qq̄ events, and a quark in 100 % of qg events. So qg is defined by the
  flavour of the emitted parton, and reco can see that only through q/g tagging of the
  extra jet.
- **qq̄ vs gg saturates at about 0.76 even at truth level.** The gain from A to B, +0.054,
  is information reco sees only through the resolution of x, |y_tt| and cos θ*: neutrino
  pz and jet assignment. That is where better tt̄ reconstruction pays off.

## PyTorch models (`scripts/train_torch.py`, UL2018, 2026-10-02)

These run in a separate conda env, `torchml`: CPU torch 2.14, with numpy, pandas,
pyarrow, scikit-learn, scipy, matplotlib, pyyaml and joblib. `latestcoffea` stays
untouched. `run_all.py --trainTorch` launches the script with `torch_config.yaml`'s
`TorchPython`.

**Inputs.**
- Every model gets the same 56 flat features as `xgb_reco`, quantile-normalised.
- The object models also get per-object arrays (`config.yaml ObjectArrays`, extraction
  hash `62defcd4ad5a`): muon, W-constrained ν, and up to 10 selected jets. Each object has
  log pT, η, sin/cos Δφ, log m, DeepJet, a type flag, and optionally qgl.
- The objects are canonically oriented:
  - all η are flipped so the full system moves to +z
  - φ is relative to the muon, mirrored so the leading jet has Δφ ≥ 0
  - charge is never used

**Models.**
- `torch_mlp` — 4 × 256 MLP on the flat features.
- `deepsets` — a per-object φ-net (64-128-128), masked sum + mean pooling, concatenated
  with the flat features, then a ρ-net.
- `part` — Particle-Transformer-lite: 3 attention layers (d = 64, 4 heads), with attention
  biased by the pairwise features ln ΔR, ln k_T, ln z and ln m²_ij. Then 2 class-attention
  layers, plus the flat features.

**Training.**
- Same event%10 split as XGBoost.
- AdamW with a one-cycle LR schedule (peak 2e-3), batch 4096, ≤ 20 epochs, early stopping
  on val AUC.
- Class balance via `pos_weight`.
- **CPU note: use 10 threads.** 20 hyper-threads oversubscribe and run 10–50× slower.

**Throughput at 10 threads:**

| model | events/s | time per full 9.1M epoch |
|---|---|---|
| MLP | 145k | 1 min |
| DeepSets | 23k | 5.6 min |
| ParT | 6k | ~25 min |

So `part` trains on a fixed 3M-event subsample.

Output: `outputs/earlySeptember_corrected/62defcd4ad5a/UL2018/training_torch/24872f4cf47f/`.

| run | train events | test AUC | qq̄ vs gg | qq̄ vs qg | train AUC | qq̄ KS p (train vs test) | purity top 10 % / top 2 % | CPU time |
|---|---|---|---|---|---|---|---|---|
| xgb_reco (reference) | 9.1M | 0.7134 | 0.697 | 0.758 | 0.723 | 5e-11 | 15.3 % / 22.9 % | 7 min |
| torch_mlp | 9.1M | 0.7146 | 0.698 | 0.760 | 0.718 | 0.012 | 15.4 % / 23.0 % | 21 min |
| **deepsets** | 9.1M | **0.7230** | **0.706** | **0.771** | 0.726 | 0.018 | **16.0 % / 23.7 %** | 112 min |
| part | 3M | 0.7201 | 0.703 | 0.767 | 0.722 | 0.36 | 15.7 % / 23.2 % | 187 min |
| deepsets_qgl (+ per-jet qgl) | 9.1M | **0.7243** | 0.707 | 0.773 | 0.727 | 0.006 | 16.0 % / 23.7 % | 113 min |
| deepsets_3M | 3M | 0.7188 | 0.702 | 0.765 | 0.721 | 0.35 | 15.7 % / 23.2 % | 37 min |

- **The object-level networks add real information.**
  - DeepSets: +0.0096 AUC over XGBoost and +0.008 over a flat MLP of the same depth. The
    flat MLP matches XGBoost, so the gain comes from the per-object inputs, not from deep
    learning as such.
  - The gain appears in every m_tt bin: 0.702 / 0.713 / 0.750 / 0.780 / 0.801, against
    XGBoost's 0.690 / 0.704 / 0.743 / 0.773 / 0.791.
- **The networks also overtrain far less than XGBoost.**
- **gg sculpting check passes** for all three models: A_C per quartile within ±0.002 of 0.
- **At equal data (3M), ParT beats DeepSets slightly: 0.7201 vs 0.7188.** Going from 3M to
  9.1M events lifts DeepSets by +0.004 (0.7188 → 0.7230). **The models are still limited by
  training-set size.** Training on all Run 2 eras together, about 4× the events, is the
  cheapest next gain. ParT on full statistics would probably reach about 0.725, but at
  ~25 min/epoch on CPU.
- **qgl adds +0.0013** on top of DeepSets: 0.7243 vs 0.7230. That is the same size as with
  XGBoost, and still not worth the data/MC risk without validation.
- **ParT** reaches 0.7201 on a third of the data, but costs about 4× more CPU per event.
  `part_qgl` and `part_objonly` were dropped for the DeepSets follow-ups
  (`torch_config_followup.yaml`, written into the same output directory).

## A_C sensitivity: optimising for the measurement instead of the qq̄ label (`scripts/asym/`, 2026-10-02)

The classifier sections above optimise a label. The measurement wants a small σ(A_C).

### Framework (`asym_eval.py`)

- s = sign(Δ|y|_true). The reco sign is q_μ · `rec_dabsy_lh`, where `rec_dabsy_lh` =
  |y_tlep| − |y_thad|; μ+ means the leptonic top is the top.
- Two statements of what A_C may do:
  - **T1, model-independent, like 005.** Event weights 1 + α·s over all tt̄, so A_C = A₀ + α.
    The optimal per-event observable is **E[s | reco]**. A qq̄ score cannot help here,
    which is why 005 found the BDT cut counterproductive.
  - **T2, deviation carried by qq̄.** Weights 1 + α·s·1(qq̄), so A_C = A₀ + α·f_qq̄.
    The optimal observable is **E[s·1(qq̄) | reco]**. This is valid only under that physics
    assumption.
- **The model.** One 4-class model, (qq̄ / other) × (s′ = q·s = ±), trained without event
  weights so the probabilities are calibrated.
  - Its inputs are charge-blind and z-flip-invariant: Δ|y| is invariant under z → −z.
  - The lepton-side / hadron-side identity comes from the new `rec_*` columns
    (`config.yaml AsymmetryColumns`, extraction hash `e627d72c38ed`):
    - χ² Δ|y|
    - Δ|y| with the second ν root
    - Δ|y| with the swapped b assignment, and its χ²
    - per-object |y|
    - `kf_dabsy_lh` from 004A, when present
  - Signed observable: O = q · (P(qq+) + P(o+) − P(qq−) − P(o−)) for T1, and
    O = q · (P(qq+) − P(qq−)) for T2. The charge multiplication enforces CP symmetry.
- **Figure of merit.** The Fisher information of the binned observable,
  I = Σ_b p_b·mean_b(t)², corrected for finite-sample bias and validated on toys (sign-only
  gives D², the optimal observable gives E[d²], noise gives 0, closure is exact).
  - σ(A_C)·√N = 1/√I for T1, and f_qq̄/√I for T2.
  - Errors come from a Poisson bootstrap.

### Results: one-parameter template fit, test folds, XGBoost

σ(A_C)·√N, lower is better; "rel" is relative to the 005 binning with χ² reco.

| observable | UL2018 T1 | rel | UL2018 T2 | rel | preVFP T1 | preVFP T2 |
|---|---|---|---|---|---|---|
| sign(Δ\|y\|), χ² | 1.984 | 1.012 | 1.867 | 1.008 | 1.939 | 1.794 |
| 005: m_tt × sign, χ² | 1.960 | 1.000 | 1.853 | 1.000 | 1.917 | 1.788 |
| Δ\|y\| in 40 bins, χ² | 1.805 | 0.921 | 1.753 | 0.946 | 1.770 | 1.694 |
| **005: m_tt × sign, 004A fit (current 005)** | — | — | — | — | **2.256** | **2.011** |
| 005 (χ²) × DeepSets qq̄-score quartile | 1.943 | 0.991 | 1.393 | 0.752 | 1.901 | 1.347 |
| **ML O_all (100 bins)** | **1.397** | **0.712** | 1.342 | 0.724 | **1.397** | 1.329 |
| **ML O_qq (100 bins)** | 1.531 | 0.781 | **1.074** | **0.580** | 1.559 | **1.072** |
| **DeepSets O_all (100 bins)** | **1.335** | **0.681** | 1.285 | 0.693 | — | — |
| **DeepSets O_qq (100 bins)** | 1.406 | 0.717 | **0.998** | **0.539** | — | — |

The DeepSets rows come from `asym_torch.py`: 4-class, the same inputs plus per-object
arrays, 20 epochs, val log-loss 0.537 against XGBoost's 0.575. They are UL2018 only, in
`asym/551436da07b9/deepsets/`. The XGBoost rows are XGBoost.

The UL2018 asym hash is `551436da07b9`. The preVFP hash, `72c7e6f9afe1`, was run before
the `Torch:` block was appended to `asym_config.yaml`; every other setting is identical,
which was checked with diff.

### Results: unfolding-like comparison (`asym_differential.py`)

This uses a free asymmetry in each 005 gen m_tt bin: 7 bins, edges 450 … 1200 with
under/overflow merged in. That is the freedom 005's unfolding has.
σ(A_C)·√N = √(fᵀF⁻¹f).

| reco binning | preVFP | rel to current 005 | UL2018 | rel to 005 (χ²) |
|---|---|---|---|---|
| **005 as it is: m_tt × sign, 004A fit** | **2.500** | 1.00 | — | — |
| m_tt × sign, χ² | 2.177 | 0.87 | 2.264 | 1.00 |
| m_tt × signed \|Δ\|y\|\| (10 bins), χ² | 1.988 | 0.80 | 2.057 | 0.91 |
| **m_tt × ML O_all (10 bins)** | **1.581** | **0.63** | **1.595** | **0.70** |
| m_tt × ML O_all (20 bins) | 1.574 | 0.63 | 1.587 | 0.70 |
| **m_tt × DeepSets O_all (10 bins)** | — | (≈ 0.58, est.) | **1.516** | **0.67** |
| ML O_all alone (cannot separate m_tt) | 1.94 ± 0.14, ill-conditioned | | 4.6, ill-conditioned | |

### Bias test

Injected asymmetry changes that the template does not assume, reported as fitted ΔA_C /
true ΔA_C:
- (a) uniform in qq̄
- (b) rising with gen m_tt
- (c) rising with gen |y_tt|
- (d) uniform in all channels

| observable (UL2018) | T1: a / b / c / d | T2: a / b / c / d |
|---|---|---|
| 005 m_tt × sign, χ² | 1.05 / 1.16 / 1.04 / 1.00 | 1.00 / 1.12 / 0.99 / 0.94 |
| 005 × qq̄-score quartile | 1.14 / 1.29 / 1.14 / 1.00 | 1.00 / 1.19 / 1.06 / **0.59** |
| **ML O_all** | 1.03 / 1.13 / 1.07 / 1.00 | 1.00 / 1.12 / 1.04 / 0.95 |
| ML O_qq | 1.18 / 1.27 / 1.24 / 1.00 | 1.00 / 1.14 / 1.16 / **0.58** |
| **DeepSets O_all** | 1.03 / 1.12 / 1.06 / 1.00 | 1.00 / 1.11 / 1.03 / 0.95 |
| DeepSets O_qq | 1.14 / 1.22 / 1.20 / 1.00 | 1.00 / 1.14 / 1.17 / **0.58** |

The preVFP numbers agree to within 0.02, and the 004A-based 005 binning is at
1.12 / 1.21 / 1.14 in T1.

### Conclusions

0. **The best observable is DeepSets O_all.**
   - In the unfolding-like comparison on UL2018 it is 0.67× the χ² 005 binning.
   - Against current 005 (004A fit) that is about 0.58×. This is estimated using preVFP's
     measured 004A/χ² ratio of 1.15, since UL2018 has no 004A.
   - That is equivalent to about 3× the luminosity, with a bias no larger than the
     baseline's.
1. **ML O_all cuts σ(A_C) by 30–37 % model-independently.** That is 0.63× the current 005
   in the unfolding-like comparison, and 0.70× the χ² 005 binning. It is equivalent to
   about 2–2.5× the luminosity.
   - Its mis-modelling biases are the **same or smaller** than the current binning's, and
     m_tt binning removes the mass-dependent part, as in 005.
   - It is identical in both eras (1.397), and on UL2018 it doesn't need the 004A fit.
   - The XGBoost importance on preVFP shows what it uses:
     - competing Δ|y| reconstructions: `rec_dabsy_lh` 30 %, `_nu2` 12 %, `kf_dabsy_lh` 5 %,
       `_bswap` 2 %
     - reliability indicators: muon |η|, m_thad, χ², m_tlep and m_Whad
2. **The simple χ² beats 004A's fit on sign dilution:** D = 0.52 vs 0.44 on identical
   preVFP events. Swapping the reconstruction alone is −13 % σ(A_C) in the unfolding-like
   comparison.
3. **The qq̄ information helps only under the T2 assumption.** There, O_qq gives 0.58×.
   But if the asymmetry change is not qq̄-shaped, the result is biased by about −42 %
   (row d). Use T1 / O_all for the inclusive A_C, and T2 only as a stated, model-dependent
   result.
4. **To deploy** in 005, replace the reco "N±" axis with signed O_all bins, keeping reco
   m_tt × 10 O_all bins. The response matrix then maps (reco m_tt, O_all bin) → (gen m_tt,
   sign). Nothing else in the unfolding changes.
5. **Caveat on 005's quoted error.** 005's README quotes a total error of 0.00204 for
   193,675 signal events. That is below the perfect-reconstruction bound
   1/√193675 = 0.00227. This study's numbers predict σ ≈ 2.5/√193675 ≈ 0.0057 for the
   current preVFP reconstruction (statistics only, no background). The 005 error looks
   like MC-statistics-sized Asimov errors and should be checked.

## Gen-level N± vs m_tt and asymmetry-injection templates (`scripts/asymInjection/`, 2026-10-02)

These are for studies with a varied (high) asymmetry. The script builds the nominal gen-level
N₊ / N₋ in gen m_tt from the selectionII skims, plus **unit templates** that, when added,
shift A_C by a known amount.

**Inputs and units.**
- Weights and normalisation are identical to 003-ObjectSelectionIII (`aaf38cf077ed`): the
  same 8 `weightList` branches × Lumi·σ/N_gen, region A. The histograms are in the same
  units as the existing earlySeptember_corrected selection histograms.
- The config is `injection_config.yaml`, with its own hash.
- The driver is `scripts/asymInjection/buildAsymInjectionHists.py` (latestcoffea).

**Definitions.**
- Last-copy tops; Δ|y| = |y_t| − |y_t̄|; s = sign(Δ|y|).
- N₊ = Σw for s > 0, and N₋ = Σw for s < 0.

**Templates.** For event i in fine gen m_tt bin j (25 GeV grid, with flow bins):

| model | per-event δᵢ | resulting ΔA_C per gen m_tt bin, for inclusive +0.01 (005 bins <300, 300–450, …, ≥1200) |
|---|---|---|
| `uniform` | c·(sᵢ − A_j) | +0.0100 in every bin |
| `qqbar` | c·1(qq̄)ᵢ·(sᵢ − A^qq̄_j) | +0.0071 / 0.0106 / 0.0092 / 0.0097 / 0.0107 / 0.0116 / 0.0122 / 0.0127 (follows f_qq̄) |
| `mttRising` | c·(⟨m⟩_j / ⟨m⟩)·(sᵢ − A_j) | +0.0055 / 0.0076 / 0.0097 / 0.0126 / 0.0154 / 0.0183 / 0.0211 / 0.0267 |

- dN± = Σ wᵢδᵢ over events with s = ±1.
- Subtracting the bin's own asymmetry A_j makes **N₊ + N₋ unchanged in every fine bin**, so
  only the split between N₊ and N₋ moves. Any binning on the 25 GeV grid, including 005's,
  inherits this.
- c is chosen so that one unit gives inclusive **ΔA_C = +0.01 exactly**, and the relation is
  linear:

      N±(ΔA) = N± + (ΔA / 0.01) · dN±        (per event: wᵢ · (1 + (ΔA/0.01)·δᵢ))

**Output.** In `outputs/earlySeptember_corrected/injection/32b7cd1f9bd2/UL2018/`:
- `.coffea`: `{"UL2018_MC_mu_SemiLeptonic": {name: boost_histogram}}`, the 003-III layout
  with Double storage.
- the same as TH1D in `.root`
- `summary.json`

Histogram names: `Nplus_<b>`, `Nminus_<b>`, `dNplus_<model>_<b>` and `dNminus_<model>_<b>`,
with `<b>` = `mttGen005` (005 gen edges 300…1200 plus flow) or `mttGenFine` (25 GeV, 300–3000
plus flow).

**UL2018 checks (`summary.json`).**
- All 391 files: 15,225,592 region-A events, none without a gen sign.
- **The weighted yield is 657,242.12, against 003-III's 657,242.13** (ratio 0.99999999).
- N₊ = 329,152.5 and N₋ = 328,089.7, so gen A_C = +0.00162 for the selected region-A
  events. f_qq̄ = 0.0548.
- Per-fine-bin yield change below 2e-11.
- Reloading the files and adding 5 units gives ΔA = +0.05000 exactly for all three models.
- The 005-binned histograms stay positive (minimum bin 39 events).
- Validity ranges, as ΔA keeping every fine bin ≥ 0:
  - uniform 0.52, qqbar 0.21, mttRising 0.10.
  - The only negative fine bin, 2850–2875 GeV, is already −0.065 nominally (one
    negative-weight event).
  - If you reweight per event and need every weight ≥ 0, the range is ΔA ∈
    [−0.62, +0.26] for uniform, [−0.018, +0.028] for qqbar (noisy A^qq̄_j in sparse bins)
    and [−0.11, +0.047] for mttRising. For histograms, only the bin-level range matters.

```bash
python scripts/asymInjection/buildAsymInjectionHists.py --tag earlySeptember_corrected   # add --sample for 5 files
```

## UL2016preVFP: like-for-like vs 004C, and 004A kinematic-fit features (2026-10-02)

**Input** is the 004A reconstruction output (`--inputStage reconstruction`,
`reconstruction/earlySeptember_corrected/802fbf296122`):
- 117 ttbar_SemiLeptonic files, 6,884,570 events. That is exactly 004C's training sample.
- In region A: 3,867,398 events, with 0 jet-count mismatches.
- This input also gives the `kf_*` features: the same symmetric definitions, built from
  004A's `Top_had/Top_lep` fit.
- Extraction hash `62defcd4ad5a`, training hash `5128b8bb51ee` (`training_config_kf.yaml`).

**004C head-to-head** (`scripts/studies/compare004C.py`). 004C's per-event `BDTScore`
(`bdtScore/.../386b4160d6fa`) is joined by event onto this chapter's preVFP test folds.
That gives the same **1,160,884 region-A events** for both, all matched.

| model | AUC | qq̄ vs gg | qq̄ vs qg | qq̄ purity top 20 % | top 10 % | top 2 % |
|---|---|---|---|---|---|---|
| 004C BDT (optimistic, see below) | 0.7031 | 0.684 | 0.755 | 12.1 % | 15.1 % | 22.3 % |
| **xgb_reco** | **0.7170 ± 0.0012** | 0.700 | 0.765 | 12.6 % | 15.7 % | 23.6 % |
| xgb_reco_kf (+ 004A fit) | 0.7180 ± 0.0012 | 0.701 | 0.766 | 12.7 % | 15.8 % | 23.6 % |
| xgb_gen_oracle | 0.7111 | 0.693 | 0.761 | 13.3 % | 18.1 % | 30.7 % |

- **This chapter beats 004C by +0.014 AUC on identical events.** It also has higher
  purity at every working point.
  - 004C's split was random and was not saved with event IDs, so about 70 % of these
    events were in 004C's training set.
  - 004C's number is therefore optimistic, and the true margin is a little larger.
- **The 004A kinematic fit adds nothing** (+0.001, within the 0.0012 error).
  - On a test file, the fit's tt̄ resolution against gen is no better than this chapter's
    simple χ²: correlation with gen m_tt 0.708 vs 0.699, |y_tt| 0.794 vs 0.814,
    pT_tt 0.657 vs 0.741.
  - "Better tt̄ reconstruction" therefore needs something stronger than 004A's fit, for
    example a learned assignment.
- The AUC is the same as UL2018 within errors (0.717 vs 0.713), and the top features are
  the same: `sys_xmin`, `sys_pt`, `tt_pt`, `centrality`, `top_pt_max`.
- **gg sculpting check passes:** A_C per quartile is 0.0019 / 0.0013 / 0.0020 / −0.0004,
  each ±0.0023.
- The qq̄ train/test KS p-value is small again (2e-20), while qg and gg are fine. This is
  the same mild qq̄ overtraining as UL2018.

### Status / next steps (updated 2026-10-02)

**Done:**
- preVFP like-for-like vs 004C: +0.014.
- 004A fit features: no gain.
- PyTorch MLP / DeepSets / ParT. DeepSets is the best: 0.7230, or 0.7243 with qgl.

**Next:**
- Train DeepSets (or ParT) on **all eras combined**, since the models are still
  data-limited.
- A GPU would make full-statistics ParT practical.
- **A_C observable:** deploy DeepSets O_all in 005, using reco m_tt × O_all bins as the
  measured axis. Then:
  - validate O_all's inputs in data/MC (region A)
  - train on all eras
  - evaluate the systematics (JES, PDF and generator-model dependence of a learned
    observable)
- Stronger regularisation scan.
- Data/MC agreement of the top features in region A, before any use on data.
- Propagate the score into the A_C measurement as an enrichment category or a weight,
  and quote the gain in σ(A_C), not AUC.
