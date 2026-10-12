# Appendix-A — Generator-level ttbar Forward-Backward Asymmetry

A gen-level study of the ttbar forward-backward asymmetry A_FB in bins of the ttbar
invariant mass, measured on the **selectionII** `ttbar_SemiLeptonic` events. It is split
by production mechanism: qq̄, qg and gg.

The goal is a truth-level reference for the reco-level asymmetry measurement (004A →
005 → 006). It shows how large the asymmetry is at parton level, which production
channels carry it, and how much is lost when the unknown incoming-quark direction is
replaced by a proxy. Nothing here is reconstructed: every quantity comes from `GenPart`.

## Physics definition

    A_FB = (N+ - N-) / (N+ + N-)         N+ : cos θ* > 0     N- : cos θ* < 0

θ* is the angle between the **top quark** and the **incoming quark**, with both boosted into
the **ttbar rest frame**.

Events are **unweighted**. Only the one nominal `ttbar_SemiLeptonic` sample is used, so
every event counts equally. The statistical error is binomial:
σ(A_FB) = sqrt((1 − A_FB²) / N).

### Object identification

| object | definition |
|---|---|
| top / antitop | `GenPart` with pdgId `+6` / `−6` and `isLastCopy` (statusFlags bit 13) |
| incoming partons | `GenPart[0]` and `GenPart[1]` (status 21, `genPartIdxMother == −1`, `isHardProcess`) |
| Mtt | invariant mass of last-copy t + t̄ |

NanoAOD stores the incoming partons with **pt = 0**, which leaves `eta` at about ±23000.
Only the **sign of eta**, i.e. the direction along the beam, is usable. That is enough: the
reference is a massless particle along ±z, and rescaling its energy does not change its
direction after the boost.

### Production type and reference direction

| `Gen_prodType` | incoming partons | reference direction for θ* |
|---|---|---|
| `1` qq̄ | quark + antiquark | the **quark** |
| `2` qg | (anti)quark + gluon | the quark, **flipped** if it is an antiquark |
| `0` gg | gluon + gluon | the **ttbar boost direction** along z (sign of p_z^tt) |
| `−1` other | anything else (e.g. qq, or tops not found) | undefined; excluded from all counts |

On the preVFP run, **no event** landed in `other`.

For comparison, `Gen_cosThetaStar_boost` is also stored for every event. It always uses the
ttbar boost direction as the reference. This is the proxy a reco-level measurement is forced
to use, since the detector does not see which beam the quark came from. The gap between the
two variables is the **reference-direction dilution**.

## Pipeline

```
selectionII skims (003-II)
   │  [0] --generateSelectionIIDatasetJSON   scan disk → inputs/selectionII_{era}_datasets.json
   │  [1] --generateProcessListJSON          keep only config TargetDatasets → task list
   │  [2] --runGenStudy                      GenAsymmetryModule via runGenStudy.py (PostProcessor)
   ▼       (or --writeBashScript → scripts/run_all_{tag}.sh)
appendixA skims ({STORAGE}/appendixA/...)   run, luminosityBlock, event, Gen_*
   │  [3] --generateDatasetJSON              scan own output → appendixA_{tag}_{era}_datasets.json
   │  [4] --computeAFB                       computeAFB.py (RDataFrame, single pass)
   ▼
outputs/{tag}/{hash}/{era}/AFB/  AFB.json, AFB_vs_Mtt.png
```

### Files

| file | role |
|---|---|
| `config.yaml` | storage, eras, `TargetDatasets`, module list, `AFB.MttBins`, `AFB.Categories` |
| `scripts/run_all.py` | master driver; steps [0]–[4] above |
| `scripts/modules/GenAsymmetryModule.py` | NanoAODTools module, writes the `Gen_*` branches |
| `scripts/runGenStudy.py` | parallel PostProcessor runner (adapted from 004A's `runReco.py`) |
| `scripts/computeAFB.py` | counts N± per category × Mtt bin, writes JSON + plot |
| `scripts/generateDatasetJSON.py` | health-checked dataset-JSON scanner (same as other chapters) |
| `scripts/utils.py` | config hash, output dirs, storage resolution (same as other chapters) |
| `scripts/keep_and_drop_input.txt` | reads only `GenPart_*` and event IDs (fast) |
| `scripts/keep_and_drop_output.txt` | writes only event IDs + `Gen_*` (small output files) |

### Output branches

| branch | type | meaning |
|---|---|---|
| `Gen_Mtt` | F | gen ttbar invariant mass [GeV]; −1 if the tops are not found |
| `Gen_cosThetaStar` | F | cos θ* w.r.t. the reference in the table above; −99 if undefined |
| `Gen_cosThetaStar_boost` | F | cos θ* w.r.t. the ttbar boost direction, for every event |
| `Gen_prodType` | I | −1 other, 0 gg, 1 qq̄, 2 qg |

## Running

The environment is the same as 004A: the `latestcoffea` conda env plus
`standalone/env_standalone.sh`.

```bash
source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source ../standalone/env_standalone.sh

python3 scripts/run_all.py --tag earlySeptember_corrected --filter UL2016preVFP --workers 15 \
    --generateSelectionIIDatasetJSON \
        --selectionIITag earlySepetember_correted --selectionIIHash 37f710b13012 \
    --generateProcessListJSON --runGenStudy --generateDatasetJSON --computeAFB
```

- `--sample` processes one file per dataset, for a quick test (about 1 min).
- `--force` reprocesses files whose output already exists. Otherwise they are skipped.
- `--filter` works as in the other chapters (`era[/DataMC[/group[/dataset]]]`).
- `--printHash` prints the config hash and exits.
- Steps can be run individually, e.g. `--computeAFB` alone redoes the counting and plot.
  Any edit to `config.yaml` (including `MttBins`) changes the hash, and output is keyed
  by hash, so after an edit rerun the full chain [1]–[4]. It only takes minutes.
  To try other binning without that, call `scripts/computeAFB.py --mttBins ...` directly
  on the existing dataset JSON.

For UL2016preVFP the 003-II input sits under the **typo tag** `earlySepetember_correted`.
For UL2016postVFP it is `earlySeptember_corrected`. Both have the same hash, `37f710b13012`.

**Timing:** about 700 Hz per worker. The full preVFP sample (117 files, 6.9M events) took
about 16 min at `--workers 15`.

## Results — UL2016preVFP

Run: tag `earlySeptember_corrected`, hash `c37976e7a0e3`, 2026-09-28. The input is
selectionII `earlySepetember_correted/37f710b13012`, 6,884,570 events.

Production mix: **69% gg, 25% qg, 5.6% qq̄**.

A_FB with the true incoming-quark reference (`Gen_cosThetaStar`):

| Mtt [GeV] | all | qq̄ | qg | gg |
|---|---|---|---|---|
| inclusive | +0.0059 ± 0.0004 | **+0.0137 ± 0.0016** | +0.0216 ± 0.0008 | −0.0004 ± 0.0005 |
| 300–500 | +0.0050 ± 0.0005 | +0.0096 ± 0.0022 | +0.0211 ± 0.0010 | −0.0013 ± 0.0006 |
| 500–700 | +0.0061 ± 0.0007 | +0.0141 ± 0.0029 | +0.0221 ± 0.0013 | −0.0002 ± 0.0008 |
| 700–900 | +0.0089 ± 0.0013 | +0.0250 ± 0.0051 | +0.0212 ± 0.0025 | +0.0031 ± 0.0015 |
| 900–1100 | +0.0073 ± 0.0023 | +0.0321 ± 0.0088 | +0.0243 ± 0.0046 | −0.0015 ± 0.0028 |

Boost-direction proxy (`Gen_cosThetaStar_boost`), inclusive: all +0.0017, qq̄ +0.0037,
qg +0.0070, gg −0.0004. The per-bin values are in `AFB.json`.

What the numbers show:
- **qq̄ A_FB is positive and rises with Mtt**, from about 1% to about 3%, the expected trend.
- **gg is consistent with zero.** This is a sanity check: gg has no preferred direction.
- **qg carries about 2.2%.** At 25% of the sample it contributes most of the inclusive "all"
  asymmetry.
- **The boost proxy keeps only about 1/4 to 1/3 of the asymmetry**: for qq̄ 0.0137 → 0.0037,
  and for all events 0.0059 → 0.0017. This is the dilution the reco-level measurement pays
  before any reconstruction effects.

## Results — UL2016preVFP, pre-selection input (selection-bias check)

Run: tag `earlySeptember_preselection`, hash `c37976e7a0e3` (same config), 2026-09-30.
The input is 002-Samples preselection `earlySeptember/f7b250452aef`, 7,415,518 events.
Command: the one above with `--inputStage preselection --inputTag earlySeptember
--inputHash f7b250452aef`.

Note that preselection is itself cut (≥1 tight muon, ≥4 jets, ≥2 medium b-tags, HLT,
MET filters), so this checks the **003-I/II selection** on top of preselection, not the
full acceptance. SelectionII keeps 92.8% of preselection events.

Production mix is unchanged: 69.4% gg, 25.0% qg, 5.6% qq̄.

| Mtt [GeV] | all | qq̄ | qg | gg |
|---|---|---|---|---|
| inclusive | +0.0060 ± 0.0004 | **+0.0156 ± 0.0016** | +0.0213 ± 0.0007 | −0.0003 ± 0.0004 |
| 300–500 | +0.0053 ± 0.0005 | +0.0114 ± 0.0021 | +0.0209 ± 0.0010 | −0.0009 ± 0.0006 |
| 500–700 | +0.0060 ± 0.0006 | +0.0156 ± 0.0028 | +0.0216 ± 0.0013 | −0.0003 ± 0.0008 |
| 700–900 | +0.0090 ± 0.0012 | +0.0282 ± 0.0050 | +0.0208 ± 0.0025 | +0.0031 ± 0.0015 |
| 900–1100 | +0.0076 ± 0.0022 | +0.0325 ± 0.0085 | +0.0234 ± 0.0044 | −0.0008 ± 0.0027 |

Boost proxy, inclusive: all +0.0017, qq̄ +0.0041, qg +0.0066, gg −0.0003.

Since selectionII is a subset of preselection, the two tables are strongly correlated. The
proper test compares the kept events with the 7.2% that were **rejected** (independent samples):
- **all, qg, gg:** consistent (|z| ≤ 1.5 inclusive).
- **qq̄:** the rejected events have A_FB = +0.037 ± 0.006, against +0.0137 ± 0.0016 kept,
  **z = −4.1**. The selection lowers the qq̄ A_FB by 0.0018 (about 12% relative). It does
  not explain most of the "low qq̄" question below: pre-selection qq̄ is still only 1.6%.
- **Boost proxy:** qq̄ shows no significant shift (z = −0.9). The bias is in the true-quark reference only.

## Open items

- **The qq̄ size looks low.** A qq̄ A_FB of about 1.4% seems small compared with the few-percent
  NLO partonic expectation (not checked against a reference yet). It may be biased by the
  selectionII acceptance. The cross-check is to run the module on a handful of
  **pre-selection** ttbar files and compare. Done (see above): preselection gives 1.6%, so
  the selection accounts for only about 0.2%. The remaining gap needs a check on
  un-preselected NanoAOD or a theory reference.
- **Not yet run:** UL2016postVFP, UL2017 and UL2018.
- **Only the nominal ttbar_SemiLeptonic sample is used.** Other samples (e.g. MC_alt, FullyLeptonic)
  would only need a `TargetDatasets` entry in `config.yaml`.

## Storage

`STORAGE` in `config.yaml` is a dict keyed by machine. The key is matched as a substring of
`socket.gethostname()` and resolved in `utils.resolve_storage_path()`. Output skims go to
`{STORAGE}/appendixA/{tag}/{config_hash}/{era}/MC_mu/SemiLeptonic/ttbar_SemiLeptonic/`.
