# 004A-Reconstruction — tt̄ lepton+jets kinematic reconstruction

This README is the **single source of documentation for this chapter**. It always describes the
current strategy, and every change to the chapter adds a dated entry to the
[Changelog](#changelog) at the bottom. Reference papers live in [`References/`](References/).

Takes the `selectionII` skims and reconstructs the leptonic and hadronic top-quark candidates
event by event. No cut string or golden JSON is re-applied — both were already handled upstream.

---

## 1. Reference standard

The standard for this chapter is the lepton+jets kinematic reconstruction of
**CMS-TOP-23-003**, *Review of top quark mass measurements in CMS*, arXiv:2403.01313,
Sec. 2.3–2.3.1, pp. 12–14, Figs. 3–4 → [`References/TOP-23-003_arXiv2403.01313.pdf`](References/TOP-23-003_arXiv2403.01313.pdf).
Its lepton+jets fitter is **HitFit**. Anything this chapter does differently is a deviation to
close (§3), not an accepted design choice.

### 1.1 Which fitter

| channel | fitter | paper ref. | ndf |
|---|---|---|---|
| **lepton+jets** | **HitFit** | [117] D0, Phys. Rev. D 58 (1998) 052001, hep-ex/9801025 | **2** |
| all-jets | KinFitter | [118] J. D'Hondt et al., CMS-NOTE-2006-023 | 3 |

The lepton+jets legends in Figs. 3–4 read "HitFit …". KinFitter is not the reference here.

### 1.2 The prescription (lepton+jets)

- **Inputs:** the lepton, the **four highest-pT jets** and `pT^miss`, each with its resolution.
- **Jet assignment:** exactly **2 of the 4 leading jets** must be b-tagged, otherwise the
  event is not used. The 2 b-tagged jets are the b candidates, the 2 untagged jets the
  hadronic-W quarks.
- **Fit parameters:** the 3-momenta of the 6 decay products → **18 unknowns**. **17 are
  measured**; the **neutrino pz is unmeasured**.
- **Constraints (3):** m(ℓν) = m(qq′) = **80.4 GeV**, and **m(t_lep) = m(t_had)**, the two
  candidates equal to each other, *not* pinned to 172.5 GeV. Pinning would bias every event
  toward the input mass.
- **Hard constraints:** "the intrinsic width effects are negligible with respect to the
  experimental resolution". The fit minimises `χ² = (x − x_m)ᵀ G (x − x_m)` (G = inverse
  covariance of the measurements) with the constraints imposed by Lagrange multipliers.
- **ndf = 17 − 18 + 3 = 2.** This holds only if the neutrino pz carries **no** χ² term. The
  W-mass roots for pz are *starting values*, not measurements: a root is a function of the
  lepton and `pT^miss` after the leptonic-W constraint has been applied, so penalising pz
  double-counts that constraint and the χ² follows neither χ²(2) nor χ²(3).
- **Permutations:** 2 b-assignments × 2 neutrino-pz starting values = **4 fits per event**,
  all fitted. Swapping the two light jets is not a separate permutation.
- **Ranking:** by the χ² probability `Pgof = P(χ²; ndf=2) = exp(−χ²/2)`
  (= `TMath::Prob(χ², 2)`). At fixed ndf, ranking by Pgof equals ranking by χ².
- **Quality cut:** **Pgof > 0.2** (χ² < 3.22), efficiency **27.4%**. It matches the resolution
  of correct permutations with their pre-fit momenta.
- **Truth classification (simulation):** *correct* means all quarks matched to a selected jet
  within **ΔR < 0.3** with the right flavour and the right top; *wrong* means all matched but the
  wrong permutation was chosen; *unmatched* means otherwise.
- The paper's lepton+jets mass measurement uses multiple permutations (weighted by Pgof). For a
  per-event observable like ours, the best-ranked permutation is used.

### 1.3 Validation targets (paper, lepton+jets simulation, Fig. 3 left)

σ_peak = Gaussian width fitted to `m_t^rec − m_t^gen` in [−40, 40] GeV:

| case | σ_peak |
|---|---|
| HitFit, correct permutation | 13 GeV |
| HitFit, Pgof > 0.2 | 16 GeV |
| HitFit, lowest χ² | 21 GeV |
| HitFit, all permutations | 25 GeV |
| No fit, inclusive | 30 GeV |
| No fit, correct permutation | 19 GeV |

With the Pgof cut, the reconstructed m_tt is "almost free of bias" against generator m_tt
(Fig. 4: bias = mean, resolution = RMS of `m_tt^rec − m_tt^gen`).

### 1.4 HitFit in CMSSW, and standalone

HitFit is `TopQuarkAnalysis/TopHitFit` in CMSSW (e.g.
`/cvmfs/cms.cern.ch/el9_amd64_gcc12/cms/cmssw/CMSSW_14_2_3/src/TopQuarkAnalysis/TopHitFit`).
From its source:

- `Fourvec_Constrainer.cc` declares `nu_z` as its single unmeasured variable.
- `Top_Fit.cc` seeds pz from both roots (`Top_Decaykin::solve_nu`, or `solve_nu_tmass` when
  that flag is set; the shipped example config sets it to `true`).
- `Constrained_Top.cc`: `lepw_mass = hadw_mass = 80.4`, `equal_side = true`, and `top_mass = 0`
  (no fixed top mass) give exactly the paper's three constraints. The CMS producer
  `TtSemiLepHitFitProducer_Muons_cfi.py` uses `maxNJets = 4`, `mW = 80.4`, `mTop = 0`.
- Resolutions use the form `σ(x) = √(C²x² + R²x + N²)` for each of p (or E/Et), η and φ, plus
  a kt (recoil) resolution for the transverse imbalance. The shipped `tqaf*Resolution.txt` tables
  are old defaults.

**Vendored in this repo: `external/HitFit/`** (since 2026-10-12; see its README).
- **What is copied:** the 26 core sources, unmodified, from `CMSSW_14_2_3`. They are built
  against a private conda-forge CLHEP 2.4.7 prefix by `external/HitFit/build.sh`.
- **How 004A calls it:** through a flat-number C++ wrapper (`hitfit_wrap::Fitter`) loaded
  into `latestcoffea`'s PyROOT by `external/HitFit/hitfit_loader.py`. Calling HitFit's own
  classes from cppyy crashes, so the wrapper is required.
- **Toy calibration** (`external/HitFit/test/toy_test.py`, run by `build.sh`): with fit
  resolutions equal to the smearing, χ² of the true assignment follows χ²(2). Median 1.378
  (χ²(2): 1.386); 77.9% pass Pgof > 0.2 (χ²(2): 80%).

---

## 2. What the code does now

`scripts/modules/RecoModule.py` (`TTbarSemilepReconstructor`), run via `runReco.py`. HitFit
fit since 2026-10-12 (#49):

1. Takes `SelMuon`, `leadingbJet`/`subleadingbJet`, `leadingJet`/`subleadingJet` and `MET`
   from 003-ObjectSelectionI's `SelectedObjects`. The jet choice is deviation 1 in §3.
2. Fits **4 permutations** with HitFit: 2 b-jet assignments × 2 neutrino-pz starting values.
   - The pz seeds are the roots of the leptonic-W quadratic (`solve_nu_tmass: false`).
   - When the discriminant is negative both roots collapse to the real part, so that seed is
     fitted once.
   - The neutrino pz is **unmeasured**.
   - The constraints are **hard**: m(ℓν) = m(qq′) = `mW` = 80.4, and m(t_lep) = m(t_had).
3. Ranks the converged permutations by χ² (ndf = 2) and keeps the best.
4. Writes the following branches:
   - `Top_lep_*`, `Top_had_*` (pt, eta, phi, mass of the fitted tops);
   - `Chi2`, and `Pgof = TMath::Prob(Chi2, 2)`;
   - `Nu_pz` (fitted neutrino pz), `Mt_fit` ± `Mt_fit_err` (HitFit's fitted top mass);
   - `perm_index` (= 2·[b swapped] + [pz root]) and `n_converged` (0–4);
   - `chi2_status`, with these codes:

   | code | meaning |
   |---|---|
   | `0` | converged |
   | `1` | upstream selection sentinel (no muon or jets) |
   | `2` | retired (was the degenerate pz quadratic of the old fit) |
   | `3` | no permutation converged; **sentinel −1 outputs**, the event is not reconstructed (005 rejects it) |
   | `4` | an object's η is outside its resolution table |

Settings live in `config.yaml` under `Modules.reconstruction`:
- `hitfit.settings`: HitFit's `Top_Fit`, `Constrained_Top` and `Chisq_Constrainer`
  parameters;
- `hitfit.resolutions`: η-dependent resolution tables. Currently the **upstream tqaf
  placeholders**, until #50.

**First check on real events** (2026-10-12). 2000 UL2016preVFP `ttbar_SemiLeptonic` events
(selectionII hash `37f710b13012`, file `_0`), compared with the previous SLSQP fit on the same
events, parton ΔR < 0.4:

| | previous SLSQP | HitFit | paper (§1.3) |
|---|---|---|---|
| σ_peak(m_t,had), correct assignment | 15.9 GeV | **10.8 GeV** | 13 GeV |
| σ_peak, Pgof > 0.2 | 17.5 GeV | **16.0 GeV** | 16 GeV |
| σ_peak, best permutation, all events | 22.3 GeV | 23.7 GeV | 21 GeV |
| Pgof > 0.2 efficiency | 28.6% | **26.9%** | 27.4% |
| correct assignment (correct / reconstructible) | 21.7% (78%) | 20.9% (75%) | — |
| Pgof > 0.2 among correct events (calibrated: 80%) | 66% | 62% | — |
| events with no converged fit | 0 (fallback) | 3 / 2000 | — |
| speed (single process) | ~5 events/s | **~194 events/s** | — |

Pgof among correct events is still below the calibrated 80%, as expected with placeholder
resolutions (#50). The full validation is #51.

---

## 3. Deviations from the reference (open)

Status 2026-10-12. Tracked in GitHub issue **#53** (tracker) and its children.

| # | deviation | issue |
|---|---|---|
| 1 | **Jet set.** 003-I takes the first 2 b-tagged jets from *all* jets (event cut ≥ 2 b-tags) plus the 2 leading remaining jets. The reference takes exactly 2 b-tags among the 4 leading jets. | #48 |
| ~~2~~ | ~~Neutrino pz penalised as a measurement.~~ **Closed 2026-10-12**: HitFit treats pz as unmeasured. | #49 |
| ~~3~~ | ~~Soft constraints.~~ **Closed 2026-10-12**: HitFit's hard constraints. | #49 |
| ~~4~~ | ~~Unfitted fallback.~~ **Closed 2026-10-12**: no converged permutation now gives sentinel outputs (`chi2_status = 3`). | #49 |
| 5 | **Resolutions.** The per-component percentage model is gone, but HitFit still runs on the upstream **tqaf placeholder** tables (old, not Run-2 UL). Per-era UL tables are still needed. | #50 |
| 6 | **Pgof cut not yet validated.** 005's `pgof_min` was set to the reference 0.2 on 2026-10-12, but 005 has not been rerun, and until 5 is closed Pgof is not a calibrated probability. | #52 |
| 8 | **pz seeding choice.** The paper does not say whether the two pz starting values come from the W mass (our setting) or from equal top masses (`solve_nu_tmass`, CMSSW example config). | #51 |
| 9 | **CRAB path untested on the grid.** Fixed 2026-10-12: HitFit is built in the submitting CMSSW release and shipped with each job. The job was emulated locally in `CMSSW_13_3_0`, giving outputs identical to local running, but no real submission has been made yet. | #54 |
| 7 | **Matching radius** in the diagnostics is ΔR < 0.4 (`--partonDR`); the reference uses 0.3. | #51 |

**Next steps.**
- **#50 resolution sources:**
  - jet pT: UL JER `PtResolution` from the 003-I `jet_jerc.json` files, times the JER scale
    factor in data;
  - jet η/φ and muon: derived from MC;
  - MET: the per-event `MET_covXX/XY/YY` already in the skims.

  They are written as per-era HitFit `EtaDepResolution` tables and pointed to from
  `config.yaml`.
- **#51:** validation against §1.3, the χ²(2) calibration check, and the comparison of the
  two pz seeding modes.
- **#52:** Pgof > 0.2 in 005 and a full reprocessing under a new tag.

---

## 4. Inputs, outputs, running

**Inputs.** `selectionII_{era}_datasets.json`, built from the 003-ObjectSelectionII output on
disk via `--generateSelectionIIDatasetJSON --selectionIITag <tag> --selectionIIHash <hash>`.
It is written into `inputs/` and into this run's `outputs/{tag}/{hash}/inputs/` snapshot.

**Outputs.**
- Skim ROOT files:
  `{STORAGE}/reconstruction/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/*_Skim.root`
- `reconstruction_{tag}_{era}_datasets.json` (`--generateDatasetJSON`), the input for
  004B-BDTVariables.

**Running.**

```
run_all.py --generateSelectionIIDatasetJSON --selectionIITag <tag> --selectionIIHash <hash>
run_all.py --generateProcessListJSON
run_all.py --writeBashScript
scripts/run_all_{tag}.sh
run_all.py --generateDatasetJSON
```

**Build HitFit once per machine** before running:
`conda activate latestcoffea && external/HitFit/build.sh`. `run_004A_workflow.sh` stops if
`external/HitFit/libHitFit.so` is missing; a direct `runReco.py` run would instead fail every
file.

`--filter`, `--force`, `--sample` and `--workers` work as in the other chapters. With HitFit, a
single process reconstructs about 194 events/s, against about 5 for the old SLSQP fit. The
old fit's era budget (~31 h at `--workers 15`) no longer applies. Measure a full-era rate
under contention before planning; do not extrapolate from a single-file timing.

A code change to `RecoModule.py` does not change the config hash, so rerunning under the same
tag silently keeps the stale outputs. Use a new tag or `--force`.

**CRAB (lxplus only).** Same pattern as 003-ObjectSelectionI/II:

```
source scripts/crab/getcrabReady.sh        # cmsenv (CMSSW_13_3_0), builds HitFit for it if needed, CRAB, proxy
run_all.py --submitReconstructionJobs [--sample]
run_all.py --checkCrabStatus [--resubmitFailedCrabJobs] [--removeSubmitFailedCrabJobs]
run_all.py --generateDatasetJSON
```

- **HitFit in the jobs.** CRAB jobs run in the submitting CMSSW release, so `libHitFit.so`
  must be built there (`cmsenv && external/HitFit/build.sh`; CMSSW mode uses the release's
  gcc and CLHEP).
  - `getcrabReady.sh` rebuilds it automatically unless `libHitFit.buildinfo` already records
    this `CMSSW_VERSION`/`SCRAM_ARCH`.
  - `submit_reconstruction_flexible.py --submit` refuses a library built anywhere else, such
    as the local conda build.
  - Rebuild with conda before running locally again.
- **What each job ships.** `submit_reconstruction_flexible.py::sandbox_files()` sends, flat:
  - `crab_script_reconstruction.py`, `RecoModule.py`, `config.yaml`;
  - `hitfit_loader.py`, `libHitFit.so`, `HitFitWrapper.h`;
  - the resolution tables named in `config.yaml`.

  The job script re-points the resolution paths at the shipped copies. CLHEP is found through
  the library's RPATH on CVMFS.
- **Job Python.** It needs only numpy, PyYAML and PyROOT from the release. The old scipy fit
  needed the LCG `sys.path` workaround; that has been removed.
- **Inputs.** `submit_reconstruction_flexible.py` builds `Data.userInputFiles` from
  `selectionII_{tag}_{era}_datasets.json`; the selectionII output is not DBS-registered. The
  job script resolves each LFN to `root://eosuser.cern.ch/...` directly, and uses a PSet
  entry as-is if it is an existing local file (for local tests).
- No golden JSON is shipped.
- **Local emulation, 2026-10-12.** A flat job directory assembled from `sandbox_files()` ran
  `crab_reconstruction.sh` in a `CMSSW_13_3_0` (el9) area from CVMFS: 126 events/s, and
  `tree.root` was identical, event by event, to the local conda run. **A real lxplus/grid
  submission is still to be tested (#54).**

**Storage.** `STORAGE` in `config.yaml` is keyed by machine (substring of
`socket.gethostname()`) and resolved in `utils.resolve_storage_path()`.

---

## 5. Diagnostics

`--makeDeltaPlots` runs `scripts/deltaMassPlots.py`. It is MC only, reading the
`ttbar_SemiLeptonic` datasets because it needs the generator tops:

- `deltaMass_hadronic.png`, `deltaMass_leptonic.png`: `m_t^reco − m_t^gen` residuals.
- `pgof.png`: Pgof split into correctly reconstructed and other events.
- `pgof_purity.png`: differential purity `s/(s+b)` per Pgof bin.
- `pgof_efficiency.png`: cumulative signal efficiency and purity for a `Pgof > cut`
  selection, plus efficiency against the physical ceiling.

`--pgofBins` sets the binning across Pgof in [0, 1].

**Truth matching** is done at parton level, in two steps:
1. An event is *reconstructible* when both b partons and both hadronic-W quarks are matched,
   one-to-one within `--partonDR` (default 0.4; the reference uses 0.3), by the four jets the
   fit is given. That fraction is the **physical ceiling** for this permutation set.
2. Among reconstructible events, the fit is *correct* when it chose the right b-jet for the
   hadronic side. This is read back by comparing the fitted `Top_had` direction with the two
   `b + lj + slj` hypotheses.

A `|m_t^reco − m_t^gen|` window is deliberately **not** used. The W-mass and equal-top-mass
constraints pull any assignment toward a top-like mass, and the generator mass is narrow
around 172.5 GeV, so a mass window partly selects on the quantity it is meant to validate.

---

## 6. Study record: jet-assignment purity (September 2026)

Measured on UL2016preVFP `ttbar_SemiLeptonic`, tag `earlySeptember_corrected`, hash
`802fbf296122`, **with the current (pre-reference) fit**. Study script:
`scripts/studies/lightjet_permutation_gain.py`. These are background for #40 and #51. The
proposals below that enlarge the permutation set or add an m_t prior are **departures from the
reference**, so they would have to be justified against it.

- **Correct assignment** (parton ΔR < 0.4 on all four jets): **21.2%**, against an
  acceptance-limited physical ceiling of **52.1%**. The ceiling for the current 4-permutation
  set is 27.5%, of which the fit converts 77%.
- **The light-jet pair is the binding constraint.** The hadronic W is always `leadingJet +
  subleadingJet`, but the W quarks are often not the two leading untagged jets. Correct rate
  by jet multiplicity: 31.0% (4 jets), 15.4% (5), 8.2% (6), 3.6% (≥ 7). b-tagging is not the
  bottleneck: 84.7% of events have both b-jets truly b-flavoured.
- **Options measured** (achieved / ceiling / fit cost):
  - all light pairs (P1): 28.4% / 44.7% / 3×;
  - P1 plus permuting all b-tagged jets: 28.9% / 47.5% / 3.9×;
  - P1 plus an m_t-prior re-ranking: 34.0% / 44.7% / 3×.

  χ² ranking converts a falling share of the ceiling as the set grows (77% → 64% → 61%), so
  the headroom is in ranking, not enumeration.
- **Background separation** with P1: six of seven backgrounds improve by 6–8 points at 50%
  signal efficiency. QCD is worse by 4 points.
- **Acceptance:** the softer W quark is the problem (median pT 33 GeV; 29% below 25 GeV).
  Lowering the reconstruction jet pool to 20 GeV raises the ceiling by 6.4 points and to
  15 GeV by 11.7 points. The achieved rate at those thresholds was not measured.
- **Do not** define "correct" with a `|m_reco − m_gen| < 30 GeV` window: it gives about 55%
  against 21% for parton matching.

---

## 7. References

1. CMS Collaboration, *Review of top quark mass measurements in CMS*, CMS-TOP-23-003,
   arXiv:2403.01313. **Reference standard.**
   [`References/TOP-23-003_arXiv2403.01313.pdf`](References/TOP-23-003_arXiv2403.01313.pdf)
2. D0 Collaboration, *Direct measurement of the top quark mass at D0*, Phys. Rev. D 58 (1998)
   052001, hep-ex/9801025. HitFit, ref. [117] of (1).
3. J. D'Hondt et al., *Fitting of event topologies with external kinematic constraints in CMS*,
   CMS-NOTE-2006-023. KinFitter, all-jets only, ref. [118] of (1).
4. CMSSW `TopQuarkAnalysis/TopHitFit` (source, `data/setting/RunHitFitConfiguration.txt`,
   `python/TtSemiLepHitFitProducer_Muons_cfi.py`), on CVMFS under
   `/cvmfs/cms.cern.ch/el9_amd64_gcc12/cms/cmssw/CMSSW_14_2_3/src/`.
5. GitHub issues (mukundshelake/NanoAODTools): **#53** tracker; #48 jet selection; #49 HitFit
   fitter; #50 resolutions; #51 validation; #52 Pgof cut and reprocessing. Related: #40
   (dilution D), #42 (pathological reco values), #47 (b-tag efficiency maps).

---

## Changelog

Newest first. Add an entry for every change to this chapter: code, config, strategy or docs.

- **2026-10-12** — CRAB path repaired for HitFit:
  - `external/HitFit/build.sh` gained a CMSSW mode (release gcc and CLHEP) and writes
    `libHitFit.buildinfo`;
  - `getcrabReady.sh` builds HitFit for the release;
  - `submit_reconstruction_flexible.py` ships HitFit and the resolution tables
    (`sandbox_files()`) and checks the library's build environment;
  - `crab_script_reconstruction.py` drops the scipy/LCG workaround, re-points resolution
    paths at the shipped copies, and accepts local files.

  Emulated locally in `CMSSW_13_3_0`, with outputs identical to the local run. Grid test
  pending (#54). In 005, `pgof_min` was set to the reference 0.2 (#52; not yet run there).
- **2026-10-12** — **RecoModule now fits with HitFit** (#49), replacing the SLSQP
  soft-constraint fit:
  - the neutrino pz is unmeasured and the constraints are hard;
  - permutations are ranked by χ², with `Pgof = TMath::Prob(χ², 2)`;
  - no permutation converged now gives `chi2_status = 3` with sentinel outputs, where before
    the unfitted momenta were written.

  Branches: `Chi2_prefit` removed (unused downstream); added `Nu_pz`, `Mt_fit`, `Mt_fit_err`,
  `perm_index` and `n_converged`.
  `config.yaml`: `mt`, `sigmaW` and `sigmatt` removed; `hitfit.settings` and
  `hitfit.resolutions` added (tqaf placeholders until #50). This changes the config hash.
  HitFit is vendored in `external/HitFit/` (build with `build.sh`), and
  `run_004A_workflow.sh` checks the library is built. The first comparison on 2000 tt̄
  events is in §2. The CRAB path is broken until HitFit is shipped with the jobs.
- **2026-10-12** — Documentation consolidated into this README. Removed
  `leptonJets_kinFit_prescription.md` and `ISSUE_jet_assignment_purity.md`; their content is in
  §1–§3 and §6. Added `References/` with the TOP-23-003 paper.
- **2026-10-12** — Standalone HitFit feasibility test (§1.4): builds unmodified against CLHEP,
  gives a χ²(2)-calibrated Pgof on toys, and is callable from PyROOT through a C++ wrapper. Not
  yet in the repo (#49).
- **2026-10-12** — Opened #48–#53 to implement the reference exactly.
- **2026-10-12** — **TOP-23-003 declared the reference standard**, with HitFit identified as
  its lepton+jets fitter (not KinFitter). The old soft-penalty "accepted design choice" wording
  was withdrawn; the deviations are listed in §3. The colleague's finding that the neutrino pz
  is penalised as a measurement (so ndf ≠ 2) was confirmed. Comments in `RecoModule.py` and
  `deltaMassPlots.py` now cite the paper. Corrected the stale statement that the fit pins m_t
  to 172.5 GeV.
- **2026-09-26** — Light-jet permutation study (`scripts/studies/lightjet_permutation_gain.py`,
  §6), for #40.
- **2026-09-20** — Pgof diagnostics truth-matched at parton level (reconstructible / correct,
  physical ceiling) instead of a mass window (`deltaMassPlots.py`).
- **2026-09-20** — Added the 004A workflow driver (`run_004A_workflow.sh`) and the Pgof
  diagnostic plots (`--makeDeltaPlots`).
- **2026-09-04** — Previous-chapter dataset JSONs are regenerated from disk
  (`--generateSelectionIIDatasetJSON`) instead of fetching a stale copy.
- **2026-08-21** — CRAB support audited and fixed (status checking, resubmission).
- **2026-08-20** — CRAB submission added (`scripts/crab/`).
- **2026-08-19** — RecoModule brought closer to the reference text:
  - the full fit now runs on **all 4 permutations**, ranked by post-fit χ², where before only
    the pre-fit-best permutation was fitted;
  - the top-mass terms changed from **two terms pinning each top to 172.5 GeV** to one
    **equal-mass** term;
  - `chi2_status` codes 1 and 2 were added.

  About 4× slower. χ²/Pgof scales are not comparable with earlier outputs. Added the previous
  README and `leptonJets_kinFit_prescription.md`, an extract of the paper section formerly
  kept as `recoDoc.pdf`.
- **2026-08-09** — RecoModule rewritten in its current SLSQP soft-constraint form. Removed
  `recoDoc.pdf`, `recoDoc_extracted.txt`, `QUICKREF.sh` and the old histogram/plotter scripts.
- **2026-06-18** — Cleanup: old README removed.
- **2026-04-15** — Added `recoDoc.pdf` (the paper's reconstruction section) and its text
  extract.
- **2026-02-27** — Initial run scripts and pipeline integration.
