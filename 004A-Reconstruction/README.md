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

**Standalone build (tested 2026-10-12).** Apart from its CMS-object (PAT) adapters, HitFit
depends only on CLHEP:
- **Build:** the 28 non-PAT source files, copied unmodified, compile with the `latestcoffea`
  conda compiler against conda-forge `clhep` 2.4.7.
- **Toy test (C++):** on HitFit's own toy generator (`gentop`), fitting the correct jet
  assignment and taking the better pz seed, 5000 events give mean χ² 2.11 and median 1.36
  (χ²(2): 2, 1.39). 79.6% pass Pgof > 0.2, against 80% for a calibrated χ²(2).
- **From Python:** loads in `latestcoffea`'s PyROOT (ROOT 6.36) through a small flat-number
  C++ wrapper, giving the same behaviour (83% pass Pgof > 0.2). Speed is about **2400 fits/s on
  one core**, against about 20 fits/s/core for the current SLSQP fit.
- Calling HitFit's classes directly from PyROOT crashes on incomplete class info, so the
  wrapper is required.

---

## 2. What the code does now

`scripts/modules/RecoModule.py` (`TTbarSemilepReconstructor`), run via `runReco.py`:

1. Solves the neutrino-pz quadratic from the leptonic-W mass constraint (1 or 2 roots; the real
   part if the discriminant is negative).
2. Builds the 4 permutations (2 b-jet assignments × pz roots) from `SelMuon`,
   `leadingbJet`/`subleadingbJet`, `leadingJet`/`subleadingJet` and `MET`. These come from
   003-ObjectSelectionI's `SelectedObjects`.
3. Runs a `scipy.optimize.minimize` (SLSQP) χ² fit on all 18 momentum components for **each**
   permutation. The W masses and the equal-top-mass condition enter as soft penalty terms
   (`sigmaW = 10`, `sigmatt = 13` GeV, `config.yaml`). The lowest post-fit χ² wins.
4. Writes `Top_lep_*`, `Top_had_*` (pt, eta, phi, mass), `Chi2`, `Chi2_prefit`,
   `Pgof = exp(−χ²/2)` and `chi2_status`:
   - `0`: fit converged;
   - `1`: upstream selection sentinel (no muon or jets);
   - `2`: degenerate pz quadratic;
   - `3`: no permutation converged, so the unfitted momenta are written. 005 rejects these.

---

## 3. Deviations from the reference (open)

Status 2026-10-12. Tracked in GitHub issue **#53** (tracker) and its children.

| # | deviation | issue |
|---|---|---|
| 1 | **Jet set.** 003-I takes the first 2 b-tagged jets from *all* jets (event cut ≥ 2 b-tags) plus the 2 leading remaining jets. The reference takes exactly 2 b-tags among the 4 leading jets. | #48 |
| 2 | **Neutrino pz is penalised as a measurement**: `((pz − pz_root)/(0.10·|pz_root|))²`. It must be unmeasured. | #49 |
| 3 | **Soft constraints** (SLSQP penalties with 10/13 GeV widths) instead of exact Lagrange-multiplier constraints. | #49 |
| 4 | **Unfitted fallback** (`chi2_status = 3`) has no counterpart in the reference. | #49 |
| 5 | **Resolution model.** Each Cartesian component gets a fixed fraction of itself (μ 5%, ν 10%, jets 15%, floor 1e-3 GeV). Near-zero components are frozen, the model is not rotation invariant, and the neutrino px/py uncertainty is far below the real `pT^miss` resolution. The reference uses per-object resolutions. | #50 |
| 6 | **Pgof cut not applied.** 005 uses `pgof_min: 0.0`; the reference applies Pgof > 0.2. Because of 2, 3 and 5, today's Pgof is not a calibrated probability. | #52 |
| 7 | **Matching radius** in the diagnostics is ΔR < 0.4 (`--partonDR`); the reference uses 0.3. | #51 |

**Planned implementation** (#49): vendor the HitFit core under `external/HitFit/` (as was done
for TUnfold, #30) with a flat-number C++ wrapper called from RecoModule (see §1.4). Resolution
sources (#50):
- **Jet pT:** UL JER `PtResolution` from the 003-I `jet_jerc.json` files, times the JER scale
  factor in data.
- **Jet η/φ and muon:** derived from MC.
- **MET:** the per-event `MET_covXX/XY/YY` already present in the skims.

Validation (#51) against §1.3 plus a χ²(2) calibration check. Then Pgof > 0.2 in 005 and a
full reprocessing under a new tag (#52).

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

`--filter`, `--force`, `--sample` and `--workers` work as in the other chapters.
Reconstruction is CPU-heavy (one SLSQP minimisation per permutation per event). Budget about
75 Hz aggregate at `--workers 15`, i.e. about 31 h per era. Do not extrapolate from a
single-file timing.

A code change to `RecoModule.py` does not change the config hash, so rerunning under the same
tag silently keeps the stale outputs. Use a new tag or `--force`.

**CRAB (lxplus only).** Same pattern as 003-ObjectSelectionI/II:

```
source scripts/crab/getcrabReady.sh
run_all.py --submitReconstructionJobs [--sample]
run_all.py --checkCrabStatus [--resubmitFailedCrabJobs] [--removeSubmitFailedCrabJobs]
run_all.py --generateDatasetJSON
```

- `scripts/crab/submit_reconstruction_flexible.py` builds `Data.userInputFiles` from
  `selectionII_{tag}_{era}_datasets.json`; the selectionII output is not DBS-registered.
- `scripts/crab/crab_script_reconstruction.py` resolves each LFN to
  `root://eosuser.cern.ch/...` directly.
- No golden JSON is shipped.
- `scipy` is not in the stock CMSSW python, so the CRAB script prepends the CVMFS LCG
  site-packages to `sys.path`, as 003-II does. The HitFit migration will need the HitFit and
  CLHEP libraries shipped with the job instead.

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
