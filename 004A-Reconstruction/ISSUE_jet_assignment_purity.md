# Jet-assignment purity in 004A reconstruction: 21% correct against a 52% ceiling

*Written as a GitHub issue body — paste into `mukundshelake/NanoAODTools` as-is.
All numbers measured on UL2016preVFP `ttbar_SemiLeptonic`, tag
`earlySeptember_corrected` / hash `802fbf296122`, September 2026. No code was
changed as a result of this study; it is a record of what was measured and what
it implies.*

## Summary

`RecoModule` assigns jets to the right ttbar decay partons **21.2%** of the
time. The acceptance-limited ceiling is **52.1%**, so the algorithm recovers
41% of what is physically reachable.

The single change with the best return is extending the permutation set over
**light-jet pairs**, which is a 004A-only change costing ~3x the fit time:
**21.2% -> 28.4%**. Everything else measured was either not worth its cost or
carries a physics bias that needs a systematic.

## How "correct" is defined (and a pitfall)

An event counts as correctly assigned when all four jets the fit used are
within `DeltaR < 0.4` of the parton they represent: the b from the
hadronically-decaying top, the b from the leptonic top, and the two quarks
from the hadronic W (compared as an unordered pair). Partons are found by
walking `GenPart_genPartIdxMother`.

**Do not use `|m_t^reco - m_t^gen| < window` for this.** The generator top mass
is 172.2 +- 6.0 GeV, so 99% of gen tops fall within 30 GeV of 172.5 — any fit
producing a top-like mass passes, whether or not the jets are right, and the
criterion is outright circular for a variant that pins the mass to 172.5. The
two definitions disagree badly:

| definition | "correct" rate |
|---|---|
| `\|m_reco - m_gen\| < 30 GeV` | ~55% |
| parton `DeltaR < 0.4` on all four jets | **21%** |

Note `deltaMassPlots.py`'s `--matchWindow` signal definition is the mass-window
kind, so its purity plots report "top-like mass", not "correct jets". Adding a
parton-matched definition alongside it would be worth doing.

## Root cause

`RecoModule.analyze` builds only 4 permutations: 2 b-jet leg assignments x 2
neutrino pz roots. The hadronic W is **always** `leadingJet + subleadingJet`,
i.e. the two highest-pT non-b-tagged jets. The light-jet *pair* is never
permuted, and which jets are b-candidates is never permuted.

- Mean selected jets: **4.84**; 52.2% of events have >=5, so an average of
  **3.15 distinct light-jet pairings exist and exactly 1 is tried**.
- Among events the truth calls correctly assigned, the pre-fit
  `leadingJet + subleadingJet` mass has median **92.7 GeV** against
  m_W = 80.4, with only 47.4% inside +-20 GeV — the pair is frequently not
  the W daughters.
- b-tagging is *not* the bottleneck: 84.7% of events have both b-jets truly
  b-flavoured, and purity among flavour-consistent events is only 58.0%
  against 55.2% inclusive (mass metric).
- Accuracy collapses with jet multiplicity. Correct-assignment rate by
  selected-jet count: **31.0% (4 jets), 15.4% (5), 8.2% (6), 3.6% (>=7)** —
  against a physical ceiling of 44.7 / 55.9 / 62.3 / 64.1%.

## Options measured

All with parton truth matching, 8-10k events, statistical error ~+-0.5%.

| option | achieved | ceiling | conversion | fit cost | wall/era |
|---|---|---|---|---|---|
| current | 21.2% | 27.5% | 77.0% | 1.0x | 33 h |
| m_t-prior re-ranking only | 24.2% | 27.5% | 88.0% | 1.0x | 33 h |
| **extended light pairs (P1)** | **28.4%** | 44.7% | 63.5% | **3.0x** | ~100 h |
| P1 + permute all b-tagged (P2) | 28.9% | 47.5% | 60.9% | 3.9x | ~132 h |
| P1 + m_t-prior re-ranking | 34.0% | 44.7% | 76.1% | 3.0x | ~100 h |
| *physical ceiling* | *52.1%* | | | | |

Ceiling-only scan of b-candidate policies (no fits needed — a set either
contains the correct assignment or not):

| policy | ceiling | % of physical | mean perms |
|---|---|---|---|
| current | 27.47% | 52.6% | 4.0 |
| + every light pair | 44.72% | 85.7% | 12.0 |
| + all b-tagged permuted | 47.48% | 91.0% | 15.8 |
| + top-3 by b-tag | 49.93% | 95.7% | 36.1 |
| + top-4 by b-tag | 51.17% | 98.1% | 72.2 |

### Key result: ranking is the bottleneck, not the permutation count

Look at the conversion column. **chi2-ranking converts a declining share of
the ceiling as the set grows: 77.0% -> 63.5% -> 60.9%.** Extra permutations
offer more ways to be confidently wrong — going from the current set to P1
gained 8.8% of events and *lost* 5.3%. Raising the ceiling further is not the
lever.

This is why:

- **P2 is not worth it**: +0.50 +- 0.45 points for +30% cost. It works only on
  the 10.7% of events with a third b-tag (15.8% -> 20.5% there).
- **top-3 / top-4 b-candidates are not worth it**: +2.5 and +3.7 ceiling points
  for ~300 h and ~600 h per era.
- **The m_jj prior does nothing** (21.24% -> 21.24%): the chi2 already contains
  `((m_jj - 80.4)/sigmaW)^2`, so it re-weights information already used.
- **The anchored refit** (replace the equal-top-mass penalty with two terms
  pinning each top to 172.5) gives 33.98% vs the m_t-prior re-ranking's 34.00%
  — identical. So there is no reason to change the chi2 objective; only the
  permutation *selection* matters.

## Recommendation

**Implement P1 (extended light-jet pairing) with plain chi2 ranking, and pass
the m_t-consistency term to 004B as a BDT feature rather than using it to rank.**

- banks +7.3 points unconditionally (21.2% -> 28.4%)
- ~100 h per era, 3x fit cost
- 004A-only (see below), no upstream re-run
- injects no mass information, so `m_reco - m_gen` stays an honest diagnostic
  and no top-mass selection bias enters 005-Unfolding

The m_t prior is worth +5.5 points on top (28.4% -> 34.0%) and its gain is
**real, not circular** — it is measured against parton truth, and preferring
permutations near 172.5 genuinely correlates with the true assignment because
real tops weigh 172.5. The reason to keep it out of the reconstruction is
downstream bias, not validity: it biases the selected sample's reconstructed
mass distribution toward 172.5 and needs a systematic for any top-mass
sensitive unfolded observable. Exposing it as a BDT input keeps it visible and
assessable instead of baked into the skims.

### Implementation notes

**No 003-ObjectSelectionI change is required.** Re-deriving 003-I's jet
selection inside 004A from the `Jet_*` collection reproduces all four stored
scalar jets (`leadingbJet_*`, `subleadingbJet_*`, `leadingJet_*`,
`subleadingJet_*`) **exactly, 100.00% on 30,000 events** — the `Jet_pt` in the
selectionII files is the post-JetJER collection 003-I selected on, and all
seven needed branches (`Jet_pt/eta/phi/mass/jetId/puId/btagDeepFlavB`) survive
into 004A's input.

The cost is config duplication: the jet selection currently lives only in
003-I (`config.yaml`'s per-era `jetCut` string, and `SelectedObjects.py`'s
per-era `bTagThreshold`: 0.2598 / 0.2489 / 0.3040 / 0.2783 for preVFP /
postVFP / 2017 / 2018). Mirror it in 004A's `config.yaml` as a `JetSelection`
block and **assert at runtime** that the re-derived leading four jets match the
stored scalar branches, failing loudly on mismatch. 003-I already writes those
four jets, so the check is free and converts silent config drift into a crash.

Also cap the light-jet candidate list (`MAX_LIGHT = 5` truncated 2.2% of
events) to bound worst-case cost.

## Background separation: checked, not a problem

The wider search lowers chi2 for background events too, which could have
eroded the process-level discrimination 004B/004C rely on. Measured, it does
not. Background efficiency at 50% signal efficiency:

| background | baseline | extended | |
|---|---|---|---|
| QCD | 10.42% | 14.66% | **+4.25 worse** |
| W+jets | 25.34% | 17.95% | -7.39 better |
| Drell-Yan | 33.36% | 25.69% | -7.67 better |
| ttbar fully-leptonic | 24.89% | 19.08% | -5.81 better |
| single top t-chan | 27.68% | 20.61% | -7.08 better |
| tW | 39.99% | 33.88% | -6.12 better |
| Data | 42.33% | 39.67% | -2.66 better |

Six of seven improve by 6-8 points. Signal's chi2 drops further than most
backgrounds' because a real ttbar event *has* a correct pairing to find.

Two qualifications: the gains concentrate at **loose** cuts (at 20-30% signal
efficiency they shrink to 1-2 points and a few backgrounds go marginally
worse), and **QCD is consistently worse** at every operating point — it has the
highest jet multiplicity (5.12 vs signal's 4.86) and by far the largest chi2
gain (-52.1 vs -7.3). QCD is estimated data-driven via ABCD in 003-III rather
than from this MC, so whether that matters depends on the ABCD regions staying
valid under a chi2/Pgof cut — worth a check.

## Raising the ceiling itself (separate, larger decision)

52.1% is acceptance, not algorithm. Only 61.2% of events have both hadronic-W
quarks matched to any jet. The softer W quark is the whole problem:

| parton | p5 | p10 | p25 | median |
|---|---|---|---|---|
| b (hadronic leg) | 33.2 | 39.4 | 53.0 | 74.2 |
| b (leptonic leg) | 33.8 | 39.6 | 53.0 | 75.0 |
| W quark, harder | 38.4 | 43.1 | 53.4 | 71.0 |
| **W quark, softer** | **10.3** | **14.6** | **23.1** | **33.4** |

29% of events have the softer W quark below the current 25 GeV threshold. But
only part of the loss is threshold-recoverable — pooling both W quarks:

| reason for no matched jet | share | fixable by pT? |
|---|---|---|
| fine | 78.5% | — |
| below pT 25 | **7.4%** | **yes** |
| no jet within DeltaR 0.4 at all | 9.6% | no (unreconstructed/merged) |
| \|eta\| > 2.4 | 4.4% | no |

Ceiling and combinatorics vs threshold:

| threshold | ceiling | gain | mean jets | mean light pairs |
|---|---|---|---|---|
| 30 GeV | 37.56% | -14.3 | 4.37 | 2.20 |
| **25 GeV (current)** | **51.83%** | — | 4.85 | 3.19 |
| 20 GeV | 58.23% | +6.39 | 5.23 | 4.36 |
| 15 GeV | 63.52% | +11.69 | 5.86 | 6.64 |
| no cut | 63.73% | +11.89 | 5.93 | 6.95 |

15 GeV is the floor (NanoAOD stores down to ~15.5 GeV at p5).

**This need not touch 003-I either.** `jetCut` is the *event selection* and
changing it would move acceptance, ABCD regions, SFs and efficiency maps — a
real physics decision. But the reconstruction's *jet pool* is separable: keep
event selection at `pt > 25, >= 4 jets` exactly as is, and let 004A build
permutations from jets down to 20 GeV. Same events, same acceptance, nothing
upstream disturbed.

**Not measured:** the *achieved* rate at lower thresholds. The ceiling gain is
known (+6.4 at 20 GeV, +11.7 at 15) but conversion declines as the permutation
count grows, and 15 GeV doubles the light pairs. Needs a fit-level scan before
committing. Keep `(pt > 50 || puId > 0)` — pileup contamination and JEC/JER
uncertainties both grow at low pT.

## Beyond hand-made rules

The conversion numbers say the headroom is in **ranking**, not enumeration. A
permutation-level classifier trained on parton-matched truth (jet kinematics,
b-tag scores, chi2, dijet mass, m_t consistency) is the standard modern
approach and should convert far more than chi2's 63.5%. Given 004C is already
a BDT chapter, the infrastructure largely exists. The intermediate step is to
write out the top few permutations per event with their features and let the
downstream BDT select, rather than committing to one permutation in 004A.

## Reproducing

Study scripts (scratchpad, not in the repo):
`refit_study.py`, `rerank_study.py` (`gen_partons`, `assignment_correct`),
`bkg_study.py`, `bperm_ceiling.py`, `bperm_achieved.py`, `wquark_pt.py`.
