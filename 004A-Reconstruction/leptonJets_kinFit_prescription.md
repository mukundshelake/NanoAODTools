# Lepton+Jets Kinematic Reconstruction Prescription

Extracted from `recoDoc.pdf`, Section 2.3 ("Kinematic reconstruction of the tt system") and
2.3.1 ("Reconstruction in the lepton+jets and all-jets channels"), pp. 11-13. Only the
lepton+jets-relevant parts are kept here; the all-jets-only details (six-jet permutations,
Pgof > 0.1 cut, etc.) are omitted. This file is the reference for
`004A-Reconstruction/scripts/modules/RecoModule.py`.

## Why a kinematic fit at all

The lepton+jets channel has exactly one neutrino in the final state (compared to two in
dilepton, zero in all-jets), so its momentum is under-constrained but not hopeless: a kinematic
fit is used to (a) check whether an event is compatible with the tt hypothesis and (b) improve
the resolution of the reconstructed top quark and tt-system quantities.

## Fit inputs and unknowns

- Inputs: the four-momenta of the lepton, the **four highest-pT (leading) jets**, and the
  missing transverse momentum vector `pT^miss`, each fed to the fit together with its
  resolution.
- Fit parameters: the three-vectors of the momenta of the six decay products (lepton, neutrino,
  2 b quarks, 2 light quarks) → **18 unknowns**.
- With these inputs, the lepton+jets fit has **2 degrees of freedom**.

## Constraints

1. The invariant masses of the two top quark candidates (leptonic and hadronic) must be
   **equal to each other** — this is an equality constraint between the two candidates, **not**
   a constraint pinning either one to a fixed value such as 172.5 GeV. Fixing the top mass
   would bias every reconstructed event toward the input value and defeat measurements that use
   the reconstructed mass/resolution as an observable.
2. The invariant masses of both W boson candidates (leptonic and hadronic) must equal
   **80.4 GeV**.

The fit minimises

```
chi2 = (x - x_m)^T G (x - x_m)
```

where `x_m`/`x` are the measured/fitted momentum vectors and `G` is the inverse covariance
matrix built from the measurement uncertainties. The mass constraints above are enforced via
Lagrange multipliers (a hard-constrained fit, not a soft penalty term added to the objective).

## Permutations (lepton+jets)

To keep combinatorics tractable:

- Exactly **two of the four leading jets** must be b-tagged. If the leading four jets do not
  split into exactly 2 b-tagged + 2 non-b-tagged, the event is not usable for this reconstruction
  and should fail (not be padded/rescued by pulling in a b-tagged jet from beyond the leading
  four).
- The 2 b-tagged jets are the b-quark candidates (`b`, `bbar`); which one pairs with the lepton
  vs. the hadronic W side gives **2 b-assignment permutations**.
- The 2 non-b-tagged jets are the candidates for the light quarks from the hadronically decaying
  W. (There is no separate permutation for swapping the two light jets — the hadronic W mass sum
  is symmetric under that swap.)
- For each b-assignment, there are **2 starting values** for the longitudinal component of the
  neutrino momentum (the two roots of the quadratic obtained from the leptonic W mass
  constraint).
- Total: **4 permutations are fit per event** (2 b-assignments x 2 neutrino pz seeds). The
  document is explicit that **the fit itself is run for all four permutations** — the ranking
  in the next step is based on the post-fit result of each, not a pre-fit shortcut used to pick
  a single permutation to fit.

## Ranking and event selection

- Permutations are ranked by the fit's **chi2 probability `Pgof`** (the goodness-of-fit
  probability, not just the raw chi2). Wrongly-assigned permutations typically have very low
  `Pgof`.
- For simulated events, a parton-jet assignment is classified as:
  - **correct** — all quarks matched within `deltaR = sqrt(deltaEta^2 + deltaPhi^2) < 0.3` to a
    selected jet, with the correct flavour assignment to the correct top quark;
  - **wrong** — all quarks matched to a selected jet, but the wrong permutation was chosen;
  - **unmatched** — not all quarks are matched unambiguously to a selected jet.
- A quality cut of **`Pgof > 0.2`** is applied in the lepton+jets channel; this matches the
  resolution obtained when only correct permutations are used with their pre-fit momenta. The
  selection efficiency of this cut is **27.4%** in lepton+jets.

## Resolution / validation figures quoted in the doc (lepton+jets, simulation)

Top quark mass resolution `sigma_peak` (Gaussian fit to `-40 < m_rec_t - m_gen_t < +40 GeV`),
from Fig. 3:

| Case                              | sigma_peak |
|------------------------------------|-----------|
| Kinematic fit, correct permutation | 13 GeV    |
| Kinematic fit, `Pgof > 0.2`        | 16 GeV    |
| Kinematic fit, lowest chi2         | 21 GeV    |
| Kinematic fit, all permutations    | 25 GeV    |
| No fit, inclusive                  | 30 GeV    |
| No fit, correct permutation only   | 19 GeV    |

The kinematic fit with the `Pgof` cut also improves the bias and resolution of the reconstructed
`m_tt` relative to the generator-level value, and is described as "almost free of bias" over the
examined `m_tt` range (Fig. 4).

## Practical implementation note

The document describes a formal hard-constrained fit with a covariance matrix and Lagrange
multipliers. `RecoModule.py` instead uses an unconstrained `scipy.optimize.minimize` (SLSQP)
run with the mass constraints added as extra soft penalty terms (each with an assumed
resolution, e.g. `sigmaW = 10 GeV`, `sigmatt = 13 GeV`). This is a reasonable practical
approximation and is treated as accepted design choice, not a gap to close.

The two prescription gaps that *were* open against this doc have been fixed in
`RecoModule.py`:

1. **All 4 permutations now get the full fit**, with the post-fit chi2 (not a pre-fit
   approximate chi2) deciding the winner; `Chi2_prefit` is kept only as a before/after
   diagnostic, reported for whichever permutation the post-fit chi2 selects.
2. **The top-mass constraint now equates the two candidates' masses to each other**
   (`(m_top_lep - m_top_had) / sigmatt) ** 2`) instead of pinning both to a fixed 172.5 GeV.

Two side effects of these fixes worth knowing about downstream:

- **Reconstruction is ~4x slower**: the SLSQP minimisation (18-dim, `scipy.optimize.minimize`)
  now runs once per permutation per event instead of once per event.
- **`Chi2`/`Pgof` values shifted**: the equal-mass constraint is one penalty term instead of
  the old two, so the chi2 scale (and therefore `Pgof = exp(-0.5*chi2)`) is not directly
  comparable to values produced by the pre-fix module. Any downstream `minPgof`/`chi2_status`
  cuts tuned against the old output should be re-checked, and `chi2_status` now also uses
  `1` (upstream selection sentinel guard) and `2` (degenerate neutrino-pz quadratic, expected
  to be unreachable in practice) as distinct failure codes.
