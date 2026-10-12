# HitFit (vendored)

HitFit is the lepton+jets kinematic fitter of the 004A reference standard,
CMS-TOP-23-003 (arXiv:2403.01313, Sec. 2.3.1; ref. [117]: D0, Phys. Rev. D 58
(1998) 052001). The code here is CMSSW's `TopQuarkAnalysis/TopHitFit`, copied
**unmodified** from `CMSSW_14_2_3` on CVMFS, and is Apache-2.0 licensed (see
`LICENSE`). See `004A-Reconstruction/README.md` and issue #49.

## Why it is here

004A runs in the `latestcoffea` conda environment (ROOT 6.36), not in CMSSW.
HitFit's core needs only CLHEP; its other CMSSW dependencies are confined to the
PAT-object translators. So this directory carries:

| path | contents |
|---|---|
| `src/`, `include/TopQuarkAnalysis/TopHitFit/interface/` | the HitFit core: 26 sources, unmodified. Left out: `Pat*HitFitTranslator.cc`, `CLHEPHitFitTranslator.cc` and `RunHitFit.cc`, plus the translator and `RunHitFit` headers. |
| `data/resolution/`, `data/setting/` | upstream resolution tables (`tqaf*Resolution.txt`) and the example `RunHitFitConfiguration.txt` |
| `wrapper/HitFitWrapper.{h,cc}` | **our code**: a flat-number interface (`hitfit_wrap::Fitter`) |
| `hitfit_loader.py` | **our code**: loads the library into PyROOT and builds a `Fitter` from a settings dict. It finds the library and header next to itself, in the repo layout or flat as in a CRAB job. |
| `test/toy_test.py` | **our code**: toy calibration test (χ² vs χ²(2)) |
| `build.sh` | builds `libHitFit.so` (conda or CMSSW mode), then runs the toy test |

Including the include path layout, the upstream sources compile as they are. To update them,
re-copy from a newer CMSSW release; never edit them in place.

## Building

`build.sh` picks the environment it runs in:

```bash
conda activate latestcoffea && external/HitFit/build.sh   # local running (004A runReco / workflow)
cmsenv && external/HitFit/build.sh                        # CRAB submission (lxplus)
```

- **conda mode.** The first run creates a private CLHEP 2.4.7.2 prefix at `envs/clhep`
  (conda-forge), so `latestcoffea` itself is not modified.
- **CMSSW mode** (selected when `CMSSW_BASE` is set). Uses the release's gcc and its CLHEP
  external, found through `scram tool tag clhep`. CRAB jobs run in the submitting release,
  so this build is what they load. Tested in `CMSSW_13_3_0` (el9_amd64_gcc12): toy results
  identical to conda mode, and a locally emulated job reproduced the conda run event by event.

**Build products** (git-ignored):
- `libHitFit.so`, linked with an RPATH to its CLHEP, so nothing has to be exported at run
  time. In CMSSW mode the CLHEP is on CVMFS, so grid nodes find it too.
- `libHitFit.buildinfo`, which records the mode, `CMSSW_VERSION`/`SCRAM_ARCH` or conda prefix,
  and the date. 004A's CRAB submitter refuses a library not built for the submitting release.

A library is tied to the environment that built it (different compilers, libstdc++ and CLHEP), so rebuild when switching.
`build.sh` ends with the toy test and fails if it does not pass.

## Why a wrapper

Binding HitFit's own classes from cppyy crashes on by-value returns of its CLHEP-based
types (incomplete class info). The wrapper takes plain numbers and returns a plain struct:

```python
from hitfit_loader import make_fitter
fitter = make_fitter(settings, udsc_res, b_res, muon_res, kt_res, mw=80.4)
r = fitter.fit(jet_pt, jet_eta, jet_phi, jet_mass,          # numpy float64[4]:
               mu_pt, mu_eta, mu_phi, mu_mass,               # hadW1, hadW2, hadB, lepB
               met_px, met_py, nuz)                          # nuz: which pz root
r.status   # 0 ok, 1 no converged fit, 2 eta outside a resolution table
r.chi2, r.mt, r.sigmt, r.nu_pz, r.lep_top, r.had_top, r.nu   # 4-vectors (px,py,pz,E)
```

One call is one jet assignment with one neutrino-pz seed; the caller loops over the
permutations. Objects are fed the way CMSSW's PAT translators feed them:
- each object's resolution is looked up by η in an `EtaDepResolution` file (separate
  b-jet and udsc-jet files);
- MET enters as (px, py, 0, |pT|);
- the kt resolution is the `met_resolution` entry of the kt file.

The W mass is applied to both W constraints with `top_mass = 0`, so the top mass is never
fixed. Equal top masses come from `equal_side = true` in the settings.

## Toy test

`test/toy_test.py` generates events with HitFit's toy generator (`gentop`), smears them with
the same resolutions given to the fit (`test/toy_res_*.txt`), fits the true assignment with
both pz seeds, and compares χ² to χ²(2).

Result on 2026-10-12, 3000 events:
- 98.5% converge;
- median χ² 1.378 (χ²(2): 1.386);
- 77.9% have Pgof > 0.2 (χ²(2): 80%).

The mean, 2.5, is pulled up by a non-Gaussian tail.
