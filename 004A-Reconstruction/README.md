# 004A-Reconstruction — ttbar Semi-Leptonic Kinematic Reconstruction

Takes the `selectionII` skims and reconstructs the leptonic and hadronic top-quark
candidates event by event. No cut string or golden JSON is re-applied — both were
already handled upstream.

## Inputs

- `selectionII_{era}_datasets.json`: built fresh from the 003-ObjectSelectionII
  output on disk via `--generateSelectionIIDatasetJSON --selectionIITag <tag>
  --selectionIIHash <hash>` (into `inputs/`, and this run's `outputs/{tag}/{hash}/
  inputs/` snapshot).

## What it does

`scripts/modules/RecoModule.py` (`TTbarSemilepReconstructor`), run via `runReco.py`:

1. Solves the neutrino-pz quadratic from the leptonic-W mass constraint (0, 1, or 2 real
   roots).
2. Builds up to 4 permutations (2 b-jet assignments × pz roots) from `SelMuon`,
   `leadingbJet`/`subleadingbJet`, `leadingJet`/`subleadingJet`, and `MET`.
3. Runs a full `scipy.optimize.minimize` (SLSQP) chi²-fit per permutation, with soft
   penalty terms for the W and (equal-)top mass constraints, and keeps the
   lowest-chi² converged fit.
4. Writes `Top_lep_*`, `Top_had_*`, `Chi2`, `Chi2_prefit`, `Pgof`, `chi2_status`.

See `leptonJets_kinFit_prescription.md` for the physics reference this implements
against, and the deliberate deviations from it (soft penalties vs. Lagrange multipliers;
equal-top-mass vs. pinned-172.5 constraint).

## Outputs

- Skim ROOT files: `{STORAGE}/reconstruction/{tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}/*_Skim.root`
- `reconstruction_{tag}_{era}_datasets.json` (via `--generateDatasetJSON`) — input for 004B-BDTVariables.
- `--makeDeltaPlots` — diagnostic plots from `deltaMassPlots.py`, MC only (it needs the
  generator tops, so only `ttbar_SemiLeptonic` datasets are read):
  - `deltaMass_hadronic.png`, `deltaMass_leptonic.png` — the reconstructed-vs-generator
    top-mass residuals.
  - `pgof.png` — the fit goodness-of-fit probability `Pgof`, split into correctly
    reconstructed and combinatorial events.
  - `pgof_purity.png` — differential purity `s/(s+b)` per `Pgof` bin.
  - `pgof_efficiency.png` — cumulative signal efficiency and purity for a `Pgof > cut`
    selection, for choosing a cut, plus efficiency against the physical ceiling.

  "Correctly reconstructed" is truth-matched at parton level, in two steps. An event
  is *reconstructible* when both b partons and both hadronic-W quarks are matched,
  one-to-one and within `--partonDR` (default 0.4), by the four jets the fit is given
  — `leading/subleadingbJet` in the b slots, `leading/subleadingJet` on the hadronic W.
  That fraction is the **physical ceiling**: everything below it was lost to acceptance
  or jet merging before the fit ran, so no re-ranking recovers it. Among reconstructible
  events, the fit is *correct* when it made its one free choice — which b-jet goes
  hadronic — the right way, read back by comparing the fitted `Top_had` direction against
  the two `b + lj + slj` hypotheses.

  Note what is deliberately *not* used: a window on `|m_t^reco - m_t^gen|`. The fit pins
  m_t to the same 172.5 GeV the generator used, so a mass window partly selects on the
  quantity it is meant to validate — a wrong assignment pulled back onto the nominal mass
  passes it. The parton-level definition is also a strict subset of the reconstructible
  events, so efficiency against the ceiling cannot exceed 1.

  On UL2016postVFP the ceiling sits near 28%, of which the fit recovers about 76%. The
  ceiling is low because the hadronic-W quarks are frequently *not* the two leading light
  jets — the limitation tracked in `ISSUE_jet_assignment_purity.md`. `--pgofBins` sets the
  binning across `Pgof` in [0,1].

## Running it

```
run_all.py --generateSelectionIIDatasetJSON --selectionIITag <tag> --selectionIIHash <hash>
run_all.py --generateProcessListJSON
run_all.py --writeBashScript
scripts/run_all_{tag}.sh
run_all.py --generateDatasetJSON
```

`--filter`, `--force`, `--sample`, `--workers` work as in the other chapters. Note:
reconstruction is CPU-heavy (one SLSQP minimisation per permutation per event), so
expect this stage to run noticeably slower than 003-I/II.

### CRAB alternative to Step 2/3 (lxplus only)

This is the chapter's main CRAB target -- it's the CPU-heavy stage. Same pattern as
003-ObjectSelectionI/II's CRAB support:

```
source scripts/crab/getcrabReady.sh
run_all.py --submitReconstructionJobs [--sample]
run_all.py --checkCrabStatus [--resubmitFailedCrabJobs] [--removeSubmitFailedCrabJobs]
run_all.py --generateDatasetJSON
```

`scripts/crab/submit_reconstruction_flexible.py` builds `Data.userInputFiles` from
`selectionII_{tag}_{era}_datasets.json` the same way (selectionII output isn't
DBS-registered either), and `scripts/crab/crab_script_reconstruction.py` resolves each
LFN to `root://eosuser.cern.ch/...` directly, bypassing `crabhelper.inputFiles()` for the
same reason as 003-I. Unlike 003-I/II, no golden JSON is shipped or needed -- neither a
cut string nor golden-JSON filtering is re-applied here (both already handled upstream in
selectionII).

The one real dependency to watch: `RecoModule`'s chi2 fit uses `scipy.optimize.minimize`,
which -- like `correctionlib`/`coffea`/`awkward` in 003-ObjectSelectionII -- is not part of
the stock CMSSW python environment and only imports on lxplus because it's pip-installed
under the user's own AFS home. `crab_script_reconstruction.py` reuses 003-II's fix:
prepending the CVMFS-hosted LCG software stack's site-packages to `sys.path`.

## Storage

`STORAGE` in `config.yaml` is a dict keyed by machine (matched as a substring of
`socket.gethostname()`), resolved in `utils.resolve_storage_path()`.
