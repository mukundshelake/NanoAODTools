# 004A-Reconstruction — ttbar Semi-Leptonic Kinematic Reconstruction

Takes the `selectionII` skims and reconstructs the leptonic and hadronic top-quark
candidates event by event. No cut string or golden JSON is re-applied — both were
already handled upstream.

## Inputs

- `inputs/selectionII_earlyApril_{era}_datasets.json` — file lists from 003-ObjectSelectionII.

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
- `reconstruction_{tag}_{era}_datasets.json` (via `--generateDatasetJSON`) — input for 004B-BDT.
- `--makeDeltaPlots` — reconstructed-vs-generator top-mass residual plots (`deltaMassPlots.py`), MC only.

## Running it

Same four-step pattern as the earlier chapters:
`--generateProcessListJSON` → `--writeBashScript` → run the generated
`scripts/run_all_{tag}.sh` → `--generateDatasetJSON`.

`--filter`, `--force`, `--sample`, `--workers` work as in the other chapters. Note:
reconstruction is CPU-heavy (one SLSQP minimisation per permutation per event), so
expect this stage to run noticeably slower than 003-I/II.

## Storage

`STORAGE` in `config.yaml` is a dict keyed by machine (matched as a substring of
`socket.gethostname()`), resolved in `utils.resolve_storage_path()`.
