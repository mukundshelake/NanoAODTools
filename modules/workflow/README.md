# Workflow Modules

This folder is the active source for module imports in the workflow runners (`main_new.py` and `main.py`).

## Source of truth
Runtime module loading now points to:
- `modules/workflow/`

## Included modules
- `LHEWeightSign.py`
- `MuonIDWeight.py`
- `MuonHLTWeight.py`
- `bTaggingWeight.py`
- `JetPUIdWeightModule_new.py`
- `RecoModule_new.py`
- `observables.py`
- `yCalculator.py`
- `BDTvariableModule.py`
- `applyBDTModule.py`

## Note
`python/postprocessing/...` remains in the repository for legacy compatibility, but workflow updates should be made in `modules/workflow/`.