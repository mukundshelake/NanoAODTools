"""Write the trained BDT's score onto each event as a `BDTScore` branch.

Applies the model `trainBDT.py` saved -- the pinned `bdt_model.pkl` under
`outputs/{tag}/{parquetHash}/{era}/bdt/{trainingHash}/` -- to the 004B feature
branches, so downstream chapters can cut on a score instead of re-running the
classifier. `predict_proba[:, 1]` is P(qqbar), matching trainBDT.py's own
convention.

Scored in ONE batch per file, in beginFile, rather than per event in analyze().
An XGBoost call on a single row costs about a millisecond, so a per-event loop
over an 8.6M-event era would take hours; a single batched call takes seconds.
analyze() then just looks the value up by the input-tree entry index, which is
what `event._entry` is, and which stays correct when PostProcessor applies a cut
or a golden JSON because those only change *which* entries reach analyze(), not
their numbering.

The feature list comes from the model file itself, not from a config: a score
computed with the columns in a different order, or with a feature the model was
not trained on, is silently wrong rather than an error.
"""

import os

import numpy as np

from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module

# Value written when a feature is missing or non-finite for an event. Negative
# is outside the [0, 1] range predict_proba can return, so a cut of the form
# `BDTScore > x` rejects these rather than silently keeping them, and they stay
# distinguishable from a genuine score of 0.
FAILED_SCORE = -1.0


class BDTScoreModule(Module):
    def __init__(self, model_path, branch_name="BDTScore"):
        super().__init__()
        self.model_path = str(model_path)
        self.branch_name = branch_name
        self._bundle = None
        self.scores = None
        self.n_failed = 0

    # -- the model is loaded once per worker process, not once per file --
    @property
    def bundle(self):
        if self._bundle is None:
            import joblib
            if not os.path.exists(self.model_path):
                raise RuntimeError(f"BDT model not found: {self.model_path}")
            self._bundle = joblib.load(self.model_path)
            missing = [k for k in ("model", "imputer", "features")
                       if k not in self._bundle]
            if missing:
                raise RuntimeError(
                    f"{self.model_path} is missing {missing}; expected the "
                    "dict trainBDT.py writes via joblib.dump"
                )
        return self._bundle

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(self.branch_name, "F")

        bundle = self.bundle
        features = list(bundle["features"])

        present = set(inputTree.GetListOfBranches().At(i).GetName()
                      for i in range(inputTree.GetListOfBranches().GetEntries()))
        absent = [f for f in features if f not in present]
        if absent:
            raise RuntimeError(
                f"{inputFile.GetName()} is missing BDT feature branches {absent}. "
                "Scoring with a subset of the trained features would be silently "
                "wrong, so this is fatal."
            )

        n = inputTree.GetEntries()
        if n == 0:
            self.scores = np.zeros(0, dtype=np.float32)
            return

        # Read the features straight off the input tree in one pass.
        import pandas as pd
        import uproot

        with uproot.open(inputFile.GetName()) as handle:
            arrays = handle["Events"].arrays(features, library="np")
        frame = pd.DataFrame({f: np.asarray(arrays[f], dtype=np.float64)
                              for f in features})

        # Rows the model cannot score are given FAILED_SCORE rather than an
        # imputed guess: the imputer exists to handle values the TRAINING data
        # also had missing, not to invent inputs for a broken event.
        finite = np.isfinite(frame.to_numpy()).all(axis=1)
        self.scores = np.full(len(frame), FAILED_SCORE, dtype=np.float32)
        if finite.any():
            transformed = bundle["imputer"].transform(frame[finite])
            self.scores[finite] = bundle["model"].predict_proba(
                transformed)[:, 1].astype(np.float32)
        self.n_failed += int((~finite).sum())

    def analyze(self, event):
        entry = event._entry
        if self.scores is None or entry >= len(self.scores):
            self.out.fillBranch(self.branch_name, FAILED_SCORE)
        else:
            self.out.fillBranch(self.branch_name, float(self.scores[entry]))
        return True

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        if self.n_failed:
            print(f"    BDTScoreModule: {self.n_failed} event(s) had a "
                  f"non-finite feature and were given {FAILED_SCORE}")
        self.scores = None


def bdtScoreModule(model_path, branch_name="BDTScore"):
    return BDTScoreModule(model_path, branch_name)
