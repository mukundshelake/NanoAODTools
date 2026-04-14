#!/usr/bin/env python3
"""
AsymmetryModel.py — Combine custom PhysicsModel for measuring a charge asymmetry A.

Place this file in the same directory as your datacards and ensure it is on
PYTHONPATH before calling text2workspace.py:

    export PYTHONPATH="${PWD}:${PYTHONPATH}"
    text2workspace.py card.txt -P AsymmetryModel:asymmetryModel -o card.root

Model
-----
Two bins per datacard: one "pos" and one "neg".
The datacard rate for both bins is set to the total MC yield N_mc_tot.
This model then scales them:

    N_pos_expected = μ · N_mc_tot · (1 + A) / 2  ·  ∏ lnN_i
    N_neg_expected = μ · N_mc_tot · (1 - A) / 2  ·  ∏ lnN_j

    POI:  A  ∈ [-1, 1]   (the asymmetry being measured)
    μ:    freely floating rateParam in the datacard (absorbs MC normalisation)
    lnN:  per-systematic κ values encoding how each source shifts pos/neg yields

Bin naming convention (must be followed by the datacard generator):
    bins ending in "_pos"  →  scaled by (1 + A) / 2
    bins ending in "_neg"  →  scaled by (1 - A) / 2
"""

from HiggsAnalysis.CombinedLimit.PhysicsModel import PhysicsModel  # noqa: E402


class AsymmetryModel(PhysicsModel):
    """
    Maps a two-bin (pos/neg) counting experiment to a charge asymmetry POI A.

    Bin names MUST end in '_pos' or '_neg' for the scaling to be applied.
    All other bins are left unscaled (yield scale = 1.0).
    """

    def doParametersOfInterest(self):
        # Define the POI: asymmetry A, initialised at 0, range [-1, 1]
        self.modelBuilder.doVar("A[0,-1,1]")
        self.modelBuilder.doSet("POI", "A")

    def done(self):
        # Build the two scale expressions in the workspace.
        # These are evaluated for every event in the pos/neg bins respectively.
        self.modelBuilder.factory_('expr::pos_scale("0.5*(1+@0)", A)')
        self.modelBuilder.factory_('expr::neg_scale("0.5*(1-@0)", A)')

    def getYieldScale(self, bin, process):
        """Return the yield scale factor for a given (bin, process) pair."""
        if bin.endswith("_pos"):
            return "pos_scale"
        if bin.endswith("_neg"):
            return "neg_scale"
        return 1.0


# Module-level instance required by Combine's -P flag
asymmetryModel = AsymmetryModel()
