from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib
import numpy as np
import os
from coffea.lookup_tools import extractor


class jetPUIdWeightProducer(Module):
    def __init__(self, era, channel, config):
        """
        Jet PU ID event weight using BTagSF Method 1a (per-jet product):
          - passing jets: w_jet = SF
          - failing jets: w_jet = (1 - SF * eff) / (1 - eff)

        This is numerically stable regardless of how many jets pass or fail,
        because we never multiply (1 - eff) over all jets simultaneously.
        The old wData/wMC product approach caused overflow when eff ~ 1 and
        several jets failed (wMC -> 0 -> wData/wMC -> inf).

        Config keys:
            efficiencyFolder : base folder; ROOT files expected at
                               <folder>/<era>/<channel>.root
                               Each ROOT file contains:
                                 Efficiency/JetPUId_pass_No    (denominator)
                                 Efficiency/JetPUId_pass_Loose (numerator)
            jetPUIdFile      : correctionlib .json.gz for PU ID SFs
        """
        super().__init__()
        self.era = era

        effiFolder = config["efficiencyFolder"]
        effiFile = os.path.join(effiFolder, era, f"{channel}.root")

        pu_ext = extractor()
        pu_ext.add_weight_sets(["* * " + effiFile])
        pu_ext.finalize()
        self.pu_eff_evaluator = pu_ext.make_evaluator()

        self.jetPUeval = correctionlib.CorrectionSet.from_file(config["jetPUIdFile"])

    @staticmethod
    def _safe_fail_weight(sf, eff):
        """(1 - sf*eff) / (1 - eff), guarded against (1-eff) ~ 0."""
        den = 1.0 - eff
        if abs(den) < 1e-8:
            return 1.0
        return (1.0 - sf * eff) / den

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch("jetPUIdWeight",     "F")
        self.out.branch("jetPUIdWeightUp",   "F")
        self.out.branch("jetPUIdWeightDown", "F")

    def analyze(self, event):
        # Jet PU ID applies only to jets with 12.5 < pT <= 50 GeV, |eta| < 5
        jets = Collection(event, "Jet")
        jets = [jet for jet in jets if 12.5 < jet.pt <= 50.0 and abs(jet.eta) < 5.0]

        # puId == 0 means the jet failed all WPs
        weight     = 1.0
        weightUp   = 1.0
        weightDown = 1.0

        for jet in jets:
            SF_nom  = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'nom',  'L')
            SF_up   = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'up',   'L')
            SF_down = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'down', 'L')

            # Per-(|eta|, pT) efficiency from ROOT file
            effPass  = self.pu_eff_evaluator['Efficiency/JetPUId_pass_Loose'](abs(jet.eta), jet.pt)
            effTotal = self.pu_eff_evaluator['Efficiency/JetPUId_pass_No'](abs(jet.eta), jet.pt)
            eff = (effPass / effTotal) if effTotal > 0 else 0.9

            if jet.puId > 0:
                # Jet passed PU ID: per-jet weight = SF (efficiency cancels)
                weight     *= SF_nom
                weightUp   *= SF_up
                weightDown *= SF_down
            else:
                # Jet failed PU ID: per-jet weight = (1 - SF*eff) / (1 - eff)
                weight     *= self._safe_fail_weight(SF_nom,  eff)
                weightUp   *= self._safe_fail_weight(SF_up,   eff)
                weightDown *= self._safe_fail_weight(SF_down, eff)

        self.out.fillBranch("jetPUIdWeight",     np.float32(weight))
        self.out.fillBranch("jetPUIdWeightUp",   np.float32(weightUp))
        self.out.fillBranch("jetPUIdWeightDown", np.float32(weightDown))

        return True


def jetPUIdWeightModule(era, channel, config):
    return jetPUIdWeightProducer(era, channel, config)
