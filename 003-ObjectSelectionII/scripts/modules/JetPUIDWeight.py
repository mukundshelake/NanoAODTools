from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib
import numpy as np
import os
from coffea.lookup_tools import extractor


class jetPUIdWeightProducer(Module):
    # Clip #1: per-jet. Verified directly on a real UL2016preVFP ttbar sample
    # that this is NOT a rare low-statistics artifact -- ~49% of per-jet
    # "failing" evaluations hit this clip. Method 1a's (1-sf*eff)/(1-eff) is
    # inherently unstable whenever eff (the Loose-WP pass efficiency) is high,
    # which is the common case for genuine jets in this correction's 12.5-50
    # GeV / |eta|<5 phase space -- any modest SF-vs-1 deviation gets amplified
    # by 1/(1-eff) regardless of how well-measured that bin is. This is the
    # same numerical-instability regime the b-tagging POG's own Method 1a
    # docs warn about; it's a property of the formula, not a bug to "fix"
    # away, and is why clip #2 below (bounding the actual per-event product)
    # is not optional defense-in-depth here -- without it, a handful of
    # clipped-but-still-large (up to 5x) per-jet factors compound
    # multiplicatively across an event's failing jets and blow the event
    # weight back up (seen directly: up to 2556 with only clip #1 in place).
    _PER_JET_CLIP = 5.0

    # Clip #2: per-event, applied to the full per-jet product. This is the
    # actual "clipped to a sane range" fix 003-ObjectSelectionIII/config.yaml's
    # weightList.MC comment asked for -- clip #1 alone does not bound the
    # event-level weight (see above). Bounds chosen wider than the ~[0.5,1.5]
    # typically-expected range (weightLookUp.jetPUIdWeight in
    # 003-ObjectSelectionII/config.yaml) so real per-event variation isn't
    # over-truncated, while still firmly ruling out the O(1e3) blowups seen
    # before this was added.
    _EVENT_WEIGHT_CLIP_LO = 0.1
    _EVENT_WEIGHT_CLIP_HI = 3.0

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

        self._n_clipped = 0
        self._n_evaluations = 0
        self._n_events_clipped = 0
        self._n_event_evaluations = 0

    def _safe_fail_weight(self, sf, eff):
        """(1 - sf*eff) / (1 - eff), guarded against (1-eff) ~ 0 and clipped
        to [0, _PER_JET_CLIP] (see class docstring -- this alone does not
        bound the per-event product; that's _EVENT_WEIGHT_CLIP_LO/HI below)."""
        den = 1.0 - eff
        if abs(den) < 1e-8:
            return 1.0
        w = (1.0 - sf * eff) / den
        clipped = max(0.0, min(w, self._PER_JET_CLIP))
        if clipped != w:
            self._n_clipped += 1
        return clipped

    def _clip_event_weight(self, w):
        clipped = max(self._EVENT_WEIGHT_CLIP_LO, min(w, self._EVENT_WEIGHT_CLIP_HI))
        if clipped != w:
            self._n_events_clipped += 1
        return clipped

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

            # Per-(pT, |eta|) efficiency from ROOT file.
            # The TH2s are filled with pT on the x-axis and |eta| on the
            # y-axis (see computeJetPUIDEfficiency.py), and coffea's
            # extractor evaluator takes arguments in that same (x, y)
            # order -- i.e. (pt, eta), NOT (eta, pt). Calling it as
            # (eta, pt) silently reads the wrong, clipped bin (pt values
            # up to 50 overflow the 0-5 "eta axis", while eta values
            # underflow the 12.5-50 "pt axis"), which was the source of
            # the huge/negative event weights.
            effPass  = self.pu_eff_evaluator['Efficiency/JetPUId_pass_Loose'](jet.pt, abs(jet.eta))
            effTotal = self.pu_eff_evaluator['Efficiency/JetPUId_pass_No'](jet.pt, abs(jet.eta))
            eff = (effPass / effTotal) if effTotal > 0 else 0.9

            if jet.puId > 0:
                # Jet passed PU ID: per-jet weight = SF (efficiency cancels)
                weight     *= SF_nom
                weightUp   *= SF_up
                weightDown *= SF_down
            else:
                # Jet failed PU ID: per-jet weight = (1 - SF*eff) / (1 - eff)
                self._n_evaluations += 3  # nom, up, down each go through _safe_fail_weight
                weight     *= self._safe_fail_weight(SF_nom,  eff)
                weightUp   *= self._safe_fail_weight(SF_up,   eff)
                weightDown *= self._safe_fail_weight(SF_down, eff)

        self._n_event_evaluations += 3  # nom, up, down each go through _clip_event_weight
        weight     = self._clip_event_weight(weight)
        weightUp   = self._clip_event_weight(weightUp)
        weightDown = self._clip_event_weight(weightDown)

        self.out.fillBranch("jetPUIdWeight",     np.float32(weight))
        self.out.fillBranch("jetPUIdWeightUp",   np.float32(weightUp))
        self.out.fillBranch("jetPUIdWeightDown", np.float32(weightDown))

        return True

    def endJob(self):
        if self._n_evaluations:
            rate = self._n_clipped / self._n_evaluations
            print(f"[jetPUIdWeightProducer] era={self.era}: clipped {self._n_clipped}/"
                  f"{self._n_evaluations} per-jet failing-weight evaluations ({rate:.4%}) -- "
                  f"expected to be common, see _PER_JET_CLIP's docstring, not a bug indicator.")
        if self._n_event_evaluations:
            rate = self._n_events_clipped / self._n_event_evaluations
            print(f"[jetPUIdWeightProducer] era={self.era}: clipped {self._n_events_clipped}/"
                  f"{self._n_event_evaluations} event-level weight evaluations ({rate:.4%}) to "
                  f"[{self._EVENT_WEIGHT_CLIP_LO}, {self._EVENT_WEIGHT_CLIP_HI}].")


def jetPUIdWeightModule(era, channel, config):
    return jetPUIdWeightProducer(era, channel, config)
