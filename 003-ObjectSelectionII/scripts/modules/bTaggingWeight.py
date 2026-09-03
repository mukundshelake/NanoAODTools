from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib
import numpy as np
import os
import awkward as ak
from coffea.lookup_tools import extractor
class bTaggingWeightProducer(Module):
    # Per-jet clip on the "failing" weight (1-sf*eff)/(1-eff) -- Method 1a is
    # inherently unstable when eff is high (1/(1-eff) amplifies any SF-vs-1
    # deviation) or the efficiency-map bin is sparse. Confirmed directly on
    # real UL2016preVFP ttbar MC: a well-measured (34845-entry) bin with
    # eff=0.971 for a ~142 GeV b-jet, combined with SF_up=1.049, already gives
    # a negative per-jet weight (-0.67) with nothing bounding it before this
    # clip existed (bTagWeightUp's observed min was -1.055 pre-fix, from an
    # event with more than one such jet compounding multiplicatively). Unlike
    # JetPUIDWeight.py's Loose-WP efficiency (pervasively 90-99%+, needing
    # both a per-jet AND an event-level clip to avoid a systemically biased
    # mean), b-tagging's Medium-WP efficiency isn't uniformly that high, so a
    # per-jet clip alone is expected to be enough here -- verify against a
    # larger sample if bTagWeight's mean drifts far from 1 despite this.
    _PER_JET_CLIP_LO = 0.0
    _PER_JET_CLIP_HI = 5.0

    @staticmethod
    def _resolve_efficiency_file(effi_folder, era, channel):
        base_dir = os.path.join(effi_folder, era)
        direct = os.path.join(base_dir, f"{channel}.root")
        if os.path.isfile(direct):
            return direct

        candidates = []
        if "TuneCPup" in channel:
            candidates.append(channel.replace("TuneCPup", "TuneCP5up"))
        if "TuneCPdown" in channel:
            candidates.append(channel.replace("TuneCPdown", "TuneCP5down"))
        if "TuneCP5up" in channel:
            candidates.append(channel.replace("TuneCP5up", "TuneCPup"))
        if "TuneCP5down" in channel:
            candidates.append(channel.replace("TuneCP5down", "TuneCPdown"))

        # Last-resort fallback to nominal sample histogram if a dedicated
        # systematic efficiency file is unavailable.
        for marker in ["_TuneCPup", "_TuneCPdown", "_TuneCP5up", "_TuneCP5down"]:
            if marker in channel:
                candidates.append(channel.split(marker)[0])

        seen = {direct}
        checked = [direct]
        for name in candidates:
            path = os.path.join(base_dir, f"{name}.root")
            if path in seen:
                continue
            seen.add(path)
            checked.append(path)
            if os.path.isfile(path):
                return path

        raise FileNotFoundError(
            f"No efficiency ROOT file found for channel '{channel}' in '{base_dir}'. "
            f"Checked: {', '.join(checked)}"
        )

    def __init__(self, config, channel):
        super().__init__()
        self.era = config['era']
        self.channel = channel
        effiFolder = config['efficiencyFolder']
        effiFile = self._resolve_efficiency_file(effiFolder, config['era'], channel)
        bTaggingFile = config['bTagSFFile']
        self.bTageval = correctionlib.CorrectionSet.from_file(bTaggingFile)
        b_eff_ext = extractor()
        b_eff_ext.add_weight_sets(["* * "+effiFile])
        b_eff_ext.finalize()
        self.b_eff_evaluator = b_eff_ext.make_evaluator()
        self.bNames = config['branchNames']
        self.bTagThreshold = config['bTagThreshold']

        self._n_clipped = 0
        self._n_evaluations = 0

    def _safe_fail_weight(self, sf, eff):
        """(1 - sf*eff) / (1 - eff), guarded against (1-eff) ~ 0 and clipped
        to [_PER_JET_CLIP_LO, _PER_JET_CLIP_HI] -- see class docstring."""
        den = 1.0 - eff
        if abs(den) < 1e-8:
            return 1.0
        w = (1.0 - sf * eff) / den
        clipped = max(self._PER_JET_CLIP_LO, min(w, self._PER_JET_CLIP_HI))
        if clipped != w:
            self._n_clipped += 1
        return clipped

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"], "F")
        self.out.branch(self.bNames["sfup"], "F")
        self.out.branch(self.bNames["sfdown"], "F")

    def analyze(self, event):
        jets = Collection(event, "Jet")
        jets = [jet for jet in jets if jet.pt > 25 and abs(jet.eta) < 2.4 and jet.jetId ==6 and (jet.puId > 0 or jet.pt > 50)]

        bTagWeight = 1.0
        bTagWeightUp = 1.0
        bTagWeightDown = 1.0

        for jet in jets:
            if jet.btagDeepFlavB > self.bTagThreshold: # They are b-tagged
                if (jet.hadronFlavour == 5 or jet.hadronFlavour == 4):
                    weight = self.bTageval['deepJet_mujets'].evaluate('central', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    weightUp = self.bTageval['deepJet_mujets'].evaluate('up', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    weightDown = self.bTageval['deepJet_mujets'].evaluate('down', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    bTagWeight *= weight
                    bTagWeightUp *= weightUp
                    bTagWeightDown *= weightDown
                elif jet.hadronFlavour == 0:
                    weight = self.bTageval['deepJet_incl'].evaluate('central', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    weightUp = self.bTageval['deepJet_incl'].evaluate('up', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    weightDown = self.bTageval['deepJet_incl'].evaluate('down', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    bTagWeight *= weight
                    bTagWeightUp *= weightUp
                    bTagWeightDown *= weightDown
            else: # They are not b-tagged
                if jet.hadronFlavour == 5:
                    SF = self.bTageval['deepJet_mujets'].evaluate('central', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    SFUp = self.bTageval['deepJet_mujets'].evaluate('up', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    SFDown = self.bTageval['deepJet_mujets'].evaluate('down', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    # ROOT TH2s are filled with pT on the x-axis and |eta| on the
                    # y-axis; the coffea evaluator takes (x, y) = (pt, eta).
                    effPass = self.b_eff_evaluator['Efficiency/FlavourB_Wp_pass_BM'](jet.pt, abs(jet.eta))
                    effTotal = self.b_eff_evaluator['Efficiency/FlavourB_Wp_pass_No'](jet.pt, abs(jet.eta))
                    eff = (effPass / effTotal) if effTotal > 0 else 0.0
                    self._n_evaluations += 3
                    weight = self._safe_fail_weight(SF, eff)
                    weightUp = self._safe_fail_weight(SFUp, eff)
                    weightDown = self._safe_fail_weight(SFDown, eff)
                    bTagWeight *= weight
                    bTagWeightUp *= weightUp
                    bTagWeightDown *= weightDown
                elif jet.hadronFlavour == 4:
                    SF = self.bTageval['deepJet_mujets'].evaluate('central', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    SFUp = self.bTageval['deepJet_mujets'].evaluate('up', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    SFDown = self.bTageval['deepJet_mujets'].evaluate('down', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    effPass = self.b_eff_evaluator['Efficiency/FlavourC_Wp_pass_BM'](jet.pt, abs(jet.eta))
                    effTotal = self.b_eff_evaluator['Efficiency/FlavourC_Wp_pass_No'](jet.pt, abs(jet.eta))
                    eff = (effPass / effTotal) if effTotal > 0 else 0.0
                    self._n_evaluations += 3
                    weight = self._safe_fail_weight(SF, eff)
                    weightUp = self._safe_fail_weight(SFUp, eff)
                    weightDown = self._safe_fail_weight(SFDown, eff)
                    bTagWeight *= weight
                    bTagWeightUp *= weightUp
                    bTagWeightDown *= weightDown
                elif jet.hadronFlavour == 0:
                    SF = self.bTageval['deepJet_incl'].evaluate('central', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    SFUp = self.bTageval['deepJet_incl'].evaluate('up', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    SFDown = self.bTageval['deepJet_incl'].evaluate('down', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    effPass = self.b_eff_evaluator['Efficiency/FlavourL_Wp_pass_BM'](jet.pt, abs(jet.eta))
                    effTotal = self.b_eff_evaluator['Efficiency/FlavourL_Wp_pass_No'](jet.pt, abs(jet.eta))
                    eff = (effPass / effTotal) if effTotal > 0 else 0.0
                    self._n_evaluations += 3
                    weight = self._safe_fail_weight(SF, eff)
                    weightUp = self._safe_fail_weight(SFUp, eff)
                    weightDown = self._safe_fail_weight(SFDown, eff)
                    bTagWeight *= weight
                    bTagWeightUp *= weightUp
                    bTagWeightDown *= weightDown
        
        self.out.fillBranch(self.bNames["sf"], bTagWeight)
        self.out.fillBranch(self.bNames["sfup"], bTagWeightUp)
        self.out.fillBranch(self.bNames["sfdown"], bTagWeightDown)

        return True  # Keep event

    def endJob(self):
        if self._n_evaluations:
            rate = self._n_clipped / self._n_evaluations
            print(f"[bTaggingWeightProducer] era={self.era} channel={self.channel}: clipped "
                  f"{self._n_clipped}/{self._n_evaluations} failing-jet weight evaluations "
                  f"({rate:.4%}) to [{self._PER_JET_CLIP_LO}, {self._PER_JET_CLIP_HI}].")

def bTaggingWeightModule(config, channel):
    return bTaggingWeightProducer(config, channel)
