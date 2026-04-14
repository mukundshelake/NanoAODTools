from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib
import numpy as np
import json


class jetPUIdWeightProducer(Module):
    def __init__(self, era, channel, config):
        """
        FIX: Paths are now read from the config dict (config YAML file) instead
        of being hard-coded to an absolute path.  The constructor signature adds
        `config` as a required argument, matching the pattern used by every other
        module in this codebase.

        Expected config YAML keys:
            efficiencyFile   : path to JSON file containing per-channel efficiency
                               maps, relative to the repo root.
            jetPUIdFile      : path to the correctionlib .json.gz file for jet
                               PU ID SFs, relative to the repo root.

        Example config (jetPUID_UL2018_config.yaml):
            efficiencyFile: "SFs/Efficiency/UL2018/UL2018_Jet_puId_effi.json"
            jetPUIdFile:    "SFs/UL2018_jet_jmar.json.gz"
        """
        super().__init__()
        self.era = era
        self.channel = channel

        effiFile    = config["efficiencyFile"]
        jetPUIdFile = config["jetPUIdFile"]

        with open(effiFile, 'r') as f:
            effiData = json.load(f)
        self.effiValues = effiData.get(channel, {})
        self.jetPUeval = correctionlib.CorrectionSet.from_file(jetPUIdFile)

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch("jetPUIdWeight", "F")
        self.out.branch("jetPUIdWeightUp", "F")
        self.out.branch("jetPUIdWeightDown", "F")

    def analyze(self, event):
        # Jet PU ID applies only to jets with 12.5 < pT <= 50 GeV
        jets = Collection(event, "Jet")
        jets = [jet for jet in jets if 12.5 < jet.pt <= 50.0 and abs(jet.eta) < 5.0]

        # puId bit value for "failed all WPs" (bit 0 = Loose, value 0 = fail all)
        FAIL_ALL = {
            'UL2016preVFP':  0,
            'UL2016postVFP': 0,
            'UL2017':        0,
            'UL2018':        0,
        }
        fail_val = FAIL_ALL.get(self.era, 0)

        wMC      = 1.0
        wData    = 1.0
        wDataUp  = 1.0
        wDataDown = 1.0

        for jet in jets:
            effi_L   = self.effiValues.get('nLoose', 1) / self.effiValues.get('nTotal', 1)
            SF_L     = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'nom',  'L')
            SF_LUp   = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'up',   'L')
            SF_LDown = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'down', 'L')

            if jet.puId == fail_val:
                # Jet failed PU ID — use (1 - SF*effi) / (1 - effi) factor
                wData    *= (1 - SF_L     * effi_L)
                wDataUp  *= (1 - SF_LUp   * effi_L)
                wDataDown *= (1 - SF_LDown * effi_L)
                wMC      *= (1 - effi_L)
            else:
                # Jet passed PU ID — use (SF*effi) / effi factor
                wData    *= (SF_L     * effi_L)
                wDataUp  *= (SF_LUp   * effi_L)
                wDataDown *= (SF_LDown * effi_L)
                wMC      *= effi_L

        jetPUWeight     = np.float32(wData    / wMC if wMC > 0 else 1.0)
        jetPUWeightUp   = np.float32(wDataUp  / wMC if wMC > 0 else 1.0)
        jetPUWeightDown = np.float32(wDataDown / wMC if wMC > 0 else 1.0)

        self.out.fillBranch("jetPUIdWeight",     jetPUWeight)
        self.out.fillBranch("jetPUIdWeightUp",   jetPUWeightUp)
        self.out.fillBranch("jetPUIdWeightDown", jetPUWeightDown)

        return True


def jetPUIdWeightModule(era, channel, config):
    return jetPUIdWeightProducer(era, channel, config)
