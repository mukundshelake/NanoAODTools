from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib
import numpy as np
import awkward as ak
from coffea.lookup_tools import extractor
import json
class jetPUIdWeightProducer(Module):
    def __init__(self, era, channel):
        super().__init__()
        self.era = era
        self.channel = channel
        effiFile = f"/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/SFs/Efficiency/{era}/{era}_Jet_puId_effi.json"
        with open(effiFile, 'r') as f:
            effiData = json.load(f)
        self.effiValues = effiData.get(channel, {})
        jetPUIdFile = f"/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/SFs/{era}_jet_jmar.json.gz"
        self.jetPUeval = correctionlib.CorrectionSet.from_file(jetPUIdFile)

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        self.out.branch("jetPUIdWeight", "F")
        self.out.branch("jetPUIdWeightUp", "F")
        self.out.branch("jetPUIdWeightDown", "F")

    def analyze(self, event):
        maps = {
            'T': {
                'UL2016preVFP': 7,
                'UL2016postVFP': 7,
                'UL2017': 7,
                'UL2018': 7,
            },
            'L': {
                'UL2016preVFP': 1,
                'UL2016postVFP': 1,
                'UL2017': 4,
                'UL2018': 4,
            },
            'M': {
                'UL2016preVFP': 3,
                'UL2016postVFP': 3,
                'UL2017': 6,
                'UL2018': 6,
            },
            'F': {
                'UL2016preVFP': 0,
                'UL2016postVFP': 0,
                'UL2017': 0,
                'UL2018': 0,
            }
        }
        """Process each event and compute highest muon pt"""
        jets = Collection(event, "Jet")
        jets = [jet for jet in jets if 12.5 < jet.pt <= 50.0 and abs(jet.eta) < 5.0]


        wMC = 1.0
        wData = 1.0
        wDataUp = 1.0
        wDataDown = 1.0

        for jet in jets:
            if jet.puId == maps['F'][self.era]: # Failed all
                effi_L = self.effiValues.get('nLoose', 1)/self.effiValues.get('nTotal', 1)
                SF_L = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'nom', 'L')
                SF_LUp = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'up', 'L')
                SF_LDown = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'down', 'L')
                wData *= (1 - SF_L * effi_L)
                wDataUp *= (1 - SF_LUp * effi_L)    
                wDataDown *= (1 - SF_LDown * effi_L)
                wMC *= (1 - effi_L)
            else:
                effi_L = self.effiValues.get('nLoose', 1)/self.effiValues.get('nTotal', 1)
                SF_L = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'nom', 'L')
                SF_LUp = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'up', 'L')
                SF_LDown = self.jetPUeval['PUJetID_eff'].evaluate(jet.eta, jet.pt, 'down', 'L')
                wData *= (SF_L * effi_L)
                wDataUp *= (SF_LUp * effi_L)
                wDataDown *= (SF_LDown * effi_L)
                wMC *= (effi_L)
        # Calculate the weights
        jetPUWeight = wData / wMC if wMC > 0 else 1.0
        jetPUWeightUp = wDataUp / wMC if wMC > 0 else 1.0
        jetPUWeightDown = wDataDown / wMC if wMC > 0 else 1.0

        # Fill the output branches
        jetPUWeight = np.float32(jetPUWeight)
        jetPUWeightUp = np.float32(jetPUWeightUp)
        jetPUWeightDown = np.float32(jetPUWeightDown)
        # Fill the output branches with the weights
        
        self.out.fillBranch("jetPUIdWeight", jetPUWeight)
        self.out.fillBranch("jetPUIdWeightUp", jetPUWeightUp)
        self.out.fillBranch("jetPUIdWeightDown", jetPUWeightDown)

        return True  # Keep event

def jetPUIdWeightModule(era, channel):
    return jetPUIdWeightProducer(era, channel)