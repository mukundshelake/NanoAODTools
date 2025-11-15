from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib
import numpy as np
import awkward as ak

class MuonIDWeightProducer(Module):
    def __init__(self, config):
        super().__init__()
        self.IDeval = correctionlib.CorrectionSet.from_file(config['IDSFFile'])
        self.muonCut = config['kinematics']['Muon']
        self.clibConfig = config['correctionLib']
        self.bNames = config['branchNames']
        # print(f"Initialized MuonIDWeightProducer with config: {self.bNames}")

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"], "F")  # Float
        self.out.branch(self.bNames["sfup"], "F")  # Float
        self.out.branch(self.bNames["sfdown"], "F")  # Float

    def analyze(self, event):
        """Process each event and compute highest muon pt"""
        # Select leading (highest pT) muon
        muons = Collection(event, "Muon")
        leading_muon_pt = 0.0
        for muon in muons:
            isGoodMuon = True
            for lowhighCut in self.muonCut["lohi"]:
                if muon[lowhighCut] < self.muonCut["lohi"][lowhighCut]['low'] or muon[lowhighCut] > self.muonCut["lohi"][lowhighCut]['high']:
                    isGoodMuon = False
                    break
            for valueCut in self.muonCut["value"]:
                if not muon[valueCut] == self.muonCut["value"][valueCut]:
                    isGoodMuon = False
                    break
            if not isGoodMuon:
                continue
            if muon.pt > leading_muon_pt:
                leading_muon_pt = muon.pt
                leading_muon_eta = muon.eta
        if leading_muon_pt > 0:  # Ensure at least one muon was selected
            IDSF = self.IDeval[self.clibConfig["weightName"]].evaluate(self.clibConfig["eraName"], abs(leading_muon_eta), leading_muon_pt, 'sf')
            IDSFUp = self.IDeval[self.clibConfig["weightName"]].evaluate(self.clibConfig["eraName"], abs(leading_muon_eta), leading_muon_pt, 'systup')
            IDSFDown = self.IDeval[self.clibConfig["weightName"]].evaluate(self.clibConfig["eraName"], abs(leading_muon_eta), leading_muon_pt, 'systdown')
            # print(f"Leading muon pt: {leading_muon_pt}, eta: {leading_muon_eta}, IDSF: {IDSF}, IDSFUp: {IDSFUp}, IDSFDown: {IDSFDown}")
        else:  # Handle cases where no muons pass the selection
            IDSF = IDSFUp = IDSFDown = 1.0  # Default scale factor
            print("No muons passed the selection. Default scale factors applied.")

        self.out.fillBranch(self.bNames["sf"], IDSF)
        self.out.fillBranch(self.bNames["sfup"], IDSFUp)
        self.out.fillBranch(self.bNames["sfdown"], IDSFDown)
        # print(f"Filled branches: {self.bNames['sf']}={IDSF}, {self.bNames['sfup']}={IDSFUp}, {self.bNames['sfdown']}={IDSFDown}")

        return True  # Keep event

def muonIDWeightModule(config):
    return MuonIDWeightProducer(config)
