from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib
import numpy as np
import os
import awkward as ak
from coffea.lookup_tools import extractor
class bTaggingWeightProducer(Module):
    def __init__(self, config, channel):
        super().__init__()
        self.era = config['era']
        self.channel = channel
        effiFolder = config['efficiencyFolder']
        effiFile = os.path.join(effiFolder, config['era'], f"{channel}.root")
        bTaggingFile = config['bTagSFFile']
        self.bTageval = correctionlib.CorrectionSet.from_file(bTaggingFile)
        b_eff_ext = extractor()
        b_eff_ext.add_weight_sets(["* * "+effiFile])
        b_eff_ext.finalize()
        self.b_eff_evaluator = b_eff_ext.make_evaluator()
        self.bNames = config['branchNames']
        self.bTagThreshold = config['bTagThreshold']

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
                    effPass = self.b_eff_evaluator['Efficiency/FlavourC_Wp_pass_BM'](abs(jet.eta), jet.pt)
                    effTotal = self.b_eff_evaluator['Efficiency/FlavourC_Wp_pass_No'](abs(jet.eta), jet.pt)
                    eff = effPass / effTotal
                    weight = (1 - SF * eff) / (1 - eff)
                    weightUp = (1 - SFUp * eff) / (1 - eff)
                    weightDown = (1 - SFDown * eff) / (1 - eff)
                    bTagWeight *= weight
                    bTagWeightUp *= weightUp
                    bTagWeightDown *= weightDown
                elif jet.hadronFlavour == 4:
                    SF = self.bTageval['deepJet_mujets'].evaluate('central', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    SFUp = self.bTageval['deepJet_mujets'].evaluate('up', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    SFDown = self.bTageval['deepJet_mujets'].evaluate('down', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    effPass = self.b_eff_evaluator['Efficiency/FlavourB_Wp_pass_BM'](abs(jet.eta), jet.pt)
                    effTotal = self.b_eff_evaluator['Efficiency/FlavourB_Wp_pass_No'](abs(jet.eta), jet.pt)
                    eff = effPass / effTotal
                    weight = (1 - SF * eff) / (1 - eff)
                    weightUp = (1 - SFUp * eff) / (1 - eff)
                    weightDown = (1 - SFDown * eff) / (1 - eff)
                    bTagWeight *= weight
                    bTagWeightUp *= weightUp
                    bTagWeightDown *= weightDown
                elif jet.hadronFlavour == 0:
                    SF = self.bTageval['deepJet_incl'].evaluate('central', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    SFUp = self.bTageval['deepJet_incl'].evaluate('up', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    SFDown = self.bTageval['deepJet_incl'].evaluate('down', 'M', jet.hadronFlavour, abs(jet.eta), jet.pt)
                    effPass = self.b_eff_evaluator['Efficiency/FlavourL_Wp_pass_BM'](abs(jet.eta), jet.pt)
                    effTotal = self.b_eff_evaluator['Efficiency/FlavourL_Wp_pass_No'](abs(jet.eta), jet.pt)
                    eff = effPass / effTotal
                    weight = (1 - SF * eff) / (1 - eff)
                    weightUp = (1 - SFUp * eff) / (1 - eff)
                    weightDown = (1 - SFDown * eff) / (1 - eff)
                    bTagWeight *= weight
                    bTagWeightUp *= weightUp
                    bTagWeightDown *= weightDown
        
        self.out.fillBranch(self.bNames["sf"], bTagWeight)
        self.out.fillBranch(self.bNames["sfup"], bTagWeightUp)
        self.out.fillBranch(self.bNames["sfdown"], bTagWeightDown)

        return True  # Keep event

def bTaggingWeightModule(config, channel):
    return bTaggingWeightProducer(config, channel)
