from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
import numpy as np
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import ROOT
class ObservablesProducer(Module):
    def __init__(self):
        super().__init__()
        self.warn_once = False  # To limit warnings

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        """Initialize output branches before event loop starts"""
        self.out = wrappedOutputTree
        self.out.branch("cosTheta", "F")
        self.out.branch("LabcosTheta", "F")
        self.out.branch("anticosTheta", "F")
        self.out.branch("yt", "F")
        self.out.branch("ytbar", "F")
        self.out.branch("ttbar_pz", "F")
        self.out.branch("ttbar_mass", "F")

        # Check if the branch exists before processing events

    def analyze(self, event):
        """Process each event and compute LHEWeightSign"""

        top_lep_pt = event.Top_lep_pt
        top_had_pt = event.Top_had_pt
        top_lep_eta = event.Top_lep_eta
        top_had_eta = event.Top_had_eta
        top_lep_phi = event.Top_lep_phi
        top_had_phi = event.Top_had_phi
        top_lep_mass = event.Top_lep_mass
        top_had_mass = event.Top_had_mass

        muons = Collection(event, "Muon")
        selected_muons = [mu for mu in muons if mu.pt > 26 and abs(mu.eta) < 2.4 and mu.tightId and mu.pfRelIso04_all < 0.06]
        mu = max(selected_muons, key=lambda m: m.pt)
        mu_charge = mu.charge

        if mu_charge > 0:
            top = ROOT.TLorentzVector()
            top.SetPtEtaPhiM(top_lep_pt, top_lep_eta, top_lep_phi, top_lep_mass)
            antitop = ROOT.TLorentzVector()
            antitop.SetPtEtaPhiM(top_had_pt, top_had_eta, top_had_phi, top_had_mass)
        else:
            antitop = ROOT.TLorentzVector()
            antitop.SetPtEtaPhiM(top_lep_pt, top_lep_eta, top_lep_phi, top_lep_mass)
            top = ROOT.TLorentzVector()
            top.SetPtEtaPhiM(top_had_pt, top_had_eta, top_had_phi, top_had_mass)

        ttbar = top + antitop
        ttbar_mass = ttbar.M()
        cosTheta_lab = top.CosTheta()
        # Boost to ttbar rest frame
        boost_vector = -ttbar.BoostVector()
        top.Boost(boost_vector)
        antitop.Boost(boost_vector)

        # Compute cosTheta in ttbar rest frame: angle between top and beam axis (z-axis)
        cosTheta = top.CosTheta()
        # Compute anticosTheta in ttbar rest frame: angle between antitop and beam axis (z-axis)
        anticosTheta = antitop.CosTheta()


        # Decide sign of cosTheta based on ttbar z momentum in lab frame
        if ttbar.Pz() < 0:
            cosTheta = -cosTheta
            cosTheta_lab = -cosTheta_lab

        # Compute rapidities
        yt = 0.5 * np.log((top.E() + top.Pz()) / (top.E() - top.Pz())) if (top.E() - top.Pz()) != 0 else 0.0
        ytbar = 0.5 * np.log((antitop.E() + antitop.Pz()) / (antitop.E() - antitop.Pz())) if (antitop.E() - antitop.Pz()) != 0 else 0.0
        ttbar_pz = ttbar.Pz()


        # Fill branches
        self.out.fillBranch("cosTheta", cosTheta)
        self.out.fillBranch("anticosTheta", anticosTheta)
        self.out.fillBranch("LabcosTheta", cosTheta_lab)
        self.out.fillBranch("yt", yt)
        self.out.fillBranch("ytbar", ytbar)
        self.out.fillBranch("ttbar_pz", ttbar_pz)
        self.out.fillBranch("ttbar_mass", ttbar_mass)

        return True  # Keep event

def observablesModule():
    return ObservablesProducer()

