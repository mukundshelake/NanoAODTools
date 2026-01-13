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
        self.inputTree = inputTree
        
        # Check for required branches
        required_branches = [
            'Top_lep_pt', 'Top_had_pt', 'Top_lep_eta', 'Top_had_eta',
            'Top_lep_phi', 'Top_had_phi', 'Top_lep_mass', 'Top_had_mass'
        ]
        
        self.branches_exist = True
        missing_branches = []
        for branch in required_branches:
            if not inputTree.GetBranch(branch):
                missing_branches.append(branch)
                self.branches_exist = False
        
        if not self.branches_exist:
            print(f"WARNING: Missing required branches: {missing_branches}")
            print("ObservablesProducer will skip all events.")
        
        self.out.branch("cosTheta", "F")
        self.out.branch("LabcosTheta", "F")
        self.out.branch("anticosTheta", "F")
        self.out.branch("yt", "F")
        self.out.branch("ytbar", "F")
        self.out.branch("ttbar_pz", "F")
        self.out.branch("ttbar_mass", "F")

    def analyze(self, event):
        """Process each event and compute LHEWeightSign"""
        
        # Skip processing if required branches don't exist
        if not self.branches_exist:
            return False
        
        # Use try-except to catch any branch access errors
        try:
            top_lep_pt = event.Top_lep_pt
            top_had_pt = event.Top_had_pt
            top_lep_eta = event.Top_lep_eta
            top_had_eta = event.Top_had_eta
            top_lep_phi = event.Top_lep_phi
            top_had_phi = event.Top_had_phi
            top_lep_mass = event.Top_lep_mass
            top_had_mass = event.Top_had_mass
        except AttributeError as e:
            if not self.warn_once:
                print(f"WARNING: Could not access branch: {e}")
                self.warn_once = True
            return False
        
        # Validate that values are reasonable (not NaN, not inf, positive pt)
        if not all([
            top_lep_pt > 0, top_had_pt > 0,
            abs(top_lep_eta) < 10, abs(top_had_eta) < 10,
            abs(top_lep_phi) <= 3.15, abs(top_had_phi) <= 3.15,
            top_lep_mass > 0, top_had_mass > 0
        ]):
            return False

        try:
            muons = Collection(event, "Muon")
            selected_muons = [mu for mu in muons if mu.pt > 26 and abs(mu.eta) < 2.4 and mu.tightId and mu.pfRelIso04_all <= 0.06]
        except (AttributeError, Exception) as e:
            if not self.warn_once:
                print(f"WARNING: Could not access Muon collection or attributes: {e}")
                self.warn_once = True
            return False
            
        if len(selected_muons) == 0:
            return False  # Skip event if no selected muons
        mu = max(selected_muons, key=lambda m: m.pt)
        mu_charge = mu.charge

        # Create TLorentzVectors with error handling
        try:
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
        except Exception as e:
            if not self.warn_once:
                print(f"WARNING: Error creating TLorentzVector: {e}")
                self.warn_once = True
            return False
        ttbar_mass = ttbar.M()
        ttbar_pz = ttbar.Pz()
        
        # Store lab frame cosTheta before boosting
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
        if ttbar_pz < 0:
            cosTheta = -cosTheta
            cosTheta_lab = -cosTheta_lab

        # Compute rapidities with proper checks to avoid log of negative or division by zero
        denom_t = top.E() - top.Pz()
        denom_tbar = antitop.E() - antitop.Pz()
        
        if denom_t > 1e-10 and (top.E() + top.Pz()) > 0:
            yt = 0.5 * np.log((top.E() + top.Pz()) / denom_t)
        else:
            yt = 0.0
            
        if denom_tbar > 1e-10 and (antitop.E() + antitop.Pz()) > 0:
            ytbar = 0.5 * np.log((antitop.E() + antitop.Pz()) / denom_tbar)
        else:
            ytbar = 0.0


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

