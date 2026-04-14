from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection


# Sentinel value written to *_pt branches when the object is not found.
_SENTINEL_PT = -1.0


class SelectedObjectsProducer(Module):
    """
    Identifies the selected leading muon and the 4 reconstructable jets
    (2 b-tagged + 2 light, taken from the 4 highest-pT selected jets) and
    writes their kinematics to flat branches.

    Selection logic mirrors the event-level cut string in config.yaml so that
    downstream modules (SF weights, reco, BDT) have a single consistent source
    of truth and do not need to re-run kinematic cuts independently.

    Output sentinel: *_pt = -1 when the object is absent (no muon found, or
    the top-4 jets do not split into exactly 2b+2l).

    Expected config keys
    --------------------
    kinematics:
      Muon:
        lohi:  {var: {low: x, high: y}, ...}
        value: {var: val, ...}
      Jet:
        pt_min:  float
        eta_max: float
        jetId:   int
    bTagThreshold: float
    branchNames:
      muon:           str   # prefix, e.g. "SelMuon"
      leadingbJet:    str   # e.g. "leadingbJet"
      subleadingbJet: str
      leadingJet:     str   # leading *light* jet among top-4
      subleadingJet:  str
    """

    _MUON_FLOAT_FIELDS = ["pt", "eta", "phi", "mass", "pfRelIso04_all"]
    _MUON_INT_FIELDS   = ["charge"]
    _JET_FLOAT_FIELDS  = ["pt", "eta", "phi", "mass", "btagDeepFlavB"]
    _JET_INT_FIELDS    = ["hadronFlavour", "jetId", "puId"]
    _JET_KEYS          = ["leadingbJet", "subleadingbJet", "leadingJet", "subleadingJet"]

    def __init__(self, config):
        super().__init__()
        self.muonCut       = config['kinematics']['Muon']
        self.jetCut        = config['kinematics']['Jet']
        self.bTagThreshold = float(config['bTagThreshold'])
        self.bNames        = config['branchNames']

        # Ensure lohi bounds are floats (YAML→JSON round-trip can produce strings)
        for bounds in self.muonCut["lohi"].values():
            bounds['low']  = float(bounds['low'])
            bounds['high'] = float(bounds['high'])

        self._jet_pt_min  = float(self.jetCut['pt_min'])
        self._jet_eta_max = float(self.jetCut['eta_max'])
        self._jet_jetId   = int(self.jetCut['jetId'])

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        muon_prefix = self.bNames["muon"]

        for field in self._MUON_FLOAT_FIELDS:
            self.out.branch(f"{muon_prefix}_{field}", "F")
        for field in self._MUON_INT_FIELDS:
            self.out.branch(f"{muon_prefix}_{field}", "I")
        self.out.branch(f"{muon_prefix}_tightId", "O")

        for jet_key in self._JET_KEYS:
            prefix = self.bNames[jet_key]
            for field in self._JET_FLOAT_FIELDS:
                self.out.branch(f"{prefix}_{field}", "F")
            for field in self._JET_INT_FIELDS:
                self.out.branch(f"{prefix}_{field}", "I")

    def analyze(self, event):
        self._fill_muon(event)
        self._fill_jets(event)
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fill_muon(self, event):
        muons    = Collection(event, "Muon")
        sel_muon = None

        for muon in muons:
            if not self._passes_muon_cuts(muon):
                continue
            if sel_muon is None or muon.pt > sel_muon.pt:
                sel_muon = muon

        prefix = self.bNames["muon"]
        if sel_muon is not None:
            self.out.fillBranch(f"{prefix}_pt",             sel_muon.pt)
            self.out.fillBranch(f"{prefix}_eta",            sel_muon.eta)
            self.out.fillBranch(f"{prefix}_phi",            sel_muon.phi)
            self.out.fillBranch(f"{prefix}_mass",           sel_muon.mass)
            self.out.fillBranch(f"{prefix}_pfRelIso04_all", sel_muon.pfRelIso04_all)
            self.out.fillBranch(f"{prefix}_charge",         sel_muon.charge)
            self.out.fillBranch(f"{prefix}_tightId",        bool(sel_muon.tightId))
        else:
            self.out.fillBranch(f"{prefix}_pt",             _SENTINEL_PT)
            self.out.fillBranch(f"{prefix}_eta",            0.0)
            self.out.fillBranch(f"{prefix}_phi",            0.0)
            self.out.fillBranch(f"{prefix}_mass",           0.0)
            self.out.fillBranch(f"{prefix}_pfRelIso04_all", 0.0)
            self.out.fillBranch(f"{prefix}_charge",         0)
            self.out.fillBranch(f"{prefix}_tightId",        False)

    def _fill_jets(self, event):
        jets     = Collection(event, "Jet")
        sel_jets = [j for j in jets if self._passes_jet_cuts(j)]

        # Sort all selected jets by pT descending, then greedily pick the first
        # two b-tagged jets as leading/subleading b-jets.  Strip those out and
        # take the next two highest-pT jets as the light-jet pair.  This avoids
        # the failure mode where the 5th/6th jet by pT carries the b-tags while
        # the top-4-only approach would incorrectly mark the event as b-jet poor.
        sorted_jets = sorted(sel_jets, key=lambda j: j.pt, reverse=True)

        bjets = []
        bjets_set = set()
        for j in sorted_jets:
            if j.btagDeepFlavB > self.bTagThreshold:
                bjets.append(j)
                bjets_set.add(id(j))
            if len(bjets) == 2:
                break

        ljets = []
        for j in sorted_jets:
            if id(j) not in bjets_set:
                ljets.append(j)
            if len(ljets) == 2:
                break

        jet_map = {
            "leadingbJet":    bjets[0] if len(bjets) >= 1 else None,
            "subleadingbJet": bjets[1] if len(bjets) >= 2 else None,
            "leadingJet":     ljets[0] if len(ljets) >= 1 else None,
            "subleadingJet":  ljets[1] if len(ljets) >= 2 else None,
        }

        for jet_key, jet in jet_map.items():
            prefix = self.bNames[jet_key]
            if jet is not None:
                self.out.fillBranch(f"{prefix}_pt",            jet.pt)
                self.out.fillBranch(f"{prefix}_eta",           jet.eta)
                self.out.fillBranch(f"{prefix}_phi",           jet.phi)
                self.out.fillBranch(f"{prefix}_mass",          jet.mass)
                self.out.fillBranch(f"{prefix}_btagDeepFlavB", jet.btagDeepFlavB)
                self.out.fillBranch(f"{prefix}_hadronFlavour", jet.hadronFlavour)
                self.out.fillBranch(f"{prefix}_jetId",         jet.jetId)
                self.out.fillBranch(f"{prefix}_puId",          jet.puId)
            else:
                self.out.fillBranch(f"{prefix}_pt",            _SENTINEL_PT)
                self.out.fillBranch(f"{prefix}_eta",           0.0)
                self.out.fillBranch(f"{prefix}_phi",           0.0)
                self.out.fillBranch(f"{prefix}_mass",          0.0)
                self.out.fillBranch(f"{prefix}_btagDeepFlavB", 0.0)
                self.out.fillBranch(f"{prefix}_hadronFlavour", -1)
                self.out.fillBranch(f"{prefix}_jetId",         -1)
                self.out.fillBranch(f"{prefix}_puId",         -1)

    def _passes_muon_cuts(self, muon):
        for var, cut in self.muonCut["lohi"].items():
            if muon[var] < cut['low'] or muon[var] > cut['high']:
                return False
        for var, val in self.muonCut["value"].items():
            if muon[var] != val:
                return False
        return True

    def _passes_jet_cuts(self, jet):
        return (
            jet.pt > self._jet_pt_min
            and abs(jet.eta) < self._jet_eta_max
            and jet.jetId == self._jet_jetId
            and (jet.pt > 50 or jet.puId > 0)
        )


def selectedObjectsModule(config):
    return SelectedObjectsProducer(config)
