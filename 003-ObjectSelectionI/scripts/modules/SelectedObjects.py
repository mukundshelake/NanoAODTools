import math

from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection


# Sentinel value written to *_pt branches when the object is not found.
_SENTINEL_PT = -1.0


class SelectedObjectsProducer(Module):
    """
    Identifies the selected leading muon and the 4 reconstructable jets
    (2 b-tagged + 2 light, taken from the 4 highest-pT selected jets), writes
    their kinematics to flat branches, and tags each event with its ABCD-plane
    region (muon isolation x W transverse mass) for a QCD-multijet background
    estimate.

    Selection logic mirrors the event-level cut string in config.yaml so that
    downstream modules (SF weights, reco, BDT) have a single consistent source
    of truth and do not need to re-run kinematic cuts independently.

    Output sentinel: *_pt = -1 when the object is absent (no muon found, or
    the top-4 jets do not split into exactly 2b+2l).

    ABCD region tagging (branchNames.abcdRegion prefix, e.g. "ABCD"): this only
    *tags* events, it never cuts -- every event that reaches this module gets a
    region. Reuses the exact muon _fill_muon() already selected (NanoAODTools
    builds each Event strictly from the input tree -- see eventloop.py -- so a
    separate module could not read a branch this module writes; tagging has to
    happen here, in the same pass, to reuse sel_muon directly).

    The second ABCD-plane axis is the W transverse mass, mTW = sqrt(2 * pT(mu)
    * MET_pt * (1 - cos(dphi(mu, MET)))) -- chosen over raw MET_pt because a
    genuine W->mu·nu decay produces a well-understood, sharply-edged mTW shape
    that isn't as susceptible to the low-MET resolution tail raw MET has (that
    tail is what caused real ttbar semi-leptonic MC to over-predict the
    tight-iso/low-MET/high-muon-pT corner of the old MET-based scheme).
    MET_pt/MET_phi are the standard NanoAOD branches -- not configurable, since
    there's no actual need to swap MET flavors here.

    Each axis (isolation, mTW) is classified with two independent thresholds,
    not one: e.g. "tight" iso is <= isoLowMax, "loose" iso is >= isoHighMin.
    With isoLowMax == isoHighMin (and mTWLowMax == mTWHighMin) every event
    still falls unambiguously on one side, exactly like a single shared
    threshold -- that's the default config today. But if the two ever diverge
    (e.g. widening isoHighMin above isoLowMax to open a gap), an event whose
    isolation or mTW falls strictly between its pair's two values doesn't
    unambiguously belong to "tight/loose" or "low/high" -- that event's region
    is undefined (see below), not assigned to a bucket by an implicit >=/<=
    tie-break. Region codes:
        0 = A: tight iso, high mTW  -- signal region
        1 = B: loose iso, high mTW
        2 = C: tight iso, low mTW
        3 = D: loose iso, low mTW   -- QCD-enriched template region
       -1 = undefined (no selected muon, OR iso/mTW falls in an open gap
            between its low/high thresholds)
    QCD estimate (computed downstream, NOT by this module): N_A ~= N_B*N_C/N_D.

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
    abcdRegion:
      isoLowMax:   float  # SelMuon_pfRelIso04_all <= this => "tight" (A/C)
      isoHighMin:  float  # SelMuon_pfRelIso04_all >= this => "loose" (B/D)
      mTWLowMax:   float  # mTW < this  => "low mTW"  (C/D)
      mTWHighMin:  float  # mTW >= this => "high mTW" (A/B)
    branchNames:
      muon:           str   # prefix, e.g. "SelMuon"
      leadingbJet:    str   # e.g. "leadingbJet"
      subleadingbJet: str
      leadingJet:     str   # leading *light* jet among top-4
      subleadingJet:  str
      abcdRegion:     str   # e.g. "ABCD"
    """

    _MUON_FLOAT_FIELDS = ["pt", "eta", "phi", "mass", "pfRelIso04_all"]
    _MUON_INT_FIELDS   = ["charge"]
    _JET_FLOAT_FIELDS  = ["pt", "eta", "phi", "mass", "btagDeepFlavB"]
    _JET_INT_FIELDS    = ["jetId", "puId"]
    _JET_INT_FIELDS_MC = ["hadronFlavour"]  # MC-only branches
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
        self._is_mc       = bool(config.get('is_mc', True))

        abcd_cfg          = config['abcdRegion']
        self.isoLowMax    = float(abcd_cfg['isoLowMax'])
        self.isoHighMin   = float(abcd_cfg['isoHighMin'])
        self.mTWLowMax    = float(abcd_cfg['mTWLowMax'])
        self.mTWHighMin   = float(abcd_cfg['mTWHighMin'])

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
            if self._is_mc:
                for field in self._JET_INT_FIELDS_MC:
                    self.out.branch(f"{prefix}_{field}", "I")
        
        # Branches for counting selected jets
        self.out.branch("sel_nJet", "I")
        self.out.branch("sel_nbjet", "I")

        abcd_prefix = self.bNames["abcdRegion"]
        self.out.branch(f"{abcd_prefix}_isTightIso", "O")
        self.out.branch(f"{abcd_prefix}_isHighMTW",  "O")
        self.out.branch(f"{abcd_prefix}_mTW",        "F")
        self.out.branch(f"{abcd_prefix}_region",     "I")

    def analyze(self, event):
        sel_muon = self._fill_muon(event)
        jets = Collection(event, "Jet")
        sel_jets = [j for j in jets if self._passes_jet_cuts(j)]
        self._fill_jets(sel_jets)
        self._fill_jet_counts(sel_jets)
        self._fill_abcd_region(event, sel_muon)
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

        return sel_muon

    def _fill_abcd_region(self, event, sel_muon):
        # See the class docstring for the full region-code convention. Tags
        # only -- never rejects an event.
        prefix = self.bNames["abcdRegion"]

        if sel_muon is not None:
            mTW = self._compute_mTW(sel_muon, event)

            # Each axis is independently "tight/loose" (iso) or "low/high"
            # (mTW) against its own pair of thresholds. With the low/high
            # pair equal (today's default), exactly one side is always true
            # -- the "else: undefined" case below is only reachable once the
            # two are deliberately set apart to open a gap.
            is_tight_iso = sel_muon.pfRelIso04_all <= self.isoLowMax
            is_loose_iso = sel_muon.pfRelIso04_all >= self.isoHighMin
            is_low_mTW   = mTW < self.mTWLowMax
            is_high_mTW  = mTW >= self.mTWHighMin

            if is_tight_iso and is_low_mTW:
                region = 2  # C
            elif is_tight_iso and is_high_mTW:
                region = 0  # A
            elif is_loose_iso and is_low_mTW:
                region = 3  # D
            elif is_loose_iso and is_high_mTW:
                region = 1  # B
            else:
                region = -1  # iso and/or mTW fell in a gap between its thresholds
        else:
            mTW = 0.0
            is_tight_iso = False
            is_high_mTW  = False
            region = -1

        self.out.fillBranch(f"{prefix}_isTightIso", is_tight_iso)
        self.out.fillBranch(f"{prefix}_isHighMTW",  is_high_mTW)
        self.out.fillBranch(f"{prefix}_mTW",        mTW)
        self.out.fillBranch(f"{prefix}_region",     region)

    @staticmethod
    def _compute_mTW(sel_muon, event):
        """W transverse mass from the selected muon and MET_pt/MET_phi (the
        standard NanoAOD MET branches -- not configurable, see class docstring).
        """
        dphi = abs(sel_muon.phi - event.MET_phi)
        if dphi > math.pi:
            dphi = 2.0 * math.pi - dphi
        return math.sqrt(2.0 * sel_muon.pt * event.MET_pt * (1.0 - math.cos(dphi)))

    def _fill_jets(self, sel_jets):
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
                if self._is_mc:
                    self.out.fillBranch(f"{prefix}_hadronFlavour", jet.hadronFlavour)
                self.out.fillBranch(f"{prefix}_jetId",         jet.jetId)
                self.out.fillBranch(f"{prefix}_puId",          jet.puId)
            else:
                self.out.fillBranch(f"{prefix}_pt",            _SENTINEL_PT)
                self.out.fillBranch(f"{prefix}_eta",           0.0)
                self.out.fillBranch(f"{prefix}_phi",           0.0)
                self.out.fillBranch(f"{prefix}_mass",          0.0)
                self.out.fillBranch(f"{prefix}_btagDeepFlavB", 0.0)
                if self._is_mc:
                    self.out.fillBranch(f"{prefix}_hadronFlavour", -1)
                self.out.fillBranch(f"{prefix}_jetId",         -1)
                self.out.fillBranch(f"{prefix}_puId",          -1)

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

    def _fill_jet_counts(self, sel_jets):
        """Count selected jets and b-tagged jets (sel_jets already passed _passes_jet_cuts)."""
        sel_nJet = len(sel_jets)
        sel_nbjet = sum(1 for j in sel_jets if j.btagDeepFlavB > self.bTagThreshold)

        self.out.fillBranch("sel_nJet", sel_nJet)
        self.out.fillBranch("sel_nbjet", sel_nbjet)


def selectedObjectsModule(config):
    return SelectedObjectsProducer(config)
