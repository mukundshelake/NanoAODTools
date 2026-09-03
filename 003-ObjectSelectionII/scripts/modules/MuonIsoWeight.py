from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
import correctionlib
import ROOT


class MuonIsoWeightProducer(Module):
    """
    Muon isolation-selection-efficiency scale factor.

    This analysis's ABCD method splits every selected muon into "tight iso"
    (SelMuon_pfRelIso04_all < 0.06, ABCD regions A/C) and "loose iso"
    (>= 0.06, regions B/D) -- see 003-ObjectSelectionI's
    SelectedObjectsProducer. The standard Muon POG
    NUM_TightRelIso_DEN_TightIDandIPCut correction (in mu_ID.json,
    correctionlib) is measured at the standard 0.15 "Tight" WP, not this
    analysis's tighter 0.06 cut, so it's only used for the loose-iso
    (>= 0.06) side. For the tight-iso (< 0.06) side, a dedicated TnP
    measurement at the 0.06 WP is used instead (UL{era}_mu_Iso_0p06.root --
    a plain ROOT TH2F, not correctionlib).

    Every event reaching this module already has a real SelMuon --
    003-ObjectSelectionI's SelectionCuts require >=1 muon before
    SelectedObjectsProducer even runs -- so unlike
    MuonIDWeightProducer/MuonHLTWeightProducer, no no-muon sentinel handling
    is needed here.

    ROOT-file uncertainty layout (checked directly with uproot before
    writing this): the "<hist>_stat"/"<hist>_syst" companion histograms'
    bin CONTENT is just a copy of the nominal SF -- the actual stat/syst
    uncertainty is stored in their bin ERROR instead. That's why this reads
    TH2F objects directly via PyROOT (GetBinContent/GetBinError) rather
    than through coffea.lookup_tools.extractor (used elsewhere in this
    chapter, e.g. bTaggingWeight.py) -- extractor's dense_lookup only
    exposes bin content, which would silently return the nominal SF again
    for the stat/syst branches instead of the real uncertainty.
    """

    def __init__(self, config):
        super().__init__()
        self.jsonEval = correctionlib.CorrectionSet.from_file(config['StandardSFFile'])
        self.jsonWeightName = config['correctionLib']['weightName']

        rootFile = config['TightIsoSFFile']
        histBase = config['correctionLib']['rootHistName']
        f = ROOT.TFile.Open(rootFile, "READ")
        self._hNom  = f.Get(histBase)
        self._hStat = f.Get(f"{histBase}_stat")
        self._hSyst = f.Get(f"{histBase}_syst")
        for h in (self._hNom, self._hStat, self._hSyst):
            h.SetDirectory(0)
        f.Close()

        # Bin range of the 0.06-WP histograms -- a lookup outside this range
        # returns bin 0/N+1 (under/overflow, always empty here), so pt/|eta|
        # get clamped just inside it before every evaluation (i.e. an
        # underflowing/overflowing value uses the first/last bin's value).
        etaAxis, ptAxis = self._hNom.GetXaxis(), self._hNom.GetYaxis()
        self._etaMin, self._etaMax = etaAxis.GetXmin(), etaAxis.GetXmax()
        self._ptMin,  self._ptMax  = ptAxis.GetXmin(),  ptAxis.GetXmax()

        self.selMuonBranch      = config['selMuonBranch']
        self.abcdTightIsoBranch = config['abcdTightIsoBranch']
        self.bNames = config['branchNames']

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(self.bNames["sf"],     "F")
        self.out.branch(self.bNames["sfstat"], "F")
        self.out.branch(self.bNames["sfsyst"], "F")

    @staticmethod
    def _clamp(value, lo, hi):
        eps = 1e-6
        return min(max(value, lo + eps), hi - eps)

    def analyze(self, event):
        pt  = getattr(event, f"{self.selMuonBranch}_pt")
        eta = abs(getattr(event, f"{self.selMuonBranch}_eta"))
        isTightIso = bool(getattr(event, self.abcdTightIsoBranch))

        if isTightIso:
            eta_c = self._clamp(eta, self._etaMin, self._etaMax)
            pt_c  = self._clamp(pt,  self._ptMin,  self._ptMax)
            binx = self._hNom.GetXaxis().FindFixBin(eta_c)
            biny = self._hNom.GetYaxis().FindFixBin(pt_c)
            IsoSF     = self._hNom.GetBinContent(binx, biny)
            IsoSFStat = self._hStat.GetBinError(binx, biny)
            IsoSFSyst = self._hSyst.GetBinError(binx, biny)
        else:
            IsoSF     = self.jsonEval[self.jsonWeightName].evaluate(eta, pt, 'nominal')
            IsoSFStat = self.jsonEval[self.jsonWeightName].evaluate(eta, pt, 'stat')
            IsoSFSyst = self.jsonEval[self.jsonWeightName].evaluate(eta, pt, 'syst')

        self.out.fillBranch(self.bNames["sf"],     IsoSF)
        self.out.fillBranch(self.bNames["sfstat"], IsoSFStat)
        self.out.fillBranch(self.bNames["sfsyst"], IsoSFSyst)
        return True


def muonIsoWeightModule(config):
    return MuonIsoWeightProducer(config)
