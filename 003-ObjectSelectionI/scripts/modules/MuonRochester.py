from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import PhysicsTools.NanoAODTools.postprocessing as _pp
import ROOT
import os


def _roccor_dir():
    """Bundled RoccoR.Run2.v3 sits alongside this framework's own postprocessing/
    package (python/postprocessing/data/roccor.Run2.v3/) -- located relative to
    the installed package rather than hardcoded, so this works the same whether
    NanoAODTools is used standalone (build/lib/python/... symlink) or from a
    CMSSW checkout."""
    # __path__, not __file__: under a CMSSW checkout this package is a namespace
    # package (no __init__.py), so its __file__ is None and dirname(None) raised
    # "expected str, bytes or os.PathLike object, not NoneType" for every dataset
    # on lxplus. __path__ is populated in both layouts.
    return os.path.join(list(_pp.__path__)[0], "data", "roccor.Run2.v3")


def _load_roccor():
    """Compile+load RoccoR.cc into ROOT exactly once per process.

    Needs `.L RoccoR.cc` (plain Cling interpretation), NOT `.L RoccoR.cc+`
    (ACLiC): ACLiC also generates a ROOT reflection dictionary, which fails
    here because RoccoR::RocOne is a private nested struct ROOT's dictionary
    generator can't introspect. Plain interpretation skips dictionary
    generation entirely -- fine since we only need to call member functions,
    not serialize RoccoR objects to a ROOT file.

    Also needs libboost-headers installed in this environment (`erf_inv` in
    RoccoR.h is the one genuinely Boost-specific call -- verified directly:
    everything else it uses resolves from plain <cmath>). Not vendored here;
    if this raises, the fix is `conda install -c conda-forge libboost-headers`
    in whatever env runs this, not a code change.
    """
    if hasattr(ROOT, "RoccoR"):
        return
    cc_path = os.path.join(_roccor_dir(), "RoccoR.cc")
    cwd = os.getcwd()
    # Under CMSSW's Cling (Clang 9, genuinely C++17), Boost.Config wrongly defines
    # BOOST_NO_CXX11_HDR_CHRONO, and Boost.Math >= 1.76 turns any such "missing
    # C++11 feature" into a hard #error ("Support for C++03 has been removed") the
    # moment erf.hpp is included -- so RoccoR.cc failed to load on lxplus even with
    # Boost present. RoccoR needs only erf_inv, never <chrono>. Pull in
    # boost/config.hpp once and drop that single false flag; its include guard
    # keeps it from being redefined when erf.hpp includes the config again.
    # Harmless where the flag was never set (e.g. the cms02 conda environment).
    ROOT.gInterpreter.Declare("#include <boost/config.hpp>\n#undef BOOST_NO_CXX11_HDR_CHRONO\n")
    try:
        os.chdir(_roccor_dir())  # RoccoR.cc #includes "RoccoR.h" by relative path
        rc = ROOT.gROOT.ProcessLine(f'.L {cc_path}')
    finally:
        os.chdir(cwd)
    if not hasattr(ROOT, "RoccoR"):
        raise RuntimeError(
            f"Failed to load RoccoR from {cc_path} (ProcessLine returned {rc}). "
            f"Check the Cling error printed above: in a conda environment the usual cause "
            f"is missing Boost headers (conda install -c conda-forge libboost-headers); "
            f"under CMSSW, Boost is present, so look for another false C++11 feature "
            f"flag like the BOOST_NO_CXX11_HDR_CHRONO one undefined above."
        )


class MuonRochesterProducer(Module):
    """
    Muon Rochester momentum-scale/resolution correction (roccor.Run2.v3),
    Data and MC both -- see RoccoR.h's kScaleDT/kSpreadMC/kSmearMC.

    Runs as its own PostProcessor pass (PreSelectionCorrectionModuleList),
    NOT a ModuleList entry: SelectionCuts' muonCut/extraMuonVetoCut reference
    Muon_pt directly in their cut string (e.g. "Sum$(Muon_pt > 26 && ...)
    == 1"), evaluated against the *input* tree before any module in the
    *main* selection pass runs -- same reasoning as JetJER.py for jets.
    Overwrites Muon_pt in place, NOT Muon_mass (Rochester corrections don't
    touch muon mass, unlike JER's jet mass rescaling).

    Data: kScaleDT(Q, pt, eta, phi) -- applies to every muon, no gen info
    needed.

    MC: kSpreadMC(Q, pt, eta, phi, genPt) when the muon has a valid gen
    match (Muon_genPartIdx >= 0) -- verified directly on real earlySeptember
    ttbar_SemiLeptonic/FullyLeptonic MC that the tight, isolated, pT>26 GeV
    SelMuon-equivalent candidate is gen-matched in 99.98-100% of events, so
    this covers essentially the whole analysis. The remaining unmatched
    muons would need kSmearMC(Q, pt, eta, phi, nTrkLayers, u), which needs
    Muon_nTrackerLayers -- not currently in this pipeline's kept branches.
    Rather than block on a 002-Samples reprocessing for a <0.1%-of-events
    edge case, those muons are left uncorrected (scale factor 1.0) here --
    a small, documented, empirically-measured approximation, not a silent
    gap.
    """

    def __init__(self, rochesterFile, isData, muonBranch="Muon"):
        super().__init__()
        _load_roccor()
        self.roc = ROOT.RoccoR(rochesterFile)
        self.isData = isData
        self.muonBranch = muonBranch

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(f"{self.muonBranch}_pt", "F", lenVar=f"n{self.muonBranch}")

    def analyze(self, event):
        muons = Collection(event, self.muonBranch)
        genparts = None if self.isData else Collection(event, "GenPart")

        new_pt = []
        for mu in muons:
            if self.isData:
                sf = self.roc.kScaleDT(mu.charge, mu.pt, mu.eta, mu.phi)
            else:
                genIdx = mu.genPartIdx
                if genIdx is not None and genIdx >= 0 and genIdx < len(genparts):
                    sf = self.roc.kSpreadMC(mu.charge, mu.pt, mu.eta, mu.phi,
                                             genparts[genIdx].pt)
                else:
                    sf = 1.0  # no gen match -- see class docstring
            new_pt.append(mu.pt * sf)

        self.out.fillBranch(f"{self.muonBranch}_pt", new_pt)
        return True


def MuonRochesterModule(config):
    return MuonRochesterProducer(
        rochesterFile=config["rochesterFile"],
        isData=config["isData"],
        muonBranch=config.get("muonBranch", "Muon"),
    )
