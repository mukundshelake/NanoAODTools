from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
import correctionlib


class JetVetoMapProducer(Module):
    """
    JME-recommended jet veto map: flags jets falling in known noisy/dead
    detector regions ("hot"/"cold" zones), identified via the phi-symmetry
    of the CMS detector. Same map for Data and MC alike -- this is a hardware
    defect, not a simulation-vs-data efficiency difference (unlike JER),
    so runs in PreSelectionCorrectionModuleList.MC *and* .Data.

    Verified directly against the fetched file (correctionlib): the
    'jetvetomap' category takes (eta, phi), returns 0.0 for clean and a
    nonzero code for vetoed (confirmed ~2.5% of eta/phi space is nonzero on
    a fine grid scan) -- no need to pre-clip inputs, correctionlib handles
    eta/phi outside the map's covered range via its own flow behavior.

    Implemented as a per-jet exclusion (zero out a vetoed jet's pt/mass),
    not an event-level veto: this pipeline already treats other per-jet
    quality flags (jetId, puId) the same way (silently drop a bad jet from
    selection/counting, rather than reject the whole event), so this stays
    consistent with that existing convention rather than introducing a new
    event-rejection mechanism whose exact recommended form (e.g. a jet-pt
    threshold for triggering a full event veto) isn't independently
    confirmed here. Runs in the same early PostProcessor pass as JetJER
    (both write Jet_pt/Jet_mass in place before SelectionCuts' jetCut/
    bjetCut ever sees them), for the same reason JER needed a separate pass:
    that cut string is evaluated against the *input* tree before any
    ModuleList module (in the *main* selection pass) runs, so an exclusion
    decided only there would be too late to affect which events/jets get
    selected.
    """

    def __init__(self, vetoMapFile, mapName, jetBranch="Jet"):
        super().__init__()
        self.evaluator = correctionlib.CorrectionSet.from_file(vetoMapFile)
        if mapName not in self.evaluator:
            raise KeyError(
                f"'{mapName}' not found in {vetoMapFile}. Available keys: "
                f"{list(self.evaluator.keys())}"
            )
        self.vetoMap = self.evaluator[mapName]
        self.jetBranch = jetBranch

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch(f"{self.jetBranch}_pt", "F", lenVar=f"n{self.jetBranch}")
        self.out.branch(f"{self.jetBranch}_mass", "F", lenVar=f"n{self.jetBranch}")

    def analyze(self, event):
        jets = Collection(event, self.jetBranch)

        new_pt, new_mass = [], []
        for jet in jets:
            vetoed = self.vetoMap.evaluate("jetvetomap", jet.eta, jet.phi) > 0
            new_pt.append(0.0 if vetoed else jet.pt)
            new_mass.append(0.0 if vetoed else jet.mass)

        self.out.fillBranch(f"{self.jetBranch}_pt", new_pt)
        self.out.fillBranch(f"{self.jetBranch}_mass", new_mass)

        return True


def JetVetoMapModule(config):
    return JetVetoMapProducer(
        vetoMapFile=config["vetoMapFile"],
        mapName=config["mapName"],
        jetBranch=config.get("jetBranch", "Jet"),
    )
