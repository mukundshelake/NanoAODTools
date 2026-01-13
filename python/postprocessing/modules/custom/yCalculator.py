import numpy as np
import awkward as ak
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection

class CollisionTypeProducer(Module):
    def __init__(self):
        super(CollisionTypeProducer, self).__init__()
        self.warn_once = False
        self.is_data = False

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.inputTree = inputTree
        
        # Check if GenPart branch exists (won't exist in data)
        if not inputTree.GetBranch("nGenPart"):
            self.is_data = True
            print("INFO: GenPart branch not found. This appears to be data - skipping parton classification.")
        
        # We'll store integer labels for clarity:
        # 1=qqbar, 2=gg, 3=qg, 4=qqprime, 0=undefined
        self.out.branch("y", "I")
        self.out.branch("qDir", "I")

    def analyze(self, event):
        # Skip MC-only processing for data files
        if self.is_data:
            self.out.fillBranch("y", 0)
            self.out.fillBranch("qDir", 0)
            return True
        
        try:
            genparts = Collection(event, "GenPart")
        except Exception as e:
            if not self.warn_once:
                print(f"WARNING: Could not access GenPart collection: {e}")
                self.warn_once = True
            self.out.fillBranch("y", 0)
            self.out.fillBranch("qDir", 0)
            return True

        if len(genparts) < 2:
            self.out.fillBranch("y", 0)
            self.out.fillBranch("qDir", 0)
            return True

        try:
            pdgIds = np.array([p.pdgId for p in genparts])
            status = np.array([p.status for p in genparts])
        except (AttributeError, Exception) as e:
            if not self.warn_once:
                print(f"WARNING: Could not access GenPart attributes: {e}")
                self.warn_once = True
            self.out.fillBranch("y", 0)
            self.out.fillBranch("qDir", 0)
            return True

        # Incoming partons typically have status == 21 in Pythia
        incoming = pdgIds[status == 21]

        if len(incoming) < 2:
            self.out.fillBranch("y", 0)
            self.out.fillBranch("qDir", 0)
            return True
        dir_value = 0
        id1, id2 = int(incoming[0]), int(incoming[1])

        abs1, abs2 = abs(id1), abs(id2)

        # Classification logic
        if abs1 == 21 and abs2 == 21:
            y_val = 2  # gg
        elif (abs1 == 21 and abs2 <= 6) or (abs2 == 21 and abs1 <= 6):
            y_val = 3  # qg
        elif abs1 <= 6 and abs2 <= 6:
            if id1 == -id2:
                y_val = 1  # qqbar
                if id1 > 0:
                    dir_value = 1
                else:
                    dir_value = -1
            elif abs1 != abs2:
                y_val = 4  # qq'
            else:
                y_val = 5  # qq (same flavor)
        else:
            y_val = 0  # undefined or something else

        self.out.fillBranch("y", y_val)
        self.out.fillBranch("qDir", dir_value)
        return True


def yCalculator():
    return CollisionTypeProducer()

