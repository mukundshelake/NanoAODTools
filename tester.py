import ROOT
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Event

# open your NanoAOD file
f = ROOT.TFile.Open("/home/mukund/Projects/SkimandSlim/NanoAODTools/selected_SemiLeptonic_2k.root")
events = Event(f)

# loop over some events
for i, event in enumerate(events):
    muons = Collection(event, "Muon")  # all muons in this event

    print(f"Event {i}: nMuon = {len(muons)}")
    for j, mu in enumerate(muons):
        print(f"  Muon {j}: pt={mu.pt:.2f}, eta={mu.eta:.2f}, tightId={mu.tightId}")

    if i > 3:
        break