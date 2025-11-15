from jinja2 import Environment, FileSystemLoader
import yaml
import json
import os

def to_nice_yaml(value, indent=0):
    """Convert Python dicts/lists into indented YAML strings"""
    text = yaml.dump(value, default_flow_style=False)
    return text.strip()

def processBDTScoreConfigs(data, env, era):
    bdtScoreTemplate = env.get_template("BDTScoreTemplate.yaml.j2")
    bdtScoreData = {
        "DataModules": data["modules"]["Data"]["BDTScore"],
        "MCModules": data["modules"]["MC"]["BDTScore"]
    }
    renderedBDTScore = bdtScoreTemplate.render(bdtScoreData)
    outputDir = data.get("tag", "outputs")
    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, f"BDTScore_{era}_config.yaml"), "w") as f:
        f.write(renderedBDTScore)
    print(f"Rendered {outputDir}/BDTScore_{era}_config.yaml")

def processBDTvariablesConfigs(data, env, era):
    bdtVariablesTemplate = env.get_template("BDTvariablesTemplate.yaml.j2")
    bdtVariablesData = {
        "DataModules": data["modules"]["Data"]["BDTvariables"],
        "MCModules": data["modules"]["MC"]["BDTvariables"]
    }
    renderedBDTvariables = bdtVariablesTemplate.render(bdtVariablesData)
    outputDir = data.get("tag", "outputs")
    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, f"BDTvariables_{era}_config.yaml"), "w") as f:
        f.write(renderedBDTvariables)
    print(f"Rendered {outputDir}/BDTvariables_{era}_config.yaml")


def processobservablesConfigs(data, env, era):
    observablesTemplate = env.get_template("observablesTemplate.yaml.j2")
    observablesData = {
        "DataModules": data["modules"]["Data"]["observables"],
        "MCModules": data["modules"]["MC"]["observables"]
    }
    renderedObservables = observablesTemplate.render(observablesData)
    outputDir = data.get("tag", "outputs")
    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, f"observables_{era}_config.yaml"), "w") as f:
        f.write(renderedObservables)
    print(f"Rendered {outputDir}/observables_{era}_config.yaml")

def processRecoConfigs(data, env, era):
    recoTemplate = env.get_template("recoTemplate.yaml.j2")
    recoData = {
        "DataModules": data["modules"]["Data"]["reco"],
        "MCModules": data["modules"]["MC"]["reco"]
    }
    renderedReco = recoTemplate.render(recoData)
    outputDir = data.get("tag", "outputs")
    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, f"reco_{era}_config.yaml"), "w") as f:
        f.write(renderedReco)
    print(f"Rendered {outputDir}/reco_{era}_config.yaml")

def processSelectionConfigs(data, env, era):
    selectionTemplate = env.get_template("selectionTemplate.yaml.j2")
    selectionData = {
        "goldenJSON": data["gJSONs"][era],
        "branchFile": data["branchFile"],

        "muon_pt_lo": data["muon"]["pt"]["lo"][era],
        "muon_abs_eta_hi": data["muon"]["abs_eta"]["hi"][era],
        "muon_iso_hi": data["muon"]["iso"]["hi"][era],

        "jet_pt_lo": data["jet"]["pt"]["lo"][era],
        "jet_abs_eta_hi": data["jet"]["abs_eta"]["hi"][era],
        "btag_threshold": data["btag_threshold"][era],

        "atleast_n_muons": data["cuts"]["atleast_n_muons"][era],
        "atleast_n_btagged_jets": data["cuts"]["atleast_n_btagged_jets"][era],
        "atleast_n_jets": data["cuts"]["atleast_n_jets"][era],

        "hltFlag": data["hltFlag"][era],

        "DataModules": data["modules"]["Data"]["selection"],
        "MCModules": data["modules"]["MC"]["selection"]
    }
    renderedSelection = selectionTemplate.render(selectionData)
    outputDir = data.get("tag", "outputs")
    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, f"selection_{era}_config.yaml"), "w") as f:
        f.write(renderedSelection)
    print(f"Rendered {outputDir}/selection_{era}_config.yaml")

def processMuonIDWeightConfigs(data, env, era):
    muonIDWeightTemplate = env.get_template("MuonIDWeightTemplate.yaml.j2")
    muonIDWeightData = {
        "IDSFFile": data["IDSFParams"]["IDSFFile"][era], 
        "muon_pt_lo": data["muon"]["pt"]["lo"][era],
        "muon_pt_hi": data["muon"]["pt"]["hi"][era],
        "muon_eta_lo": data["muon"]["eta"]["lo"][era],
        "muon_eta_hi": data["muon"]["eta"]["hi"][era],
        "muon_iso_lo": data["muon"]["iso"]["lo"][era],
        "muon_iso_hi": data["muon"]["iso"]["hi"][era],
        "weightName": data["IDSFParams"]["weightName"],
        "eraName": data["IDSFParams"]["eraName"][era],
        "nominalBranch": data["IDSFParams"]["nominalBranch"],
        "upBranch": data["IDSFParams"]["upBranch"],
        "downBranch": data["IDSFParams"]["downBranch"]
    }
    renderedMuonIDWeight = muonIDWeightTemplate.render(muonIDWeightData)
    outputDir = data.get("tag", "outputs")
    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, f"muonID_{era}_config.yaml"), "w") as f:
        f.write(renderedMuonIDWeight)
    print(f"Rendered {outputDir}/muonID_{era}_config.yaml")


def processlheWeightConfigs(data, env, era):
    lheSignWeightTemplate = env.get_template("lheWeightSignTemplate.yaml.j2")
    lheSignWeightData = {
        "nominalBranch": data["lheWeightSignParams"]["nominalBranch"]
    }
    renderedlheSignWeight = lheSignWeightTemplate.render(lheSignWeightData)
    outputDir = data.get("tag", "outputs")
    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, f"lheWeightSign_{era}_config.yaml"), "w") as f:
        f.write(renderedlheSignWeight)
    print(f"Rendered {outputDir}/lheWeightSign_{era}_config.yaml")


def processMuonHLTWeightConfigs(data, env, era):
    muonHLTWeightTemplate = env.get_template("MuonHLTWeightTemplate.yaml.j2")
    muonHLTWeightData = {
        "HLTSFFile": data["HLTSFParams"]["HLTSFFile"][era], 
        "muon_pt_lo": data["muon"]["pt"]["lo"][era],
        "muon_pt_hi": data["muon"]["pt"]["hi"][era],
        "muon_eta_lo": data["muon"]["eta"]["lo"][era],
        "muon_eta_hi": data["muon"]["eta"]["hi"][era],
        "muon_iso_lo": data["muon"]["iso"]["lo"][era],
        "muon_iso_hi": data["muon"]["iso"]["hi"][era],
        "weightName": data["HLTSFParams"]["weightName"][era],
        "nominalBranch": data["HLTSFParams"]["nominalBranch"],
        "statBranch": data["HLTSFParams"]["statBranch"],
        "systBranch": data["HLTSFParams"]["systBranch"]
    }
    renderedMuonHLTWeight = muonHLTWeightTemplate.render(muonHLTWeightData)
    outputDir = data.get("tag", "outputs")
    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, f"muonHLT_{era}_config.yaml"), "w") as f:
        f.write(renderedMuonHLTWeight)
    print(f"Rendered {outputDir}/muonHLT_{era}_config.yaml")

def processbTagWeightConfig(data, env, era):
    bTagWeightTemplate = env.get_template("bTagWeightTemplate.yaml.j2")
    bTagWeightData = {
        "efficiencyFolder": data["efficiencyFolder"],
        "era": era,
        "bTagSFFile": data["bTagSFParams"]["bTagSFFile"][era],
        "bTagThreshold": data["btag_threshold"][era],
        "nominalBranch": data["bTagSFParams"]["nominalBranch"],
        "upBranch": data["bTagSFParams"]["upBranch"],
        "downBranch": data["bTagSFParams"]["downBranch"]
    }
    renderedbTagWeight = bTagWeightTemplate.render(bTagWeightData)
    outputDir = data.get("tag", "outputs")
    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, f"bTagging_{era}_config.yaml"), "w") as f:
        f.write(renderedbTagWeight)
    print(f"Rendered {outputDir}/bTagging_{era}_config.yaml")


def processProcessFlowConfig(data, env):
    processFlowTemplate = env.get_template("processFlowTemplate.yaml.j2")
    renderedProcessFlow = processFlowTemplate.render(data)
    outputDir = data.get("tag", "outputs")
    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, "processFlow_config.yaml"), "w") as f:
        f.write(renderedProcessFlow)
    print(f"Rendered {outputDir}/processFlow_config.yaml")

if __name__ == "__main__":
    env = Environment(loader=FileSystemLoader("Templates"), trim_blocks=True, lstrip_blocks=True)
    env.filters["to_nice_yaml"] = to_nice_yaml
    with open("masterConfig.yaml") as f:
        data = yaml.safe_load(f)
    processProcessFlowConfig(data, env)
    for era in data["eras"]:
        processSelectionConfigs(data, env, era)
        processRecoConfigs(data, env, era)
        processobservablesConfigs(data, env, era)
        processBDTvariablesConfigs(data, env, era)
        processBDTScoreConfigs(data, env, era)

        processMuonIDWeightConfigs(data, env, era)
        processMuonHLTWeightConfigs(data, env, era)
        processlheWeightConfigs(data, env, era)
        processbTagWeightConfig(data, env, era)

        for wt in ['jetPUID', 'Reco']:
            outputDir = data.get("tag", "outputs")
            with open(os.path.join(outputDir, f"{wt}_{era}_config.yaml"), "w") as f:
                f.write("Test: test")
            print(f"Rendered {outputDir}/{wt}_{era}_config.yaml")

