#!/usr/bin/env python3
from pathlib import Path
import socket
import yaml


FEATURES_17 = [
    "JetHT", "pTSum", "FW1", "FW2", "FW3", "AL",
    "Sxx", "Syy", "Sxy", "Sxz", "Syz", "Szz",
    "S", "P", "A", "p2in", "p2out",
]


def pick_host_value(obj, host):
    if not isinstance(obj, dict):
        return obj
    if host in obj:
        return obj[host]
    if "localhost.localdomain" in obj:
        return obj["localhost.localdomain"]
    return next(iter(obj.values()))


def dump_yaml(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        yaml.safe_dump(data, handle, sort_keys=False)


def build_process_flow(master):
    stages = master["stages"]
    eras = master["eras"]
    tag = master["tag"]
    flow = {}
    for index, stage in enumerate(stages):
        if index == 0:
            continue
        previous_stage = stages[index - 1]
        flow[stage] = {}
        for era in eras:
            flow[stage][era] = {
                "inputStage": previous_stage,
                "inputTag": "nfs" if stage == "selection" else tag,
            }
    return flow


def selection_cuts(master, era):
    mu_pt = master["muon"]["pt"]["lo"][era]
    mu_abs_eta = master["muon"]["abs_eta"]["hi"][era]
    jet_pt = master["jet"]["pt"]["lo"][era]
    jet_abs_eta = master["jet"]["abs_eta"]["hi"][era]
    btag = master["btag_threshold"][era]
    n_mu = master["cuts"]["atleast_n_muons"][era]
    n_b = master["cuts"]["atleast_n_btagged_jets"][era]
    n_j = master["cuts"]["atleast_n_jets"][era]
    hlt = master["hltFlag"][era]

    return [
        f"Sum$(Muon_pt > {mu_pt} && abs(Muon_eta) < {mu_abs_eta} && Muon_tightId && Muon_pfRelIso04_all <= 0.06) >= {n_mu}",
        f"Sum$(Jet_pt > {jet_pt} && abs(Jet_eta) < {jet_abs_eta} && Jet_btagDeepFlavB > {btag} && Jet_jetId == 6 && (Jet_pt > 50 || Jet_puId > 0)) >= {n_b}",
        f"Sum$(Jet_pt > {jet_pt} && abs(Jet_eta) < {jet_abs_eta} && Jet_jetId == 6 && (Jet_pt > 50 || Jet_puId > 0)) >= {n_j}",
        hlt,
        "Flag_goodVertices && Flag_globalSuperTightHalo2016Filter && Flag_HBHENoiseFilter && Flag_HBHENoiseIsoFilter && Flag_EcalDeadCellTriggerPrimitiveFilter && Flag_BadPFMuonFilter && Flag_BadPFMuonDzFilter && Flag_eeBadScFilter",
    ]


def build_stage_config(master, host, stage, era):
    data_modules = master["modules"]["Data"].get(stage)
    mc_modules = master["modules"]["MC"].get(stage)

    cfg = {
        "modules": {
            "Data": data_modules,
            "MC": mc_modules,
        }
    }

    if stage == "selection":
        golden = pick_host_value(master["gJSONs"][era], host)
        cfg = {
            "goldenJSON": golden,
            "gJSON": golden,
            "branchsel": master["branchFile"],
            "cuts": selection_cuts(master, era),
            "noFlagCutsFor": ["MC_alt"],
            "modules": {
                "Data": data_modules,
                "MC": mc_modules,
            }
        }

    return cfg


def build_lhe_config(master):
    return {"branchNames": {"sf": master["lheWeightSignParams"]["nominalBranch"]}}


def build_muon_id_config(master, host, era):
    return {
        "IDSFFile": pick_host_value(master["IDSFParams"]["IDSFFile"][era], host),
        "kinematics": {
            "Muon": {
                "lohi": {
                    "pt": {
                        "low": master["muon"]["pt"]["lo"][era],
                        "high": master["muon"]["pt"]["hi"][era],
                    },
                    "eta": {
                        "low": master["muon"]["eta"]["lo"][era],
                        "high": master["muon"]["eta"]["hi"][era],
                    },
                    "pfRelIso04_all": {
                        "low": -1.0,
                        "high": 0.06,
                    },
                },
                "value": {
                    "tightId": 1,
                },
            },
        },
        "correctionLib": {
            "weightName": master["IDSFParams"]["weightName"],
            "eraName": master["IDSFParams"]["eraName"][era],
        },
        "branchNames": {
            "sf": master["IDSFParams"]["nominalBranch"],
            "sfup": master["IDSFParams"]["upBranch"],
            "sfdown": master["IDSFParams"]["downBranch"],
        },
    }


def build_muon_hlt_config(master, host, era):
    return {
        "HLTSFFile": pick_host_value(master["HLTSFParams"]["HLTSFFile"][era], host),
        "kinematics": {
            "Muon": {
                "lohi": {
                    "pt": {
                        "low": master["muon"]["pt"]["lo"][era],
                        "high": master["muon"]["pt"]["hi"][era],
                    },
                    "eta": {
                        "low": master["muon"]["eta"]["lo"][era],
                        "high": master["muon"]["eta"]["hi"][era],
                    },
                    "pfRelIso04_all": {
                        "low": -1.0,
                        "high": 0.06,
                    },
                },
                "value": {
                    "tightId": 1,
                },
            },
        },
        "correctionLib": {
            "weightName": master["HLTSFParams"]["weightName"][era],
        },
        "branchNames": {
            "sf": master["HLTSFParams"]["nominalBranch"],
            "sfstat": master["HLTSFParams"]["statBranch"],
            "sfsyst": master["HLTSFParams"]["systBranch"],
        },
    }


def build_btag_config(master, host, era):
    return {
        "bTagSFFile": pick_host_value(master["bTagSFParams"]["bTagSFFile"][era], host),
        "efficiencyFolder": pick_host_value(master["efficiencyFolder"], host),
        "era": era,
        "bTagThreshold": master["btag_threshold"][era],
        "branchNames": {
            "sf": master["bTagSFParams"]["nominalBranch"],
            "sfup": master["bTagSFParams"]["upBranch"],
            "sfdown": master["bTagSFParams"]["downBranch"],
        },
    }


def build_apply_bdt_config():
    return {
        "moduleConfigs": {
            "applyBDT": {
                "model_path": "004B-BDT/scripts/BDT/outputs/bdt/bdt_model.pkl",
                "branch_name": "BDTScore",
                "branch_map": {feature: feature for feature in FEATURES_17},
            }
        }
    }


def build_jetpuid_config(master, host, era):
    return {
        "efficiencyFile": f"SFs/Efficiency/{era}/{era}_Jet_puId_effi.json",
        "jetPUIdFile": pick_host_value(
            {
                "default": {
                    "UL2016preVFP": "SFs/UL2016preVFP_jet_jmar.json.gz",
                    "UL2016postVFP": "SFs/UL2016postVFP_jet_jmar.json.gz",
                    "UL2017": "SFs/UL2017_jet_jmar.json.gz",
                    "UL2018": "SFs/UL2018_jet_jmar.json.gz",
                }[era]
            },
            host,
        ),
    }


def build_reco_config():
    return {
        "minPgof": None,
    }


def main():
    here = Path(__file__).resolve().parent
    master_path = here / "masterConfig.yaml"

    with master_path.open() as handle:
        master = yaml.safe_load(handle)

    host = socket.gethostname()
    tag = master["tag"]
    out_dir = here / tag
    out_dir.mkdir(parents=True, exist_ok=True)

    process_flow_cfg = {
        "tag": tag,
        "DatasetJSONFolder": pick_host_value(master["DatasetJSONFolder"], host),
        "outputDirBase": pick_host_value(master["outputDirBase"], host),
        "processFlow": build_process_flow(master),
    }
    dump_yaml(out_dir / "processFlow_config.yaml", process_flow_cfg)

    eras = master["eras"]
    stage_names = list(process_flow_cfg["processFlow"].keys())
    for stage in stage_names:
        for era in eras:
            dump_yaml(out_dir / f"{stage}_{era}_config.yaml", build_stage_config(master, host, stage, era))

    for era in eras:
        dump_yaml(out_dir / f"lheWeightSign_{era}_config.yaml", build_lhe_config(master))
        dump_yaml(out_dir / f"muonID_{era}_config.yaml", build_muon_id_config(master, host, era))
        dump_yaml(out_dir / f"muonHLT_{era}_config.yaml", build_muon_hlt_config(master, host, era))
        dump_yaml(out_dir / f"bTagging_{era}_config.yaml", build_btag_config(master, host, era))
        dump_yaml(out_dir / f"applyBDT_{era}_config.yaml", build_apply_bdt_config())
        dump_yaml(out_dir / f"jetPUID_{era}_config.yaml", build_jetpuid_config(master, host, era))
        dump_yaml(out_dir / f"Reco_{era}_config.yaml", build_reco_config())

        for module_name in ["BDTVariable", "Observables", "yCalculator"]:
            dump_yaml(out_dir / f"{module_name}_{era}_config.yaml", {})

    print(f"Generated YAML configs in: {out_dir}")


if __name__ == "__main__":
    main()
