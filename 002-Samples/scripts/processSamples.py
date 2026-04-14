import utils
import os
import json

if __name__ == "__main__":
    # Read the configuration file
    config = utils.read_yaml("../config.yaml")
    outputHash = utils.generate_hash("../config.yaml")
    print(f"Config file hash: {outputHash}")
    
    # Download the golden JSON files
    for era, url in config["golden_json_urls"].items():
        output_path = os.path.join("../outputs",outputHash, "golden_jsons")
        output_filename = f"golden_json_{era}.txt"
        utils.download_file(url, output_path, output_filename)
        print(f"Downloaded golden JSON for {era} to {os.path.join(output_path, output_filename)}")
    
    # Convert the downloaded golden JSON text files to proper JSON format
    for era in config["golden_json_urls"].keys():
        txt_file = os.path.join("../outputs",outputHash, "golden_jsons", f"golden_json_{era}.txt")
        json_file_dir = os.path.join("../outputs",outputHash, "golden_jsons")
        json_file = f"golden_json_{era}.json"
        utils.txt_to_json(txt_file, json_file_dir, json_file)
        print(f"Converted {txt_file} to {os.path.join(json_file_dir, json_file)}")

    # Get the lumi info for each era and save it to a CSV file
    for era in config["golden_json_urls"].keys():
        output_path = os.path.join("../outputs",outputHash, "lumi_info")
        output_filename = f"lumi_info_{era}.csv"
        utils.get_lumi_info(os.path.join("../outputs",outputHash, "golden_jsons", f"golden_json_{era}.json"), brilcalc_path=config.get("brilcalc_cmd", "brilcalc"), lumi_unit=config.get("lumi_unit", "/pb"), output_csv_file=os.path.join(output_path, output_filename), normtag=config.get("normtag", "/cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json"))
        print(f"Saved lumi info for {era} to {os.path.join(output_path, output_filename)}")

    # Get the dataset file list for each dataset and save it to a JSON file
    for era in config["DASQueries"].keys():
        for group in config["DASQueries"][era].keys():
            for dataset in config["DASQueries"][era][group].keys():
                query = config["DASQueries"][era][group][dataset]
                output_path = os.path.join("../outputs",outputHash, "dataset_file_lists", era, group)
                output_filename = f"{dataset.replace('/', '_')}_files.json"
                fileList = utils.get_DAS_file_list(query, das_client_path=config.get("dasgoclient_cmd", "dasgoclient"))
                os.makedirs(output_path, exist_ok=True)
                with open(os.path.join(output_path, output_filename), 'w') as f:
                    json.dump(fileList, f, indent=4)
                print(f"Saved file list for {dataset} to {os.path.join(output_path, output_filename)}")
