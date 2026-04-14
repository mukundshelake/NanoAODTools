# This is the utils file for the 002-Samples package. It contains utility functions that are used across the package.


import os


def download_file(url, output_path, output_filename, overwrite=False):
    """
    Downloads a file from the given URL and saves it to the specified output path.
    
    Args:
        url (str): The URL of the file to download.
        output_path (str): The path where the downloaded file should be saved.
        output_filename (str): The name of the file to save.
        overwrite (bool): Whether to overwrite the file if it already exists.
    """
    import requests
    
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful

    os.makedirs(output_path, exist_ok=True)
    file_path = os.path.join(output_path, output_filename)
    if not overwrite and os.path.exists(file_path):
        return
    with open(file_path, 'wb') as f:
        f.write(response.content)

def read_yaml(file_path):
    """
    Reads a YAML file and returns its contents as a dictionary.
    
    Args:
        file_path (str): The path to the YAML file.
    Returns:
        dict: The contents of the YAML file as a dictionary.
    """
    import yaml
    
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

def generate_hash(file_path):
    """
    Generates a hash for the given file.
    
    Args:
        file_path (str): The path to the file for which to generate the hash.
    Returns:
        str: The generated hash for the file.
    """
    import hashlib
    
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def txt_to_json(txt_file, json_file_dir, json_file):
    """
    Converts a text file containing JSON-like data to a proper JSON file.
    
    Args:
        txt_file (str): The path to the input text file.
        json_file_dir (str): The directory where the output JSON file should be saved.
        json_file (str): The name of the output JSON file.
    """
    import json
    
    with open(txt_file, 'r') as f:
        data = f.read()
    os.makedirs(json_file_dir, exist_ok=True)
    json_file_path = os.path.join(json_file_dir, json_file)
    
    # Convert the string to a JSON object
    json_data = json.loads(data)
    
    with open(json_file_path, 'w') as f:
        json.dump(json_data, f, indent=4)

def get_lumi_info(golden_json_file, brilcalc_path="brilcalc", lumi_unit="/pb", output_csv_file="lumi_info.csv", normtag="/cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json"):
    """
    Calculates the total luminosity from a golden JSON file using brilcalc tool.
    
    Args:
        golden_json_file (str): The path to the golden JSON file.
        brilcalc_path (str or list): The brilcalc executable or full command string
            (e.g. a singularity invocation). A string is split with shlex.split.
        lumi_unit (str): The luminosity unit to pass to brilcalc -u (e.g. /pb, /fb, /nb).
        output_csv_file (str): Path for the output CSV file.
        normtag (str): Path to the normtag JSON file. Must be accessible inside the
            execution environment (e.g. bind-mounted into singularity with --bind /cvmfs:/cvmfs).
    Returns:
        float: The total luminosity calculated from the golden JSON file.
    """
    import subprocess
    import shlex
    import shutil
    import tempfile
    
    if isinstance(brilcalc_path, str):
        brilcalc_cmd = shlex.split(brilcalc_path)
    else:
        brilcalc_cmd = list(brilcalc_path)

    output_csv_abs = os.path.abspath(output_csv_file)
    os.makedirs(os.path.dirname(output_csv_abs), exist_ok=True)

    # brilcalc runs inside a singularity container that may not have /eos or
    # paths with leading-zero segments (e.g. "002-Samples") mounted/parseable.
    # Use temp files for both input and output so the container only sees /tmp paths,
    # then copy the result to the intended destination afterwards.
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_in:
        tmp_in_path = tmp_in.name
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp_out:
        tmp_out_path = tmp_out.name
    try:
        shutil.copy2(os.path.abspath(golden_json_file), tmp_in_path)
        command = brilcalc_cmd + ["lumi", "--normtag", normtag, "-u", lumi_unit, "-i", tmp_in_path, "-o", tmp_out_path]
        subprocess.run(command, check=True)
        shutil.copy2(tmp_out_path, output_csv_abs)
    finally:
        os.unlink(tmp_in_path)
        if os.path.exists(tmp_out_path):
            os.unlink(tmp_out_path)

def get_DAS_file_list(dataset, das_client_path="dasgoclient"):
    """
    Retrieves a list of files from a given dataset using the DAS client.
    
    Args:
        dataset (str): The dataset path to query (e.g. /Sample/.../NANOAODSIM).
        das_client_path (str): The path to the DAS client executable.
    Returns:
        list: A list of file paths retrieved from the DAS query.
    """
    import subprocess
    
    command = [das_client_path, f"-query=file dataset={dataset}"]
    result = subprocess.run(command, stdout=subprocess.PIPE, check=True)
    
    return [line for line in result.stdout.decode().splitlines() if line.strip()]

