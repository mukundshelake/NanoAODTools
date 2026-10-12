#!/usr/bin/env python3
"""
Utility functions for managing configs, provenance, and outputs.
"""

import hashlib
import json
import os
import socket
import subprocess
import yaml
from datetime import datetime
from pathlib import Path
import urllib.request
import urllib.error


def compute_config_hash(config_path):
    """
    Compute SHA256 hash of config file content.
    
    Args:
        config_path: Path to config.yaml
    
    Returns:
        str: First 12 characters of SHA256 hash
    """
    with open(config_path, 'rb') as f:
        content = f.read()
    hash_obj = hashlib.sha256(content)
    return hash_obj.hexdigest()[:12]


def get_git_info():
    """
    Get current git commit SHA and branch.
    
    Returns:
        dict: Git metadata
    """
    try:
        commit = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            stderr=subprocess.DEVNULL
        ).decode().strip()
        
        branch = subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            stderr=subprocess.DEVNULL
        ).decode().strip()
        
        # Check for uncommitted changes
        status = subprocess.check_output(
            ['git', 'status', '--porcelain'],
            stderr=subprocess.DEVNULL
        ).decode().strip()
        
        return {
            'commit': commit,
            'branch': branch,
            'has_uncommitted_changes': bool(status)
        }
    except:
        return {
            'commit': 'unknown',
            'branch': 'unknown',
            'has_uncommitted_changes': False
        }


def create_output_directory(base_dir, config_path, inputs_folder):
    """
    Create hash-based output directory and copy config.
    
    Args:
        base_dir: Base outputs directory
        config_path: Path to config.yaml
        inputs_folder: Path to inputs folder
    
    Returns:
        tuple: (output_dir_path, config_hash, is_new_run)
    """
    config_hash = compute_config_hash(config_path)
    output_dir = Path(base_dir) / config_hash
    
    is_new_run = not output_dir.exists()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy config to output directory
    import shutil
    shutil.copy2(config_path, output_dir / 'config.yaml')
    
    # Copy inputs folder to output directory
    if inputs_folder.exists():
        shutil.copytree(inputs_folder, output_dir / 'inputs', dirs_exist_ok=True)
    
    return output_dir, config_hash, is_new_run


def update_run_history(history_file, config_hash, metadata=None):
    """
    Append run information to run_history.txt
    
    Args:
        history_file: Path to run_history.txt
        config_hash: Config hash for this run
        metadata: Optional dict with additional info
    """
    timestamp = datetime.now().isoformat()
    user = os.environ.get('USER', 'unknown')
    
    git_info = get_git_info()
    
    entry = {
        'timestamp': timestamp,
        'config_hash': config_hash,
        'user': user,
        'git_commit': git_info['commit'],
        'git_branch': git_info['branch'],
        'uncommitted_changes': git_info['has_uncommitted_changes']
    }
    
    if metadata:
        entry.update(metadata)
    
    with open(history_file, 'a') as f:
        f.write(json.dumps(entry) + '\n')


def update_latest_symlink(base_dir, config_hash):
    """
    Update 'latest' symlink to point to current output directory.
    
    Args:
        base_dir: Base outputs directory
        config_hash: Config hash for current run
    """
    latest_link = Path(base_dir) / 'latest'
    target = Path(config_hash)
    
    # Remove old symlink if exists
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    
    # Create new symlink
    latest_link.symlink_to(target)


def create_output_metadata(config_hash, script_name, status='generated'):
    """
    Create metadata dict for output JSON files.
    
    Args:
        config_hash: Config hash used for this run
        script_name: Name of script that generated output
        status: Output status (placeholder/generated/validated)
    
    Returns:
        dict: Metadata dictionary
    """
    git_info = get_git_info()
    
    return {
        'status': status,
        'version': '0.1',
        'provenance': {
            'config_hash': config_hash,
            'git_commit': git_info['commit'],
            'git_branch': git_info['branch'],
            'uncommitted_changes': git_info['has_uncommitted_changes'],
            'script': script_name,
            'timestamp': datetime.now().isoformat(),
            'user': os.environ.get('USER', 'unknown')
        }
    }


def save_output_json(output_path, data, table_id, caption, config_hash, script_name):
    """
    Save output JSON with metadata.
    
    Args:
        output_path: Path to save JSON
        data: Data payload
        table_id: LaTeX table label
        caption: Table caption
        config_hash: Config hash
        script_name: Script name
    """
    output = {
        'table_id': table_id,
        'caption': caption,
        'data': data,
        'metadata': create_output_metadata(config_hash, script_name)
    }
    
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)


def load_config(config_path):
    """Load YAML config file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def resolve_storage_path(config):
    """
    Resolve the STORAGE path for the machine this script is running on.

    STORAGE in config.yaml may be a plain string (single machine) or a dict
    mapping a machine-identifying key to a path, e.g.:
        STORAGE:
          cms2:      "/mnt/disk2/mukund/DataFiles"
          localhost: "/nfs/disk3/mukund/DataFiles"
          lxplus:    "/eos/user/m/mshelake/DataFiles/"
    In the dict case, the key is matched as a substring of socket.gethostname()
    (not an exact match), so "lxplus" matches "lxplus789.cern.ch".
    """
    storage = config.get('STORAGE', '/path/to/storage')
    if isinstance(storage, str):
        return storage

    hostname = socket.gethostname()
    for key, path in storage.items():
        if key in hostname:
            return path

    raise ValueError(
        f"Could not resolve STORAGE path: hostname '{hostname}' does not match "
        f"any key in config STORAGE ({list(storage.keys())})."
    )


def lfn_path_for_local_file(local_path, storage_base, lfn_base):
    """
    Translate an absolute file path under storage_base (the resolved STORAGE
    path for the current machine) into its /store/... LFN equivalent under
    lfn_base, for CRAB's Data.userInputFiles.

    Only meaningful when storage_base is itself an EOS mount of lfn_base, i.e.
    when running on lxplus with STORAGE.lxplus pointing at the EOS mount of
    LFN_Base -- CRAB submission only makes sense there anyway.
    """
    local_path = str(local_path)
    storage_base = str(storage_base).rstrip('/')
    lfn_base = str(lfn_base).rstrip('/')
    if not local_path.startswith(storage_base + '/'):
        raise ValueError(
            f"File path '{local_path}' is not under STORAGE base '{storage_base}'; "
            f"cannot derive its LFN. CRAB submission must run on lxplus, with "
            f"STORAGE resolving to the EOS mount of LFN_Base."
        )
    return lfn_base + local_path[len(storage_base):]


def validate_output_status(outputs_dir, current_config_hash):
    """
    Check which outputs exist and their status relative to current config.
    
    Args:
        outputs_dir: Base outputs directory
        current_config_hash: Hash of current config.yaml
    
    Returns:
        dict: Status information
    """
    outputs_path = Path(outputs_dir)
    
    # Find all output directories (12-char hex names)
    output_dirs = [d for d in outputs_path.iterdir() 
                   if d.is_dir() and len(d.name) == 12 and d.name != 'placeholder']
    
    status = {
        'current_hash': current_config_hash,
        'current_exists': (outputs_path / current_config_hash).exists(),
        'total_runs': len(output_dirs),
        'all_hashes': [d.name for d in sorted(output_dirs, key=lambda x: x.stat().st_mtime)]
    }
    
    return status
    
def validate_golden_jsons(config):
    """
    Check if all golden JSON files exist locally.
    
    Args:
        config: Configuration dict from config.yaml
    
    Returns:
        dict: Status of each golden JSON file
    """
    golden_jsons = config.get('golden_jsons', {})
    status = {}
    
    for year, json_info in golden_jsons.items():
        filename = json_info['filename']
        # Check for .json extension version
        filename_json = Path(filename).stem + '.json'
        filepath = Path('data/golden_jsons') / filename_json
        status[year] = {
            'filename': filename_json,
            'exists': filepath.exists(),
            'path': str(filepath)
        }
    
    return status
