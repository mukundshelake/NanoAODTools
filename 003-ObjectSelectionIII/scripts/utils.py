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
