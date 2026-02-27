#!/usr/bin/env python3
"""
Utility functions for 004-Reconstruction Data/MC plotting workflow

Provides:
- Config hashing for reproducible outputs
- Output directory management
- Run history logging
- Helper functions following 002-Samples pattern
"""

import hashlib
import json
import yaml
from pathlib import Path
from datetime import datetime


def compute_config_hash(config_path):
    """
    Compute SHA256 hash of config.yaml file
    
    Args:
        config_path: Path to config.yaml
        
    Returns:
        First 12 characters of hex digest (e.g., 'a1b2c3d4e5f6')
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'rb') as f:
        content = f.read()
    
    hash_obj = hashlib.sha256(content)
    return hash_obj.hexdigest()[:12]


def setup_output_dir(base_dir, config_hash, config_path):
    """
    Create hash-based output directory and copy config for reproducibility
    
    Args:
        base_dir: Base outputs directory (e.g., 'outputs/')
        config_hash: Hash string from compute_config_hash()
        config_path: Path to config.yaml to copy
        
    Returns:
        Path object to the created output directory
    """
    base_dir = Path(base_dir)
    base_dir.mkdir(exist_ok=True)
    
    output_dir = base_dir / config_hash
    output_dir.mkdir(exist_ok=True)
    
    # Copy config.yaml to output directory for reproducibility
    config_copy = output_dir / 'config.yaml'
    if not config_copy.exists():
        import shutil
        shutil.copy(config_path, config_copy)
    
    return output_dir


def update_latest_symlink(base_dir, config_hash):
    """
    Update 'latest' symlink to point to most recent run
    
    Args:
        base_dir: Base outputs directory (e.g., 'outputs/')
        config_hash: Hash string of the current run
    """
    base_dir = Path(base_dir)
    latest_link = base_dir / 'latest'
    
    # Remove existing symlink if present
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    
    # Create new symlink
    latest_link.symlink_to(config_hash, target_is_directory=True)


def log_run(run_history_path, config_hash, config_summary):
    """
    Append run information to run_history.txt
    
    Args:
        run_history_path: Path to run_history.txt
        config_hash: Hash string of this run
        config_summary: Dict with run details (era, tag, variables, etc.)
    """
    run_history_path = Path(run_history_path)
    run_history_path.parent.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    with open(run_history_path, 'a') as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"Config Hash: {config_hash}\n")
        f.write(f"Era: {config_summary.get('era', 'N/A')}\n")
        f.write(f"Tag: {config_summary.get('tag', 'N/A')}\n")
        f.write(f"Variables: {', '.join(config_summary.get('variables', []))}\n")
        f.write(f"Chi2 Filter: {config_summary.get('apply_chi2_filter', 'N/A')}\n")
        f.write(f"Output Dir: outputs/{config_hash}/\n")


def load_config(config_path):
    """
    Load and validate config.yaml
    
    Args:
        config_path: Path to config.yaml
        
    Returns:
        Dict containing configuration
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Validate required fields (updated for new structure with reco and observables)
    required_sections = ['analysis', 'outputs']
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required section in config.yaml: {section}")
    
    # Check for at least one analysis type (reco, observables, bdtvariables, or bdtvariables_parton)
    if 'reco' not in config and 'observables' not in config and 'bdtvariables' not in config and 'bdtvariables_parton' not in config:
        raise ValueError("Config must contain at least one of 'reco', 'observables', 'bdtvariables', or 'bdtvariables_parton' sections")
    
    return config


def get_variables_to_plot(config):
    """
    Get list of variables to plot from config
    
    Args:
        config: Dict from load_config()
        
    Returns:
        List of variable names
    """
    variables = config.get('variables', [])
    
    if variables == "all" or variables == ["all"]:
        # Return all 13 reconstruction variables
        return [
            "Top_lep_pt", "Top_lep_eta", "Top_lep_phi", "Top_lep_mass",
            "Top_had_pt", "Top_had_eta", "Top_had_phi", "Top_had_mass",
            "Chi2_prefit", "Chi2", "Pgof", "chi2_status"
        ]
    
    return variables


def format_coffea_filename(tag, era):
    """
    Format the .coffea output filename
    
    Args:
        tag: Tag string (e.g., 'midNov')
        era: Era string (e.g., 'UL2017')
        
    Returns:
        Formatted filename string
    """
    return f"{tag}_{era}_reco.coffea"


def find_coffea_files(output_dir, pattern="*_reco.coffea"):
    """
    Find all .coffea files in output directory
    
    Args:
        output_dir: Path to search
        pattern: Glob pattern for filenames
        
    Returns:
        List of Path objects
    """
    output_dir = Path(output_dir)
    return list(output_dir.glob(pattern))


if __name__ == '__main__':
    # Simple test
    print("Testing utils.py...")
    
    # Test config hashing
    try:
        config_hash = compute_config_hash('../config.yaml')
        print(f"✓ Config hash: {config_hash}")
    except Exception as e:
        print(f"✗ Config hash failed: {e}")
    
    print("Utils module ready.")
