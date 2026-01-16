#!/usr/bin/env python3
"""
Machine-specific environment configuration loader.

This utility loads paths and settings from .env file,
allowing the same code to run on multiple machines with different paths.

Usage:
    import setup_env
    env = setup_env.load_env()
    data_path = env['DATA_PATH']
"""

import os
from pathlib import Path


def load_env(env_file=None):
    """
    Load machine-specific environment variables from .env file.
    
    Args:
        env_file: Path to .env file (default: .env in repository root)
    
    Returns:
        dict: Configuration dictionary
    
    Raises:
        FileNotFoundError: If .env file doesn't exist
    """
    if env_file is None:
        # Find repository root (where .env lives)
        env_file = Path(__file__).parent / '.env'
    else:
        env_file = Path(env_file)
    
    if not env_file.exists():
        raise FileNotFoundError(
            f"Environment file not found: {env_file}\n"
            f"Please create .env from .env.template:\n"
            f"  cp .env.template .env\n"
            f"  vim .env"
        )
    
    env = {}
    
    # Parse .env file
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            
            # Parse KEY=VALUE
            if '=' not in line:
                continue
                
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            # Remove quotes if present
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            elif value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            
            env[key] = value
    
    return env


def get_path(path_type, env=None):
    """
    Get a specific path from configuration.
    
    Args:
        path_type: One of 'data', 'mc', 'output', 'eos', 'work'
        env: Environment dict (loads from .env if None)
    
    Returns:
        Path: Path object for the requested type
    """
    if env is None:
        env = load_env()
    
    path_map = {
        'data': 'DATA_PATH',
        'nanoaod': 'DATA_PATH',
        'mc': 'MC_PATH',
        'output': 'OUTPUT_PATH',
        'outputs': 'OUTPUT_PATH',
        'eos': 'EOS_PATH',
        'work': 'WORK_DIR'
    }
    
    key = path_map.get(path_type, path_type.upper() + '_PATH')
    value = env.get(key, '')
    
    if not value:
        raise KeyError(f"Path '{path_type}' not configured in .env ({key})")
    
    return Path(value)


def is_lxplus():
    """
    Check if running on lxplus cluster.
    
    Returns:
        bool: True if on lxplus, False otherwise
    """
    hostname = os.environ.get('HOSTNAME', '')
    return 'lxplus' in hostname


def is_cmssw_available(env=None):
    """
    Check if CMSSW is available on this machine.
    
    Args:
        env: Environment dict (loads from .env if None)
    
    Returns:
        bool: True if CMSSW_BASE is set and exists
    """
    if env is None:
        env = load_env()
    
    cmssw_base = env.get('CMSSW_BASE', '').strip()
    
    if not cmssw_base:
        return False
    
    return Path(cmssw_base).exists()


def get_cmssw_base(env=None):
    """
    Get CMSSW_BASE directory.
    
    Args:
        env: Environment dict (loads from .env if None)
    
    Returns:
        str: CMSSW_BASE path, or empty string if not available
    """
    if env is None:
        env = load_env()
    
    return env.get('CMSSW_BASE', '').strip()


def validate_paths(env=None):
    """
    Validate that all configured paths exist.
    
    Args:
        env: Environment dict (loads from .env if None)
    
    Returns:
        dict: Validation results {path_name: bool}
    """
    if env is None:
        env = load_env()
    
    results = {}
    path_keys = ['DATA_PATH', 'MC_PATH', 'OUTPUT_PATH', 'EOS_PATH', 'WORK_DIR']
    
    for key in path_keys:
        if key in env and env[key]:
            path = Path(env[key])
            results[key] = path.exists()
        else:
            results[key] = None  # Not configured
    
    return results


def print_config(env=None):
    """
    Print current configuration (for debugging).
    
    Args:
        env: Environment dict (loads from .env if None)
    """
    if env is None:
        env = load_env()
    
    print("=" * 60)
    print("Machine Configuration")
    print("=" * 60)
    print(f"\nEnvironment: {'lxplus' if is_lxplus() else 'local'}")
    print(f"CMSSW available: {is_cmssw_available(env)}")
    
    print("\nPath Configuration:")
    for key in ['DATA_PATH', 'MC_PATH', 'OUTPUT_PATH', 'EOS_PATH', 'WORK_DIR']:
        if key in env and env[key]:
            print(f"  {key}: {env[key]}")
    
    print("\nCMSSW Configuration:")
    cmssw = env.get('CMSSW_BASE', '').strip()
    if cmssw:
        print(f"  CMSSW_BASE: {cmssw}")
        print(f"  Exists: {Path(cmssw).exists()}")
    else:
        print("  CMSSW not configured")
    
    print("\nPath Validation:")
    validation = validate_paths(env)
    for key, exists in validation.items():
        if exists is None:
            status = "not configured"
        elif exists:
            status = "✓ exists"
        else:
            status = "✗ missing"
        print(f"  {key}: {status}")
    
    print("=" * 60)


if __name__ == '__main__':
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Load machine configuration')
    parser.add_argument('--env', type=str, help='Path to .env file')
    args = parser.parse_args()
    
    try:
        env = load_env(args.env if args.env else None)
        print_config(env)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
