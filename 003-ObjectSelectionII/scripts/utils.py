#!/usr/bin/env python3
"""
Utility functions for managing configs, provenance, and outputs.
"""

import hashlib
import json
import os
import shlex
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


def create_output_directory(base_dir, config_path, inputs_folder, sfs_folder=None):
    """
    Create hash-based output directory and copy config.
    
    Args:
        base_dir: Base outputs directory
        config_path: Path to config.yaml
        inputs_folder: Path to inputs folder
        sfs_folder: Path to SFs folder (optional). Copied to output_dir/SFs/ so
                    relative SF paths in processListJSON configs resolve correctly
                    when runSelectionII.py chdirs to the run folder.
    
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

    # Copy SFs folder to output directory so relative SF paths work from the run folder.
    # Re-synced (not copy-once) every invocation, same as inputs_folder above: SFs/ is
    # shared, external state (correctionlib files, efficiency maps) that can change
    # without the config hash changing, e.g. after --computeJetPUIDEfficiency or
    # --computeBTaggingEfficiency regenerate an efficiency map into the repo-root SFs/.
    # Some entries under SFs/ (e.g. GoldenJSON/*.txt) are themselves symlinks, and
    # os.symlink() refuses to overwrite an existing link -- so a merge-in-place
    # (dirs_exist_ok=True) fails on the second run. Removing and recopying avoids that.
    if sfs_folder is not None and Path(sfs_folder).exists():
        sfs_dst = output_dir / 'SFs'
        if sfs_dst.exists():
            shutil.rmtree(sfs_dst)
        shutil.copytree(sfs_folder, sfs_dst, symlinks=True)
    
    return output_dir, config_hash, is_new_run


def fetch_and_snapshot(source_path, inputs_folder, output_dir, filename):
    """
    Copy a file fetched from a previous chapter into both the local inputs/
    folder and this run's hash-versioned outputs/inputs/ snapshot.

    create_output_directory() only snapshots inputs_folder -> output_dir/inputs
    as it exists at the start of a run_all.py invocation, which is before any
    --fetchFromPreviousChapter step runs within that same invocation. Without
    this explicit dual-write, the file just fetched in this invocation would be
    missing from that invocation's own outputs/inputs/ snapshot (it would only
    show up in the snapshot on a later, separate invocation).

    Returns:
        tuple: (local_path, output_path)
    """
    import shutil
    local_path = Path(inputs_folder) / filename
    output_path = Path(output_dir) / 'inputs' / filename
    local_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, local_path)
    shutil.copy2(source_path, output_path)
    return local_path, output_path


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


# --------------------------------------------------------------------------- #
#  Correctionlib SF fetching (from CVMFS-hosted jsonpog-integration)          #
# --------------------------------------------------------------------------- #
# What to fetch, per era, from the central jsonpog-integration POG/ tree:
#   POG/<pog>/<era_dir>/<source_filename>
# 'outputs' lists the repo-local filename suffix(es) to write it to under
# SFs/UL<era>_<suffix> -- muon_Z.json.gz is deliberately duplicated under two
# names (mu_ID.json and mu_HLT.json both contain the full muon_Z correction
# set; the module config just picks the correction key it needs out of
# either copy). 'gunzip' controls whether the .gz is decompressed on the way
# in, matching what's already checked into this repo's SFs/ layout.
SF_FETCH_SPECS = [
    {"pog": "MUO", "source_filename": "muon_Z.json.gz",  "outputs": ["mu_ID.json", "mu_HLT.json"], "gunzip": True},
    {"pog": "JME", "source_filename": "jmar.json.gz",     "outputs": ["jet_jmar.json.gz"],           "gunzip": False},
    {"pog": "BTV", "source_filename": "btagging.json.gz", "outputs": ["jet_Btagging.json"],          "gunzip": True},
]


def cvmfs_era_dir(era: str) -> str:
    """Translate this repo's era string to jsonpog-integration's era directory name.

    "UL2018" -> "2018_UL", "UL2016preVFP" -> "2016preVFP_UL", etc. -- strip the
    leading "UL" and move it to a suffix instead.
    """
    if not era.startswith("UL"):
        raise ValueError(f"Unrecognized era '{era}': expected a 'UL...' era string.")
    return era[2:] + "_UL"


def resolve_sf_source(config):
    """
    Resolve where to read the jsonpog-integration POG/ tree from, for the machine this
    script is running on.

    Returns (base_path, ssh_host):
      ssh_host is None -> base_path is a local filesystem path (a CVMFS mount on this
                           machine); read files directly.
      ssh_host is a str -> base_path is the path AS IT EXISTS on ssh_host; every file
                            must be read remotely (see ssh_read_file()) instead of via
                            local filesystem I/O.

    SFSource in config.yaml is a dict keyed the same way as STORAGE (a substring of
    socket.gethostname()):
        SFSource:
          lxplus: "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG"

    If the current hostname doesn't match any SFSource key (e.g. a machine with no local
    CVMFS mount), SFSourceSSHRelay -- a plain hostname/alias string -- is used as a
    fallback: relay every fetch through that host over SSH instead, reusing SFSource's
    "lxplus" entry as the path valid there (the only CVMFS-reachable endpoint any of
    this repo's known machines can reach). The SSH session runs fully interactively (see
    ssh_read_file()), so the caller needs a real, attached terminal -- password/2FA
    prompts land there exactly like a manual `ssh <relay>` login would.
        SFSourceSSHRelay: "lxplus.cern.ch"
    """
    sf_source = config.get('SFSource', {})
    hostname = socket.gethostname()
    for key, path in sf_source.items():
        if key in hostname:
            return path, None

    relay_host = config.get('SFSourceSSHRelay')
    relay_base = sf_source.get('lxplus')
    if relay_host and relay_base:
        return relay_base, relay_host

    raise ValueError(
        f"Could not resolve SFSource path: hostname '{hostname}' does not match "
        f"any key in config SFSource ({list(sf_source.keys())}), and no usable "
        f"SFSourceSSHRelay fallback is configured (needs both SFSourceSSHRelay and an "
        f"SFSource.lxplus entry to relay through). Add a direct SFSource entry for "
        f"this machine, or configure SFSourceSSHRelay, the same way STORAGE is "
        f"configured."
    )


# SSH multiplexing options shared by every relayed fetch: the first call authenticates
# interactively (password/2FA land on the real terminal, since stdin/stderr are left
# attached -- only stdout, the file's own bytes, is captured) and opens a background
# ControlMaster; every subsequent call in this process (or a concurrent one) reuses that
# same connection via ControlPath, so the user is prompted once per session, not once per
# file. ControlPersist keeps the master alive for a while after the last call returns.
_SSH_RELAY_CONTROL_OPTS = [
    "-o", "ControlMaster=auto",
    "-o", "ControlPersist=600",
    "-o", "ControlPath=~/.ssh/cm-sf-%r@%h:%p",
]


def ssh_read_file(ssh_host, remote_path):
    """Read one remote file's raw bytes via `ssh <ssh_host> cat <remote_path>`.

    Runs interactively (stdin/stderr inherited from the caller's terminal, so SSH's own
    password/2FA prompts work normally) while capturing only stdout -- the file's bytes.
    Raises FileNotFoundError if the remote `cat` fails (missing file, permission, etc).
    """
    cmd = ["ssh"] + _SSH_RELAY_CONTROL_OPTS + [ssh_host, "cat", shlex.quote(str(remote_path))]
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    if result.returncode != 0:
        raise FileNotFoundError(
            f"ssh {ssh_host} cat {remote_path} failed (exit code {result.returncode})"
        )
    return result.stdout


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
