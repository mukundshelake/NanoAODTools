"""Config loading, path resolution and provenance for 005-Unfolding.

Mirrors 002-Samples/scripts/utils.py so the chapter versions its outputs the
same way every other chapter does: outputs live under

    {STORAGE}/unfolding/{tag}/{config_hash}/{era}/

with `config_hash` the first 12 hex characters of the SHA256 of config.yaml, so
a config change can never silently overwrite results produced under different
settings.
"""

import hashlib
import os
import socket
import subprocess
from pathlib import Path

import numpy as np
import yaml

CHAPTER = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = CHAPTER / "config.yaml"


def load(path=None):
    """Load config.yaml and attach its hash under the 'config_hash' key."""
    path = Path(path or DEFAULT_CONFIG)
    raw = path.read_bytes()
    cfg = yaml.safe_load(raw)
    cfg["config_hash"] = hashlib.sha256(raw).hexdigest()[:12]
    cfg["config_path"] = str(path)
    return cfg


def storage_root(cfg):
    """Resolve STORAGE for this machine, matching the key as a hostname substring."""
    storage = cfg["STORAGE"]
    if isinstance(storage, str):
        return Path(storage)
    hostname = socket.gethostname()
    for key, path in storage.items():
        if key in hostname:
            return Path(path)
    raise ValueError(
        f"hostname {hostname!r} matches no STORAGE key {list(storage)}; "
        "add it to config.yaml"
    )


def input_dir(cfg, era, datamc, sample):
    """Directory of BDTScore ROOT files for one sample."""
    return storage_root(cfg) / "BDTScore" / cfg["InputTag"] / era / datamc / sample


def output_dir(cfg, era, tag, create=True):
    """Versioned output directory for this config."""
    path = storage_root(cfg) / "unfolding" / tag / cfg["config_hash"] / era
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def gen_mtt_edges(cfg):
    return np.array(cfg["binning"]["gen_mtt_edges"], dtype=float)


def reco_mtt_edges(cfg):
    return np.array(cfg["binning"]["reco_mtt_edges"], dtype=float)


def scheme(cfg):
    return cfg["binning"]["scheme"]


def y0(cfg):
    return float(cfg["binning"]["y0"])


def sample_norm(cfg, era, sample):
    """(Xsec, Ngen) for a sample, searched across the MC groups.

    Ngen is the sum of SIGNED generator weights, not the raw event count --
    see the normalisation note in config.yaml.
    """
    groups = cfg["NgenandXsec"][era]["MC_mu"]
    for group in groups.values():
        if sample in group:
            entry = group[sample]
            return float(entry["Xsec"]), float(entry["Ngen"])
    raise KeyError(f"no Ngen/Xsec for {sample!r} in era {era}")


def lumi_scale(cfg, era, sample):
    """Xsec * Lumi / Ngen -- the per-event scale to an expected data yield."""
    xsec, ngen = sample_norm(cfg, era, sample)
    return xsec * float(cfg["DataLumiInfo"][era]["Lumi"]) / ngen


def background_samples(cfg, era):
    """Every MC sample except the signal."""
    groups = cfg["NgenandXsec"][era]["MC_mu"]
    return sorted(s for group in groups.values() for s in group if s != cfg["Signal"])


def git_commit():
    try:
        return subprocess.check_output(
            ["git", "-C", str(CHAPTER), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def provenance(cfg, script):
    """Metadata block to stamp onto every output."""
    import datetime
    return {
        "config_hash": cfg["config_hash"],
        "git_commit": git_commit(),
        "script": script,
        "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
        "user": os.environ.get("USER", "unknown"),
        "host": socket.gethostname(),
    }
