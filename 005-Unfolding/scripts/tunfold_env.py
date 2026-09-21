"""Load the vendored standalone TUnfold and expose it under its plain class names.

ROOT no longer ships TUnfold in its default build (issue #30), so
`gSystem.Load("libUnfold")` fails everywhere here. `external/TUnfold_V17.9`
vendors the upstream standalone distribution instead; build it once with

    conda activate latestcoffea && external/TUnfold_V17.9/build.sh

Upstream names the compiled classes `TUnfoldV17`, `TUnfoldDensityV17`, ... and
exposes the plain names only through C preprocessor `#define`s in the headers.
Macros do not survive into PyROOT, so `ROOT.TUnfoldDensity` stays undefined even
after the library loads. This module aliases them, so every script in the
chapter can write `ROOT.TUnfoldDensity` and mean it.

Usage:

    from tunfold_env import ROOT, load_tunfold
    load_tunfold()
    unfold = ROOT.TUnfoldDensity(h_matrix, ...)
"""

import os

import ROOT

TUNFOLD_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "external", "TUnfold_V17.9",
)

# Plain name -> upstream versioned name.
_CLASSES = [
    "TUnfold",
    "TUnfoldSys",
    "TUnfoldDensity",
    "TUnfoldBinning",
    "TUnfoldBinningXML",
    "TUnfoldIterativeEM",
]

_loaded = False


def load_tunfold(quiet=True):
    """Load libunfold.so and alias the versioned classes. Idempotent."""
    global _loaded
    if _loaded:
        return ROOT

    if quiet:
        # TUnfold is chatty at kInfo about every bin it does not unfold.
        ROOT.gErrorIgnoreLevel = ROOT.kWarning

    lib = os.path.join(TUNFOLD_DIR, "libunfold.so")
    if not os.path.exists(lib):
        raise RuntimeError(
            f"{lib} not found.\n"
            "Build it first:\n"
            "    conda activate latestcoffea\n"
            f"    {os.path.join(TUNFOLD_DIR, 'build.sh')}"
        )
    if ROOT.gSystem.Load(lib) < 0:
        raise RuntimeError(f"gSystem.Load failed for {lib}")

    missing = []
    for name in _CLASSES:
        versioned = getattr(ROOT, name + "V17", None)
        if versioned is None:
            missing.append(name + "V17")
            continue
        setattr(ROOT, name, versioned)
    if missing:
        raise RuntimeError(
            f"libunfold.so loaded but {missing} are absent from its dictionary; "
            "rebuild with build.sh"
        )

    _loaded = True
    return ROOT


def tunfold_version():
    load_tunfold()
    return str(ROOT.TUnfold.GetTUnfoldVersion())
