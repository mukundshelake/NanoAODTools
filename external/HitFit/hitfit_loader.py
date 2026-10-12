"""Load the vendored HitFit (libHitFit.so) into PyROOT and build fitters.

Build the library once with

    conda activate latestcoffea && external/HitFit/build.sh    # local running
    cmsenv && external/HitFit/build.sh                         # CRAB submission

The loader finds libHitFit.so and HitFitWrapper.h next to itself, either in the
repo layout (wrapper/HitFitWrapper.h) or flat, as in a CRAB job sandbox.

Usage:

    from hitfit_loader import make_fitter
    fitter = make_fitter(settings, udsc_res, b_res, muon_res, kt_res, mw=80.4)
    r = fitter.fit(jet_pt, jet_eta, jet_phi, jet_mass,      # numpy float64[4]
                   mu_pt, mu_eta, mu_phi, mu_mass, met_px, met_py, nuz)
    r.status, r.chi2, r.mt, r.sigmt, r.nu_pz, r.lep_top, r.had_top

`settings` is a dict of HitFit Defaults_Text parameters (Top_Fit,
Constrained_Top, Fourvec_Constrainer, Chisq_Constrainer). It is written to a
temporary file because Defaults_Text only reads files. The *_res arguments are
EtaDepResolution file paths; kt_res is a file holding `met_resolution = C,R,N`.
"""

import os
import tempfile

import ROOT

HITFIT_DIR = os.path.dirname(os.path.abspath(__file__))
LIBRARY = os.path.join(HITFIT_DIR, "libHitFit.so")

_loaded = False


def load():
    """Load libHitFit.so and declare the wrapper. Idempotent."""
    global _loaded
    if _loaded:
        return ROOT
    if not os.path.exists(LIBRARY):
        raise RuntimeError(
            f"{LIBRARY} not found.\nBuild it first:\n"
            "    conda activate latestcoffea\n"
            f"    {os.path.join(HITFIT_DIR, 'build.sh')}")
    if ROOT.gSystem.Load(LIBRARY) < 0:
        raise RuntimeError(f"gSystem.Load failed for {LIBRARY}")
    # Repo layout keeps the header in wrapper/; a CRAB job sandbox is flat.
    for header_dir in (os.path.join(HITFIT_DIR, "wrapper"), HITFIT_DIR):
        if os.path.exists(os.path.join(header_dir, "HitFitWrapper.h")):
            ROOT.gInterpreter.AddIncludePath(header_dir)
            break
    else:
        raise RuntimeError(f"HitFitWrapper.h not found next to {__file__}")
    if not ROOT.gInterpreter.Declare('#include "HitFitWrapper.h"'):
        raise RuntimeError("could not declare HitFitWrapper.h")
    _loaded = True
    return ROOT


def _format(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def write_settings(settings, path):
    """Write a dict as a HitFit Defaults_Text file ("key = value" lines)."""
    with open(path, "w") as f:
        for key, value in settings.items():
            f.write(f"{key} = {_format(value)}\n")


def make_fitter(settings, udsc_res, b_res, muon_res, kt_res, mw=80.4):
    """Return a hitfit_wrap::Fitter configured from `settings` (dict)."""
    load()
    for path in (udsc_res, b_res, muon_res, kt_res):
        if not os.path.exists(path):
            raise FileNotFoundError(path)
    fd, settings_path = tempfile.mkstemp(prefix="hitfit_settings_", suffix=".txt")
    os.close(fd)
    try:
        write_settings(settings, settings_path)
        return ROOT.hitfit_wrap.Fitter(settings_path, udsc_res, b_res, muon_res, kt_res, float(mw))
    finally:
        # Defaults_Text reads the whole file in its constructor.
        os.remove(settings_path)
