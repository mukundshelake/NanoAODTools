#!/bin/bash
# Build the Python environment run_003_workflow.sh uses on lxplus: a venv on top
# of CMSSW_13_3_0's Python 3.9, carrying the coffea stack cms02's `latestcoffea`
# conda env provides.
#
# Why it exists: 003-II's efficiency maps and lookupWeights, 003-III's
# histogramming and 004B all import coffea.dataset_tools, which only exists in
# calendar-versioned coffea. Under plain cmsenv, lxplus only has coffea 0.7.21
# (from ~/.local), so those steps cannot run there at all.
#
# Why on top of CMSSW rather than standalone: ROOT and PhysicsTools.NanoAODTools
# keep coming from CMSSW (--system-site-packages), so PostProcessor steps behave
# exactly as before. Everything else is installed into the venv itself -- nothing
# resolves from ~/.local -- so an unrelated `pip install --user` can't change it.
#
# Pins match cms02's latestcoffea, except where Python 3.9 forces older:
#   dask   2024.8.0 (cms02: 2025.3.0 -- newer requires Python >= 3.10)
#   numpy  1.23.5   (cms02: 2.3.5    -- also keeps numpy 1.x under CMSSW's PyROOT,
#                                       which was built against it)
#   pillow 11.3.0   (cms02: 12.0.0   -- newer requires Python >= 3.10)
#
# Usage:  ./setup_lxplus_venv.sh [VENV_DIR]     (refuses to touch an existing one)
set -eo pipefail

VENV="${1:-/eos/user/m/mshelake/venvs/latestcoffea}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -e "$VENV" ]]; then
    echo "Error: $VENV already exists. Remove it first to rebuild." >&2
    exit 1
fi

source "$REPO/003-ObjectSelectionI/scripts/crab/setupEnv.sh"
python3 -m venv --system-site-packages "$VENV"
source "$VENV/bin/activate"
python3 -m pip install -q --upgrade pip

# cms02 pins; pip resolves dask-awkward/dask-histogram against the dask below.
python3 -m pip install -q \
    "coffea==2025.11.0" "awkward==2.8.10" "uproot==5.6.8" \
    "dask-awkward==2025.9.0" "dask-histogram==2025.2.0" \
    "hist==2.9.0" "correctionlib==2.7.0" "mplhep==0.4.1" "vector==1.7.0" \
    "reportlab==5.0.0" tqdm
# Pinned into the venv explicitly (--ignore-installed): pip would otherwise treat
# these as satisfied by ~/.local, silently coupling the venv to it.
python3 -m pip install -q --ignore-installed --no-deps \
    "numpy==1.23.5" "dask==2024.8.0" "numba==0.59.1" "llvmlite==0.42.0" \
    "scipy==1.13.1" "pyyaml==6.0.1" "pillow==11.3.0"

python3 - <<'PY'
import coffea, coffea.dataset_tools, awkward, uproot, reportlab, ROOT
print(f"OK: coffea {coffea.__version__}, awkward {awkward.__version__}, "
      f"uproot {uproot.__version__}, ROOT {ROOT.gROOT.GetVersion()}")
PY
echo "Built $VENV"
