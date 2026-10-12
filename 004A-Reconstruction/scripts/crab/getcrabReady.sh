# Resolve this script's own directory so checkStatus.py is found regardless of
# where getcrabReady.sh is invoked from (it does not have to be run from inside
# scripts/crab/ itself).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

cd /eos/user/m/mshelake/Analysis/CMSSW_13_3_0/src; cmsenv; cd -; echo "cmsenv done"

# RecoModule fits with the vendored HitFit; the jobs load libHitFit.so built in
# THIS release (see external/HitFit/build.sh). Rebuild unless the existing
# library was built here.
HITFIT_DIR="$(cd "$SCRIPT_DIR/../../../external/HitFit" && pwd)"
if ! grep -qx "cmssw_version=$CMSSW_VERSION" "$HITFIT_DIR/libHitFit.buildinfo" 2>/dev/null \
   || ! grep -qx "scram_arch=$SCRAM_ARCH" "$HITFIT_DIR/libHitFit.buildinfo" 2>/dev/null; then
    echo "Building HitFit for $CMSSW_VERSION/$SCRAM_ARCH ..."
    (unset CONDA_PREFIX; "$HITFIT_DIR/build.sh") || { echo "HitFit build failed"; return 1 2>/dev/null || exit 1; }
fi
echo "Setting up CRAB environment and initializing VOMS proxy..."
source /cvmfs/cms.cern.ch/crab3/crab.sh
voms-proxy-init --voms cms -valid 192:00
python3 "$SCRIPT_DIR/checkStatus.py" --resubmit -d .