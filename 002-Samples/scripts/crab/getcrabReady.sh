# Resolve this script's own directory so checkStatus.py is found regardless of
# where getcrabReady.sh is invoked from (it does not have to be run from inside
# scripts/crab/ itself).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

cd /eos/user/m/mshelake/Analysis/CMSSW_13_3_0/src; cmsenv; cd -; echo "cmsenv done"
echo "Setting up CRAB environment and initializing VOMS proxy..."
source /cvmfs/cms.cern.ch/crab3/crab.sh
voms-proxy-init --voms cms -valid 192:00
python3 "$SCRIPT_DIR/checkStatus.py" --resubmit -d .