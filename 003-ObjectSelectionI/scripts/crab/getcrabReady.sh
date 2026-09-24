# Resolve this script's own directory so checkStatus.py is found regardless of
# where getcrabReady.sh is invoked from (it does not have to be run from inside
# scripts/crab/ itself).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

# cmsenv + CRAB client -- shared with unattended runs via setupEnv.sh.
source "$SCRIPT_DIR/setupEnv.sh"; echo "cmsenv + CRAB environment done"
echo "Initializing VOMS proxy..."
voms-proxy-init --voms cms -valid 192:00
python3 "$SCRIPT_DIR/checkStatus.py" --resubmit -d .
