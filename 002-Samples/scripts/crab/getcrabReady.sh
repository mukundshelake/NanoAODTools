cd /eos/user/m/mshelake/Analysis/CMSSW_13_3_0/src; cmsenv; cd -; echo "cmsenv done"
echo "Setting up CRAB environment and initializing VOMS proxy..."
source /cvmfs/cms.cern.ch/crab3/crab.sh
voms-proxy-init --voms cms -valid 192:00
python3 crab/checkStatus.py --resubmit -d .