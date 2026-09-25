# Environment for this chapter's lxplus work: CMSSW (cmsenv) plus the CRAB
# client. Source it; it restores the caller's working directory.
#
# Split out of getcrabReady.sh so unattended runs (run_003_workflow.sh under
# nohup, in particular) can get the same environment without the rest of that
# script: voms-proxy-init prompts for the grid certificate passphrase, which
# cannot be answered without a terminal, and checkStatus.py --resubmit -d .
# resubmits CRAB tasks in whatever directory happens to be current. Neither
# belongs in environment setup. getcrabReady.sh sources this and then does
# both, so the interactive command behaves exactly as before.
#
# cmsenv is an interactive alias/function from ~/.bashrc and does not exist in
# a non-interactive shell; `eval "$(scram runtime -sh)"` is what it expands to.
source /cvmfs/cms.cern.ch/cmsset_default.sh
pushd /eos/user/m/mshelake/Analysis/CMSSW_13_3_0/src >/dev/null
eval "$(scram runtime -sh)"
popd >/dev/null
# crab.sh parses $1 as a CRAB type (prod|pre|dev). A sourced file sees its
# caller's positional arguments, so sourcing it directly handed it whatever $1
# the caller had -- e.g. setup_lxplus_venv.sh's venv path, rejected as an
# "Invalid CRAB type". Sourcing it inside a function gives it an empty argument
# list; the environment it exports persists either way.
_setupEnv_crab() { source /cvmfs/cms.cern.ch/crab3/crab.sh; }
_setupEnv_crab
unset -f _setupEnv_crab
