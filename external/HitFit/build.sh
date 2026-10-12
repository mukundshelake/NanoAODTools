#!/bin/bash
# Build the vendored HitFit core + flat wrapper into libHitFit.so.
#
# HitFit (CMSSW TopQuarkAnalysis/TopHitFit) is the lepton+jets kinematic fitter
# of the 004A reference standard, CMS-TOP-23-003 (see 004A-Reconstruction/
# README.md and issue #49). Its core depends only on CLHEP.
#
# Two build environments, picked automatically:
#
#   conda (local running):   conda activate latestcoffea && ./build.sh
#       CLHEP comes from a private conda prefix here (envs/clhep, created on
#       first run, git-ignored), so latestcoffea itself is not modified.
#
#   CMSSW (CRAB submission): cmsenv && ./build.sh
#       Selected when CMSSW_BASE is set. Uses the release's compiler and its
#       CLHEP external, so the library loads inside CRAB jobs, which run in that
#       same release. Build it in the CMSSW area you submit from.
#
# The library carries an RPATH to its CLHEP, so nothing is exported at run
# time. libHitFit.buildinfo records which environment built it; the CRAB
# submitter refuses a library not built for the submitting CMSSW release.
# A library built in one environment does not load in the other: rebuild when
# switching.
#
# Products (git-ignored): libHitFit.so, libHitFit.buildinfo.
set -euo pipefail
cd "$(dirname "$0")"
HERE="$(pwd)"

if [ -n "${CMSSW_BASE:-}" ]; then
    MODE=cmssw
    CXX=g++
    # scram only answers from inside the CMSSW area. The clhep tool carries
    # LIBDIR and CLHEP_BASE; the headers live under CLHEP_BASE/include.
    CLHEP_BASE="$(cd "$CMSSW_BASE" && scram tool tag clhep CLHEP_BASE)"
    CLHEP_INC="$CLHEP_BASE/include"
    CLHEP_LIB="$(cd "$CMSSW_BASE" && scram tool tag clhep LIBDIR)"
    PYTHON=python3
elif [ -n "${CONDA_PREFIX:-}" ]; then
    MODE=conda
    CXX="$CONDA_PREFIX/bin/g++"
    [ -x "$CXX" ] || CXX=g++
    CLHEP_PREFIX="$HERE/envs/clhep"
    CLHEP_VERSION="2.4.7.2"
    if [ ! -f "$CLHEP_PREFIX/lib/libCLHEP.so" ]; then
        echo "==> creating private CLHEP $CLHEP_VERSION prefix at $CLHEP_PREFIX"
        conda create -y -q -p "$CLHEP_PREFIX" -c conda-forge --override-channels "clhep=$CLHEP_VERSION"
    fi
    CLHEP_INC="$CLHEP_PREFIX/include"
    CLHEP_LIB="$CLHEP_PREFIX/lib"
    PYTHON=python
else
    echo "Set up an environment first: 'conda activate latestcoffea' (local) or 'cmsenv' (CRAB)."
    exit 1
fi
[ -f "$CLHEP_INC/CLHEP/Vector/LorentzVector.h" ] || { echo "CLHEP headers not found under $CLHEP_INC"; exit 1; }

echo "==> [$MODE] compiling HitFit core + wrapper with $CXX (CLHEP: $CLHEP_LIB)"
"$CXX" -std=c++17 -O2 -fPIC -shared \
    -I"$HERE/include" -I"$HERE/wrapper" -I"$CLHEP_INC" \
    "$HERE"/src/*.cc "$HERE/wrapper/HitFitWrapper.cc" \
    -L"$CLHEP_LIB" -lCLHEP -Wl,-rpath,"$CLHEP_LIB" \
    -o "$HERE/libHitFit.so"

{
    echo "mode=$MODE"
    echo "cmssw_version=${CMSSW_VERSION:-}"
    echo "scram_arch=${SCRAM_ARCH:-}"
    echo "conda_prefix=${CONDA_PREFIX:-}"
    echo "clhep_lib=$CLHEP_LIB"
    echo "built=$(date -Iseconds) on $(hostname)"
} > "$HERE/libHitFit.buildinfo"

echo "==> toy calibration test"
HITFIT_CLHEP_INCLUDE="$CLHEP_INC" "$PYTHON" "$HERE/test/toy_test.py" --events 3000
echo "Done: $HERE/libHitFit.so ($MODE)"
