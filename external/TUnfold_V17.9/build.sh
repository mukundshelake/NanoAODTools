#!/bin/bash
# Build the standalone TUnfold V17.9 shared library + ROOT dictionary.
#
# Why this exists: ROOT dropped TUnfold from its default build (the `unfold`
# CMake option is OFF), so `gSystem.Load("libUnfold")` fails in every conda env
# here and ROOT.TUnfoldDensity is undefined -- see issue #30. This builds the
# upstream standalone distribution against the env's own ROOT instead.
#
# The one non-obvious requirement: the conda environment must be *activated*,
# not merely on PATH. rootcling locates the C system headers through
# CONDA_BUILD_SYSROOT, which only etc/conda/activate.d/activate-root.sh sets.
# Without it the dictionary generation dies on a missing <features.h>.
#
# Usage:
#   conda activate latestcoffea
#   ./build.sh
#
# Source: https://www.desy.de/~sschmitt/TUnfold/TUnfold_V17.9.tgz
set -euo pipefail
cd "$(dirname "$0")"

command -v root-config >/dev/null || { echo "root-config not found; activate the conda env first"; exit 1; }
if [ -z "${CONDA_BUILD_SYSROOT:-}" ]; then
    echo "CONDA_BUILD_SYSROOT is unset."
    echo "Run 'conda activate <env>' (not just PATH= ...) so activate-root.sh runs."
    exit 1
fi

SOURCES="TUnfoldV17 TUnfoldSysV17 TUnfoldDensityV17 TUnfoldBinningV17 TUnfoldBinningXMLV17 TUnfoldIterativeEMV17"

echo "==> generating ROOT dictionary"
rm -f TUnfoldDict.cxx TUnfoldDict_rdict.pcm libunfold.rootmap
rootcling -f TUnfoldDict.cxx -rmf libunfold.rootmap -rml libunfold.so -I. \
    TUnfoldIterativeEM.h TUnfoldDensity.h TUnfoldBinningXML.h LinkDef.h

echo "==> compiling"
for src in $SOURCES TUnfoldDict; do
    echo "    $src.cxx"
    g++ -std=c++17 -O2 -fPIC -I. $(root-config --cflags) -c "$src.cxx" -o "$src.o"
done

echo "==> linking libunfold.so"
g++ -shared -o libunfold.so *.o $(root-config --libs) -lXMLParser
rm -f ./*.o

echo "==> verifying"
python - <<'PYEOF'
import os, ROOT
lib = os.path.join(os.getcwd(), "libunfold.so")
assert ROOT.gSystem.Load(lib) >= 0, "failed to load libunfold.so"
for name in ("TUnfold", "TUnfoldSys", "TUnfoldDensity", "TUnfoldBinning",
             "TUnfoldBinningXML", "TUnfoldIterativeEM"):
    assert hasattr(ROOT, name + "V17"), f"{name}V17 missing from the dictionary"
print("OK, TUnfold version", ROOT.TUnfoldV17.GetTUnfoldVersion())
PYEOF
echo "Done: $(pwd)/libunfold.so"
