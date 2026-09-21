# TUnfold V17.9 (vendored)

Upstream standalone distribution from
<https://www.desy.de/~sschmitt/TUnfold/TUnfold_V17.9.tgz>, GPLv3 (see `COPYING`).

## Why it is here

ROOT no longer builds TUnfold by default -- the `unfold` CMake option is OFF, so
there is no `libUnfold` in any of the conda environments on this machine and
`ROOT.TUnfoldDensity` is undefined. `005-Unfolding` needs it. Rather than
rebuild ROOT with `-Dunfold=ON`, this vendors the upstream standalone package
and builds it against whichever ROOT the active environment provides.

## Building

```bash
conda activate latestcoffea      # must be activated, not just on PATH
./build.sh
```

`latestcoffea` is currently the only environment with ROOT **and** pandas,
uproot, awkward and yaml together, so it is the one environment that can run
the chapter end to end. `root_tunfold` and `nano_env` lack pandas.

The activation requirement is not cosmetic: `rootcling` finds the C system
headers via `CONDA_BUILD_SYSROOT`, which is exported by
`etc/conda/activate.d/activate-root.sh`. Building with only `PATH` and
`ROOTSYS` set fails on a missing `<features.h>`.

## Using it from Python

Do not call `gSystem.Load` directly; use the chapter's loader, which also
aliases the versioned class names:

```python
from tunfold_env import ROOT, load_tunfold
load_tunfold()
unfold = ROOT.TUnfoldDensity(...)
```

Upstream compiles the classes as `TUnfoldDensityV17` etc. and exposes the plain
names through C preprocessor `#define`s, which do not survive into PyROOT --
hence the aliasing step.

## Build products

`libunfold.so`, `TUnfoldDict.cxx`, `TUnfoldDict_rdict.pcm` and
`libunfold.rootmap` are generated and git-ignored. Re-run `build.sh` after
changing ROOT version or environment.
