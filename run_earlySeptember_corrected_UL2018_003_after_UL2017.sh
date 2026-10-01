#!/bin/bash
# Waits for the UL2017 003-II/III driver to finish, then runs the full UL2018
# 003 chain (003-I, then 003-II -> 003-III) if UL2017 succeeded. Running them
# one after the other also keeps the two eras from overwriting each other's
# generated scripts/run_all_earlySeptember_corrected.sh.
#
# Usage: bash run_earlySeptember_corrected_UL2018_003_after_UL2017.sh

set -eo pipefail

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
UL2017_STATUS="$REPO/run_logs/earlySeptember_corrected_UL2017_003II_III/STATUS.txt"
TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[earlySeptember_corrected/UL2018 chain] $1" >/dev/null 2>&1 || true
}

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting for UL2017 003-II/III to finish..."
until grep -q '^RESULT=' "$UL2017_STATUS"; do sleep 60; done
if ! grep -q '^RESULT=SUCCESS' "$UL2017_STATUS"; then
    echo "UL2017 003-II/III did not succeed; not starting UL2018." >&2
    notify "UL2017 003-II/III failed, so UL2018 was NOT started."
    exit 1
fi

cd "$REPO"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] UL2017 done; starting UL2018 003-I."
bash run_earlySeptember_corrected_UL2018_003I.sh > run_logs/earlySeptember_corrected_UL2018_003I_driver.out 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] UL2018 003-I done; starting 003-II -> 003-III."
bash run_earlySeptember_corrected_UL2018_003II_III.sh > run_logs/earlySeptember_corrected_UL2018_003II_III_driver.out 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] UL2018 003 chain done."
