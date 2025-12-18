#!/usr/bin/env bash

set -eo pipefail

# -------------------------------
# Configuration
# -------------------------------
LUMI_DIR="lumi"
NORMTAG="/cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json"
BRIL_ENV="/cvmfs/cms-bril.cern.ch/cms-lumi-pog/brilws-docker/brilws-env"
UNIT="/pb"

# -------------------------------
# Setup BRIL environment
# -------------------------------
echo "🔧 Sourcing BRIL environment"
source "$BRIL_ENV"

# IMPORTANT: aliases are not expanded by default in scripts
if ! alias brilcalc >/dev/null 2>&1; then
  echo "❌ brilcalc alias not found after sourcing BRIL env"
  exit 1
fi

# Extract the actual Singularity command behind the alias
BRILCALC_CMD=$(alias brilcalc | sed "s/^alias brilcalc='//; s/'$//")

echo "✅ Using brilcalc command:"
echo "   $BRILCALC_CMD"
echo

# -------------------------------
# Find golden JSONs
# -------------------------------
mapfile -t GOLDEN_JSONS < <(find "$LUMI_DIR" -type f -name "*_golden.json" | sort)

if [ "${#GOLDEN_JSONS[@]}" -eq 0 ]; then
  echo "❌ No *_golden.json files found under $LUMI_DIR"
  exit 1
fi

echo "📄 Found ${#GOLDEN_JSONS[@]} golden JSON files"
echo

# -------------------------------
# Run brilcalc + extract lumi
# -------------------------------
for json in "${GOLDEN_JSONS[@]}"; do
  csv="${json%.json}.csv"
  value_json="${json%.json}_value.json"

  echo "▶️  Processing dataset:"
  echo "    JSON : $json"
  echo "    CSV  : $csv"

  # ---- Run brilcalc ----
  eval "$BRILCALC_CMD lumi \
    --normtag \"$NORMTAG\" \
    -u \"$UNIT\" \
    -i \"$json\" \
    -o \"$csv\""

  # ---- Extract recorded lumi (pb) ----
  lumi_pb=$(
    grep '^#.*totrecorded' -A1 "$csv" \
      | tail -n1 \
      | awk -F',' '{print $NF}'
  )

  if [ -z "$lumi_pb" ]; then
    echo "⚠️  Could not extract lumi from $csv"
    continue
  fi

  # ---- Write value JSON ----
  cat > "$value_json" <<EOF
{
  "recorded_lumi_pb": $lumi_pb
}
EOF

  echo "    ✅ recorded_lumi_pb = $lumi_pb"
  echo "    📝 Written: $value_json"
  echo
done

echo "🎉 All brilcalc jobs and lumi extractions completed successfully"

