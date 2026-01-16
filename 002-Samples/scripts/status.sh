#!/bin/bash
# Demo script showing the hash-based workflow

echo "=== Current config hash ==="
python3 scripts/run_all.py 2>&1 | head -1

echo -e "\n=== Existing outputs ==="
ls -1 outputs/ | grep -E '^[0-9a-f]{12}$' || echo "None"

echo -e "\n=== Run history (last 3 entries) ==="
tail -3 run_history.txt | python3 -m json.tool 2>/dev/null | grep -E '"timestamp"|"config_hash"|"user"' | head -9

echo -e "\n=== Available tags ==="
ls -1 outputs/tags/ 2>/dev/null || echo "None"

echo -e "\n=== Comparison demo ==="
echo "To compare two runs:"
echo "  diff outputs/<hash1>/config.yaml outputs/<hash2>/config.yaml"
echo "  diff <(cat outputs/<hash1>/dataSamples.json | jq .) <(cat outputs/<hash2>/dataSamples.json | jq .)"
