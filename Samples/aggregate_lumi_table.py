#!/usr/bin/env python3
import json
import glob
import os

PB_TO_FB = 1.0 / 1000.0

def era_label_from_filename(fname):
    # processed_dataset_UL2016preVFP.json → 2016 preVFP
    era = fname.replace("processed_dataset_", "").replace(".json", "")
    era = era.replace("UL", "")
    era = era.replace("preVFP", " preVFP")
    era = era.replace("postVFP", " postVFP")
    return era

def main():
    files = sorted(glob.glob("processed_dataset_*.json"))
    if not files:
        raise RuntimeError("No processed_dataset_*.json files found")

    table = {}
    grand_total_pb = 0.0
    grand_run_min = None
    grand_run_max = None

    for fname in files:
        era_label = era_label_from_filename(fname)

        with open(fname) as f:
            data = json.load(f)

        rows = []
        era_total_pb = 0.0
        era_run_min = None
        era_run_max = None

        for dset, info in sorted(data.items()):
            lumi_pb = info["recorded_lumi_pb"]

            rows.append({
                "run_range": f"{info['run_min']}–{info['run_max']}",
                "dataset": info["query"],
                "lumi_fb": lumi_pb * PB_TO_FB
            })

            era_total_pb += lumi_pb
            era_run_min = info["run_min"] if era_run_min is None else min(era_run_min, info["run_min"])
            era_run_max = info["run_max"] if era_run_max is None else max(era_run_max, info["run_max"])

        table[era_label] = {
            "rows": rows,
            "era_run_range": f"{era_run_min}–{era_run_max}",
            "era_lumi_fb": era_total_pb * PB_TO_FB
        }

        grand_total_pb += era_total_pb
        grand_run_min = era_run_min if grand_run_min is None else min(grand_run_min, era_run_min)
        grand_run_max = era_run_max if grand_run_max is None else max(grand_run_max, era_run_max)

    # ------------------------------------------------------------
    # Write aggregated JSON (authoritative)
    # ------------------------------------------------------------
    out_json = "aggregated_lumi_table.json"
    with open(out_json, "w") as f:
        json.dump({
            "eras": table,
            "sum_total": {
                "run_range": f"{grand_run_min}–{grand_run_max}",
                "lumi_fb": grand_total_pb * PB_TO_FB
            }
        }, f, indent=2)

    print(f"✅ Written {out_json}")

    # ------------------------------------------------------------
    # Write Markdown table (for notes / conveners)
    # ------------------------------------------------------------
    out_md = "aggregated_lumi_table.md"
    with open(out_md, "w") as f:
        f.write("| Era | Run range | Dataset | $\\mathcal{L}_{\\mathrm{int}}$ [fb$^{-1}$] |\n")
        f.write("|-----|-----------|---------|-----------------------------------|\n")

        for era, info in table.items():
            first = True
            for row in info["rows"]:
                f.write(
                    f"| {era if first else ''} "
                    f"| {row['run_range']} "
                    f"| `{row['dataset']}` "
                    f"| {row['lumi_fb']:.1f} |\n"
                )
                first = False

            f.write(
                f"|  | **{info['era_run_range']}** "
                f"| **Total** "
                f"| **{info['era_lumi_fb']:.1f}** |\n"
            )

        f.write(
            f"| **Sum Total** | **{grand_run_min}–{grand_run_max}** |  | **{grand_total_pb * PB_TO_FB:.1f}** |\n"
        )

    print(f"✅ Written {out_md}")

if __name__ == "__main__":
    main()

