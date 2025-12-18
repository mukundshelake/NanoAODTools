#!/usr/bin/env python3
import json
import glob
import os

PB_TO_FB = 1.0 / 1000.0

ERA_ORDER = [
    "UL2016preVFP",
    "UL2016postVFP",
    "UL2017",
    "UL2018",
]

ERA_LABEL = {
    "UL2016preVFP": "2016 preVFP",
    "UL2016postVFP": "2016 postVFP",
    "UL2017": "2017",
    "UL2018": "2018",
}

def tex_escape(s):
    """Escape underscores for LaTeX texttt"""
    return s.replace("_", "\\_")

def load_era_data(era):
    fname = f"processed_dataset_{era}.json"
    if not os.path.exists(fname):
        return None
    with open(fname) as f:
        return json.load(f)

def main():
    lines = []

    # ---- Header (verbatim from your template) ----
    lines.extend([
        "\\begin{table}[hbpt]",
        "\\caption{List of data samples over different run periods.}",
        "\\label{tab:datasamples}",
        "\\begin{center}",
        "%\\hspace*{-1cm}",
        "%\\fontsize{8}{9.2}\\selectfont",
        "\\resizebox{\\textwidth}{!}{",
        "\\begin{tabular}{cccc}",
        "Era & Run range & Dataset name & $\\mathcal{L}_{\\text{int}}$ (\\unit{fb}$^{-1}$) \\\\ \\\\",
        "\\hline \\hline \\\\",
    ])

    grand_lumi_pb = 0.0
    grand_run_min = None
    grand_run_max = None

    for era in ERA_ORDER:
        data = load_era_data(era)
        if not data:
            continue

        era_label = ERA_LABEL[era]
        datasets = sorted(
            data.items(),
            key=lambda x: x[1]["run_min"]
        )

        nrows = len(datasets)
        era_lumi_pb = 0.0
        era_run_min = None
        era_run_max = None

        for i, (name, info) in enumerate(datasets):
            run_min = info["run_min"]
            run_max = info["run_max"]
            lumi_fb = info["recorded_lumi_pb"] * PB_TO_FB
            query = tex_escape(info["query"])

            era_lumi_pb += info["recorded_lumi_pb"]
            era_run_min = run_min if era_run_min is None else min(era_run_min, run_min)
            era_run_max = run_max if era_run_max is None else max(era_run_max, run_max)

            if i == 0:
                lines.append(
                    f"\\multirow{{{nrows}}}{{*}}{{{era_label}}} & "
                    f"{run_min}--{run_max} & "
                    f"\\texttt{{{query}}} & {lumi_fb:.1f} \\\\"
                )
            else:
                lines.append(
                    f" & {run_min}--{run_max} & "
                    f"\\texttt{{{query}}} & {lumi_fb:.1f} \\\\"
                )

        # Era total
        lines.extend([
            f"\\cline{{2-4}} \\\\",
            f" & {era_run_min}--{era_run_max} & & {era_lumi_pb * PB_TO_FB:.1f} \\\\ \\\\",
            "\\hline \\\\",
        ])

        grand_lumi_pb += era_lumi_pb
        grand_run_min = era_run_min if grand_run_min is None else min(grand_run_min, era_run_min)
        grand_run_max = era_run_max if grand_run_max is None else max(grand_run_max, era_run_max)

    # ---- Sum total ----
    lines.extend([
        "\\hline \\hline \\\\",
        f" Sum Total & {grand_run_min}--{grand_run_max} & & {grand_lumi_pb * PB_TO_FB:.1f} \\\\",
        "\\end{tabular}}",
        "\\end{center}",
        "\\end{table}",
    ])

    out = "\n".join(lines)
    with open("lumi_table.tex", "w") as f:
        f.write(out)

    print("✅ Written lumi_table.tex (ready for Overleaf)")

if __name__ == "__main__":
    main()

