#!/usr/bin/env python3
"""
BDTvariablesPartonPlotter.py - Plot BDT variables for parton-classification analysis

Reads .coffea files produced by BDTvariablesPartonProcessor.py and creates overlay plots
for the 17 BDT variables showing qq (quark-quark), gg (gluon-gluon), and qg (quark-gluon)
categories. Histograms are normalized to unit area for shape comparison.

Dependencies:
- coffea: pip install coffea
- hist: pip install hist
- root: Install ROOT with PyROOT support (https://root.cern/install/)

Usage:
    python scripts/BDTvariablesPartonPlotter.py outputs/midNov_UL2017_bdtvariables_parton.coffea
"""

import argparse
import logging
import os
import sys
import math

# Try to import required packages with error handling
try:
    from coffea.util import load
    import hist
    import ROOT
    from ROOT import TH1F, TCanvas, TLegend, TLatex, gPad, kRed, kBlue, kGreen
except ImportError as e:
    print(f"Error: Missing required dependency - {str(e)}")
    print("Please install the required packages:")
    print("  pip install coffea hist")
    print("And install ROOT with PyROOT support from https://root.cern/install/")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Variable Configuration for BDT Variables ---
# Maps internal variable names to plotting labels
VARIABLE_CONFIG = {
    # Jet kinematic variables
    "nJet": {"label": "Number of Jets"},
    "JetHT": {"label": "Jet H_{T} (GeV)"},
    "pTSum": {"label": "p_{T} Sum (GeV)"},
    
    # Fox-Wolfram moments
    "FW1": {"label": "Fox-Wolfram H_{1}"},
    "FW2": {"label": "Fox-Wolfram H_{2}"},
    "FW3": {"label": "Fox-Wolfram H_{3}"},
    
    # Longitudinal alignment
    "AL": {"label": "Alignment A_{L}"},
    
    # Sphericity tensor elements
    "Sxx": {"label": "S_{xx}"},
    "Syy": {"label": "S_{yy}"},
    "Sxy": {"label": "S_{xy}"},
    "Sxz": {"label": "S_{xz}"},
    "Syz": {"label": "S_{yz}"},
    "Szz": {"label": "S_{zz}"},
    
    # Event shape variables
    "S": {"label": "Sphericity"},
    "P": {"label": "Planarity"},
    "A": {"label": "Aplanarity"},
    
    # Momentum flow variables
    "p2in": {"label": "p_{2}^{in}"},
    "p2out": {"label": "p_{2}^{out}"}
}


def coffea_hist_to_root(coffea_hist, hist_name, category):
    """
    Convert Coffea histogram to ROOT TH1F.
    
    Args:
        coffea_hist: Coffea histogram object
        hist_name: Name for ROOT histogram
        category: Parton category (qq, gg, qg)
    
    Returns:
        ROOT TH1F histogram
    """
    # Get bin edges and values
    axis = coffea_hist.axes[0]
    edges = axis.edges
    values = coffea_hist.values()
    
    # Try to get variances - may be None for unweighted histograms
    variances = coffea_hist.variances()
    if variances is None:
        # For unweighted histograms, variance = value (Poisson)
        variances = values
    
    # Create ROOT histogram (empty title - will be set in plot_variable)
    root_hist = TH1F(
        f"{hist_name}_{category}",
        "",
        len(edges) - 1,
        edges[0],
        edges[-1]
    )
    
    # Fill ROOT histogram with bin contents and errors
    for i, (val, var) in enumerate(zip(values, variances)):
        root_hist.SetBinContent(i + 1, val)
        root_hist.SetBinError(i + 1, math.sqrt(var) if var > 0 else 0)
    
    return root_hist


def plot_variable(variable_name, histograms_dict, output_dir):
    """
    Create overlay plot for one variable with qq/gg/qg categories.
    
    Args:
        variable_name: Name of the variable to plot
        histograms_dict: Dictionary containing histograms for all categories
        output_dir: Directory to save output plots
    """
    logger.info(f"Plotting {variable_name}...")
    
    # Extract histograms for this variable from each category
    qq_hist_name = f"qq_{variable_name}"
    gg_hist_name = f"gg_{variable_name}"
    qg_hist_name = f"qg_{variable_name}"
    
    # Check if histograms exist
    if not all(name in histograms_dict for name in [qq_hist_name, gg_hist_name, qg_hist_name]):
        logger.warning(f"Missing histograms for {variable_name}, skipping...")
        return
    
    # Convert Coffea histograms to ROOT
    qq_hist_root = coffea_hist_to_root(histograms_dict[qq_hist_name], variable_name, "qq")
    gg_hist_root = coffea_hist_to_root(histograms_dict[gg_hist_name], variable_name, "gg")
    qg_hist_root = coffea_hist_to_root(histograms_dict[qg_hist_name], variable_name, "qg")
    
    # Normalize to unit area
    for h in [qq_hist_root, gg_hist_root, qg_hist_root]:
        integral = h.Integral()
        if integral > 0:
            h.Scale(1.0 / integral)
        else:
            logger.warning(f"Histogram {h.GetName()} has zero integral, skipping normalization")
    
    # Set histogram styles
    qq_hist_root.SetLineColor(kBlue)
    qq_hist_root.SetLineWidth(2)
    qq_hist_root.SetMarkerColor(kBlue)
    qq_hist_root.SetMarkerStyle(20)
    
    gg_hist_root.SetLineColor(kRed)
    gg_hist_root.SetLineWidth(2)
    gg_hist_root.SetMarkerColor(kRed)
    gg_hist_root.SetMarkerStyle(21)
    
    qg_hist_root.SetLineColor(kGreen + 2)
    qg_hist_root.SetLineWidth(2)
    qg_hist_root.SetMarkerColor(kGreen + 2)
    qg_hist_root.SetMarkerStyle(22)
    
    # Create canvas
    variable_label = VARIABLE_CONFIG[variable_name]["label"]
    canvas = TCanvas(f"canvas_{variable_name}", variable_label, 800, 600)
    canvas.SetLeftMargin(0.12)
    canvas.SetRightMargin(0.05)
    canvas.SetTopMargin(0.10)
    canvas.SetBottomMargin(0.12)
    
    # Remove statistics box
    ROOT.gStyle.SetOptStat(0)
    
    # Find maximum for y-axis
    max_val = max(qq_hist_root.GetMaximum(), gg_hist_root.GetMaximum(), qg_hist_root.GetMaximum())
    
    # Set histogram title and axis labels
    qq_hist_root.SetTitle(variable_label)
    qq_hist_root.SetMaximum(max_val * 1.35)  # Reduced headroom for better fit
    qq_hist_root.SetMinimum(0)
    qq_hist_root.GetXaxis().SetTitle(variable_label)
    qq_hist_root.GetYaxis().SetTitle("Normalized Events")
    qq_hist_root.GetXaxis().SetTitleSize(0.045)
    qq_hist_root.GetYaxis().SetTitleSize(0.045)
    qq_hist_root.GetXaxis().SetLabelSize(0.04)
    qq_hist_root.GetYaxis().SetLabelSize(0.04)
    qq_hist_root.GetYaxis().SetTitleOffset(1.3)
    
    qq_hist_root.Draw("HIST")
    gg_hist_root.Draw("HIST SAME")
    qg_hist_root.Draw("HIST SAME")
    
    # Add legend - positioned to avoid overlap with histograms
    legend = TLegend(0.60, 0.68, 0.92, 0.88)
    legend.SetBorderSize(1)
    legend.SetFillStyle(1001)
    legend.SetFillColor(10)
    legend.SetTextSize(0.038)
    legend.AddEntry(qq_hist_root, "qq (quark-quark)", "l")
    legend.AddEntry(gg_hist_root, "gg (gluon-gluon)", "l")
    legend.AddEntry(qg_hist_root, "qg (quark-gluon)", "l")
    legend.Draw()
    
    # Add CMS label with Preliminary
    cms_text = TLatex()
    cms_text.SetNDC()
    cms_text.SetTextFont(61)
    cms_text.SetTextSize(0.055)
    cms_text.DrawLatex(0.12, 0.94, "CMS")
    
    prelim_text = TLatex()
    prelim_text.SetNDC()
    prelim_text.SetTextFont(52)
    prelim_text.SetTextSize(0.042)
    prelim_text.DrawLatex(0.21, 0.94, "Preliminary")
    
    # Add process info on the side (shifted right to avoid y-axis overlap)
    info_text = TLatex()
    info_text.SetNDC()
    info_text.SetTextFont(42)
    info_text.SetTextSize(0.038)
    info_text.DrawLatex(0.15, 0.86, "t#bar{t} #rightarrow #mu+jets")
    info_text.DrawLatex(0.15, 0.81, "Parton Classification")
    
    # Save plot as PNG and ROOT macro (.C)
    output_png = os.path.join(output_dir, f"{variable_name}_parton.png")
    output_c = os.path.join(output_dir, f"{variable_name}_parton.C")
    canvas.SaveAs(output_png)
    canvas.SaveAs(output_c)
    
    # Modify the .C file to add canvas update commands for interactive viewing
    try:
        with open(output_c, 'r') as f:
            c_content = f.read()
        # Add canvas update before the closing brace
        c_content = c_content.rstrip()
        if c_content.endswith('}'):
            c_content = c_content[:-1] + '   canvas_' + variable_name + '->Update();\n   gPad->Update();\n}\n'
        with open(output_c, 'w') as f:
            f.write(c_content)
    except Exception as e:
        logger.warning(f"Could not modify .C file: {e}")
    
    logger.info(f"  Saved: {output_png}")
    logger.info(f"  Saved: {output_c}")
    
    # Clean up
    canvas.Close()


def main(coffea_file, output_dir=None):
    """
    Main execution function for parton-classification plotting.
    
    Args:
        coffea_file: Path to .coffea file with histograms
        output_dir: Directory to save plots (default: "plots")
    """
    # Check if coffea file exists
    if not os.path.exists(coffea_file):
        logger.error(f"Coffea file not found: {coffea_file}")
        sys.exit(1)
    
    # Load histograms
    logger.info(f"Loading histograms from: {coffea_file}")
    output_hists = load(coffea_file)
    
    # Extract histograms from the first (and only) dataset
    dataset_name = list(output_hists.keys())[0]
    logger.info(f"Dataset: {dataset_name}")
    
    # Handle nested structure: output_hists[dataset][dataset] or output_hists[dataset]
    dataset_content = output_hists[dataset_name]
    if isinstance(dataset_content, dict) and dataset_name in dataset_content:
        # Nested structure: {dataset: {dataset: histograms}}
        histograms_dict = dataset_content[dataset_name]
    elif isinstance(dataset_content, dict):
        # Direct structure: {dataset: histograms}
        histograms_dict = dataset_content
    else:
        logger.error(f"Unexpected data structure in coffea file")
        sys.exit(1)
    
    logger.info(f"Found {len(histograms_dict)} histograms")
    logger.info(f"Histogram keys: {list(histograms_dict.keys())[:10]}...")  # Show first 10
    
    # Create output directory for plots
    if output_dir is None:
        output_dir = "plots"
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Plots will be saved to: {output_dir}/")
    
    # Get list of variables to plot (without category prefix)
    variables_to_plot = list(VARIABLE_CONFIG.keys())
    
    logger.info(f"Plotting {len(variables_to_plot)} variables...")
    
    # Plot each variable
    for variable in variables_to_plot:
        try:
            plot_variable(variable, histograms_dict, output_dir)
        except Exception as e:
            logger.error(f"Error plotting {variable}: {e}")
            continue
    
    logger.info("✓ All plots completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Plot BDT variables for parton-classification analysis"
    )
    parser.add_argument(
        "coffea_file",
        help="Path to .coffea file with histograms (e.g., outputs/midNov_UL2017_bdtvariables_parton.coffea)"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to save plots (default: plots)"
    )
    args = parser.parse_args()
    
    main(args.coffea_file, args.output_dir)
