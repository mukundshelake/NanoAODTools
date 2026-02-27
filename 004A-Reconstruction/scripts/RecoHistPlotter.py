#!/usr/bin/env python3
"""
RecoHistPlotter.py - Plot Data/MC distributions for reconstructed ttbar variables

Reads .coffea files produced by RecoDataMCHist.py and creates stacked Data/MC plots
with ratio panels for the 13 reconstruction branches from RecoModule.py.

Dependencies:
- coffea: pip install coffea
- hist: pip install hist
- root: Install ROOT with PyROOT support (https://root.cern/install/)

Usage:
    python scripts/RecoHistPlotter.py outputs/UL2017_data_midNov_reco.coffea
"""

import argparse
import logging
import os
import sys
import json
import math

# Try to import required packages with error handling
try:
    from coffea.util import load
    import hist
    import ROOT
    from ROOT import TH1F
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

# --- Variable Configuration for Reconstruction Branches ---
# Maps internal variable names to plotting labels and binning options
VARIABLE_CONFIG = {
    # Leptonic top 4-vector
    "Top_lep_pt": {
        "label": "Leptonic Top p_{T} (GeV)", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 1.0, 
        "legendStyle": "vertical"
    },
    "Top_lep_eta": {
        "label": "Leptonic Top #eta", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 1.0, 
        "legendStyle": "wide"
    },
    "Top_lep_phi": {
        "label": "Leptonic Top #phi", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 1.0, 
        "legendStyle": "wide"
    },
    "Top_lep_mass": {
        "label": "Leptonic Top Mass (GeV)", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 1.0, 
        "legendStyle": "vertical"
    },
    
    # Hadronic top 4-vector
    "Top_had_pt": {
        "label": "Hadronic Top p_{T} (GeV)", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 1.0, 
        "legendStyle": "vertical"
    },
    "Top_had_eta": {
        "label": "Hadronic Top #eta", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 1.0, 
        "legendStyle": "wide"
    },
    "Top_had_phi": {
        "label": "Hadronic Top #phi", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 1.0, 
        "legendStyle": "wide"
    },
    "Top_had_mass": {
        "label": "Hadronic Top Mass (GeV)", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 1.0, 
        "legendStyle": "vertical"
    },
    
    # Fit quality variables
    "Chi2_prefit": {
        "label": "Pre-fit #chi^{2}", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 0.1, 
        "legendStyle": "vertical"
    },
    "Chi2": {
        "label": "Fitted #chi^{2}", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 0.1, 
        "legendStyle": "vertical"
    },
    "Pgof": {
        "label": "P(#chi^{2})", 
        "rebin": None, 
        "max_val": 10000000.0, 
        "min_val": 1.0, 
        "legendStyle": "wide"
    },
    "chi2_status": {
        "label": "Reconstruction Status", 
        "rebin": [0, 1, 2, 3, 4], 
        "renameBins": ['Success', 'Jet Fail', 'Nu Fail', 'Fit Fail', 'Other'], 
        "rebinType": "addOverflow", 
        "max_val": 100000000.0, 
        "min_val": 1.0, 
        "legendStyle": "wide"
    }
}
# ---

def rebin_with_custom_edges(histogram, new_edges, rebin_type=None):
    old_axis = histogram.axes[0]
    if rebin_type == "addOverflow":
        # Add overflow bin
        new_edges = new_edges + [new_edges[-1] + (new_edges[-1] - new_edges[-2])]
        newHist = hist.Hist.new.Variable(new_edges, name=old_axis.name, label=old_axis.label).Double()
        for i in range(len(new_edges) - 1):
            low = new_edges[i]*1j
            high = new_edges[i+1]*1j
            content = histogram[low:high].sum()
            if i == len(new_edges) - 2:  # Last bin (overflow)
                content += histogram[high:].sum()
            newHist[i] = content
    else:
        newHist = hist.Hist.new.Variable(new_edges, name=old_axis.name, label=old_axis.label).Double()
        for i in range(len(new_edges) - 1):
            low = new_edges[i]*1j
            high = new_edges[i+1]*1j
            content = histogram[low:high].sum()
            newHist[i] = content
    return newHist

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Plot reconstruction histograms from Coffea analysis',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('input_file', help='Path to input Coffea file (e.g. outputs/UL2017_data_midNov_reco.coffea)')
    parser.add_argument('--output-dir', default='../plots', help='Output directory for plots')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Logging level')
    parser.add_argument('--sample-info', default=None,
                       help='Path to JSON file with cross sections and generated events (auto-detected from era if not provided)')
    return parser.parse_args()

def load_sample_info(json_path):
    """Load cross sections and generated events from JSON file."""
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        required_keys = ['cross_sections', 'generated_events', 'category_map', 'Luminosity', 'luminosity_uncertainty', 'era']
        if not all(key in data for key in required_keys):
            missing_keys = [key for key in required_keys if key not in data]
            raise ValueError(f"JSON file must contain {', '.join(required_keys)} keys. Missing: {', '.join(missing_keys)}")
        logger.info(f"Loaded sample info from: {json_path}")
        logger.info(f"Era: {data['era']}")
        logger.info(f"Luminosity: {data['Luminosity']:.1f} pb^-1 ({data['Luminosity']/1000:.2f} fb^-1)")
        logger.info(f"Luminosity uncertainty: {data['luminosity_uncertainty']*100:.2f}%")
        return data
    except FileNotFoundError:
        logger.error(f"Sample info file not found: {json_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from file: {json_path}")
        raise
    except ValueError as e:
        logger.error(f"Invalid JSON format in {json_path}: {e}")
        raise

def process_histograms(out_merged, variable, luminosity, sample_info):
    """Process and scale histograms using info from JSON and apply rebinning."""
    category_map = sample_info['category_map']
    merged_histos = {}
    xsecs = sample_info['cross_sections']
    Ngenerated_events = sample_info['generated_events']
    var_config = VARIABLE_CONFIG.get(variable)
    if not var_config:
        # This case should ideally be caught by argparse choices, but check anyway
        logger.error(f"Variable '{variable}' not found in VARIABLE_CONFIG.")
        raise ValueError(f"Configuration for variable '{variable}' not found.")

    rebin_arg = var_config.get("rebin") # Could be slice or int
    rebin_type = var_config.get("rebinType", None)

    for category, dataset_list in category_map.items():
        merged = None
        for dataset in dataset_list:
            if dataset not in out_merged:
                logger.warning(f"Missing dataset: {dataset} in input file. Skipping.")
                continue

            try:
                # Check if histogram exists for the variable
                if variable not in out_merged[dataset]['histos']:
                    logger.warning(f"Histogram for variable '{variable}' not found in dataset {dataset}. Skipping.")
                    continue
                h = out_merged[dataset]['histos'][variable]

                # --- Scaling ---
                if "Run" not in dataset:  # Skip data scaling
                    if dataset not in xsecs or dataset not in Ngenerated_events:
                        logger.warning(f"Missing sample info for {dataset} in JSON file. Skipping scaling.")
                    else:
                        # Ensure Ngenerated_events is not zero
                        n_gen = Ngenerated_events[dataset]
                        if n_gen <= 0:
                             logger.warning(f"Ngenerated_events is zero or negative for {dataset}. Skipping scaling.")
                        else:
                            scale = (xsecs[dataset] * luminosity) / n_gen
                            logger.debug(f"Scaling {dataset} by {scale} (xsec={xsecs[dataset]}, lumi={luminosity}, Ngen={n_gen})")
                            h *= scale
                # --- End Scaling ---

                # Accumulate histograms
                merged = h if merged is None else merged + h

            except KeyError as e:
                logger.error(f"KeyError accessing data for {dataset} (variable: {variable}): {str(e)}. Check input file structure.")
                continue # Skip this dataset if structure is unexpected
            except Exception as e:
                 logger.error(f"Unexpected error processing dataset {dataset} for variable {variable}: {e}")
                 continue

        # --- Rebinning ---
        if merged is not None:
            if rebin_arg is not None:
                try:
                    logger.info(f"Applying rebinning '{rebin_arg}' for {variable} in category {category}")

                    if isinstance(rebin_arg, slice):
                        merged = merged[rebin_arg]

                    elif isinstance(rebin_arg, int):
                        merged = merged.rebin(rebin_arg)

                    elif isinstance(rebin_arg, (list, tuple)) and all(isinstance(x, (int, float)) for x in rebin_arg):
                        merged = rebin_with_custom_edges(merged, rebin_arg, rebin_type)

                    elif isinstance(rebin_arg, tuple) and len(rebin_arg) > 1 and all(isinstance(x, (int, float, complex)) for x in rebin_arg):
                        step = rebin_arg[2] if len(rebin_arg) > 2 else 1j
                        merged = merged[rebin_arg[0]:rebin_arg[1]:step]

                    else:
                        logger.warning(f"Unsupported rebinning argument type '{type(rebin_arg)}' for variable '{variable}'. Skipping rebinning.")

                except Exception as e:
                    logger.error(f"Failed to rebin histogram for {category} with argument '{rebin_arg}': {e}")

            # --- Zero out bins below threshold ---
            zero_below = var_config.get("zero_bins_below")
            if zero_below is not None:
                try:
                    axis = merged.axes[0]
                    for i, edge in enumerate(axis.edges[:-1]):
                        if edge < zero_below:
                            merged.view()[i] = 0.0
                    logger.info(f"Zeroed bins below {zero_below} for {variable} in category {category}")
                except Exception as e:
                    logger.error(f"Failed to zero bins below {zero_below} for {category}: {e}")
            # --- End Zero out bins ---

            merged_histos[category] = merged
        # --- End Rebinning ---
    
    return merged_histos

def create_root_histograms(merged_histos, variable):
    """Create ROOT histograms from coffea histograms with proper bin errors."""
    import numpy as np
    root_histos = {}
    # Use variable name from config if available, otherwise use the key
    var_label = VARIABLE_CONFIG.get(variable, {}).get("label", variable)

    for category, histo in merged_histos.items():
        # Basic check if it looks like a coffea/hist histogram
        if not hasattr(histo, 'axes') or not histo.axes or not hasattr(histo, 'values'):
             logger.warning(f"Skipping category '{category}' for variable '{variable}': object doesn't look like a valid histogram.")
             continue

        try:
            # Assuming 1D histogram for now
            if len(histo.axes) != 1:
                logger.warning(f"Skipping non-1D histogram for category '{category}', variable '{variable}'.")
                continue
            axis = histo.axes[0]
            size = axis.size
            # Use edges array directly for ROOT TH1F definition
            edges = np.array(axis.edges, dtype=float)

            # Create ROOT histogram with variable binning if necessary
            root_hist = TH1F(
                f"{category}_{variable}", # Unique name per variable/category
                f"{category};{var_label};Events", # More precise Y-axis label
                size, edges # Pass the bin edges array
            )
        except Exception as e:
            logger.error(f"Error creating ROOT histogram structure for {category} ({variable}): {e}")
            continue

        # Fill the ROOT histogram with content and errors
        try:
            values = histo.values()
            if values is None:
                logger.error(f"Values are None for {category} ({variable}). Skipping fill.")
                continue
            # variances = histo.variances() if hasattr(histo, 'variances') else values
            # if variances is None:
            #     logger.warning(f"Variances not found for {category} ({variable}). Using values for errors.")
            # if len(values) != size:
            #      logger.error(f"Mismatch between axis size ({size}) and values length ({len(values)}) for {category}, {variable}. Skipping fill.")
            #      continue
            # if len(variances) != size:
            #      logger.warning(f"Mismatch between axis size ({size}) and variances length ({len(variances)}) for {category}, {variable}. Using sqrt(content) for errors.")
            #      variances = values # Fallback

            for i in range(size):
                bin_content = values[i]
                variance = values[i]

                # ROOT bin index starts from 1
                root_bin = i + 1

                # Set bin content and error
                bin_error = np.sqrt(variance) if variance >= 0 else 0.0

                root_hist.SetBinContent(root_bin, bin_content)
                root_hist.SetBinError(root_bin, bin_error)
                if "renameBins" in VARIABLE_CONFIG[variable]:
                    # Use renamed bins if provided
                    root_hist.GetXaxis().SetBinLabel(root_bin, VARIABLE_CONFIG[variable]["renameBins"][i])

            root_histos[category] = root_hist # Add successfully created hist

        except Exception as e:
            logger.error(f"Error filling ROOT histogram for {category} ({variable}): {e}")
            # Don't add the histogram if filling failed
    
    return root_histos

def save_plots(root_histos, variable, output_dir, luminosity_pb, args, era, lumi_uncertainty):
    """Save plots to output directory with proper styling and ratio plot."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")

    # Use label from config, default to variable name
    var_label = VARIABLE_CONFIG.get(variable, {}).get("label", variable)
    luminosity_fb = luminosity_pb / 1000.0 # Convert pb^-1 to fb^-1 for display

    # Define category order for stacking
    # Make this configurable later if needed
    stack_order = [
        "ttbarSemiLeptonic", "ttbarFullyLeptonic", "Diboson", "SingleTop", 
        "WJets", "DrellYan", "QCD"
    ]
    plot_categories = ["Data"] + stack_order

    # Check if all required histograms exist in the input dict
    available_categories = set(root_histos.keys())
    required_for_plot = set(plot_categories)
    missing_histos = list(required_for_plot - available_categories)

    if missing_histos:
        logger.error(f"Cannot create plot for '{variable}'. Missing histograms for categories: {', '.join(missing_histos)}")
        return # Stop if essential histograms are missing

    # Filter out categories from stack_order that are not available, though we checked above
    stack_order_available = [cat for cat in stack_order if cat in available_categories]
    if not stack_order_available:
         logger.error(f"No background histograms available to stack for variable '{variable}'.")
         return

    try:
        # --- Get Histograms ---
        hist_data = root_histos["Data"]
        background_histos = {cat: root_histos[cat] for cat in stack_order_available}

        # --- Styling ---
        hist_data.SetMarkerStyle(20)
        hist_data.SetMarkerSize(1.0) # Slightly larger markers
        hist_data.SetLineColor(ROOT.kBlack)
        hist_data.SetLineWidth(2)

        # Define colors (consider moving to config later)
        colors = {
            "ttbarSemiLeptonic": 432,  
            "ttbarFullyLeptonic": 616, 
            "DrellYan": 416,       
            "WJets": 600,          
            "Diboson": 632,         
            "SingleTop": 800,        
            "QCD": 920               
        }
        for cat in stack_order_available:
            hist = background_histos[cat]
            hist.SetFillColor(colors.get(cat, ROOT.kWhite)) # Use defined color or white
            hist.SetLineColor(ROOT.kBlack)
            hist.SetLineWidth(1)

        # Disable statistics box for all involved histograms
        for hist in [hist_data] + list(background_histos.values()):
             if hasattr(hist, 'SetStats'):
                hist.SetStats(False)
        # --- End Styling ---


        # --- Create Stack ---
        # Use var_label in the title string
        stack = ROOT.THStack(f"stack_{variable}", f";{var_label};Events")
        for cat in stack_order_available:
             stack.Add(background_histos[cat])

        # Get total MC histogram for ratio and uncertainty band
        if not stack.GetHists():
             logger.error(f"Stack for variable '{variable}' is empty. Cannot proceed.")
             return
        stack_total = stack.GetStack().Last().Clone(f"total_mc_{variable}")
        if variable == "n_jets":
            logger.info(f"\nDebug: n_jets bin contents")
            logger.info(f"{'Bin':<5} {'Content':<10} {'Error':<10}")
            for i in range(1, stack_total.GetNbinsX()+1):
                logger.info(f"{i:<5} {stack_total.GetBinContent(i):<10.2f} {stack_total.GetBinError(i):<10.2f}")
            logger.info(f"Overflow: {stack_total.GetBinContent(stack_total.GetNbinsX()+1):.2f}")
        if not stack_total or stack_total.Integral() <= 0:
             logger.error(f"Total MC histogram for '{variable}' is empty or invalid. Cannot create ratio plot.")
             return
        # --- End Create Stack ---


        # --- Create Canvas and Pads ---
        canvas = ROOT.TCanvas(f"canvas_{variable}", f"{var_label} Plot", 800, 800)
        canvas.SetRightMargin(0.04) # Adjust margins
        canvas.SetLeftMargin(0.15)
        canvas.SetTopMargin(0.08)

        # Upper pad for main plot
        pad1 = ROOT.TPad(f"pad1_{variable}", "pad1", 0, 0.3, 1, 1)
        pad1.SetBottomMargin(0.02) # Make pads closer
        pad1.SetLeftMargin(0.15)
        pad1.SetRightMargin(0.04)
        pad1.SetTopMargin(0.08)
        pad1.Draw()
        pad1.cd()
        pad1.SetLogy()
        
        # --- End Create Canvas and Pads ---


        # --- Draw Main Plot ---
        # Get min/max values from config (now required)
        var_config = VARIABLE_CONFIG[variable]
        max_val = var_config["max_val"]
        min_val = var_config["min_val"]

        stack.SetMaximum(max_val)
        stack.SetMinimum(min_val)
        # Set maximum for data hist as well for consistency if needed, though stack usually dominates
        hist_data.SetMaximum(max_val)
        hist_data.SetMinimum(min_val)

        stack.Draw("HIST") # Draw filled background stack

        hist_data.Draw("E1 SAME") # Draw data with error bars ('E1' style)

        # Explicitly set y-axis range for pad1
        stack.GetYaxis().SetRangeUser(min_val, max_val)
        hist_data.GetYaxis().SetRangeUser(min_val, max_val)

        # Set axis titles on the stack (since it's drawn first)
        stack.GetYaxis().SetTitle("Events per bin")
        stack.GetYaxis().SetTitleOffset(1.2)
        stack.GetYaxis().SetTitleSize(0.05)
        stack.GetYaxis().SetLabelSize(0.04)
        stack.GetXaxis().SetLabelSize(0) # Hide X labels on top pad
        stack.GetXaxis().SetTitleSize(0) # Hide X title on top pad


        # Redraw axes to be on top
        pad1.RedrawAxis()
        # --- End Draw Main Plot ---


        # --- Add Legend ---
        # Get legend style from config (default to 'compact' if not specified)
        legend_style = VARIABLE_CONFIG[variable].get("legendStyle", "compact")
        

        
        # Apply style-specific settings
        if legend_style == "vertical":
            legend = ROOT.TLegend(0.68, 0.60, 0.95, 0.90) # Adjusted position/size
            legend.SetBorderSize(0)
            legend.SetFillStyle(0) # Transparent background
            legend.SetTextSize(0.04)
        elif legend_style == "wide":
            legend = ROOT.TLegend(0.55, 0.70, 0.95, 0.90) # Adjusted position/size
            legend.SetBorderSize(0)
            legend.SetFillStyle(0) # Transparent background
            legend.SetTextSize(0.03)
            legend.SetNColumns(2)
            legend.SetColumnSeparation(0.02)
            legend.SetEntrySeparation(0.005)
        else: # compact (default)
            legend = ROOT.TLegend(0.65, 0.60, 0.95, 0.90) # Adjusted position/size
            legend.SetBorderSize(0)
            legend.SetFillStyle(0) # Transparent background
            legend.SetTextSize(0.035)
            legend.SetNColumns(2)
            legend.SetColumnSeparation(0.05)
            legend.SetEntrySeparation(0.01)
        legend.AddEntry(hist_data, "Data", "lep")
        # Add background entries in specified stack order for legend clarity
        for cat in reversed(stack_order_available):
            # Use category name directly as label
            legend.AddEntry(background_histos[cat], cat, "f")
        legend.Draw()
        # --- End Add Legend ---


        # --- Add Text (CMS, Lumi) ---
        cms_text_bold = ROOT.TLatex()
        cms_text_bold.SetNDC()
        cms_text_bold.SetTextFont(61) # Bold
        cms_text_bold.SetTextSize(0.05)
        cms_text_bold.DrawLatex(0.18, 0.85, "CMS") # Position adjusted

        cms_text_prelim = ROOT.TLatex()
        cms_text_prelim.SetNDC()
        cms_text_prelim.SetTextFont(52) # Italic
        cms_text_prelim.SetTextSize(0.04)
        # cms_text_prelim.DrawLatex(0.18 + cms_text_bold.GetXsize()*3.1, 0.85, "Preliminary") # Position adjusted
        cms_text_prelim.DrawLatex(0.26, 0.85, "Preliminary") 
        # Add channel label (μ + jets)
        channel_text = ROOT.TLatex()
        channel_text.SetNDC()
        channel_text.SetTextFont(52) # Italic
        channel_text.SetTextSize(0.04)
        channel_text.DrawLatex(0.20, 0.80, "#mu + jets") # Positioned below CMS Preliminary



        lumi_text = ROOT.TLatex()
        lumi_text.SetNDC()
        lumi_text.SetTextFont(42) # Regular
        lumi_text.SetTextSize(0.04)
        # Display lumi in fb^-1
        lumi_text.DrawLatex(0.60, 0.93, f"{luminosity_fb:.1f} fb^{{-1}} (13 TeV, {era})") # Position adjusted

        # --- End Add Text ---


        # --- Create Ratio Plot ---
        canvas.cd() # Go back to canvas before creating second pad
        pad2 = ROOT.TPad(f"pad2_{variable}", "pad2", 0, 0.05, 1, 0.3)
        pad2.SetTopMargin(0.02) # Make pads closer
        pad2.SetBottomMargin(0.35)
        pad2.SetLeftMargin(0.15)
        pad2.SetRightMargin(0.04)
        pad2.SetGridy()
        pad2.Draw()
        pad2.cd()

        ratio = hist_data.Clone(f"ratio_{variable}")
        ratio.Divide(stack_total)
        ratio.SetTitle("") # Remove title from ratio plot itself
        ratio.SetMarkerStyle(20)
        ratio.SetMarkerSize(1.0)
        ratio.SetLineWidth(2)
        ratio.SetStats(False)

        # Adjust ratio plot axes
        ratio.GetYaxis().SetTitle("Data / MC")
        ratio.GetYaxis().SetRangeUser(0.5, 1.5)
        ratio.GetYaxis().SetNdivisions(505)
        ratio.GetYaxis().CenterTitle()
        ratio.GetYaxis().SetTitleSize(0.12)
        ratio.GetYaxis().SetTitleOffset(0.5) # Adjusted offset
        ratio.GetYaxis().SetLabelSize(0.1)

        ratio.GetXaxis().SetTitle(var_label) # Set X title only on bottom plot
        ratio.GetXaxis().SetTitleSize(0.14)
        ratio.GetXaxis().SetTitleOffset(1.1) # Adjusted offset
        ratio.GetXaxis().SetLabelSize(0.12)

        ratio.Draw("E1") # Draw ratio with error bars

        # Add MC uncertainty band
        uncertainty_band = ROOT.TGraphAsymmErrors(stack_total)
        logger.info(f"\n{'='*80}")
        logger.info(f"Uncertainty band ranges for {variable}:")
        logger.info(f"{'Bin':<5} {'Bin Center':<12} {'MC Events':<12} {'Stat Unc %':<12} {'Lumi Unc %':<12} {'Total Unc %':<12} {'Band Range':<20}")
        logger.info(f"{'-'*80}")
        
        for i in range(1, stack_total.GetNbinsX() + 1):
            x = stack_total.GetBinCenter(i)
            y = 1.0 # Ratio is 1
            mc_val = stack_total.GetBinContent(i)
            mc_err = stack_total.GetBinError(i)
            # Combine statistical and luminosity uncertainties in quadrature
            stat_rel_err = mc_err / mc_val if mc_val > 0 else 0.0
            rel_err = math.sqrt(stat_rel_err**2 + lumi_uncertainty**2) if mc_val > 0 else 0.0
            
            # Calculate band range
            lower_edge = 1.0 - rel_err
            upper_edge = 1.0 + rel_err
            
            # Log the uncertainty information
            logger.info(f"{i:<5} {x:<12.2f} {mc_val:<12.1f} {stat_rel_err*100:<12.2f} {lumi_uncertainty*100:<12.2f} {rel_err*100:<12.2f} [{lower_edge:.4f}, {upper_edge:.4f}]")
            
            uncertainty_band.SetPoint(i - 1, x, y)
            # X errors are half bin width, Y errors are relative combined error
            uncertainty_band.SetPointError(i - 1, stack_total.GetBinWidth(i)/2., stack_total.GetBinWidth(i)/2., rel_err, rel_err)
        
        logger.info(f"{'='*80}\n")

        uncertainty_band.SetFillColorAlpha(ROOT.kGray + 1, 0.4) # Lighter grey, more transparent
        uncertainty_band.SetFillStyle(1001) # Solid fill
        uncertainty_band.SetMarkerSize(0)
        uncertainty_band.Draw("E2 SAME") # Draw band behind points

        # Redraw ratio points on top
        ratio.Draw("E1 SAME")

        # Add reference line at 1.0
        line = ROOT.TLine(ratio.GetXaxis().GetXmin(), 1.0, ratio.GetXaxis().GetXmax(), 1.0)
        line.SetLineColor(2) # Red
        line.SetLineStyle(2) # Dashed
        line.SetLineWidth(1)
        line.Draw("SAME")
        # --- End Create Ratio Plot ---


        # --- Save Plot ---
        # Use input file basename + variable name for output filename
        input_basename = os.path.splitext(os.path.basename(args.input_file))[0]
        output_filename = f"{input_basename}_{variable}.png"
        output_path = os.path.join(output_dir, output_filename)
        canvas.SaveAs(output_path)
        logger.info(f"Saved plot to: {output_path}")
        # --- End Save Plot ---

    except Exception as e:
        logger.error(f"Error creating plots for variable '{variable}': {str(e)}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging

def main():
    args = parse_arguments()
    logging.getLogger().setLevel(args.log_level)
    
    logger.info(f"Input file: {args.input_file}")
    
    try:
        # Extract era from input filename (e.g., midNov_UL2017_reco_sample.coffea -> UL2017)
        import os
        input_basename = os.path.basename(args.input_file)
        parts = input_basename.split('_')
        tag = parts[0]  # Extract tag (e.g., 'midNov')
        era = parts[1]  # Extract era (e.g., 'UL2017')
        # Auto-detect sample info file if not provided
        if args.sample_info is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            # Use tag extracted from filename
            args.sample_info = os.path.join(script_dir, '..', 'configs', f'{tag}_{era}_reco_lumiXinfo.json')
            logger.info(f"Auto-detected sample info file: {args.sample_info}")
        
        # Load input file
        out_merged = load(args.input_file)
        logger.info("Successfully loaded input file")
        
        # Load sample info from JSON (era-specific config)
        sample_info = load_sample_info(args.sample_info)
        
        # Extract values (now they're single values, not dicts)
        luminosity = sample_info['Luminosity']
        lumi_uncertainty = sample_info['luminosity_uncertainty']
        era_label = sample_info['era']
        logger.info(f"Luminosity: {luminosity:.1f} pb^-1 ({luminosity/1000:.2f} fb^-1)")
        logger.info(f"Luminosity uncertainty: {lumi_uncertainty*100:.2f}%")

        # Get available variables from first dataset's histograms
        first_dataset = next(iter(out_merged.values()))
        if isinstance(first_dataset, dict):
            available_vars = set(first_dataset.get('histos', {}).keys())
        else:
            # Handle case where dataset is accessed differently
            available_vars = set()
            for dataset in out_merged.values():
                if hasattr(dataset, 'histos'):
                    available_vars.update(dataset.histos.keys())
                elif isinstance(dataset, dict) and 'histos' in dataset:
                    available_vars.update(dataset['histos'].keys())
        config_vars = set(VARIABLE_CONFIG.keys())
        
        # Find intersection of available and configured variables
        vars_to_process = available_vars & config_vars
        logger.info(f"Found {len(vars_to_process)} variables to process: {', '.join(vars_to_process)}")

        for variable in vars_to_process:
            logger.info(f"Processing variable: {variable}")
            
            # Process histograms
            merged_histos = process_histograms(
                out_merged, variable, luminosity, sample_info
            )
            
            # Create ROOT histograms
            root_histos = create_root_histograms(merged_histos, variable)

            # Check if any ROOT histograms were actually created
            if not root_histos:
                logger.error(f"No ROOT histograms were created for {variable}. Skipping.")
                continue

            # Save plots
            save_plots(root_histos, variable, args.output_dir, luminosity, args, era_label, lumi_uncertainty)
        
        logger.info(f"Processing completed for {len(vars_to_process)} variables")
        
    except Exception as e:
        logger.error(f"Error in processing: {str(e)}")
        raise

if __name__ == "__main__":
    main()