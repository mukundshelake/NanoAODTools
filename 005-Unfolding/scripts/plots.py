"""Plotting for 005-Unfolding.

Kept separate from unfold.py so that the unfolding logic is readable without
200 lines of ROOT cosmetics interleaved, and so validate.py can reuse the same
figures. Every function takes already-computed histograms and writes both .pdf
and .png next to each other.
"""

import os

import ROOT

ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(0)
ROOT.gStyle.SetOptFit(0)


def _save(canvas, outdir, name):
    os.makedirs(outdir, exist_ok=True)
    for ext in ("pdf", "png"):
        canvas.SaveAs(os.path.join(outdir, f"{name}.{ext}"))
    return os.path.join(outdir, name)


def truth_vs_unfolded(h_truth, h_unfolded, outdir, era, tau, n_mtt_bins,
                      name="truth_vs_unfolded", subtitle="Closure test"):
    """Unrolled truth (filled) against the unfolded points, split at the N+/N- boundary."""
    canvas = ROOT.TCanvas("c_comp", "Truth vs Unfolded", 900, 700)
    pad = ROOT.TPad("pad1", "", 0.0, 0.0, 1.0, 1.0)
    pad.SetLeftMargin(0.14)
    pad.SetRightMargin(0.05)
    pad.SetTopMargin(0.12)
    pad.SetBottomMargin(0.25)
    pad.Draw()
    pad.cd()

    truth = h_truth.Clone("h_truth_draw")
    unfolded = h_unfolded.Clone("h_unfolded_draw")
    ymax = max(truth.GetMaximum(), unfolded.GetMaximum()) * 1.35

    truth.SetLineColor(ROOT.kBlue + 1)
    truth.SetLineWidth(2)
    truth.SetFillColorAlpha(ROOT.kBlue + 1, 0.20)
    truth.GetYaxis().SetTitle("Events")
    truth.GetYaxis().SetTitleSize(0.055)
    truth.GetYaxis().SetTitleOffset(1.1)
    truth.GetYaxis().SetLabelSize(0.045)
    truth.GetXaxis().SetLabelSize(0.045)
    truth.GetXaxis().LabelsOption("v")
    truth.GetXaxis().SetLabelOffset(0.01)
    truth.SetMaximum(ymax)
    truth.SetMinimum(0)
    truth.Draw("HIST")

    unfolded.SetMarkerStyle(20)
    unfolded.SetMarkerSize(1.3)
    unfolded.SetMarkerColor(ROOT.kBlack)
    unfolded.SetLineColor(ROOT.kBlack)
    unfolded.SetLineWidth(2)
    unfolded.Draw("E1 SAME")

    # The N+/N- boundary: regularisation must never smooth across it (issue #35).
    divider = ROOT.TLine(n_mtt_bins, 0, n_mtt_bins, ymax)
    divider.SetLineColor(ROOT.kRed)
    divider.SetLineStyle(2)
    divider.SetLineWidth(2)
    divider.Draw()

    legend = ROOT.TLegend(0.62, 0.68, 0.92, 0.86)
    legend.SetBorderSize(0)
    legend.SetTextSize(0.045)
    legend.AddEntry(truth, "Gen-level truth", "lf")
    legend.AddEntry(unfolded, "Unfolded", "lep")
    legend.Draw()

    latex = ROOT.TLatex()
    latex.SetNDC(True)
    latex.SetTextSize(0.05)
    latex.SetTextFont(62)
    latex.DrawLatex(0.16, 0.91, f"{subtitle}  {era}")
    latex.SetTextFont(42)
    latex.SetTextSize(0.04)
    latex.DrawLatex(0.16, 0.84, f"#tau = {tau:.2e}")

    canvas.cd()
    _save(canvas, outdir, name)
    return canvas


def lcurve(graph, best_index, tau, outdir, name="lcurve"):
    import ctypes

    canvas = ROOT.TCanvas("c_lcurve", "L-curve", 700, 600)
    canvas.SetLeftMargin(0.14)
    canvas.SetRightMargin(0.05)
    canvas.SetTopMargin(0.10)
    canvas.SetBottomMargin(0.14)

    graph.SetTitle("L-curve;log_{10}(#chi^{2}_{L});log_{10}(Regularisation)")
    graph.SetMarkerStyle(20)
    graph.SetMarkerSize(0.5)
    graph.SetMarkerColor(ROOT.kBlue + 1)
    graph.SetLineColor(ROOT.kBlue + 1)
    graph.Draw("AL")

    x = ctypes.c_double(0.0)
    y = ctypes.c_double(0.0)
    graph.GetPoint(best_index, x, y)
    marker = ROOT.TGraph(1)
    marker.SetPoint(0, x.value, y.value)
    marker.SetMarkerStyle(29)
    marker.SetMarkerSize(2.5)
    marker.SetMarkerColor(ROOT.kRed)
    marker.Draw("P SAME")

    latex = ROOT.TLatex()
    latex.SetNDC(True)
    latex.SetTextSize(0.04)
    latex.DrawLatex(0.16, 0.92, f"Optimal #tau = {tau:.2e}")

    _save(canvas, outdir, name)
    return canvas, marker


def response_matrix(h_matrix, outdir, era, name="response_matrix"):
    canvas = ROOT.TCanvas("c_matrix", "Response matrix", 900, 800)
    canvas.SetLeftMargin(0.18)
    canvas.SetRightMargin(0.15)
    canvas.SetBottomMargin(0.22)
    canvas.SetTopMargin(0.08)

    matrix = h_matrix.Clone("h_matrix_draw")
    matrix.GetXaxis().LabelsOption("v")
    matrix.GetXaxis().SetLabelSize(0.03)
    matrix.GetYaxis().SetLabelSize(0.03)
    matrix.Draw("COLZ")

    latex = ROOT.TLatex()
    latex.SetNDC(True)
    latex.SetTextSize(0.035)
    latex.SetTextFont(62)
    latex.DrawLatex(0.18, 0.94, f"Response matrix  {era}")

    _save(canvas, outdir, name)
    return canvas


def correlation_matrix(h_cov, outdir, era, name="correlation_matrix"):
    """Unfolded correlation matrix -- the unfolded bins are strongly correlated,
    which is why A_C errors must be propagated through the covariance (issue #39)."""
    n = h_cov.GetNbinsX()
    corr = ROOT.TH2D(name, "Unfolded correlation;Gen bin;Gen bin", n, 0, n, n, 0, n)
    for i in range(1, n + 1):
        for j in range(1, n + 1):
            di = h_cov.GetBinContent(i, i) ** 0.5
            dj = h_cov.GetBinContent(j, j) ** 0.5
            corr.SetBinContent(i, j, h_cov.GetBinContent(i, j) / (di * dj)
                               if di > 0 and dj > 0 else 0.0)
        corr.GetXaxis().SetBinLabel(i, h_cov.GetXaxis().GetBinLabel(i))
        corr.GetYaxis().SetBinLabel(i, h_cov.GetXaxis().GetBinLabel(i))

    canvas = ROOT.TCanvas("c_corr", "Correlation", 900, 800)
    canvas.SetLeftMargin(0.18)
    canvas.SetRightMargin(0.15)
    canvas.SetBottomMargin(0.22)
    corr.SetMinimum(-1.0)
    corr.SetMaximum(1.0)
    corr.GetXaxis().LabelsOption("v")
    corr.GetXaxis().SetLabelSize(0.03)
    corr.GetYaxis().SetLabelSize(0.03)
    corr.Draw("COLZ")

    latex = ROOT.TLatex()
    latex.SetNDC(True)
    latex.SetTextSize(0.035)
    latex.SetTextFont(62)
    latex.DrawLatex(0.18, 0.94, f"Unfolded correlation  {era}")

    _save(canvas, outdir, name)
    return canvas, corr


def asymmetry_vs_mtt(values, stat, total, edges, outdir, era,
                     name="asymmetry_vs_mtt"):
    """A_C per m_tt bin with stat and stat+syst bands (issue #39).

    `values`, `stat` and `total` carry n_mtt + 1 entries, the last being the
    inclusive asymmetry, which is drawn separately as a band across the full
    range rather than as another m_tt point -- it is a different quantity, not
    an extra bin.
    """
    import array

    n = len(edges) - 1
    edge_array = array.array("d", [float(e) for e in edges])

    h_total = ROOT.TH1D(name + "_total", "", n, edge_array)
    h_stat = ROOT.TH1D(name + "_stat", "", n, edge_array)
    for i in range(n):
        for hist, err in ((h_total, total), (h_stat, stat)):
            hist.SetBinContent(i + 1, values[i])
            hist.SetBinError(i + 1, err[i])

    canvas = ROOT.TCanvas("c_ac", "A_C vs m_tt", 900, 700)
    canvas.SetLeftMargin(0.15)
    canvas.SetRightMargin(0.05)
    canvas.SetTopMargin(0.10)
    canvas.SetBottomMargin(0.13)

    span = max(abs(values[i]) + total[i] for i in range(n)) * 1.6 or 0.01
    h_total.SetTitle(";m_{t#bar{t}} [GeV];A_{C}")
    h_total.GetYaxis().SetTitleSize(0.05)
    h_total.GetYaxis().SetTitleOffset(1.4)
    h_total.GetXaxis().SetTitleSize(0.05)
    h_total.SetMinimum(-span)
    h_total.SetMaximum(span)
    h_total.SetFillColorAlpha(ROOT.kAzure - 9, 0.7)
    h_total.SetLineColor(ROOT.kAzure + 2)
    h_total.SetMarkerStyle(0)
    h_total.Draw("E2")

    h_stat.SetFillColorAlpha(ROOT.kAzure + 2, 0.55)
    h_stat.SetLineColor(ROOT.kAzure + 2)
    h_stat.SetMarkerStyle(0)
    h_stat.Draw("E2 SAME")

    points = h_total.Clone(name + "_points")
    points.SetFillStyle(0)
    points.SetMarkerStyle(20)
    points.SetMarkerSize(1.2)
    points.SetMarkerColor(ROOT.kBlack)
    points.SetLineColor(ROOT.kBlack)
    points.Draw("P SAME")

    zero = ROOT.TLine(edges[0], 0.0, edges[-1], 0.0)
    zero.SetLineStyle(2)
    zero.SetLineColor(ROOT.kGray + 2)
    zero.Draw()

    # Inclusive value as a horizontal band across the whole range.
    inclusive = ROOT.TBox(edges[0], values[-1] - total[-1],
                          edges[-1], values[-1] + total[-1])
    inclusive.SetFillColorAlpha(ROOT.kOrange + 1, 0.22)
    inclusive.SetLineColor(ROOT.kOrange + 2)
    inclusive.Draw()
    inclusive_line = ROOT.TLine(edges[0], values[-1], edges[-1], values[-1])
    inclusive_line.SetLineColor(ROOT.kOrange + 2)
    inclusive_line.SetLineWidth(2)
    inclusive_line.Draw()

    legend = ROOT.TLegend(0.55, 0.72, 0.93, 0.88)
    legend.SetBorderSize(0)
    legend.SetTextSize(0.035)
    legend.AddEntry(points, "A_{C} unfolded", "lep")
    legend.AddEntry(h_stat, "stat", "f")
    legend.AddEntry(h_total, "stat #oplus syst", "f")
    legend.AddEntry(inclusive_line,
                    f"inclusive = {values[-1]:+.4f} #pm {total[-1]:.4f}", "l")
    legend.Draw()

    latex = ROOT.TLatex()
    latex.SetNDC(True)
    latex.SetTextFont(62)
    latex.SetTextSize(0.045)
    latex.DrawLatex(0.17, 0.92, f"Charge asymmetry  {era}")

    _save(canvas, outdir, name)
    return canvas, (h_total, h_stat, points, inclusive, inclusive_line, legend, zero)
