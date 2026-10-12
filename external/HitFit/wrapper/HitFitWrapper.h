// Flat-number interface to the vendored HitFit, for use from PyROOT.
//
// HitFit's own classes (Lepjets_Event, Fourvec, Column_Vector, ...) are CLHEP
// based and crash cppyy when bound directly (incomplete class info on
// by-value returns), so 004A talks to HitFit only through this class: plain
// doubles in, a plain struct out.
//
// One call fits ONE jet-parton assignment with ONE neutrino-pz starting value.
// The caller loops over the permutations (2 b-assignments x 2 pz seeds in the
// TOP-23-003 lepton+jets prescription) and ranks them.
//
// Object handling follows CMSSW's PAT translators
// (TopQuarkAnalysis/TopHitFit/src/Pat*HitFitTranslator.cc): each object's
// Vector_Resolution is looked up by its eta in an EtaDepResolution file (b and
// udsc jets have separate files), MET enters as (px, py, 0, |pT|), and the kt
// (recoil) resolution comes from the "met_resolution" entry of the kt file.
#ifndef HITFIT_WRAPPER_H
#define HITFIT_WRAPPER_H

#include <memory>
#include <string>

namespace hitfit_wrap {

  struct FitResult {
    // 0 = converged; 1 = no converged fit (HitFit returned a negative chi2,
    // which also covers its optional pre-fit mass cuts); 2 = an object's eta
    // is outside its resolution table.
    int status = 1;
    double chi2 = -1;           // HitFit chi2 (ndf = 2 for lepton+jets)
    double mt = 0, sigmt = 0;   // fitted top mass and its uncertainty
    double nu_pz = 0;           // fitted neutrino pz
    double lep_top[4] = {0, 0, 0, 0};   // fitted (px, py, pz, E)
    double had_top[4] = {0, 0, 0, 0};
    double nu[4] = {0, 0, 0, 0};
  };

  class Fitter {
  public:
    // settings_file: HitFit Defaults_Text file (Top_Fit, Constrained_Top,
    //   Fourvec_Constrainer and Chisq_Constrainer parameters).
    // *_res_file: EtaDepResolution files; kt_res_file holds "met_resolution".
    // mw: W mass for both W constraints. The top mass is never fixed
    //   (top_mass = 0); equal top masses come from equal_side in settings.
    Fitter(const std::string& settings_file,
           const std::string& udsc_res_file,
           const std::string& b_res_file,
           const std::string& muon_res_file,
           const std::string& kt_res_file,
           double mw);
    ~Fitter();

    // Jets in assignment order: [0] hadronic-W quark 1, [1] hadronic-W
    // quark 2, [2] hadronic b, [3] leptonic b. nuz selects the pz root.
    FitResult fit(const double* jet_pt, const double* jet_eta,
                  const double* jet_phi, const double* jet_mass,
                  double mu_pt, double mu_eta, double mu_phi, double mu_mass,
                  double met_px, double met_py, bool nuz);

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
  };

}  // namespace hitfit_wrap

#endif
