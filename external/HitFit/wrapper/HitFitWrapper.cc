#include "HitFitWrapper.h"

#include <cmath>
#include <stdexcept>

#include "TopQuarkAnalysis/TopHitFit/interface/Defaults_Text.h"
#include "TopQuarkAnalysis/TopHitFit/interface/EtaDepResolution.h"
#include "TopQuarkAnalysis/TopHitFit/interface/Lepjets_Event.h"
#include "TopQuarkAnalysis/TopHitFit/interface/Resolution.h"
#include "TopQuarkAnalysis/TopHitFit/interface/Top_Decaykin.h"
#include "TopQuarkAnalysis/TopHitFit/interface/Top_Fit.h"
#include "TopQuarkAnalysis/TopHitFit/interface/matutil.h"

using namespace hitfit;

namespace hitfit_wrap {

  struct Fitter::Impl {
    Defaults_Text settings;
    Top_Fit fitter;
    EtaDepResolution udsc_res, b_res, muon_res;
    Resolution kt_res;

    Impl(const std::string& settings_file,
         const std::string& udsc_res_file,
         const std::string& b_res_file,
         const std::string& muon_res_file,
         const std::string& kt_res_file,
         double mw)
        : settings(settings_file),
          fitter(Top_Fit_Args(settings), mw, mw, 0.0),
          udsc_res(udsc_res_file),
          b_res(b_res_file),
          muon_res(muon_res_file),
          kt_res(Defaults_Text(kt_res_file).get_string("met_resolution")) {}
  };

  Fitter::Fitter(const std::string& settings_file,
                 const std::string& udsc_res_file,
                 const std::string& b_res_file,
                 const std::string& muon_res_file,
                 const std::string& kt_res_file,
                 double mw)
      : impl_(new Impl(settings_file, udsc_res_file, b_res_file, muon_res_file, kt_res_file, mw)) {}

  Fitter::~Fitter() = default;

  namespace {
    Fourvec from_pt_eta_phi_m(double pt, double eta, double phi, double m) {
      const double px = pt * std::cos(phi), py = pt * std::sin(phi), pz = pt * std::sinh(eta);
      return Fourvec(px, py, pz, std::sqrt(px * px + py * py + pz * pz + m * m));
    }

    void copy4(const Fourvec& v, double* out) {
      out[0] = v.px();
      out[1] = v.py();
      out[2] = v.pz();
      out[3] = v.e();
    }
  }  // namespace

  FitResult Fitter::fit(const double* jet_pt, const double* jet_eta,
                        const double* jet_phi, const double* jet_mass,
                        double mu_pt, double mu_eta, double mu_phi, double mu_mass,
                        double met_px, double met_py, bool nuz) {
    static const int kTypes[4] = {hadw1_label, hadw2_label, hadb_label, lepb_label};
    FitResult r;
    Lepjets_Event ev(0, 0);

    try {
      double eta = mu_eta;
      ev.add_lep(Lepjets_Event_Lep(from_pt_eta_phi_m(mu_pt, mu_eta, mu_phi, mu_mass), muon_label,
                                   impl_->muon_res.GetResolution(eta)));
      for (int i = 0; i < 4; ++i) {
        eta = jet_eta[i];
        const bool is_b = (kTypes[i] == hadb_label || kTypes[i] == lepb_label);
        const Vector_Resolution res = is_b ? impl_->b_res.GetResolution(eta) : impl_->udsc_res.GetResolution(eta);
        ev.add_jet(Lepjets_Event_Jet(from_pt_eta_phi_m(jet_pt[i], jet_eta[i], jet_phi[i], jet_mass[i]),
                                     kTypes[i], res));
      }
    } catch (const std::runtime_error&) {
      r.status = 2;
      return r;
    }
    ev.met() = Fourvec(met_px, met_py, 0.0, std::hypot(met_px, met_py));
    ev.kt_res() = impl_->kt_res;

    bool nuz_flag = nuz;
    double umwhad = 0, utmass = 0, mt = 0, sigmt = 0;
    Column_Vector pullx, pully;
    const double chi2 = impl_->fitter.fit_one_perm(ev, nuz_flag, umwhad, utmass, mt, sigmt, pullx, pully);

    if (!(chi2 >= 0) || !std::isfinite(chi2)) {
      r.status = 1;
      return r;
    }

    r.status = 0;
    r.chi2 = chi2;
    r.mt = mt;
    r.sigmt = sigmt;
    r.nu_pz = ev.met().z();
    copy4(Top_Decaykin::lept(ev), r.lep_top);
    copy4(Top_Decaykin::hadt(ev), r.had_top);
    copy4(ev.met(), r.nu);
    return r;
  }

}  // namespace hitfit_wrap
