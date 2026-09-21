"""Gen- and reco-level kinematics for 005-Unfolding.

Pure numpy, like binning.py, so it imports both here and in a bare lxplus
python where gen_acceptance.py runs. Everything in this chapter that turns
four-momenta into rapidities, invariant masses or a top/antitop assignment
goes through this module -- see binning.py's docstring for why duplication is
not tolerated in this chapter.
"""

import numpy as np

# Muon selection thresholds are era-dependent (002-Samples/config.yaml
# Pre-SelectionCuts). Kept here because select_muon_index() needs them and
# because hard-coding a single value is what issue #33 is about.
MUON_PT_THRESHOLD = {
    "UL2016preVFP": 26.0,
    "UL2016postVFP": 26.0,
    "UL2017": 29.0,
    "UL2018": 27.0,
}
MUON_ETA_MAX = 2.4
MUON_ISO_MAX = 0.2   # 003-ObjectSelectionI uses <=, not <


def rapidity(pt, eta, mass):
    """y = 0.5 ln((E+pz)/(E-pz)).

    Returns nan, deliberately, when E -> |pz| or the inputs are unphysical,
    rather than the silent +-inf the naive expression produces (issue #42).
    A massive top never reaches E == |pz|, so a nan here means the inputs were
    garbage -- most importantly RecoModule._fill_failure()'s -1 sentinels,
    which the naive formula turns into an innocuous-looking y = 0.757 and so
    misclassifies a failed fit as a central top.
    """
    pt = np.asarray(pt, dtype=np.float64)
    eta = np.asarray(eta, dtype=np.float64)
    mass = np.asarray(mass, dtype=np.float64)

    with np.errstate(invalid="ignore"):
        pz = pt * np.sinh(eta)
        e = np.sqrt((pt * np.cosh(eta)) ** 2 + mass ** 2)
        num, den = e + pz, e - pz
        y = 0.5 * np.log(num / den)

    valid = (pt >= 0) & (mass >= 0) & np.isfinite(num) & np.isfinite(den) & (num > 0) & (den > 0)
    return np.where(valid, y, np.nan)


def invariant_mass(p1, p2):
    """Invariant mass of two (pt, eta, phi, mass) tuples."""
    def components(p):
        pt, eta, phi, mass = (np.asarray(x, dtype=np.float64) for x in p)
        return (pt * np.cos(phi), pt * np.sin(phi), pt * np.sinh(eta),
                np.sqrt((pt * np.cosh(eta)) ** 2 + mass ** 2))

    px1, py1, pz1, e1 = components(p1)
    px2, py2, pz2, e2 = components(p2)
    m2 = (e1 + e2) ** 2 - (px1 + px2) ** 2 - (py1 + py2) ** 2 - (pz1 + pz2) ** 2
    return np.sqrt(np.maximum(m2, 0.0))


def assign_top_antitop(y_lep, y_had, muon_charge):
    """Assign the leptonic and hadronic tops to t and tbar from the muon charge.

    THE CONVENTION, which this chapter previously had inverted (issue #29):

        The top quark has charge +2/3 and decays  t -> W+ b -> mu+ nu b.
        So a  mu+  means the LEPTONIC side is the TOP.
        A     mu-  comes from tbar -> W- bbar -> mu- nubar bbar,
                   so the leptonic side is the ANTITOP.

    Getting this backwards swaps y_t and y_tbar in every event, which makes the
    response matrix anti-diagonal and flips the sign of A_C.

    Verified on ttbar_SemiLeptonic UL2016preVFP (1.25M events) two independent
    ways -- dR matching the reconstructed Top_lep to the gen t/tbar last copy
    gives mu+ -> gen top 85.1% of the time (the ~15% minority being tau->mu and
    b->mu cascades), and walking GenPart_genPartIdxMother from the hard-process
    gen muon up to a |pdgId|==6 gives top -> mu+ 590,456 times against
    antitop -> mu- 590,549 times, with no cross terms.

    Events with muon_charge == 0 (no muon selected) yield nan for both, so they
    stay countable rather than silently landing in a rapidity bin.
    """
    y_lep = np.asarray(y_lep, dtype=np.float64)
    y_had = np.asarray(y_had, dtype=np.float64)
    muon_charge = np.asarray(muon_charge)

    mu_plus = muon_charge > 0
    mu_minus = muon_charge < 0
    assigned = mu_plus | mu_minus

    y_top = np.where(mu_plus, y_lep, y_had)
    y_antitop = np.where(mu_plus, y_had, y_lep)
    return (np.where(assigned, y_top, np.nan),
            np.where(assigned, y_antitop, np.nan))


def select_muon_index(muon_pt, muon_eta, muon_tight_id, muon_iso, era):
    """Index of the analysis-selected muon: highest-pT survivor of the 003-I cuts.

    The BDTScore trees no longer carry the SelMuon_* scalar branches, so the
    selection has to be reapplied rather than read off. Taking ak.firsts of the
    raw Muon collection instead picks the wrong muon in ~1.4% of events
    (issue #33); 45% of events have more than one muon.

    Returns an awkward index array suitable for `Muon_charge[idx]`, and a mask
    of events with at least one selected muon.
    """
    import awkward as ak

    if era not in MUON_PT_THRESHOLD:
        raise ValueError(f"unknown era {era!r}; expected one of {sorted(MUON_PT_THRESHOLD)}")

    passes = (
        (muon_pt > MUON_PT_THRESHOLD[era])
        & (abs(muon_eta) < MUON_ETA_MAX)
        & muon_tight_id
        & (muon_iso <= MUON_ISO_MAX)
    )
    index = ak.argmax(ak.where(passes, muon_pt, -999.0), axis=1, keepdims=True)
    return index, ak.to_numpy(ak.sum(passes, axis=1)) > 0
