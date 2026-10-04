#!/usr/bin/env python3
"""
Feature construction for the qqbar-vs-(gg,qg) classifier (Appendix-B).

Reads one selectionII skim with uproot and returns a flat pandas DataFrame, one row
per region-A event. Everything is vectorised (numpy / awkward); no event loop.

Feature design rules
--------------------
* Reco-level only. Truth (GenPart, Generator_*) is written to separate gen_* /
  label columns for labels and diagnostics, never meant as a training input.
* Symmetric under charge conjugation and under z -> -z. No feature uses the muon
  charge or a signed rapidity, so a classifier cannot learn sign(Delta|y|) and
  sculpt the charge asymmetry it will later be used to measure. Rapidities enter
  as |y|; pairs of tops enter as max/min, never as (top, antitop).

Physics handles on the initial state:
  - longitudinal boost / x1,x2 asymmetry (valence quark x sea antiquark)
  - m_tt (qqbar fraction rises with the partonic x)
  - recoil and extra radiation (gluons radiate ~C_A/C_F = 9/4 more)
  - production angle and spin correlation of the tt pair
"""

import awkward as ak
import numpy as np
import pandas as pd
import uproot

MU_MASS = 0.10566

READ_BRANCHES = [
    'run', 'luminosityBlock', 'event', 'ABCD_region',
    'SelMuon_pt', 'SelMuon_eta', 'SelMuon_phi', 'SelMuon_charge',
    'MET_pt', 'MET_phi',
    'Jet_pt', 'Jet_eta', 'Jet_phi', 'Jet_mass', 'Jet_btagDeepFlavB', 'Jet_jetId', 'Jet_puId', 'Jet_qgl',
    'sel_nJet', 'sel_nbjet',
]
MC_BRANCHES = [
    'Generator_id1', 'Generator_id2', 'Generator_x1', 'Generator_x2', 'Generator_weight',
    'GenPart_pdgId', 'GenPart_statusFlags', 'GenPart_pt', 'GenPart_eta', 'GenPart_phi', 'GenPart_mass',
]

# 004A kinematic-fit output; read only when present (004A reconstruction skims as input).
FIT_BRANCHES = ['Top_had_pt', 'Top_had_eta', 'Top_had_phi', 'Top_had_mass',
                'Top_lep_pt', 'Top_lep_eta', 'Top_lep_phi', 'Top_lep_mass', 'Chi2', 'Pgof', 'chi2_status']

# Production type, same convention as Appendix-A's Gen_prodType.
PROD_GG, PROD_QQBAR, PROD_QG, PROD_OTHER = 0, 1, 2, -1


# ---------------------------------------------------------------------------
# small 4-vector helpers on plain numpy arrays (px, py, pz, E as a tuple)
# ---------------------------------------------------------------------------
def p4(pt, eta, phi, m):
    pt, eta, phi, m = (np.asarray(x, dtype=np.float64) for x in (pt, eta, phi, m))
    px, py, pz = pt * np.cos(phi), pt * np.sin(phi), pt * np.sinh(eta)
    return px, py, pz, np.sqrt(px**2 + py**2 + pz**2 + m**2)


def add(*vs):
    return tuple(sum(c) for c in zip(*vs))


def mass(v):
    px, py, pz, e = v
    return np.sqrt(np.maximum(e**2 - px**2 - py**2 - pz**2, 0.0))


def pt(v):
    return np.hypot(v[0], v[1])


def rapidity(v):
    px, py, pz, e = v
    return 0.5 * np.log(np.maximum(e + pz, 1e-9) / np.maximum(e - pz, 1e-9))


def boost(v, frame):
    """Boost v into the rest frame of `frame` (both 4-tuples, broadcastable)."""
    bx, by, bz = (frame[i] / frame[3] for i in range(3))
    b2 = bx**2 + by**2 + bz**2
    b2 = np.minimum(b2, 1 - 1e-12)
    g = 1.0 / np.sqrt(1.0 - b2)
    bp = bx * v[0] + by * v[1] + bz * v[2]
    g2 = np.where(b2 > 0, (g - 1.0) / np.where(b2 > 0, b2, 1.0), 0.0)
    px = v[0] + g2 * bp * bx - g * bx * v[3]
    py = v[1] + g2 * bp * by - g * by * v[3]
    pz = v[2] + g2 * bp * bz - g * bz * v[3]
    e = g * (v[3] - bp)
    return px, py, pz, e


def unit3(v):
    n = np.sqrt(v[0]**2 + v[1]**2 + v[2]**2)
    n = np.where(n > 0, n, 1.0)
    return v[0] / n, v[1] / n, v[2] / n


def dot3(a, b):
    return a[0] * b[0] + a[1] * b[1] + a[2] * b[2]


def take(arr2d, idx):
    """arr2d[i, idx[i]] for each row i."""
    return np.take_along_axis(arr2d, idx[:, None], axis=1)[:, 0]


# ---------------------------------------------------------------------------
def neutrino_pz(mu, met_px, met_py, mW, return_other=False):
    """W-mass-constrained neutrino pz; smaller |pz| root, real part if complex."""
    mpx, mpy, mpz, me = mu
    mu_pt2 = mpx**2 + mpy**2
    a = 0.5 * mW**2 + mpx * met_px + mpy * met_py
    disc = a**2 * mpz**2 / mu_pt2**2 - (me**2 * (met_px**2 + met_py**2) - a**2) / mu_pt2
    base = a * mpz / mu_pt2
    root = np.sqrt(np.maximum(disc, 0.0))
    s1, s2 = base + root, base - root
    pz = np.where(np.abs(s1) < np.abs(s2), s1, s2)
    pz_other = np.where(np.abs(s1) < np.abs(s2), s2, s1)
    complex_sol = disc < 0
    if return_other:
        return np.where(complex_sol, base, pz), np.where(complex_sol, base, pz_other), complex_sol
    return np.where(complex_sol, base, pz), complex_sol


def event_shapes(vecs3):
    """Sphericity, aplanarity from the quadratic momentum tensor; vecs3 = (N,K,3) with NaN padding."""
    p = np.nan_to_num(vecs3)
    t = np.einsum('nki,nkj->nij', p, p)
    norm = np.einsum('nki,nki->n', p, p)
    t = t / np.where(norm > 0, norm, 1.0)[:, None, None]
    lam = np.linalg.eigvalsh(t)[:, ::-1]  # descending
    return 1.5 * (lam[:, 1] + lam[:, 2]), 1.5 * lam[:, 2]


# ---------------------------------------------------------------------------
def gen_columns(a):
    """Labels and gen-level diagnostics (never training inputs)."""
    id1, id2 = ak.to_numpy(a['Generator_id1']), ak.to_numpy(a['Generator_id2'])
    g1, g2 = id1 == 21, id2 == 21
    q1, q2 = (np.abs(id1) <= 6) & (id1 != 0), (np.abs(id2) <= 6) & (id2 != 0)
    prod = np.full(len(id1), PROD_OTHER, dtype=np.int8)
    prod[g1 & g2] = PROD_GG
    prod[(g1 & q2) | (g2 & q1)] = PROD_QG
    prod[q1 & q2 & (id1 == -id2)] = PROD_QQBAR

    out = {'prodType': prod, 'y': (prod == PROD_QQBAR).astype(np.int8),
           'gen_x1': ak.to_numpy(a['Generator_x1']).astype(np.float32),
           'gen_x2': ak.to_numpy(a['Generator_x2']).astype(np.float32),
           'genWeight': ak.to_numpy(a['Generator_weight']).astype(np.float32)}

    # last-copy top / antitop (statusFlags bit 13), as Appendix-A
    pid, flags = a['GenPart_pdgId'], a['GenPart_statusFlags']
    last = (flags & (1 << 13)) != 0
    tops = {}
    for name, sel in (('t', (pid == 6) & last), ('tb', (pid == -6) & last)):
        first = ak.firsts(ak.local_index(pid)[sel])
        ok = ~ak.is_none(first)
        idx = ak.to_numpy(ak.fill_none(first, 0))
        cols = [ak.to_numpy(ak.fill_none(ak.firsts(a[f'GenPart_{c}'][ak.singletons(ak.Array(idx))]), 0.0))
                for c in ('pt', 'eta', 'phi', 'mass')]
        tops[name] = (p4(*cols), ak.to_numpy(ok))
    (t, okt), (tb, oktb) = tops['t'], tops['tb']
    tt = add(t, tb)
    ok = okt & oktb
    nan = np.float32(np.nan)
    out['gen_tt_m'] = np.where(ok, mass(tt), nan).astype(np.float32)
    out['gen_tt_pt'] = np.where(ok, pt(tt), nan).astype(np.float32)
    out['gen_tt_absy'] = np.where(ok, np.abs(rapidity(tt)), nan).astype(np.float32)
    # charge-asymmetry observable, kept only to check the classifier does not sculpt it
    out['gen_delta_absy'] = np.where(ok, np.abs(rapidity(t)) - np.abs(rapidity(tb)), nan).astype(np.float32)
    return out


# ---------------------------------------------------------------------------
def compute_features(path, cfg, era, is_mc=True, entry_stop=None):
    """Returns (DataFrame, stats dict)."""
    tree = uproot.open(path)['Events']
    branches = READ_BRANCHES + (MC_BRANCHES if is_mc else [])
    has_fit = all(b in tree.keys() for b in FIT_BRANCHES)
    if has_fit:
        branches = branches + FIT_BRANCHES
    a = tree.arrays(branches, entry_stop=entry_stop)
    n_in = len(a)
    a = a[a['ABCD_region'] == cfg['Region']]
    n = len(a)
    stats = {'n_in': n_in, 'n_region': n}
    if n == 0:
        return pd.DataFrame(), stats

    jc, thr = cfg['Jet'], cfg['BTagThreshold'][era]
    tr = cfg['TopReco']

    # ---- selected jets (reproduces 003-I), pT ordered -------------------------
    jsel = ((a['Jet_pt'] > jc['pt_min']) & (abs(a['Jet_eta']) < jc['eta_max']) &
            (a['Jet_jetId'] == jc['jetId']) & ((a['Jet_pt'] > jc['puId_pt_max']) | (a['Jet_puId'] > 0)))
    jets = ak.zip({k: a[f'Jet_{k}'][jsel] for k in ('pt', 'eta', 'phi', 'mass', 'btagDeepFlavB', 'qgl')})
    jets = jets[ak.argsort(jets.pt, ascending=False)]
    isb = jets.btagDeepFlavB > thr
    nj, nb = ak.to_numpy(ak.num(jets)), ak.to_numpy(ak.sum(isb, axis=1))
    stats['njet_mismatch'] = int(np.sum(nj != ak.to_numpy(a['sel_nJet'])))
    stats['nbjet_mismatch'] = int(np.sum(nb != ak.to_numpy(a['sel_nbjet'])))
    good = (nj >= 4) & (nb >= 2)
    stats['n_fail_jets'] = int(np.sum(~good))
    a, jets, isb, nj, nb = a[good], jets[good], isb[good], nj[good], nb[good]
    n = len(a)
    stats['n_out'] = n

    # first two b-tagged jets (pT order) are the b candidates; all others are light candidates
    flat_isb = ak.to_numpy(ak.flatten(isb)).astype(np.int32)
    counts = ak.to_numpy(ak.num(isb))
    offs = np.repeat(np.cumsum(counts) - counts, counts)
    csum = np.cumsum(flat_isb)
    csum = csum - np.concatenate([[0], csum])[offs]  # per-event cumulative count
    is_bcand = ak.unflatten((flat_isb == 1) & (csum <= 2), counts)

    PAD = max(cfg['NJetsStored'], 10)

    def padded(arr, fill=np.nan):
        return ak.to_numpy(ak.fill_none(ak.pad_none(arr, PAD, clip=True), fill)).astype(np.float64)

    J = {k: padded(jets[k]) for k in ('pt', 'eta', 'phi', 'mass', 'btagDeepFlavB', 'qgl')}
    Jv = p4(np.nan_to_num(J['pt']), np.nan_to_num(J['eta']), np.nan_to_num(J['phi']), np.nan_to_num(J['mass']))
    jvalid = ~np.isnan(J['pt'])

    bj = jets[is_bcand]
    lj = jets[~is_bcand]
    B = {k: ak.to_numpy(bj[k][:, :2]).astype(np.float64) for k in ('pt', 'eta', 'phi', 'mass')}
    Bv = p4(B['pt'], B['eta'], B['phi'], B['mass'])               # (n,2)
    NL = tr['MaxLightJets']
    L = {k: ak.to_numpy(ak.fill_none(ak.pad_none(lj[k], NL, clip=True), np.nan)).astype(np.float64)
         for k in ('pt', 'eta', 'phi', 'mass', 'qgl')}
    Lv = p4(np.nan_to_num(L['pt']), np.nan_to_num(L['eta']), np.nan_to_num(L['phi']), np.nan_to_num(L['mass']))
    lvalid = ~np.isnan(L['pt'])

    # ---- muon, MET, neutrino ---------------------------------------------------
    mu = p4(ak.to_numpy(a['SelMuon_pt']), ak.to_numpy(a['SelMuon_eta']), ak.to_numpy(a['SelMuon_phi']),
            np.full(n, MU_MASS))
    met_pt, met_phi = ak.to_numpy(a['MET_pt']).astype(np.float64), ak.to_numpy(a['MET_phi']).astype(np.float64)
    mex, mey = met_pt * np.cos(met_phi), met_pt * np.sin(met_phi)
    nupz, nupz2, complex_sol = neutrino_pz(mu, mex, mey, tr['mW'], return_other=True)
    nu = (mex, mey, nupz, np.sqrt(mex**2 + mey**2 + nupz**2))
    W_lep = add(mu, nu)

    # ---- chi2 permutations: b assignment (2) x light pair (<= C(NL,2)) ---------
    pairs = [(i, j) for i in range(NL) for j in range(i + 1, NL)]
    best_chi2 = np.full(n, np.inf)
    best = np.zeros((n, 3), dtype=np.int64)  # (k_bhad, i, j)
    best_k = np.full((n, 2), np.inf)         # best chi2 for each b assignment
    best_pair_k = np.zeros((n, 2, 2), dtype=np.int64)
    for k in (0, 1):
        bh = tuple(c[:, k] for c in Bv)
        bl = tuple(c[:, 1 - k] for c in Bv)
        m_tlep = mass(add(W_lep, bl))
        for (i, j) in pairs:
            Wh = add(tuple(c[:, i] for c in Lv), tuple(c[:, j] for c in Lv))
            chi2 = (((mass(add(Wh, bh)) - tr['mTop']) / tr['sigmaTopHad'])**2 +
                    ((m_tlep - tr['mTop']) / tr['sigmaTopLep'])**2 +
                    ((mass(Wh) - tr['mW']) / tr['sigmaW'])**2)
            chi2 = np.where(lvalid[:, i] & lvalid[:, j], chi2, np.inf)
            upd = chi2 < best_chi2
            best_chi2 = np.where(upd, chi2, best_chi2)
            best[upd] = (k, i, j)
            updk = chi2 < best_k[:, k]
            best_k[:, k] = np.where(updk, chi2, best_k[:, k])
            best_pair_k[updk, k] = (i, j)

    kb, li, lj_ = best[:, 0], best[:, 1], best[:, 2]
    bh = tuple(take(c, kb) for c in Bv)
    bl = tuple(take(c, 1 - kb) for c in Bv)
    q1 = tuple(take(c, li) for c in Lv)
    q2 = tuple(take(c, lj_) for c in Lv)
    W_had = add(q1, q2)
    t_had = add(W_had, bh)
    t_lep = add(W_lep, bl)
    tt = add(t_had, t_lep)

    # ---- full visible+nu system ----------------------------------------------
    jsum = tuple(np.where(jvalid, c, 0.0).sum(axis=1) for c in Jv)  # PAD=10 covers ~all events
    sysv = add(mu, nu, jsum)
    sys_m, sys_pt, sys_y = mass(sysv), pt(sysv), rapidity(sysv)
    sqrts = cfg['SqrtS']
    mT_sys = np.sqrt(sys_m**2 + sys_pt**2)
    xa, xb = mT_sys / sqrts * np.exp(sys_y), mT_sys / sqrts * np.exp(-sys_y)

    tt_m, tt_pt, tt_y = mass(tt), pt(tt), rapidity(tt)
    mT_tt = np.sqrt(tt_m**2 + tt_pt**2)
    txa, txb = mT_tt / sqrts * np.exp(tt_y), mT_tt / sqrts * np.exp(-tt_y)
    y_h, y_l = rapidity(t_had), rapidity(t_lep)

    # ---- angles in the tt rest frame ------------------------------------------
    th_tt = boost(t_had, tt)
    zhat = (np.zeros(n), np.zeros(n), np.ones(n))
    khat = unit3(th_tt)                                 # helicity axis (hadronic top direction)
    cos_theta_star = dot3(khat, zhat)
    # spin analysers: lepton in the leptonic-top frame, d-quark proxy (softer W jet in
    # the hadronic-top frame) in the hadronic-top frame. Both via the tt frame.
    tl_tt = boost(t_lep, tt)
    l_rest = unit3(boost(boost(mu, tt), tl_tt))
    q1_rest = boost(boost(q1, tt), th_tt)
    q2_rest = boost(boost(q2, tt), th_tt)
    d_rest = unit3(tuple(np.where(q1_rest[3] < q2_rest[3], c1, c2) for c1, c2 in zip(q1_rest, q2_rest)))
    cos_phi = dot3(l_rest, d_rest)
    # helicity-basis kk correlation: l along -k (leptonic top moves along -k in tt frame)
    c_kk = dot3(l_rest, tuple(-c for c in khat)) * dot3(d_rest, khat)

    # ---- extra (non-tt) jets: ISR handles -------------------------------------
    used = np.zeros((n, PAD), dtype=bool)
    for v in (bh, bl, q1, q2):
        used |= (np.abs(J['pt'] - pt(v)[:, None]) < 1e-3)
    extra = jvalid & ~used
    extra_ht = np.where(extra, J['pt'], 0.0).sum(axis=1)
    first_extra = np.argmax(extra, axis=1)
    has_extra = extra.any(axis=1)
    nanv = np.float64(np.nan)
    ex_pt = np.where(has_extra, take(J['pt'], first_extra), nanv)
    ex_eta = np.where(has_extra, np.abs(take(J['eta'], first_extra)), nanv)
    ex_qgl = np.where(has_extra, take(J['qgl'], first_extra), nanv)

    # ---- event shapes over muon + nu + jets -----------------------------------
    objs = np.stack([np.concatenate([mu[i][:, None], nu[i][:, None], np.where(jvalid, Jv[i], np.nan)], axis=1)
                     for i in range(3)], axis=-1)
    sph, apl = event_shapes(objs)

    ht = np.nansum(J['pt'], axis=1)
    jE = np.where(jvalid, Jv[3], 0.0).sum(axis=1)

    f = {
        'run': ak.to_numpy(a['run']), 'luminosityBlock': ak.to_numpy(a['luminosityBlock']),
        'event': ak.to_numpy(a['event']).astype(np.uint64),
        'fold': (ak.to_numpy(a['event']) % 10).astype(np.int8),
        'muCharge': ak.to_numpy(a['SelMuon_charge']).astype(np.int8),  # bookkeeping only
        # --- global
        'n_jets': nj, 'n_bjets': nb,
        'mu_pt': pt(mu), 'mu_abseta': np.abs(ak.to_numpy(a['SelMuon_eta'])),
        'met_pt': met_pt, 'nu_complex': complex_sol.astype(np.int8), 'nu_abspz': np.abs(nupz),
        'ht': ht, 'centrality': ht / np.where(jE > 0, jE, 1.0),
        'sys_m': sys_m, 'sys_pt': sys_pt, 'sys_absy': np.abs(sys_y), 'sys_abspz': np.abs(sysv[2]),
        'sys_xmax': np.maximum(xa, xb), 'sys_xmin': np.minimum(xa, xb),
        'sphericity': sph, 'aplanarity': apl,
        # --- chi2 tt reconstruction
        'log_chi2': np.log10(best_chi2 + 1e-6),
        'm_thad': mass(t_had), 'm_tlep': mass(t_lep), 'm_whad': mass(W_had),
        'tt_m': tt_m, 'tt_pt': tt_pt, 'tt_absy': np.abs(tt_y), 'tt_abspz': np.abs(tt[2]),
        'tt_xmax': np.maximum(txa, txb), 'tt_xmin': np.minimum(txa, txb),
        'top_absy_max': np.maximum(np.abs(y_h), np.abs(y_l)), 'top_absy_min': np.minimum(np.abs(y_h), np.abs(y_l)),
        'top_absdy': np.abs(y_h - y_l),
        'top_pt_max': np.maximum(pt(t_had), pt(t_lep)), 'top_pt_min': np.minimum(pt(t_had), pt(t_lep)),
        'abs_cos_theta_star': np.abs(cos_theta_star),
        'cos_phi_ld': cos_phi, 'c_kk': c_kk,
        # --- extra radiation
        'n_extra': (nj - 4), 'extra_ht': extra_ht,
        'extra1_pt': ex_pt, 'extra1_abseta': ex_eta, 'extra1_qgl': ex_qgl,
        'wjets_qgl_min': np.fmin(take(L['qgl'], li), take(L['qgl'], lj_)),
    }
    for i in range(cfg['NJetsStored']):
        f[f'jet{i}_pt'] = J['pt'][:, i]
        f[f'jet{i}_abseta'] = np.abs(J['eta'][:, i])
        f[f'jet{i}_btag'] = J['btagDeepFlavB'][:, i]

    # ---- 004A kinematic-fit tops (same symmetric definitions as the chi2 block) ----
    if has_fit:
        kh = p4(*[ak.to_numpy(a[f'Top_had_{c}']) for c in ('pt', 'eta', 'phi', 'mass')])
        kl = p4(*[ak.to_numpy(a[f'Top_lep_{c}']) for c in ('pt', 'eta', 'phi', 'mass')])
        ktt = add(kh, kl)
        km, kpt, ky = mass(ktt), pt(ktt), rapidity(ktt)
        kmT = np.sqrt(km**2 + kpt**2)
        kxa, kxb = kmT / sqrts * np.exp(ky), kmT / sqrts * np.exp(-ky)
        kyh, kyl = rapidity(kh), rapidity(kl)
        kcos = dot3(unit3(boost(kh, ktt)), zhat)
        ok = ak.to_numpy(a['Top_had_pt']) > 0
        nanv = np.float64(np.nan)
        for k, v in {'kf_tt_m': km, 'kf_tt_pt': kpt, 'kf_tt_absy': np.abs(ky), 'kf_tt_abspz': np.abs(ktt[2]),
                     'kf_tt_xmax': np.maximum(kxa, kxb), 'kf_tt_xmin': np.minimum(kxa, kxb),
                     'kf_top_absy_max': np.maximum(np.abs(kyh), np.abs(kyl)),
                     'kf_top_absy_min': np.minimum(np.abs(kyh), np.abs(kyl)),
                     'kf_top_absdy': np.abs(kyh - kyl),
                     'kf_top_pt_max': np.maximum(pt(kh), pt(kl)), 'kf_top_pt_min': np.minimum(pt(kh), pt(kl)),
                     'kf_abs_cos_theta_star': np.abs(kcos),
                     'kf_log_chi2': np.log10(ak.to_numpy(a['Chi2']).astype(np.float64) + 1e-6),
                     'kf_pgof': ak.to_numpy(a['Pgof'])}.items():
            f[k] = np.where(ok, v, nanv)
        f['kf_status'] = ak.to_numpy(a['chi2_status']).astype(np.int8)

    # ---- lepton-side vs hadron-side columns for the A_C-sensitivity study ------
    # (scripts/asym). Not charge-symmetric by construction: they distinguish the
    # leptonic from the hadronic top, so Delta|y|_reco = muCharge * rec_dabsy_lh
    # (mu+ => leptonic top is the top). NEVER add these to the qqbar-classifier
    # feature sets -- with muCharge they reveal sign(Delta|y|).
    if cfg.get('AsymmetryColumns'):
        def dabsy(th, tl):
            return np.abs(rapidity(tl)) - np.abs(rapidity(th))
        ks = 1 - kb  # alternative b assignment, with its own best W pair
        si = np.take_along_axis(best_pair_k[:, :, 0], ks[:, None], 1)[:, 0]
        sj = np.take_along_axis(best_pair_k[:, :, 1], ks[:, None], 1)[:, 0]
        bh_s, bl_s = tuple(take(c, ks) for c in Bv), tuple(take(c, kb) for c in Bv)
        th_s = add(tuple(take(c, si) for c in Lv), tuple(take(c, sj) for c in Lv), bh_s)
        tl_s = add(W_lep, bl_s)
        nu2 = (mex, mey, nupz2, np.sqrt(mex**2 + mey**2 + nupz2**2))
        tl_n2 = add(mu, nu2, bl)
        chi2_swap = np.take_along_axis(best_k, ks[:, None], 1)[:, 0]
        f.update({
            'rec_absy_tlep': np.abs(rapidity(t_lep)), 'rec_absy_thad': np.abs(rapidity(t_had)),
            'rec_dabsy_lh': dabsy(t_had, t_lep),
            'rec_dabsy_lh_bswap': np.where(np.isfinite(chi2_swap), dabsy(th_s, tl_s), np.nan),
            'rec_dabsy_lh_nu2': dabsy(t_had, tl_n2),
            'rec_log_chi2_bswap': np.log10(np.where(np.isfinite(chi2_swap), chi2_swap, np.nan) + 1e-6),
            'rec_tlep_pt': pt(t_lep), 'rec_thad_pt': pt(t_had),
            'rec_absy_mu': np.abs(rapidity(mu)), 'rec_absy_blep': np.abs(rapidity(bl)),
            'rec_absy_bhad': np.abs(rapidity(bh)), 'rec_absy_whad': np.abs(rapidity(W_had)),
            'rec_absy_nu': np.abs(rapidity(nu)),
            'rec_dabsy_b': np.abs(rapidity(bl)) - np.abs(rapidity(bh)),
            'rec_nu_dpz': np.abs(nupz2 - nupz),
        })
        if has_fit:
            f['kf_dabsy_lh'] = np.where(ok, dabsy(kh, kl), np.nan)

    # ---- per-object arrays for set-based networks (ObjectArrays) --------------
    # Canonical orientation: flip every eta so the full system moves to +z (z -> -z
    # symmetry), phi relative to the muon, mirrored so the leading jet has dphi >= 0.
    # Object 0 = muon, 1 = neutrino (W-constrained), 2.. = selected jets in pT order.
    # Missing jets: pt = 0 and every other field 0 (the network masks on pt > 0).
    oa = cfg.get('ObjectArrays')
    if oa:
        zs = np.where(sysv[2] < 0, -1.0, 1.0)
        mu_phi = ak.to_numpy(a['SelMuon_phi']).astype(np.float64)
        dphi = lambda ph: (ph - mu_phi + np.pi) % (2 * np.pi) - np.pi
        ysgn = np.where(np.nan_to_num(dphi(J['phi'][:, 0])) < 0, -1.0, 1.0)
        nu_pt = np.hypot(mex, mey)
        objs = [
            {'pt': pt(mu), 'eta': zs * ak.to_numpy(a['SelMuon_eta']), 'dphi': np.zeros(n), 'mass': np.full(n, MU_MASS),
             'btag': np.zeros(n), 'qgl': np.zeros(n), 'isMu': np.ones(n), 'isNu': np.zeros(n)},
            {'pt': nu_pt, 'eta': zs * np.arcsinh(nupz / np.maximum(nu_pt, 1e-6)), 'dphi': ysgn * dphi(met_phi),
             'mass': np.zeros(n), 'btag': np.zeros(n), 'qgl': np.zeros(n), 'isMu': np.zeros(n), 'isNu': np.ones(n)},
        ]
        for i in range(oa['NJets']):
            v = jvalid[:, i] if i < PAD else np.zeros(n, bool)
            g = (lambda k: np.where(v, J[k][:, i], 0.0)) if i < PAD else (lambda k: np.zeros(n))
            objs.append({'pt': g('pt'), 'eta': zs * g('eta'), 'dphi': np.where(v, ysgn * dphi(g('phi')), 0.0),
                         'mass': g('mass'), 'btag': g('btagDeepFlavB'), 'qgl': g('qgl'),
                         'isMu': np.zeros(n), 'isNu': np.zeros(n)})
        for i, o in enumerate(objs):
            for k, v in o.items():
                f[f'obj{i}_{k}'] = v

    df = pd.DataFrame(f)
    for c in df.columns:
        if df[c].dtype == np.float64:
            df[c] = df[c].astype(np.float32)
    if is_mc:
        g = gen_columns(a)
        df = pd.concat([df, pd.DataFrame(g)], axis=1)
        stats['prodType_counts'] = {int(k): int(v) for k, v in zip(*np.unique(g['prodType'], return_counts=True))}
    return df, stats
