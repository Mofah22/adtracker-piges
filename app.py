"""
PPT Media Review Engine v3
===========================
Nouveautés vs v2 :
- Unités en millions sur tous les graphiques de valeur
- Étiquettes % sur le graphique répartition par média (stacked 100%)
- Saisonnalité : étiquettes uniquement sur les 3 pics par année
- Top annonceurs : max 15, format millions
- Slides dynamiques : une slide par média présent (AF, TV, PR, RD, CN)
  → clonage complet des charts + rels + slide XML
"""

import io
import zipfile
import re
import copy
from pathlib import Path
from typing import Optional
from copy import deepcopy

import pandas as pd
from lxml import etree

# ─────────────────────────────────────────────
# NAMESPACES
# ─────────────────────────────────────────────
CNS  = "http://schemas.openxmlformats.org/drawingml/2006/chart"
ANS  = "http://schemas.openxmlformats.org/drawingml/2006/main"
PNS  = "http://schemas.openxmlformats.org/presentationml/2006/main"
RNS  = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKNS = "http://schemas.openxmlformats.org/package/2006/relationships"
CTNS = "http://schemas.openxmlformats.org/package/2006/content-types"

def ctag(n): return f"{{{CNS}}}{n}"
def atag(n): return f"{{{ANS}}}{n}"
def ptag(n): return f"{{{PNS}}}{n}"
def rstag(n): return f"{{{RNS}}}{n}"

# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────
MEDIA_LABELS = {
    "AF": "Affichage (OOH)",
    "TV": "TV",
    "PR": "Presse",
    "RD": "Radio",
    "CN": "Cinéma",
}
MEDIA_SHORT = {"AF": "OOH", "TV": "TV", "PR": "PR", "RD": "RD", "CN": "CN"}
MONTHS_FR = ["Jan","Fév","Mar","Avr","Mai","Juin","Juil","Août","Sep","Oct","Nov","Déc"]
TOP_N_ANNONCEURS = 15
TOP_N_SUPPORT    = 12

# Format étiquettes millions
FMT_MILLIONS = "0.0"     # valeurs déjà en millions dans le cache
FMT_PERCENT  = '0%'


# ─────────────────────────────────────────────
# 1. MOTEUR DE CALCUL
# ─────────────────────────────────────────────
class MediaCalculator:
    def __init__(self, df: pd.DataFrame, secteur_filter: str, sous_secteur_filter: Optional[str]):
        self.raw = df.copy()
        self.raw.columns = [c.strip() for c in self.raw.columns]
        tarif_col = next((c for c in self.raw.columns if "tarif" in c.lower()), None)
        if not tarif_col:
            raise ValueError("Colonne tarif introuvable.")
        self.raw.rename(columns={tarif_col: "tarif"}, inplace=True)
        self.raw["tarif"] = pd.to_numeric(self.raw["tarif"], errors="coerce").fillna(0)

        df_f = self.raw.copy()
        if secteur_filter:
            df_f = df_f[df_f["Secteur"].str.strip() == secteur_filter.strip()]
        if sous_secteur_filter:
            df_f = df_f[df_f["SousSecteur"].str.strip() == sous_secteur_filter.strip()]
        self.df = df_f.copy()

        self.years     = sorted(self.df["Anp"].dropna().unique().tolist())
        self.year_last = self.years[-1] if self.years else None
        self.year_prev = self.years[-2] if len(self.years) >= 2 else None
        self.year_prev2= self.years[-3] if len(self.years) >= 3 else None

        # Médias présents avec investissement > 0
        self.medias_present = sorted([
            m for m in ["AF","TV","PR","RD","CN"]
            if self.df[self.df["media"]==m]["tarif"].sum() > 0
        ])

    def total_by_year(self) -> dict:
        return self.df.groupby("Anp")["tarif"].sum().reindex(self.years, fill_value=0).to_dict()

    def total_by_year_media(self) -> pd.DataFrame:
        pt = self.df.groupby(["Anp","media"])["tarif"].sum().unstack(fill_value=0).reindex(self.years, fill_value=0)
        for m in ["AF","TV","PR","RD","CN"]:
            if m not in pt.columns: pt[m] = 0
        return pt

    def media_mix_last_year(self) -> dict:
        if not self.year_last: return {}
        row = self.total_by_year_media().loc[self.year_last]
        total = row.sum()
        return {k: v/total*100 for k,v in row.items() if v > 0} if total else {}

    def seasonality_by_year(self) -> pd.DataFrame:
        pt = self.df.groupby(["Anp","moisp"])["tarif"].sum().unstack(fill_value=0).reindex(columns=range(1,13), fill_value=0)
        pt.index = [int(y) for y in pt.index]
        return pt

    def top_annonceurs_by_year(self, year, n=TOP_N_ANNONCEURS) -> pd.Series:
        return self.df[self.df["Anp"]==year].groupby("Marque")["tarif"].sum().sort_values(ascending=False).head(n)

    def _focus(self, code): return self.df[self.df["media"]==code].copy()

    def total_focus_by_year(self, code) -> dict:
        return self._focus(code).groupby("Anp")["tarif"].sum().reindex(self.years, fill_value=0).to_dict()

    def top_ann_focus_last(self, code, n=TOP_N_ANNONCEURS) -> pd.Series:
        return self._focus(code)[self._focus(code)["Anp"]==self.year_last].groupby("Marque")["tarif"].sum().sort_values(ascending=False).head(n)

    def split_support_last(self, code, n=TOP_N_SUPPORT) -> pd.Series:
        return self._focus(code)[self._focus(code)["Anp"]==self.year_last].groupby("supportp")["tarif"].sum().sort_values(ascending=False).head(n)

    def evol_pct(self, new, old):
        return (new-old)/abs(old)*100 if old and old != 0 else None

    def sos(self, code=None) -> dict:
        top   = self.top_ann_focus_last(code) if code else self.top_annonceurs_by_year(self.year_last)
        total = self.total_focus_by_year(code).get(self.year_last,0) if code else self.total_by_year().get(self.year_last,0)
        return {b: v/total*100 for b,v in top.items()} if total else {}

    def summary_stats(self) -> dict:
        totals = self.total_by_year()
        mix    = self.media_mix_last_year()
        s = {"years": self.years, "year_last": self.year_last, "totals": totals,
             "total_last": totals.get(self.year_last,0),
             "total_prev": totals.get(self.year_prev,0),
             "total_prev2": totals.get(self.year_prev2,0),
             "evol_vs_prev":  self.evol_pct(totals.get(self.year_last,0), totals.get(self.year_prev,0)),
             "evol_vs_prev2": self.evol_pct(totals.get(self.year_last,0), totals.get(self.year_prev2,0)),
             "media_mix": mix,
             "dominant_media": max(mix, key=mix.get) if mix else None,
             "dominant_media_pct": max(mix.values()) if mix else 0,
        }
        seas = self.seasonality_by_year()
        if self.year_last and self.year_last in seas.index:
            peak = int(seas.loc[self.year_last].idxmax())
            s["peak_month"] = MONTHS_FR[peak-1]
            s["peak_value"] = seas.loc[self.year_last, peak]

        top_ann = self.top_annonceurs_by_year(self.year_last) if self.year_last else pd.Series()
        if len(top_ann):
            s["top1_ann"] = top_ann.index[0]
            s["top1_val"] = top_ann.iloc[0]
            s["top1_sos"] = self.sos().get(top_ann.index[0], 0)
        if len(top_ann) > 1:
            s["top3_sos"] = sum(list(self.sos().values())[:3])

        for code in ["AF","TV","PR","RD","CN"]:
            sub = self.total_focus_by_year(code)
            if sub.get(self.year_last,0) > 0:
                s[f"{code}_last"] = sub.get(self.year_last,0)
                s[f"{code}_prev"] = sub.get(self.year_prev,0)
                s[f"{code}_evol"] = self.evol_pct(sub.get(self.year_last,0), sub.get(self.year_prev,0))
                sup = self.split_support_last(code)
                if len(sup):
                    tot = sub.get(self.year_last,0)
                    s[f"{code}_top_sup"]     = sup.index[0]
                    s[f"{code}_top_sup_pct"] = sup.iloc[0]/tot*100 if tot else 0
                ann = self.top_ann_focus_last(code)
                if len(ann):
                    s[f"{code}_top1_ann"] = ann.index[0]
                    s[f"{code}_top1_sos"] = self.sos(code).get(ann.index[0],0)
        return s


# ─────────────────────────────────────────────
# 2. COMMENTAIRES IA
# ─────────────────────────────────────────────
def generate_comments_via_claude(stats: dict, secteur: str, label: str, api_key: str) -> dict:
    import requests, json, re as re2

    yl  = stats.get("year_last","")
    yp  = stats["years"][-2] if len(stats["years"])>=2 else ""
    yp2 = stats["years"][-3] if len(stats["years"])>=3 else ""

    def fm(v):  return f"{v/1e6:.1f} M MAD" if v else "N/A"
    def fp(v):  return f"{'+'if v and v>0 else''}{v:.1f}%" if v is not None else "N/A"
    def ypl(y): return str(int(y)) if y else "N-1"

    ctx = f"""Secteur: {secteur} | Sous-secteur: {label} | Période: {yp2}–{yl}
GLOBAL: {yl}: {fm(stats.get('total_last'))} | {ypl(yp)}: {fm(stats.get('total_prev'))} | {ypl(yp2)}: {fm(stats.get('total_prev2'))}
Évol vs {ypl(yp)}: {fp(stats.get('evol_vs_prev'))} | Évol vs {ypl(yp2)}: {fp(stats.get('evol_vs_prev2'))}
Mix: {', '.join(f"{k}:{v:.0f}%" for k,v in stats.get('media_mix',{}).items())} | Pic: {stats.get('peak_month','')} ({fm(stats.get('peak_value'))})
Top annonceur: {stats.get('top1_ann','')} — {fm(stats.get('top1_val'))} — SOS {stats.get('top1_sos',0):.0f}%
""" + "\n".join(
        f"{c}: {fm(stats.get(f'{c}_last'))} | Évol {fp(stats.get(f'{c}_evol'))} | Top: {stats.get(f'{c}_top_sup','')} ({stats.get(f'{c}_top_sup_pct',0):.0f}%) | Leader: {stats.get(f'{c}_top1_ann','')} (SOS {stats.get(f'{c}_top1_sos',0):.0f}%)"
        for c in ["AF","TV","PR","RD","CN"] if stats.get(f"{c}_last")
    )

    medias_present = [c for c in ["AF","TV","PR","RD","CN"] if stats.get(f"{c}_last")]
    media_keys = {c: f"slide_{c.lower()}" for c in medias_present}

    slides_json = '{\n  "slide2_global": "...",\n  "slide2_headline": "...",\n  "slide3_annonceurs": "..."'
    for c in medias_present:
        slides_json += f',\n  "slide_{c.lower()}": "commentaire {MEDIA_LABELS.get(c,c)}"'
    slides_json += "\n}"

    prompt = f"""Expert media planner Maroc. Génère commentaires analytiques CONCIS pour Media Review PPT.
Données:\n{ctx}

JSON uniquement (2-4 phrases par commentaire, chiffres clés en M MAD et %, ton professionnel):
{slides_json}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-6", "max_tokens": 2000,
                  "messages": [{"role":"user","content":prompt}]},
            timeout=30,
        )
        resp.raise_for_status()
        text = re2.sub(r"^```json\s*|```$","", resp.json()["content"][0]["text"].strip(), flags=re2.MULTILINE).strip()
        return json.loads(text)
    except Exception:
        yp_l = ypl(yp); yp2_l = ypl(yp2)
        out = {
            "slide2_global":    f"Mix médias {yl} : {', '.join(f'{MEDIA_LABELS.get(k,k)} {v:.0f}%' for k,v in stats.get('media_mix',{}).items())}. Pic saisonnalité : {stats.get('peak_month','')} ({fm(stats.get('peak_value'))}).",
            "slide2_headline":  f"{yl} : {fm(stats.get('total_last'))} ({fp(stats.get('evol_vs_prev'))} vs {yp_l}), ({fp(stats.get('evol_vs_prev2'))} vs {yp2_l})",
            "slide3_annonceurs":f"Leader {yl} : {stats.get('top1_ann','')} avec {fm(stats.get('top1_val'))} (SOS {stats.get('top1_sos',0):.0f}%). Top 3 = {stats.get('top3_sos',0):.0f}% du marché.",
        }
        for c in ["AF","TV","PR","RD","CN"]:
            if stats.get(f"{c}_last"):
                out[f"slide_{c.lower()}"] = f"{MEDIA_LABELS.get(c,c)} {yl} : {fm(stats.get(f'{c}_last'))} ({fp(stats.get(f'{c}_evol'))} vs {yp_l}). {stats.get(f'{c}_top_sup','')} = {stats.get(f'{c}_top_sup_pct',0):.0f}%. Leader : {stats.get(f'{c}_top1_ann','')} (SOS {stats.get(f'{c}_top1_sos',0):.0f}%)."
        return out


# ─────────────────────────────────────────────
# 3. HELPERS XML CHARTS
# ─────────────────────────────────────────────

import math

def smart_max(values_mad: list) -> float:
    """Calcule un max d'axe arrondi. Entrée en MAD, sortie en MILLIONS (valeurs déjà converties)."""
    vals = [v for v in values_mad if v is not None and v > 0]
    if not vals:
        return 1.0
    raw_max = max(vals) / 1e6
    magnitude = 10 ** math.floor(math.log10(raw_max))
    for mult in [1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 25, 30, 40, 50]:
        candidate = magnitude * mult
        if candidate >= raw_max * 1.10:
            return candidate  # en millions
    return raw_max * 1.2


def _fix_val_axis(root, max_val_millions: float):
    """
    Met à jour tous les axes Y (valAx).
    Valeurs déjà en MILLIONS dans le cache → pas de dispUnits, max en millions.
    """
    for val_ax in root.findall(f".//{ctag('valAx')}"):
        # Supprimer dispUnits (valeurs déjà en millions, plus besoin de division)
        for child in list(val_ax):
            if child.tag == ctag("dispUnits"):
                val_ax.remove(child)

        # numFmt "0.0" sur les millions → affiche "6.4", "18.2"
        nf = val_ax.find(ctag("numFmt"))
        if nf is None:
            nf = etree.SubElement(val_ax, ctag("numFmt"))
        nf.set("formatCode", "0.0")
        nf.set("sourceLinked", "0")

        # Max en millions
        scaling = val_ax.find(ctag("scaling"))
        if scaling is None:
            scaling = etree.Element(ctag("scaling"))
            val_ax.insert(0, scaling)
        max_el = scaling.find(ctag("max"))
        if max_el is None:
            max_el = etree.SubElement(scaling, ctag("max"))
        max_el.set("val", str(round(max_val_millions, 2)))

        min_el = scaling.find(ctag("min"))
        if min_el is None:
            min_el = etree.SubElement(scaling, ctag("min"))
        min_el.set("val", "0")

def _set_num_fmt(dlbls_el, fmt_code: str):
    """Change le format des étiquettes dans un dLbls."""
    nf = dlbls_el.find(ctag("numFmt"))
    if nf is None:
        nf = etree.SubElement(dlbls_el, ctag("numFmt"))
        dlbls_el.insert(0, nf)
    nf.set("formatCode", fmt_code)
    nf.set("sourceLinked", "0")


def _set_show_flags(dlbls_el, show_val="0", show_pct="0"):
    """Active/désactive val et percent dans dLbls."""
    for tag, val in [("showVal", show_val), ("showPercent", show_pct),
                     ("showLegendKey","0"), ("showCatName","0"),
                     ("showSerName","0"), ("showBubbleSize","0")]:
        el = dlbls_el.find(ctag(tag))
        if el is not None:
            el.set("val", val)


def _rebuild_cache(ser_el, categories: list, values: list, divide_by: float = 1.0):
    """Met à jour catégories + valeurs dans le cache XML d'une série.
    divide_by : diviser les valeurs avant écriture (ex: 1e6 pour millions)
    """
    # Catégories
    cat_el = ser_el.find(ctag("cat"))
    if cat_el is not None:
        # numRef ou strRef
        for ref_type in ["numRef", "strRef"]:
            ref = cat_el.find(ctag(ref_type))
            if ref is not None:
                for cache_type in ["numCache", "strCache"]:
                    cache = ref.find(ctag(cache_type))
                    if cache is not None:
                        for pt in cache.findall(ctag("pt")): cache.remove(pt)
                        pc = cache.find(ctag("ptCount"))
                        if pc is None: pc = etree.SubElement(cache, ctag("ptCount"))
                        pc.set("val", str(len(categories)))
                        for i,c in enumerate(categories):
                            pt = etree.SubElement(cache, ctag("pt"))
                            pt.set("idx", str(i))
                            v = etree.SubElement(pt, ctag("v"))
                            v.text = str(c)
                        f_el = ref.find(ctag("f"))
                        if f_el is not None:
                            m = re.match(r"(.+)!\$([A-Z]+)\$\d+:\$[A-Z]+\$\d+", f_el.text or "")
                            if m: f_el.text = f"{m.group(1)}!${m.group(2)}$2:${m.group(2)}${len(categories)+1}"

    # Valeurs
    val_el = ser_el.find(ctag("val"))
    if val_el is not None:
        num_ref = val_el.find(ctag("numRef"))
        if num_ref is not None:
            cache = num_ref.find(ctag("numCache"))
            if cache is None: cache = etree.SubElement(num_ref, ctag("numCache"))
            for pt in cache.findall(ctag("pt")): cache.remove(pt)
            # Forcer le formatCode du cache à "0.0" pour que PPT l'utilise correctement
            fc = cache.find(ctag("formatCode"))
            if fc is None: fc = etree.SubElement(cache, ctag("formatCode"))
            fc.text = "0.0"
            pc = cache.find(ctag("ptCount"))
            if pc is None: pc = etree.SubElement(cache, ctag("ptCount"))
            pc.set("val", str(len(values)))
            for i,v in enumerate(values):
                if v is None: continue
                pt = etree.SubElement(cache, ctag("pt"))
                pt.set("idx", str(i))
                ve = etree.SubElement(pt, ctag("v"))
                if v is None:
                    ve.text = ""
                else:
                    v_out = v / divide_by if divide_by != 1.0 else v
                    ve.text = str(v_out)
            f_el = num_ref.find(ctag("f"))
            if f_el is not None:
                m = re.match(r"(.+)!\$([A-Z]+)\$\d+:\$[A-Z]+\$\d+", f_el.text or "")
                if m: f_el.text = f"{m.group(1)}!${m.group(2)}$2:${m.group(2)}${len(values)+1}"


def _set_series_name(ser_el, name: str):
    tx_v = ser_el.find(f".//{ctag('tx')}//{ctag('v')}")
    if tx_v is not None:
        tx_v.text = str(name)


def _build_peak_dlbls(ser_el, values: list, top_n: int = 3) -> None:
    """
    Pour la saisonnalité : supprime tous les dLbl individuels,
    puis ajoute des dLbl uniquement pour les top_n pics.
    """
    dlbls = ser_el.find(ctag("dLbls"))
    if dlbls is None:
        return

    # Supprimer dLbl individuels existants
    for dl in dlbls.findall(ctag("dLbl")):
        dlbls.remove(dl)

    # Format global millions + masquer val par défaut
    _set_num_fmt(dlbls, FMT_MILLIONS)
    sv = dlbls.find(ctag("showVal"))
    if sv is not None: sv.set("val","0")

    if not values:
        return

    # Trouver les indices des top_n pics
    vals_clean = [(i, v) for i,v in enumerate(values) if v is not None and v > 0]
    vals_clean.sort(key=lambda x: -x[1])
    peak_indices = {i for i,_ in vals_clean[:top_n]}

    # Créer un dLbl pour chaque pic (avant showVal global)
    for idx in sorted(peak_indices):
        dl = etree.Element(ctag("dLbl"))
        idx_el = etree.SubElement(dl, ctag("idx"))
        idx_el.set("val", str(idx))
        nf = etree.SubElement(dl, ctag("numFmt"))
        nf.set("formatCode", FMT_MILLIONS)
        nf.set("sourceLinked","0")
        pos = etree.SubElement(dl, ctag("dLblPos"))
        pos.set("val","t")
        for tag in ["showLegendKey","showVal","showCatName","showSerName","showPercent","showBubbleSize"]:
            e = etree.SubElement(dl, ctag(tag))
            e.set("val","1" if tag=="showVal" else "0")
        # Insérer avant showVal global
        dlbls.insert(0, dl)


def process_chart1_annual(chart_xml: bytes, cats: list, vals: list) -> bytes:
    """Investissements annuels — format millions sur les étiquettes et l'axe."""
    root = etree.fromstring(chart_xml)
    sers = root.findall(f".//{ctag('ser')}")
    if sers:
        _rebuild_cache(sers[0], cats, vals, divide_by=1e6)
        _set_series_name(sers[0], "Total")
        dlbls = root.find(f".//{ctag('dLbls')}")
        if dlbls is not None:
            _set_num_fmt(dlbls, FMT_MILLIONS)
            _set_show_flags(dlbls, show_val="1", show_pct="0")
    _fix_val_axis(root, smart_max(vals))
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


def process_chart2_stacked(chart_xml: bytes, years: list, media_matrix: pd.DataFrame) -> bytes:
    """Répartition par média stacked 100% — étiquettes en %."""
    root = etree.fromstring(chart_xml)
    sers = root.findall(f".//{ctag('ser')}")
    media_order = ["AF","PR","RD","TV","CN"]

    for i, m in enumerate(media_order):
        if i >= len(sers): break
        vals = [media_matrix.loc[y,m] if m in media_matrix.columns else None for y in years]
        vals_clean = [v if v and v>0 else None for v in vals]
        _rebuild_cache(sers[i], [int(y) for y in years], vals_clean, divide_by=1e6)
        _set_series_name(sers[i], m)

        # Ajouter dLbls sur la série si absent (PowerPoint ignore le dLbls global pour stacked)
        dlbls = sers[i].find(ctag("dLbls"))
        if dlbls is None:
            dlbls = etree.SubElement(sers[i], ctag("dLbls"))

        # Vider et reconstruire proprement
        for child in list(dlbls):
            dlbls.remove(child)

        nf = etree.SubElement(dlbls, ctag("numFmt"))
        nf.set("formatCode", "0%")
        nf.set("sourceLinked", "0")
        for tag, val in [("showLegendKey","0"),("showVal","0"),("showCatName","0"),
                         ("showSerName","0"),("showPercent","1"),("showBubbleSize","0")]:
            el = etree.SubElement(dlbls, ctag(tag))
            el.set("val", val)

    # dLbls global sur barChart (fallback)
    bar_chart = root.find(f".//{ctag('barChart')}")
    if bar_chart is not None:
        dlbls_g = bar_chart.find(ctag("dLbls"))
        if dlbls_g is None:
            dlbls_g = etree.SubElement(bar_chart, ctag("dLbls"))
        _set_num_fmt(dlbls_g, "0%")
        _set_show_flags(dlbls_g, show_val="0", show_pct="1")

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


def process_chart3_seasonality(chart_xml: bytes, years: list, seas: pd.DataFrame) -> bytes:
    """Saisonnalité — valeurs en millions, étiquettes sur 3 pics par année."""
    root = etree.fromstring(chart_xml)
    sers = root.findall(f".//{ctag('ser')}")
    all_vals = []
    for i, y in enumerate(years):
        ser_idx = i + 1
        if ser_idx >= len(sers): break
        ser = sers[ser_idx]
        month_vals = [seas.loc[y,m] if y in seas.index and m in seas.columns else 0
                      for m in range(1,13)]
        all_vals.extend(month_vals)
        _rebuild_cache(ser, MONTHS_FR, month_vals, divide_by=1e6)
        _set_series_name(ser, str(int(y)))
        _build_peak_dlbls(ser, month_vals, top_n=3)
    _fix_val_axis(root, smart_max(all_vals))
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


def process_chart_annonceurs(chart_xml: bytes, cats: list, vals: list, year_label: str) -> bytes:
    """Bar annonceurs — format millions, max 15."""
    root = etree.fromstring(chart_xml)
    sers = root.findall(f".//{ctag('ser')}")
    if sers:
        cats = cats[:TOP_N_ANNONCEURS]
        vals = vals[:TOP_N_ANNONCEURS]
        _rebuild_cache(sers[0], cats, vals, divide_by=1e6)
        _set_series_name(sers[0], year_label)
        dlbls = root.find(f".//{ctag('dLbls')}")
        if dlbls is not None:
            _set_num_fmt(dlbls, FMT_MILLIONS)
            _set_show_flags(dlbls, show_val="1", show_pct="0")
    _fix_val_axis(root, smart_max(vals))
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


def process_chart_media_annual(chart_xml: bytes, cats: list, vals: list, label: str) -> bytes:
    """Trend annuel média (bar clustered) — format millions."""
    root = etree.fromstring(chart_xml)
    sers = root.findall(f".//{ctag('ser')}")
    if sers:
        _rebuild_cache(sers[0], cats, vals, divide_by=1e6)
        _set_series_name(sers[0], label)
        dlbls = root.find(f".//{ctag('dLbls')}")
        if dlbls is not None:
            _set_num_fmt(dlbls, FMT_MILLIONS)
            _set_show_flags(dlbls, show_val="1", show_pct="0")
    _fix_val_axis(root, smart_max(vals))
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


def process_chart_top_ann(chart_xml: bytes, cats: list, vals: list, label: str) -> bytes:
    """Top annonceurs focus média — format millions."""
    root = etree.fromstring(chart_xml)
    sers = root.findall(f".//{ctag('ser')}")
    if sers:
        cats = cats[:TOP_N_ANNONCEURS]
        vals = vals[:TOP_N_ANNONCEURS]
        _rebuild_cache(sers[0], cats, vals, divide_by=1e6)
        _set_series_name(sers[0], label)
        dlbls = root.find(f".//{ctag('dLbls')}")
        if dlbls is not None:
            _set_num_fmt(dlbls, FMT_MILLIONS)
            _set_show_flags(dlbls, show_val="1", show_pct="0")
    _fix_val_axis(root, smart_max(vals))
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


def process_chart_pie(chart_xml: bytes, cats: list, vals: list, label: str) -> bytes:
    """Pie répartition — garder % comme dans le template."""
    root = etree.fromstring(chart_xml)
    sers = root.findall(f".//{ctag('ser')}")
    if sers:
        _rebuild_cache(sers[0], cats, vals, divide_by=1e6)
        _set_series_name(sers[0], label)
        # Garder le format existant (%)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


# ─────────────────────────────────────────────
# 4. GESTION DES SLIDES DYNAMIQUES
# ─────────────────────────────────────────────

def _replace_tf_in_xml(txBody, new_text: str):
    """Remplace le contenu texte d'un txBody XML."""
    pns = ANS
    def qtag(n): return f"{{{pns}}}{n}"

    font_sz = font_b = None
    for p in txBody.findall(qtag("p")):
        for r in p.findall(qtag("r")):
            rpr = r.find(qtag("rPr"))
            if rpr is not None:
                font_sz = rpr.get("sz")
                font_b  = rpr.get("b")
                break
        if font_sz: break

    first_pPr = None
    paras = txBody.findall(qtag("p"))
    if paras:
        first_pPr = paras[0].find(qtag("pPr"))

    for p in list(txBody.findall(qtag("p"))): txBody.remove(p)

    for line in new_text.split("\n"):
        p_el = etree.SubElement(txBody, qtag("p"))
        if first_pPr is not None: p_el.insert(0, deepcopy(first_pPr))
        if not line.strip():
            end = etree.SubElement(p_el, qtag("endParaRPr"))
            end.set("lang","fr-FR"); end.set("dirty","0")
            continue
        r_el  = etree.SubElement(p_el, qtag("r"))
        rpr   = etree.SubElement(r_el, qtag("rPr"))
        rpr.set("lang","fr-FR"); rpr.set("dirty","0")
        if font_sz: rpr.set("sz", font_sz)
        if font_b:  rpr.set("b", font_b)
        t_el  = etree.SubElement(r_el, qtag("t"))
        t_el.text = line


def update_slide_texts(slide_xml: bytes, updates: dict) -> bytes:
    """Met à jour les zones de texte d'une slide par nom de shape."""
    root = etree.fromstring(slide_xml)
    for sp in root.findall(f".//{ptag('sp')}"):
        nv = sp.find(f".//{ptag('cNvPr')}")
        if nv is None: continue
        name = nv.get("name","")
        if name not in updates: continue
        txBody = sp.find(f".//{ptag('txBody')}")
        if txBody is None:
            txBody = sp.find(f".//{atag('txBody')}")
        if txBody is None: continue
        _replace_tf_in_xml(txBody, updates[name])
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


def clone_media_slide(
    template_bytes: dict,   # {filename: bytes} du zip original
    media_code: str,
    new_slide_num: int,
    new_chart_base: int,    # numéro de départ pour les nouveaux charts
    new_emb_base: int,      # numéro de départ pour les embeddings
    calc: MediaCalculator,
    comments: dict,
    label: str,
) -> dict:
    """
    Clone la slide OOH (slide4) en adaptant le contenu pour n'importe quel média.
    Retourne un dict {path: bytes} des nouveaux fichiers à ajouter au zip.
    """
    new_files = {}
    yl = calc.year_last
    years = calc.years
    years_range = f"{years[0]} – {yl}" if years else ""
    media_label = MEDIA_LABELS.get(media_code, media_code)
    media_short = MEDIA_SHORT.get(media_code, media_code)

    # Données du média
    media_totals  = calc.total_focus_by_year(media_code)
    top_ann       = calc.top_ann_focus_last(media_code, n=TOP_N_ANNONCEURS)
    top_sup       = calc.split_support_last(media_code, n=TOP_N_SUPPORT)

    # Noms des nouveaux fichiers
    new_slide_path   = f"ppt/slides/slide{new_slide_num}.xml"
    new_slide_rels   = f"ppt/slides/_rels/slide{new_slide_num}.xml.rels"
    chart_annual_id  = new_chart_base
    chart_ann_id     = new_chart_base + 1
    chart_pie_id     = new_chart_base + 2
    emb_annual_id    = new_emb_base
    emb_ann_id       = new_emb_base + 1
    emb_pie_id       = new_emb_base + 2

    chart_annual_path = f"ppt/charts/chart{chart_annual_id}.xml"
    chart_ann_path    = f"ppt/charts/chart{chart_ann_id}.xml"
    chart_pie_path    = f"ppt/charts/chart{chart_pie_id}.xml"
    emb_annual_path   = f"ppt/embeddings/Microsoft_Excel_Worksheet{emb_annual_id}.xlsx"
    emb_ann_path      = f"ppt/embeddings/Microsoft_Excel_Worksheet{emb_ann_id}.xlsx"
    emb_pie_path      = f"ppt/embeddings/Microsoft_Excel_Worksheet{emb_pie_id}.xlsx"

    # ── Cloner chart7 → trend annuel ─────────────────────────────────
    ch_annual_xml = template_bytes["ppt/charts/chart7.xml"]
    cats_ann_yr = [int(y) for y in years]
    vals_ann_yr = [media_totals.get(y,0) for y in years]
    ch_annual_new = process_chart_media_annual(ch_annual_xml, cats_ann_yr, vals_ann_yr, media_label)
    new_files[chart_annual_path] = ch_annual_new

    # ── Cloner chart8 → top annonceurs ───────────────────────────────
    ch_ann_xml = template_bytes["ppt/charts/chart8.xml"]
    ch_ann_new = process_chart_top_ann(ch_ann_xml,
                                        list(top_ann.index), list(top_ann.values),
                                        f"Top annonceurs {media_short} {int(yl)}")
    new_files[chart_ann_path] = ch_ann_new

    # ── Cloner chart9 → pie répartition support ───────────────────────
    ch_pie_xml = template_bytes["ppt/charts/chart9.xml"]
    sup_label = "ville" if media_code == "AF" else "support" if media_code in ("TV","PR") else "station"
    ch_pie_new = process_chart_pie(ch_pie_xml,
                                    list(top_sup.index), list(top_sup.values),
                                    media_short)
    new_files[chart_pie_path] = ch_pie_new

    # ── Embeddings (copie des originaux — les charts XML sont la vraie source) ──
    new_files[emb_annual_path] = template_bytes.get("ppt/embeddings/Microsoft_Excel_Worksheet6.xlsx", b"")
    new_files[emb_ann_path]    = template_bytes.get("ppt/embeddings/Microsoft_Excel_Worksheet7.xlsx", b"")
    new_files[emb_pie_path]    = template_bytes.get("ppt/embeddings/Microsoft_Excel_Worksheet8.xlsx", b"")

    # ── Rels chart → embedding ────────────────────────────────────────
    for chart_id, emb_id in [(chart_annual_id, emb_annual_id),
                              (chart_ann_id,    emb_ann_id),
                              (chart_pie_id,    emb_pie_id)]:
        orig_rels = template_bytes[f"ppt/charts/_rels/chart7.xml.rels"]
        rels_root = etree.fromstring(orig_rels)
        RELS_NS2 = "http://schemas.openxmlformats.org/package/2006/relationships"
        for rel in rels_root.findall(f"{{{RELS_NS2}}}Relationship"):
            if "embeddings" in rel.get("Target",""):
                rel.set("Target", f"../embeddings/Microsoft_Excel_Worksheet{emb_id}.xlsx")
        new_files[f"ppt/charts/_rels/chart{chart_id}.xml.rels"] = etree.tostring(
            rels_root, xml_declaration=True, encoding="UTF-8", standalone=True)

    # ── Slide XML (clone de slide4) ───────────────────────────────────
    slide4_xml = template_bytes["ppt/slides/slide4.xml"]
    slide_root = etree.fromstring(slide4_xml)

    # Mettre à jour les rIds des graphicFrames
    slide4_rels_xml = template_bytes["ppt/slides/_rels/slide4.xml.rels"]
    orig_rels_root  = etree.fromstring(slide4_rels_xml)
    RELS_NS2 = "http://schemas.openxmlformats.org/package/2006/relationships"

    # Mapping ancien rId chart → nouveau chart path
    old_chart_map = {}
    for rel in orig_rels_root.findall(f"{{{RELS_NS2}}}Relationship"):
        if "charts/chart" in rel.get("Target",""):
            old_chart_map[rel.get("Id")] = rel.get("Target")

    # Construire nouvelles rels
    new_rels_root = etree.fromstring(slide4_rels_xml)
    chart_assignments = {
        "rId2": f"../charts/chart{chart_annual_id}.xml",  # trend annuel
        "rId3": f"../charts/chart{chart_ann_id}.xml",     # top annonceurs
        "rId4": f"../charts/chart{chart_pie_id}.xml",     # pie
    }
    for rel in new_rels_root.findall(f"{{{RELS_NS2}}}Relationship"):
        rid = rel.get("Id")
        if rid in chart_assignments:
            rel.set("Target", chart_assignments[rid])
    new_files[new_slide_rels] = etree.tostring(
        new_rels_root, xml_declaration=True, encoding="UTF-8", standalone=True)

    # Mettre à jour les textes de la slide
    comment_key = f"slide_{media_code.lower()}"
    sup_display_label = "ville" if media_code=="AF" else "support" if media_code in ("TV","PR") else "station"
    text_updates = {
        "TextBox 2":  f"Investissement média {media_label} — {label}",
        "TextBox 3":  f"FY {years_range} | Millions MAD | Source : Imperium",
        "TextBox 8":  f"Investissements {media_short}",
        "TextBox 9":  f"Répartition {int(yl)} par {sup_display_label}",
        "TextBox 10": f"Top annonceurs {media_short} (FY {int(yl)})",
        "TextBox 11": "Points clés",
        "TextBox 15": comments.get(comment_key, ""),
    }
    slide_bytes = update_slide_texts(etree.tostring(slide_root, xml_declaration=True,
                                                     encoding="UTF-8", standalone=True),
                                      text_updates)
    new_files[new_slide_path] = slide_bytes

    return new_files


# ─────────────────────────────────────────────
# 5. INJECTION PRINCIPALE
# ─────────────────────────────────────────────

class PPTInjector:
    def __init__(self, template_path: str):
        self.template_path = template_path
        with open(template_path, "rb") as f:
            self.template_bytes_raw = f.read()

    def generate(self, calc: MediaCalculator, comments: dict,
                 secteur: str, sous_secteur: str) -> bytes:

        years      = calc.years
        year_last  = calc.year_last
        year_prev  = calc.year_prev
        totals     = calc.total_by_year()
        mm         = calc.total_by_year_media()
        seas       = calc.seasonality_by_year()
        label      = sous_secteur or secteur
        yrange     = f"{years[0]} – {year_last}" if years else ""
        medias     = calc.medias_present  # ex: ["AF","TV","PR","RD"]

        # ── Lire tout le ZIP en mémoire ───────────────────────────────
        original: dict[str, bytes] = {}
        with zipfile.ZipFile(io.BytesIO(self.template_bytes_raw), "r") as zin:
            for item in zin.infolist():
                original[item.filename] = zin.read(item.filename)

        # ── Préparer les mises à jour des charts statiques ────────────

        # Chart1 — investissements annuels
        ch1 = process_chart1_annual(
            original["ppt/charts/chart1.xml"],
            [int(y) for y in years],
            [totals.get(y,0) for y in years]
        )

        # Chart2 — stacked 100% par média
        ch2 = process_chart2_stacked(original["ppt/charts/chart2.xml"], years, mm)

        # Chart3 — saisonnalité mensuelle
        ch3 = process_chart3_seasonality(original["ppt/charts/chart3.xml"], years, seas)

        # Charts annonceurs slide3
        years_ann = years[-3:] if len(years)>=3 else years  # 2023 gauche, 2024 milieu, 2025 droite
        ch4 = ch5 = ch6 = None
        for i, (chart_id, y) in enumerate(zip([4,5,6], years_ann + [None]*(3-len(years_ann)))):
            if y is None: continue
            top = calc.top_annonceurs_by_year(y, n=TOP_N_ANNONCEURS)
            xml = process_chart_annonceurs(
                original[f"ppt/charts/chart{chart_id}.xml"],
                list(top.index), list(top.values), str(int(y))
            )
            if chart_id==4: ch4=xml
            elif chart_id==5: ch5=xml
            elif chart_id==6: ch6=xml

        # Charts slides médias fixes (4=OOH/AF, 5=TV, 6=RD dans template)
        # On les traite aussi
        def make_media_charts(code, chart_annual_id, chart_pie_id, chart_ann_id):
            mt = calc.total_focus_by_year(code)
            ta = calc.top_ann_focus_last(code)
            ts = calc.split_support_last(code)
            media_label = MEDIA_LABELS.get(code, code)
            ch_a = process_chart_media_annual(
                original[f"ppt/charts/chart{chart_annual_id}.xml"],
                [int(y) for y in years], [mt.get(y,0) for y in years], media_label
            )
            ch_p = process_chart_pie(
                original[f"ppt/charts/chart{chart_pie_id}.xml"],
                list(ts.index), list(ts.values), MEDIA_SHORT.get(code,code)
            )
            ch_n = process_chart_top_ann(
                original[f"ppt/charts/chart{chart_ann_id}.xml"],
                list(ta.index), list(ta.values), f"Top {MEDIA_SHORT.get(code,code)}"
            )
            return ch_a, ch_p, ch_n

        # Slide4 = AF/OOH: chart7=annual, chart8=top ann, chart9=pie
        ch7, ch9, ch8 = make_media_charts("AF", 7, 9, 8) if "AF" in medias else (
            original["ppt/charts/chart7.xml"], original["ppt/charts/chart9.xml"], original["ppt/charts/chart8.xml"])

        # Slide5 = TV: chart10=annual, chart11=pie, chart12=top ann
        ch10, ch11, ch12 = make_media_charts("TV", 10, 11, 12) if "TV" in medias else (
            original["ppt/charts/chart10.xml"], original["ppt/charts/chart11.xml"], original["ppt/charts/chart12.xml"])

        # Slide6 = RD: chart15=annual, chart13=pie, chart14=top ann
        ch15, ch13, ch14 = make_media_charts("RD", 15, 13, 14) if "RD" in medias else (
            original["ppt/charts/chart15.xml"], original["ppt/charts/chart13.xml"], original["ppt/charts/chart14.xml"])

        # ── Textes slides fixes ───────────────────────────────────────
        slide_texts = {
            "ppt/slides/slide1.xml": {
                "Title 1": f"Media Review\n{label}\n| {yrange}",
            },
            "ppt/slides/slide2.xml": {
                "Text 0": f"Investissements média — {label}",
                "Text 1": f"{yrange} | Millions MAD | Source : Imperium",
                "Text 3": comments.get("slide2_headline","") + "\n\n" + comments.get("slide2_global",""),
            },
            "ppt/slides/slide3.xml": {
                "TextBox 1": f"Investissement média par annonceur — {label}",
                "TextBox 2": f"Classement annonceurs | Millions MAD | {' - '.join(str(int(y)) for y in years[-3:])} | Source : Imperium",
                "ZoneTexte 10": comments.get("slide3_annonceurs",""),
            },
            "ppt/slides/slide4.xml": {
                "TextBox 2": f"Investissement média AF (OOH) — {label}",
                "TextBox 3": f"FY {yrange} | Millions MAD | Source : Imperium",
                "TextBox 9": f"Répartition {int(year_last)} par ville",
                "TextBox 10": f"Top annonceurs OOH (FY {int(year_last)})",
                "TextBox 15": comments.get("slide_af",""),
            },
            "ppt/slides/slide5.xml": {
                "TextBox 2": f"Investissement média TV — {label}",
                "TextBox 3": f"FY {yrange} | Millions MAD | Source : Imperium",
                "TextBox 8": "Investissements TV",
                "TextBox 9": f"Répartition {int(year_last)} par support",
                "TextBox 10": f"Top annonceurs TV (FY {int(year_last)})",
                "TextBox 15": comments.get("slide_tv",""),
            },
            "ppt/slides/slide6.xml": {
                "TextBox 1": f"Investissement média Radio — {label}",
                "TextBox 2": f"{yrange} | Millions MAD | Source : Imperium",
                "TextBox 7": "Investissements Radio",
                "TextBox 8": f"Split par station — FY {int(year_last)}",
                "TextBox 9": f"Top annonceurs RD — FY {int(year_last)}",
                "TextBox 14": comments.get("slide_rd",""),
            },
        }

        # ── Slides supplémentaires pour médias hors AF/TV/RD ──────────
        extra_medias = [m for m in medias if m not in ("AF","TV","RD")]
        extra_files: dict[str, bytes] = {}
        extra_slide_ids = []  # (slide_num, slide_path)

        next_slide_num = 7
        next_chart_id  = 16
        next_emb_id    = 15

        for media_code in extra_medias:
            cloned = clone_media_slide(
                original, media_code, next_slide_num,
                next_chart_id, next_emb_id,
                calc, comments, label
            )
            extra_files.update(cloned)
            extra_slide_ids.append((next_slide_num, f"ppt/slides/slide{next_slide_num}.xml"))
            next_slide_num += 3
            next_chart_id  += 3
            next_emb_id    += 3

        # ── Construire le nouveau ZIP ─────────────────────────────────
        chart_updates = {
            "ppt/charts/chart1.xml": ch1,
            "ppt/charts/chart2.xml": ch2,
            "ppt/charts/chart3.xml": ch3,
            "ppt/charts/chart7.xml": ch7,
            "ppt/charts/chart8.xml": ch8,
            "ppt/charts/chart9.xml": ch9,
            "ppt/charts/chart10.xml": ch10,
            "ppt/charts/chart11.xml": ch11,
            "ppt/charts/chart12.xml": ch12,
            "ppt/charts/chart13.xml": ch13,
            "ppt/charts/chart14.xml": ch14,
            "ppt/charts/chart15.xml": ch15,
        }
        if ch4: chart_updates["ppt/charts/chart4.xml"] = ch4
        if ch5: chart_updates["ppt/charts/chart5.xml"] = ch5
        if ch6: chart_updates["ppt/charts/chart6.xml"] = ch6

        out_zip = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(self.template_bytes_raw), "r") as zin:
            with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zout:
                for item in zin.infolist():
                    data = zin.read(item.filename)

                    if item.filename in chart_updates:
                        data = chart_updates[item.filename]
                    elif item.filename in slide_texts:
                        try: data = update_slide_texts(data, slide_texts[item.filename])
                        except: pass

                    # Mettre à jour presentation.xml si slides extra
                    elif item.filename == "ppt/presentation.xml" and extra_slide_ids:
                        data = _add_slides_to_presentation(data, extra_slide_ids)

                    # Mettre à jour [Content_Types].xml si slides extra
                    elif item.filename == "[Content_Types].xml" and extra_slide_ids:
                        data = _add_content_types(data, extra_slide_ids)

                    # Mettre à jour ppt/_rels/presentation.xml.rels si slides extra
                    elif item.filename == "ppt/_rels/presentation.xml.rels" and extra_slide_ids:
                        data = _add_prs_rels(data, extra_slide_ids)

                    zout.writestr(item, data)

                # Ajouter les nouveaux fichiers (slides extra + leurs charts + rels)
                for path, content in extra_files.items():
                    zout.writestr(path, content)

        return out_zip.getvalue()


def _add_slides_to_presentation(prs_xml: bytes, extra_slide_ids: list) -> bytes:
    """Ajoute les nouvelles slides dans sldIdLst de presentation.xml."""
    root = etree.fromstring(prs_xml)
    PRS_NS = "http://schemas.openxmlformats.org/presentationml/2006/main"
    sld_id_lst = root.find(f"{{{PRS_NS}}}sldIdLst")
    if sld_id_lst is None:
        return prs_xml

    # Trouver le max id existant
    max_id = max(int(s.get("id",256)) for s in sld_id_lst) + 1

    for slide_num, _ in extra_slide_ids:
        sld_id = etree.SubElement(sld_id_lst, f"{{{PRS_NS}}}sldId")
        sld_id.set("id", str(max_id))
        sld_id.set(f"{{{RNS}}}id", f"rId_extra_{slide_num}")
        max_id += 1

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


def _add_content_types(ct_xml: bytes, extra_slide_ids: list) -> bytes:
    """Ajoute les Content-Types pour les nouvelles slides."""
    root = etree.fromstring(ct_xml)
    SLIDE_CT = "application/vnd.openxmlformats-officedocument.presentationml.slide+xml"
    for slide_num, slide_path in extra_slide_ids:
        # Vérifier si déjà présent
        existing = [el for el in root if el.get("PartName") == f"/{slide_path}"]
        if not existing:
            override = etree.SubElement(root, f"{{{CTNS}}}Override")
            override.set("PartName", f"/{slide_path}")
            override.set("ContentType", SLIDE_CT)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


def _add_prs_rels(rels_xml: bytes, extra_slide_ids: list) -> bytes:
    """Ajoute les relations dans ppt/_rels/presentation.xml.rels."""
    root = etree.fromstring(rels_xml)
    RELS_NS2 = "http://schemas.openxmlformats.org/package/2006/relationships"
    SLIDE_TYPE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide"
    for slide_num, slide_path in extra_slide_ids:
        rid = f"rId_extra_{slide_num}"
        rel = etree.SubElement(root, f"{{{RELS_NS2}}}Relationship")
        rel.set("Id", rid)
        rel.set("Type", SLIDE_TYPE)
        rel.set("Target", slide_path.replace("ppt/",""))
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)
