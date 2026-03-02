import io
import re
import zipfile
import unicodedata
from datetime import datetime, date, time, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
import openpyxl
from copy import copy as pycopy

# ✅ AJOUT (pour bordures + gris sur Total)
from openpyxl.styles import PatternFill, Border, Side

# =========================
# CONFIG (2 modes)
# =========================
APP_DIR = Path(__file__).resolve().parent

TEMPLATE_IMPERIUM_PATH = APP_DIR / "TEMPLATE_SUIVI_FINAL.xlsx"
TEMPLATE_YUMI_PATH = APP_DIR / "SUIVI GATO.xlsx"   # <-- template YUMI (commité sur GitHub)

HEADER_ROW = 9
DATA_START_ROW = 10
DECALAGE_MINUTES = 45
TV_DAY_CUTOFF_HOUR = 6  # 00:00-05:59 => fin journée TV => +1440

# -------------------------
# IMPERIUM (inchangé)
# -------------------------
FINAL_COLUMNS_IMPERIUM = [
    "datep",
    "supportp",
    "heure de diffusion",
    "Code PM",
    "Commentaire",
    "Message",
    "Produit",
    "Marque",
    "RaisonSociale",
    "FormatSec",
    "Avant",
    "Apres",
    "rangE",
    "encombE",
]

# -------------------------
# YUMI (exact template GATO)
# -------------------------
FINAL_COLUMNS_YUMI = [
    "Date",
    "Chaîne",
    "N° Mois",
    "Année",
    "Annonceur",
    "Marque",
    "Produit",
    "H.Début",
    "H.Fin",
    "Durée",
    "Code Ecran",
    "Code Ecran PM",
    "Commentaire",
    "Programme après",
    "Programme avant",
    "Position",
    "Rang",
    "Encombrement",
    "Storyboard",
    "TM%",
    "TME",
]

# =========================
# UI config
# =========================
st.set_page_config(page_title="Suivi Pige — Automatisation (PM 2026 unique)", page_icon="📊", layout="wide")

st.markdown("""
<style>
.main { background-color: #f8fafc; }
.stButton>button {
    width: 100%;
    border-radius: 8px;
    height: 3.5em;
    background-color: #7289DA;
    color: white;
    font-weight: bold;
    border: none;
}
.stDownloadButton>button {
    width: 100%;
    border-radius: 8px;
    background-color: #43b581 !important;
    color: white !important;
}
</style>
""", unsafe_allow_html=True)

# =========================
# STATE
# =========================
if "client_files" not in st.session_state:
    st.session_state.client_files = None
if "zip_bytes" not in st.session_state:
    st.session_state.zip_bytes = None
if "last_run_info" not in st.session_state:
    st.session_state.last_run_info = None

# =========================
# Utils
# =========================

def norm_txt(x):
    if x is None:
        return ""
    s = str(x).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_brand(name: str) -> str:
    s = norm_txt(name)
    s = re.sub(r"\bPM\b", " ", s)
    s = re.sub(r"\bRAMADAN\b", " ", s)
    s = re.sub(r"\bTV\b|\bRADIO\b|\bOOH\b", " ", s)
    s = re.sub(r"\bV\d+\b", " ", s)
    s = re.sub(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b", " ", s)
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ✅ brand key sans espaces (ORBLANC == OR BLANC)
def brand_key(name: str) -> str:
    return normalize_brand(name).replace(" ", "")

def normalize_support(sup: str) -> str:
    s = norm_txt(sup)
    s = s.replace(" ", "")
    s = re.sub(r"[^A-Z0-9]+", "", s)
    s = s.replace("TV", "")
    if s in ("AOULA", "ALAOUOLA", "ALAOULA"):
        return "ALAOULA"
    return s

def safe_sheet_name(s: str) -> str:
    s = re.sub(r"[:\\/*?\[\]]", " ", str(s))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:31] if len(s) > 31 else s

def to_excel_time(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except:
        pass

    if isinstance(val, time):
        return val
    if isinstance(val, datetime):
        return val.time()
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.to_pydatetime().time()

    if isinstance(val, (float, int)):
        seconds = int(round(float(val) * 86400))
        seconds = max(0, min(seconds, 86399))
        return time(seconds // 3600, (seconds % 3600) // 60, seconds % 60)

    try:
        s = str(val).strip().replace("h", ":").replace("H", ":")
        t = pd.to_datetime(s, errors="coerce")
        if pd.isna(t):
            return None
        return t.to_pydatetime().time()
    except:
        return None

def parse_codepm_time(code_pm: str):
    """
    Retourne: (match_time, overnight_bool, tv_minutes)
    tv_minutes garde hh>=24 => 2500R -> 1500
    """
    if code_pm is None:
        return None, False, None

    s = str(code_pm).strip().upper()
    m = re.match(r"(\d{3,4})", s)
    if not m:
        return None, False, None

    hhmm = m.group(1)
    if len(hhmm) == 3:
        hh = int(hhmm[0])
        mm = int(hhmm[1:])
    else:
        hh = int(hhmm[:2])
        mm = int(hhmm[2:])

    if mm < 0 or mm > 59:
        return None, False, None

    tv_minutes = hh * 60 + mm
    overnight = hh >= 24
    hh_mod = hh % 24
    return time(hh_mod, mm, 0), overnight, tv_minutes

def real_tv_minutes(t: time, cutoff_hour: int = TV_DAY_CUTOFF_HOUR):
    if t is None:
        return None
    m = t.hour * 60 + t.minute
    if t.hour < cutoff_hour:
        m += 1440
    return m

def make_zip(files: dict[str, bytes]) -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in files.items():
            z.writestr(name, data)
    return bio.getvalue()

def find_column(df: pd.DataFrame, candidates: list[str]):
    cols = {c: norm_txt(c) for c in df.columns}
    for c, cn in cols.items():
        for cand in candidates:
            if norm_txt(cand) in cn:
                return c
    return None

# =========================
# Template loader (2 modes)
# =========================
def load_template_workbook(mode: str) -> openpyxl.Workbook:
    if mode == "Suivi YUMI":
        return openpyxl.load_workbook(TEMPLATE_YUMI_PATH)
    return openpyxl.load_workbook(TEMPLATE_IMPERIUM_PATH)

# =========================
# PM 2026.xlsx parsing (one workbook, many sheets) - commun
# =========================
def parse_sheet_name(sheet_name: str, known_support_norms: set[str] | None = None):
    tokens = sheet_name.strip().split()
    if not tokens:
        return None, None, None

    vocab = set(known_support_norms or set())
    vocab |= {"2M", "MBC5", "ALAOULA"}

    best = None
    for k in range(1, min(4, len(tokens)) + 1):
        sup = " ".join(tokens[-k:])
        sup_norm = normalize_support(sup)
        if sup_norm in vocab:
            brand = " ".join(tokens[:-k]).strip()
            if brand:
                best = (brand, sup, sup_norm)
    if best:
        return best

    sup = tokens[-1]
    brand = " ".join(tokens[:-1]).strip() or sheet_name
    return brand, sup, normalize_support(sup)

def read_pm_2026_workbook(pm_bytes: bytes, known_support_norms: set[str]) -> pd.DataFrame:
    wb = openpyxl.load_workbook(io.BytesIO(pm_bytes), data_only=True)

    recs = []
    for sh in wb.sheetnames:
        ws = wb[sh]
        brand, sup_disp, sup_norm = parse_sheet_name(sh, known_support_norms)
        if not brand:
            continue

        for r in range(2, ws.max_row + 1):
            d = ws.cell(r, 1).value
            code = ws.cell(r, 2).value

            if d is None and code is None:
                continue
            if code is None or str(code).strip() == "":
                continue

            d_parsed = pd.to_datetime(d, errors="coerce", dayfirst=True)
            if pd.isna(d_parsed):
                continue
            d_date = d_parsed.date()

            codepm = str(code).strip()
            _, overnight, tvm = parse_codepm_time(codepm)

            recs.append({
                "PM_FILE_BRAND": brand,
                "PM_FILE_BRAND_N": brand_key(brand),
                "Date": pd.to_datetime(d_date),
                "date_only": d_date,
                "supportp": sup_disp,
                "support_norm": sup_norm,
                "Code PM": codepm,
                "Overnight": overnight,
                "PM_TV_MIN": tvm,
            })

    return pd.DataFrame(recs)

# =========================
# Data Imperium -> DF suivi (INCHANGÉ)
# =========================
def build_final_df_from_imperium(df_imp: pd.DataFrame, max_date: date) -> pd.DataFrame:
    col_date = find_column(df_imp, ["datep", "date"])
    col_sup  = find_column(df_imp, ["supportp", "support", "chaine", "station"])
    col_time = find_column(df_imp, ["heurep", "heure"])
    col_mar  = find_column(df_imp, ["marque"])
    if not all([col_date, col_sup, col_time, col_mar]):
        raise ValueError("DATA IMPERIUM: colonnes minimales manquantes (datep/supportp/heurep/marque).")

    df = df_imp.copy()
    df["datep"] = pd.to_datetime(df[col_date], errors="coerce")
    df = df[df["datep"].dt.date <= max_date]

    out = pd.DataFrame()
    out["datep"] = df["datep"]
    out["supportp"] = df[col_sup].astype(str).str.strip()
    out["support_norm"] = out["supportp"].apply(normalize_support)
    out["heure de diffusion"] = df[col_time]
    out["Marque"] = df[col_mar].astype(str).str.strip()
    out["Marque_norm"] = out["Marque"].apply(brand_key)

    out["Message"] = df[find_column(df_imp, ["message", "storyboard"])] if find_column(df_imp, ["message", "storyboard"]) else None
    out["Produit"] = df[find_column(df_imp, ["produit"])] if find_column(df_imp, ["produit"]) else None
    out["RaisonSociale"] = df[find_column(df_imp, ["raisonsociale", "raison sociale"])] if find_column(df_imp, ["raisonsociale", "raison sociale"]) else None
    out["FormatSec"] = df[find_column(df_imp, ["formatsec", "format"])] if find_column(df_imp, ["formatsec", "format"]) else None
    out["Avant"] = df[find_column(df_imp, ["avant"])] if find_column(df_imp, ["avant"]) else None
    out["Apres"] = df[find_column(df_imp, ["apres", "après"])] if find_column(df_imp, ["apres", "après"]) else None
    out["rangE"] = df[find_column(df_imp, ["range", "rang"])] if find_column(df_imp, ["range", "rang"]) else None
    out["encombE"] = df[find_column(df_imp, ["encombe", "encombrement"])] if find_column(df_imp, ["encombe", "encombrement"]) else None

    out["Code PM"] = None
    out["Commentaire"] = None
    return out

# =========================
# Data YUMI -> DF suivi (copie toutes colonnes template)
# =========================
def build_final_df_from_yumi(df_yumi: pd.DataFrame, max_date: date) -> pd.DataFrame:
    col_date   = find_column(df_yumi, ["date"])
    col_chaine = find_column(df_yumi, ["chaine", "chaîne", "support"])
    col_hdeb   = find_column(df_yumi, ["h.debut", "h début", "heure debut", "hdeb", "début"])
    col_marque = find_column(df_yumi, ["marque"])
    if not all([col_date, col_chaine, col_hdeb, col_marque]):
        raise ValueError("DATA YUMI: colonnes minimales manquantes (Date/Chaîne/H.Début/Marque).")

    df = df_yumi.copy()
    df["__Date__"] = pd.to_datetime(df[col_date], errors="coerce")
    df = df[df["__Date__"].dt.date <= max_date]

    out = pd.DataFrame()
    for col in FINAL_COLUMNS_YUMI:
        if col == "Date":
            out[col] = df["__Date__"]
        elif col == "Chaîne":
            out[col] = df[col_chaine].astype(str).str.strip()
        elif col == "H.Début":
            out[col] = df[col_hdeb]
        else:
            if col in df.columns:
                out[col] = df[col]
            else:
                fallback = find_column(df, [col])
                out[col] = df[fallback] if fallback else None

    out["support_norm"] = out["Chaîne"].apply(normalize_support)
    out["Marque_norm"] = out["Marque"].apply(brand_key)

    out["Code Ecran PM"] = None
    out["Commentaire"] = None

    return out

# =========================
# Insert minimal rows (Imperium)
# =========================
def insert_minimal_row_imperium(d: date, sup_display: str, codepm: str, comment: str):
    row = {c: None for c in FINAL_COLUMNS_IMPERIUM}
    row["datep"] = d
    row["supportp"] = sup_display
    row["Code PM"] = codepm
    row["Commentaire"] = comment
    return row

# =========================
# Matching IMPERIUM (INCHANGÉ)
# =========================
def fill_codepm_commentaire_per_client(df_client: pd.DataFrame, pm_client: pd.DataFrame, max_date: date):
    df = df_client.copy()
    df["date_only"] = pd.to_datetime(df["datep"], errors="coerce").dt.date
    df["t_real"] = df["heure de diffusion"].apply(to_excel_time)

    if pm_client is None or pm_client.empty:
        base = df.copy()
        for col in FINAL_COLUMNS_IMPERIUM:
            if col not in base.columns:
                base[col] = None
        return base[FINAL_COLUMNS_IMPERIUM]

    pm = pm_client.copy()
    pm = pm[pm["date_only"].notna()]
    pm = pm[pm["date_only"] <= max_date]

    out_all = []
    backlog = {}

    supports_real = set(df["support_norm"].dropna().unique())
    supports_pm = set(pm["support_norm"].dropna().unique())
    all_supports = sorted(list(supports_real | supports_pm))

    for sn in all_supports:
        backlog.setdefault(sn, 0)

        real_s = df[df["support_norm"] == sn].copy()
        pm_s = pm[pm["support_norm"] == sn].copy()

        if not real_s.empty:
            sup_display = str(real_s.iloc[0]["supportp"])
        elif not pm_s.empty:
            sup_display = str(pm_s.iloc[0]["supportp"])
        else:
            sup_display = str(sn)

        dates_real = set(real_s["date_only"].dropna().unique())
        dates_pm = set(pm_s["date_only"].dropna().unique())
        all_dates = sorted(list(dates_real | dates_pm))

        def pick_closest(avail, t_real):
            if avail.empty:
                return None, None
            if t_real is None:
                pick = avail.iloc[0]
                return pick, None
            rt = real_tv_minutes(t_real)
            tmp = avail.copy()
            tmp["diff"] = tmp["PM_TV_MIN"].apply(lambda pm_m: abs(rt - pm_m) if pm_m is not None else 999999)
            pick = tmp.sort_values("diff").iloc[0]
            return pick, float(pick["diff"])

        for d in all_dates:
            if d > max_date:
                continue

            real_day = real_s[real_s["date_only"] == d].copy()
            real_day["_rt"] = real_day["t_real"].apply(lambda t: real_tv_minutes(t))
            real_day = real_day.sort_values("_rt", na_position="last").drop(columns=["_rt"], errors="ignore")

            pm_day = pm_s[pm_s["date_only"] == d].copy().sort_values("PM_TV_MIN", na_position="last")

            real_n = len(real_day)
            pm_n = len(pm_day)

            used = set()
            filled_rows = []
            inserted_rows = []

            if real_n == 0 and pm_n > 0:
                for _, p in pm_day.iterrows():
                    inserted_rows.append(insert_minimal_row_imperium(d, sup_display, p["Code PM"], "Non diffusé"))
                backlog[sn] += pm_n

            elif real_n == pm_n:
                for i in range(real_n):
                    r = real_day.iloc[i].copy()
                    p = pm_day.iloc[i]
                    r["Code PM"] = p["Code PM"]
                    diff = None
                    if r["t_real"] is not None and p["PM_TV_MIN"] is not None:
                        diff = abs(real_tv_minutes(r["t_real"]) - p["PM_TV_MIN"])
                    if (not bool(p.get("Overnight", False))) and diff is not None and diff > DECALAGE_MINUTES:
                        r["Commentaire"] = "Décalage"
                    else:
                        r["Commentaire"] = None
                    filled_rows.append(r)

            elif real_n < pm_n:
                for i in range(real_n):
                    r = real_day.iloc[i].copy()
                    avail = pm_day.loc[~pm_day.index.isin(used)]
                    pick, diff = pick_closest(avail, r["t_real"])
                    if pick is not None:
                        used.add(pick.name)
                        r["Code PM"] = pick["Code PM"]
                        if (not bool(pick.get("Overnight", False))) and diff is not None and diff > DECALAGE_MINUTES:
                            r["Commentaire"] = "Décalage"
                        else:
                            r["Commentaire"] = None
                    else:
                        r["Code PM"] = None
                        r["Commentaire"] = "Passage supplémentaire"
                    filled_rows.append(r)

                remaining = pm_day.loc[~pm_day.index.isin(used)]
                for _, p in remaining.iterrows():
                    inserted_rows.append(insert_minimal_row_imperium(d, sup_display, p["Code PM"], "Non diffusé"))
                backlog[sn] += len(remaining)

            else:
                for i in range(real_n):
                    r = real_day.iloc[i].copy()
                    if i < pm_n:
                        p = pm_day.iloc[i]
                        r["Code PM"] = p["Code PM"]
                        diff = None
                        if r["t_real"] is not None and p["PM_TV_MIN"] is not None:
                            diff = abs(real_tv_minutes(r["t_real"]) - p["PM_TV_MIN"])
                        if (not bool(p.get("Overnight", False))) and diff is not None and diff > DECALAGE_MINUTES:
                            r["Commentaire"] = "Décalage"
                        else:
                            r["Commentaire"] = None
                    else:
                        r["Code PM"] = None
                        if backlog[sn] > 0:
                            r["Commentaire"] = "Compensation"
                            backlog[sn] -= 1
                        else:
                            r["Commentaire"] = "Passage supplémentaire"
                    filled_rows.append(r)

            df_filled = pd.DataFrame(filled_rows) if filled_rows else pd.DataFrame()
            df_insert = pd.DataFrame(inserted_rows) if inserted_rows else pd.DataFrame(columns=FINAL_COLUMNS_IMPERIUM)

            def sort_key_tv(row):
                t = to_excel_time(row.get("heure de diffusion"))
                if t is not None:
                    return real_tv_minutes(t)
                _, _, tvm = parse_codepm_time(row.get("Code PM"))
                return tvm

            if not df_filled.empty:
                df_filled["_sort_t"] = df_filled.apply(lambda r: sort_key_tv(r), axis=1)
            if not df_insert.empty:
                df_insert["_sort_t"] = df_insert.apply(lambda r: sort_key_tv(r), axis=1)

            out_day = []
            if not df_filled.empty:
                df_filled["supportp"] = sup_display
                out_day.append(df_filled)
            if not df_insert.empty:
                out_day.append(df_insert)

            out_day = pd.concat(out_day, ignore_index=True) if out_day else pd.DataFrame(columns=FINAL_COLUMNS_IMPERIUM)
            out_day = out_day.sort_values("_sort_t", na_position="last").drop(columns=["_sort_t"], errors="ignore")
            out_all.append(out_day[FINAL_COLUMNS_IMPERIUM])

    return pd.concat(out_all, ignore_index=True)[FINAL_COLUMNS_IMPERIUM] if out_all else df[FINAL_COLUMNS_IMPERIUM]

# =========================
# Matching YUMI (même logique, colonnes YUMI)
# =========================
def fill_codeecranpm_commentaire_per_client_yumi(df_client: pd.DataFrame, pm_client: pd.DataFrame, max_date: date):
    df = df_client.copy()
    df["date_only"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["t_real"] = df["H.Début"].apply(to_excel_time)

    if pm_client is None or pm_client.empty:
        base = df.copy()
        for col in FINAL_COLUMNS_YUMI:
            if col not in base.columns:
                base[col] = None
        return base[FINAL_COLUMNS_YUMI]

    pm = pm_client.copy()
    pm = pm[pm["date_only"].notna()]
    pm = pm[pm["date_only"] <= max_date]

    out_all = []
    backlog = {}

    supports_real = set(df["support_norm"].dropna().unique())
    supports_pm = set(pm["support_norm"].dropna().unique())
    all_supports = sorted(list(supports_real | supports_pm))

    for sn in all_supports:
        backlog.setdefault(sn, 0)

        real_s = df[df["support_norm"] == sn].copy()
        pm_s = pm[pm["support_norm"] == sn].copy()

        if not real_s.empty:
            sup_display = str(real_s.iloc[0]["Chaîne"])
        elif not pm_s.empty:
            sup_display = str(pm_s.iloc[0]["supportp"])
        else:
            sup_display = str(sn)

        dates_real = set(real_s["date_only"].dropna().unique())
        dates_pm = set(pm_s["date_only"].dropna().unique())
        all_dates = sorted(list(dates_real | dates_pm))

        def pick_closest(avail, t_real):
            if avail.empty:
                return None, None
            if t_real is None:
                pick = avail.iloc[0]
                return pick, None
            rt = real_tv_minutes(t_real)
            tmp = avail.copy()
            tmp["diff"] = tmp["PM_TV_MIN"].apply(lambda pm_m: abs(rt - pm_m) if pm_m is not None else 999999)
            pick = tmp.sort_values("diff").iloc[0]
            return pick, float(pick["diff"])

        def insert_minimal_row_yumi(dte, chaine, codepm):
            # Ligne "Non diffusé" minimale + mois/année remplis
            row = {c: None for c in FINAL_COLUMNS_YUMI}
            dts = pd.to_datetime(dte)

            row["Date"] = dts
            row["Chaîne"] = chaine
            row["N° Mois"] = int(dts.month) if not pd.isna(dts) else None
            row["Année"] = int(dts.year) if not pd.isna(dts) else None

            row["Code Ecran PM"] = codepm
            row["Commentaire"] = "Non diffusé"
            return row

        for d in all_dates:
            if d > max_date:
                continue

            real_day = real_s[real_s["date_only"] == d].copy()
            real_day["_rt"] = real_day["t_real"].apply(lambda t: real_tv_minutes(t))
            real_day = real_day.sort_values("_rt", na_position="last").drop(columns=["_rt"], errors="ignore")

            pm_day = pm_s[pm_s["date_only"] == d].copy().sort_values("PM_TV_MIN", na_position="last")

            real_n = len(real_day)
            pm_n = len(pm_day)

            used = set()
            filled_rows = []
            inserted_rows = []

            if real_n == 0 and pm_n > 0:
                for _, p in pm_day.iterrows():
                    inserted_rows.append(insert_minimal_row_yumi(d, sup_display, p["Code PM"]))
                backlog[sn] += pm_n

            elif real_n == pm_n:
                for i in range(real_n):
                    r = real_day.iloc[i].copy()
                    p = pm_day.iloc[i]
                    r["Code Ecran PM"] = p["Code PM"]

                    diff = None
                    if r["t_real"] is not None and p["PM_TV_MIN"] is not None:
                        diff = abs(real_tv_minutes(r["t_real"]) - p["PM_TV_MIN"])
                    if (not bool(p.get("Overnight", False))) and diff is not None and diff > DECALAGE_MINUTES:
                        r["Commentaire"] = "Décalage"
                    else:
                        r["Commentaire"] = None

                    filled_rows.append(r)

            elif real_n < pm_n:
                for i in range(real_n):
                    r = real_day.iloc[i].copy()
                    avail = pm_day.loc[~pm_day.index.isin(used)]
                    pick, diff = pick_closest(avail, r["t_real"])
                    if pick is not None:
                        used.add(pick.name)
                        r["Code Ecran PM"] = pick["Code PM"]
                        if (not bool(pick.get("Overnight", False))) and diff is not None and diff > DECALAGE_MINUTES:
                            r["Commentaire"] = "Décalage"
                        else:
                            r["Commentaire"] = None
                    else:
                        r["Code Ecran PM"] = None
                        r["Commentaire"] = "Passage supplémentaire"
                    filled_rows.append(r)

                remaining = pm_day.loc[~pm_day.index.isin(used)]
                for _, p in remaining.iterrows():
                    inserted_rows.append(insert_minimal_row_yumi(d, sup_display, p["Code PM"]))
                backlog[sn] += len(remaining)

            else:
                for i in range(real_n):
                    r = real_day.iloc[i].copy()
                    if i < pm_n:
                        p = pm_day.iloc[i]
                        r["Code Ecran PM"] = p["Code PM"]

                        diff = None
                        if r["t_real"] is not None and p["PM_TV_MIN"] is not None:
                            diff = abs(real_tv_minutes(r["t_real"]) - p["PM_TV_MIN"])
                        if (not bool(p.get("Overnight", False))) and diff is not None and diff > DECALAGE_MINUTES:
                            r["Commentaire"] = "Décalage"
                        else:
                            r["Commentaire"] = None
                    else:
                        r["Code Ecran PM"] = None
                        if backlog[sn] > 0:
                            r["Commentaire"] = "Compensation"
                            backlog[sn] -= 1
                        else:
                            r["Commentaire"] = "Passage supplémentaire"
                    filled_rows.append(r)

            df_filled = pd.DataFrame(filled_rows) if filled_rows else pd.DataFrame()
            df_insert = pd.DataFrame(inserted_rows) if inserted_rows else pd.DataFrame(columns=FINAL_COLUMNS_YUMI)

            def sort_key_tv_yumi(row):
                t = to_excel_time(row.get("H.Début"))
                if t is not None:
                    return real_tv_minutes(t)
                _, _, tvm = parse_codepm_time(row.get("Code Ecran PM"))
                return tvm

            if not df_filled.empty:
                df_filled["_sort_t"] = df_filled.apply(lambda r: sort_key_tv_yumi(r), axis=1)
            if not df_insert.empty:
                df_insert["_sort_t"] = df_insert.apply(lambda r: sort_key_tv_yumi(r), axis=1)

            out_day = []
            if not df_filled.empty:
                df_filled["Chaîne"] = sup_display
                out_day.append(df_filled)
            if not df_insert.empty:
                out_day.append(df_insert)

            out_day = pd.concat(out_day, ignore_index=True) if out_day else pd.DataFrame(columns=FINAL_COLUMNS_YUMI)
            out_day = out_day.sort_values("_sort_t", na_position="last").drop(columns=["_sort_t"], errors="ignore")
            out_all.append(out_day[FINAL_COLUMNS_YUMI])

    out_df = pd.concat(out_all, ignore_index=True) if out_all else df.copy()
    return out_df[FINAL_COLUMNS_YUMI]

# =========================
# Styles (copie style ligne 10 du template) + Finalize
# =========================
def apply_row_style_from_template(style_row_cells, ws, row_idx, final_cols):
    for c in range(1, len(final_cols) + 1):
        src = style_row_cells[c - 1]
        dst = ws.cell(row_idx, c)
        dst._style = pycopy(src._style)
        dst.number_format = src.number_format
        dst.font = pycopy(src.font)
        dst.fill = pycopy(src.fill)
        dst.border = pycopy(src.border)
        dst.alignment = pycopy(src.alignment)
        dst.protection = pycopy(src.protection)

def finalize_sheet(ws, style_row_cells, final_cols, total_col_name: str):
    # enlever "Cible"
    ws["A6"].value = None

    ws["B4"].value = date.today()
    ws["B4"].number_format = "dd/mm/yyyy"
    ws["B5"].value = ws["H10"].value

    total_col_idx = 4
    if total_col_name in final_cols:
        total_col_idx = final_cols.index(total_col_name) + 1

    last_data_row = 9
    for r in range(ws.max_row, DATA_START_ROW - 1, -1):
        if (ws.cell(r, 1).value not in (None, "")) or (ws.cell(r, 2).value not in (None, "")) or (ws.cell(r, total_col_idx).value not in (None, "")):
            last_data_row = max(r, DATA_START_ROW)
            break
    if last_data_row < DATA_START_ROW:
        last_data_row = DATA_START_ROW

    count_vals = 0
    for r in range(DATA_START_ROW, last_data_row + 1):
        if ws.cell(r, total_col_idx).value not in (None, ""):
            count_vals += 1

    total_row = last_data_row + 1

    ws.insert_rows(total_row)
    apply_row_style_from_template(style_row_cells, ws, total_row, final_cols)

    ws.cell(total_row, 1).value = "Total"
    ws.cell(total_row, 2).value = count_vals

    grey_fill = PatternFill(fill_type="solid", fgColor="D9D9D9")
    thin = Side(style="thin", color="000000")
    thin_border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for col in (1, 2):
        cell = ws.cell(total_row, col)
        cell.fill = grey_fill
        cell.border = thin_border
        cell.font = pycopy(cell.font)
        cell.font = cell.font.copy(bold=True)

    empty_border = Border()
    empty_fill = PatternFill(fill_type=None)
    for col in range(3, len(final_cols) + 1):
        cell = ws.cell(total_row, col)
        cell.value = None
        cell.border = empty_border
        cell.fill = empty_fill

# =========================
# Build workbooks (générique)
# =========================
def build_client_workbook_from_template(template_wb: openpyxl.Workbook, client_name: str, df_client: pd.DataFrame, final_cols: list[str], mode: str) -> bytes:
    wb = template_wb
    template_ws = wb.worksheets[0]

    style_row_cells = [template_ws.cell(DATA_START_ROW, c) for c in range(1, len(final_cols) + 1)]

    def reset_sheet(ws):
        if ws.max_row > DATA_START_ROW:
            ws.delete_rows(DATA_START_ROW + 1, ws.max_row - DATA_START_ROW)
        for c in range(1, len(final_cols) + 1):
            ws.cell(DATA_START_ROW, c).value = None
        for c, col in enumerate(final_cols, start=1):
            ws.cell(HEADER_ROW, c).value = col
        ws.sheet_view.showGridLines = False

    reset_sheet(template_ws)

    if mode == "Suivi Imperium":
        supports = list(df_client["supportp"].dropna().unique())
        get_sub = lambda sup: df_client[df_client["supportp"] == sup].copy()
        sheet_title = lambda sup: safe_sheet_name(f"{client_name} - {str(sup).strip()}")
        total_col_name = "Code PM"
    else:
        supports = list(df_client["Chaîne"].dropna().unique())
        get_sub = lambda sup: df_client[df_client["Chaîne"] == sup].copy()
        sheet_title = lambda sup: safe_sheet_name(f"{client_name} - {str(sup).strip()}")
        total_col_name = "Code Ecran PM"

    if not supports:
        supports = ["Support"]

    for sup in supports:
        ws = wb.copy_worksheet(template_ws)
        ws.title = sheet_title(sup)
        reset_sheet(ws)

        sub = get_sub(sup)

        for i in range(len(sub)):
            r_idx = DATA_START_ROW + i
            if r_idx > DATA_START_ROW:
                ws.insert_rows(r_idx)
                apply_row_style_from_template(style_row_cells, ws, r_idx, final_cols)

            for c, col in enumerate(final_cols, start=1):
                val = sub.iloc[i][col] if col in sub.columns else None
                if col in ("heure de diffusion", "H.Début", "H.Fin"):
                    val = to_excel_time(val)
                ws.cell(r_idx, c).value = val

        finalize_sheet(ws, style_row_cells, final_cols, total_col_name=total_col_name)

    wb.remove(template_ws)
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

# =========================
# UI
# =========================
st.title("📊 Suivi Pige — Automatisation (PM 2026 unique)")

mode = st.radio("Type de suivi", ["Suivi Imperium", "Suivi YUMI"], horizontal=True)
st.caption("PM unique : 1 feuille = 1 client + 1 chaîne | Col A=Date | Col B=Ecran")

template_ok = False
try:
    _ = load_template_workbook(mode)
    template_ok = True
    st.success("Template OK ✅")
except Exception as e:
    st.error(f"Template introuvable ❌ : {e}")

data_in = st.file_uploader("1) Uploader DATA IMPERIUM" if mode == "Suivi Imperium" else "1) Uploader DATA YUMI", type=["xlsx"])
pm_file = st.file_uploader("2) Uploader PM 2026 (1 fichier)", type=["xlsx"])
max_date = st.date_input("3) Date max (N-1 par défaut)", value=date.today() - timedelta(days=1))

if st.button("Lancer la génération", use_container_width=True, disabled=(not template_ok)):
    if not data_in:
        st.warning("Upload DATA.")
    elif not pm_file:
        st.warning("Upload PM 2026.xlsx.")
    else:
        try:
            with st.spinner("Génération en cours..."):
                df_in = pd.read_excel(data_in)

                if mode == "Suivi Imperium":
                    df_all = build_final_df_from_imperium(df_in, max_date=max_date)
                    final_cols = FINAL_COLUMNS_IMPERIUM
                    client_col = "Marque"
                else:
                    df_all = build_final_df_from_yumi(df_in, max_date=max_date)
                    final_cols = FINAL_COLUMNS_YUMI
                    client_col = "Marque"

                known_supports = set(df_all["support_norm"].dropna().unique()) | {"2M", "MBC5", "ALAOULA"}
                pmv_all = read_pm_2026_workbook(pm_file.getvalue(), known_supports)

                client_files = {}

                for client_name in sorted(df_all[client_col].dropna().unique()):
                    df_client_raw = df_all[df_all[client_col] == client_name].copy()
                    client_norm = brand_key(client_name)

                    pm_client = pmv_all[pmv_all["PM_FILE_BRAND_N"] == client_norm].copy()
                    if pm_client.empty and not pmv_all.empty:
                        pm_client = pmv_all[
                            pmv_all["PM_FILE_BRAND_N"].apply(lambda x: (x in client_norm) or (client_norm in x))
                        ].copy()

                    if mode == "Suivi Imperium":
                        df_client_done = fill_codepm_commentaire_per_client(df_client_raw, pm_client, max_date=max_date)
                    else:
                        df_client_done = fill_codeecranpm_commentaire_per_client_yumi(df_client_raw, pm_client, max_date=max_date)

                    df_client_done = df_client_done.copy()
                    for helper in ("support_norm", "Marque_norm", "date_only", "t_real"):
                        df_client_done.drop(columns=[helper], inplace=True, errors="ignore")

                    template_wb = load_template_workbook(mode)
                    xlsx_bytes = build_client_workbook_from_template(template_wb, client_name, df_client_done, final_cols, mode=mode)
                    client_files[f"Suivi_{client_name}.xlsx"] = xlsx_bytes

                st.session_state.client_files = client_files
                st.session_state.zip_bytes = make_zip(client_files)
                st.session_state.last_run_info = f"{len(client_files)} fichiers générés"

        except Exception as e:
            st.error(f"Erreur: {e}")

if st.session_state.client_files:
    st.success(st.session_state.last_run_info)
    st.download_button(
        "📦 Télécharger ZIP",
        data=st.session_state.zip_bytes,
        file_name=f"Suivis_{mode.replace(' ', '_')}_{max_date.isoformat()}.zip",
        mime="application/zip",
        use_container_width=True
    )
    st.divider()
    cols = st.columns(3)
    for i, (fname, data) in enumerate(st.session_state.client_files.items()):
        with cols[i % 3]:
            st.download_button(
                f"📥 {fname}",
                data=data,
                file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
