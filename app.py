import io
import re
import zipfile
import unicodedata
from datetime import datetime, date, time, timedelta

import pandas as pd
import streamlit as st
import openpyxl
from copy import copy as pycopy

# ============================================================
# CONFIG
# ============================================================
TEMPLATE_PATH    = "TEMPLATE_SUIVI_FINAL.xlsx"
HEADER_ROW       = 9
DATA_START_ROW   = 10
DECALAGE_MINUTES = 45

# Colonnes internes utilisées pour le traitement
FINAL_COLUMNS = [
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

# ============================================================
# STREAMLIT CONFIG
# ============================================================
st.set_page_config(page_title="Suivi Pige — Automatisation", page_icon="📊", layout="wide")
st.markdown("""
<style>
.main { background-color: #f8fafc; }
.stButton>button {
    width:100%; border-radius:8px; height:3.5em;
    background-color:#7289DA; color:white; font-weight:bold; border:none;
}
.stDownloadButton>button {
    width:100%; border-radius:8px;
    background-color:#43b581 !important; color:white !important;
}
</style>
""", unsafe_allow_html=True)

# ============================================================
# SESSION STATE
# ============================================================
for key in ["client_files", "zip_bytes", "last_run_info"]:
    if key not in st.session_state:
        st.session_state[key] = None

# ============================================================
# UTILITAIRES TEXTE
# ============================================================

def norm_txt(x) -> str:
    """Normalise : strip, NFKD, majuscules, espaces simples."""
    if x is None:
        return ""
    s = str(x).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    return re.sub(r"\s+", " ", s).strip()


def normalize_brand(name: str) -> str:
    """Normalise un nom de marque pour le matching PM ↔ Imperium."""
    s = norm_txt(name)
    # Supprimer les mots parasites courants dans les noms de fichier PM
    for pat in [r"\bPM\b", r"\bRAMADAN\b", r"\bTV\b", r"\bRADIO\b", r"\bOOH\b",
                r"\bV\d+\b", r"\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b"]:
        s = re.sub(pat, " ", s)
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_channel(sup: str) -> str:
    """
    Normalise un nom de chaîne TV/Radio pour le matching PM ↔ Imperium.
    On retire les espaces et la ponctuation, mais on NE retire PAS 'TV' ni
    'RADIO' car ils font partie du nom de la chaîne (ex: HIT RADIO, 2MTV).
    """
    s = norm_txt(sup)
    s = re.sub(r"[^A-Z0-9]+", "", s)   # retire tout sauf alphanum
    return s


def safe_sheet_name(s: str) -> str:
    s = re.sub(r"[:\\/*?\[\]]", " ", str(s))
    return re.sub(r"\s+", " ", s).strip()[:31]


def find_column(df: pd.DataFrame, candidates: list):
    """Trouve la première colonne dont le nom normalisé contient un candidat."""
    for col in df.columns:
        cn = norm_txt(col)
        for cand in candidates:
            if norm_txt(cand) in cn:
                return col
    return None

# ============================================================
# UTILITAIRES TEMPS
# ============================================================

def to_excel_time(val):
    """Convertit n'importe quelle représentation d'heure en datetime.time."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if isinstance(val, time):
        return val
    if isinstance(val, datetime):
        return val.time()
    if isinstance(val, pd.Timestamp):
        return None if pd.isna(val) else val.to_pydatetime().time()
    try:
        import numpy as np
        if isinstance(val, np.datetime64):
            t = pd.to_datetime(val, errors="coerce")
            return None if pd.isna(t) else t.to_pydatetime().time()
    except Exception:
        pass
    if isinstance(val, (float, int)):
        sec = max(0, min(int(round(float(val) * 86400)), 86399))
        return time(sec // 3600, (sec % 3600) // 60, sec % 60)
    try:
        s = str(val).strip().replace("h", ":").replace("H", ":")
        t = pd.to_datetime(s, errors="coerce")
        return None if pd.isna(t) else t.to_pydatetime().time()
    except Exception:
        return None


def parse_codepm_time(code_pm: str):
    """
    Extrait l'heure d'un code PM de la forme '1600R', '2500R', etc.
    Retourne (datetime.time, overnight_bool).
    Les codes >= 2400 sont des passages overnight (lendemain).
    """
    if not code_pm:
        return None, False
    m = re.match(r"(\d{3,4})", str(code_pm).strip())
    if not m:
        return None, False
    hhmm = m.group(1)
    hh, mm = (int(hhmm[0]), int(hhmm[1:])) if len(hhmm) == 3 else (int(hhmm[:2]), int(hhmm[2:]))
    if not (0 <= mm <= 59):
        return None, False
    overnight = hh >= 24
    if overnight:
        hh -= 24
    return (time(hh, mm, 0), overnight) if 0 <= hh <= 23 else (None, False)


def minutes_diff(t1: time, t2: time):
    """Différence absolue en minutes entre deux datetime.time."""
    if t1 is None or t2 is None:
        return None
    a = t1.hour * 60 + t1.minute + t1.second / 60
    b = t2.hour * 60 + t2.minute + t2.second / 60
    return abs(a - b)

# ============================================================
# UTILITAIRES ZIP / TEMPLATE
# ============================================================

def make_zip(files: dict) -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in files.items():
            z.writestr(name, data)
    return bio.getvalue()


def load_template_workbook() -> openpyxl.Workbook:
    return openpyxl.load_workbook(TEMPLATE_PATH)

# ============================================================
# PARSING PM — feuille par feuille
# ============================================================

def is_date_like_any(v) -> bool:
    if isinstance(v, (datetime, date, pd.Timestamp)):
        return True
    s = str(v).strip()
    return bool(re.search(r"\d{4}-\d{2}-\d{2}", s)) or bool(re.search(r"\d{1,2}[/-]\d{1,2}", s))


def merged_value(ws, r: int, c: int):
    """Retourne la valeur d'une cellule en tenant compte des cellules fusionnées."""
    val = ws.cell(r, c).value
    if val is not None and str(val).strip() != "":
        return val
    for rng in ws.merged_cells.ranges:
        if rng.min_row <= r <= rng.max_row and rng.min_col <= c <= rng.max_col:
            return ws.cell(rng.min_row, rng.min_col).value
    return val


def extract_marque_from_filename(fname: str) -> str:
    """Extrait le nom de marque depuis le nom de fichier PM."""
    base = fname.rsplit(".", 1)[0].replace("_", " ")
    base = re.sub(r"^PM\s+", "", base, flags=re.IGNORECASE)
    base = re.split(r"\bRAMADAN\b", base, flags=re.IGNORECASE)[0]
    return re.sub(r"\s+", " ", base).strip()


def _parse_one_pm_sheet(ws, pm_brand: str) -> list:
    """
    Parse une feuille PM au format grille (colonnes = dates, lignes = chaînes/écrans).
    Retourne une liste de dicts. Retourne [] si la feuille n'est pas reconnue.

    Structure attendue :
      - Une ligne avec "CHAINE" et "ECRAN" dans les premières colonnes
      - Une ligne avec des dates en colonnes (après la ligne CHAINE/ECRAN)
      - Les lignes de données : colonne CHAINE = nom de la chaîne,
        colonne ECRAN = code PM (ex: 1600R), colonnes dates = 1 si prévu
    """
    max_col = min(ws.max_column, 240)
    max_row = ws.max_row

    # 1. Trouver la ligne d'en-têtes (contient CHAINE et ECRAN)
    meta_row = None
    for r in range(1, min(max_row, 120) + 1):
        vals = [norm_txt(ws.cell(r, c).value) for c in range(1, min(max_col, 90) + 1)]
        if any("CHAINE" in v for v in vals) and any("ECRAN" in v for v in vals):
            meta_row = r
            break
    if meta_row is None:
        return []

    # 2. Trouver les colonnes CHAINE et ECRAN
    chaine_col = ecran_col = None
    for c in range(1, min(max_col, 90) + 1):
        v = norm_txt(ws.cell(meta_row, c).value)
        if "CHAINE" in v and chaine_col is None:
            chaine_col = c
        if "ECRAN" in v and ecran_col is None:
            ecran_col = c
    if chaine_col is None or ecran_col is None:
        return []

    # 3. Trouver la ligne des dates (≥ 2 dates dans la ligne)
    date_row = None
    for r in range(meta_row, min(max_row, meta_row + 80) + 1):
        cnt = sum(1 for c in range(1, max_col + 1) if is_date_like_any(ws.cell(r, c).value))
        if cnt >= 2:
            date_row = r
            break
    if date_row is None:
        return []

    # 4. Construire la map colonne → date
    date_map = {}
    for c in range(1, max_col + 1):
        v = ws.cell(date_row, c).value
        if is_date_like_any(v):
            d = pd.to_datetime(v, errors="coerce")
            if pd.notna(d):
                date_map[c] = d.date()

    if not date_map:
        return []

    # 5. Parcourir les lignes de données
    recs      = []
    last_sup  = None
    pm_brand_n = normalize_brand(pm_brand)

    for r in range(date_row + 1, max_row + 1):
        raw_sup    = ws.cell(r, chaine_col).value
        raw_codepm = ws.cell(r, ecran_col).value

        # Arrêt sur ligne "TOTAL"
        if norm_txt(raw_sup).startswith("TOTAL"):
            break

        # Héritage du support si cellule vide (cellules fusionnées)
        if raw_sup is None or str(raw_sup).strip() == "":
            raw_sup = last_sup
        else:
            last_sup = raw_sup

        if raw_sup is None:
            continue
        if raw_codepm is None or str(raw_codepm).strip() == "":
            continue

        codepm_str = str(raw_codepm).strip()
        t_pm, overnight = parse_codepm_time(codepm_str)
        sup_str = str(raw_sup).strip()

        for c, d in date_map.items():
            cell_val = merged_value(ws, r, c)
            if cell_val is None:
                continue
            s = str(cell_val).strip().upper()
            # Ignorer les cellules vides, zéros ou marqueurs négatifs
            if s in ("", "0", ".", "-", "OFF", "NAN", "NONE", "X"):
                continue

            recs.append({
                "PM_FILE_BRAND":   pm_brand,
                "PM_FILE_BRAND_N": pm_brand_n,
                "date_only":       d,
                "supportp_pm":     sup_str,            # nom brut de la chaîne dans le PM
                "channel_norm":    normalize_channel(sup_str),  # clé de matching
                "Code PM":         codepm_str,
                "Heure_PM":        t_pm,
                "Overnight":       overnight,
            })

    return recs


def pm_grid_to_vertical_openpyxl(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Lit TOUTES les feuilles du fichier PM (1 feuille = 1 chaîne).
    Concatène les résultats. Les feuilles non reconnues sont ignorées.
    """
    wb       = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    pm_brand = extract_marque_from_filename(filename)
    all_recs = []

    for sheet_name in wb.sheetnames:
        ws   = wb[sheet_name]
        recs = _parse_one_pm_sheet(ws, pm_brand)
        all_recs.extend(recs)

    if not all_recs:
        raise ValueError(
            f"PM '{filename}' : aucune feuille reconnue parmi {wb.sheetnames}. "
            "Vérifier que les colonnes CHAINE et ECRAN sont présentes."
        )

    df = pd.DataFrame(all_recs)
    # Dédoublonnage : même chaîne + même date + même code PM
    df = df.drop_duplicates(subset=["PM_FILE_BRAND_N", "channel_norm", "date_only", "Code PM"])
    return df

# ============================================================
# PARSING DATA IMPERIUM
# ============================================================

def build_final_df_from_imperium(df_imp: pd.DataFrame, max_date: date) -> pd.DataFrame:
    """
    Construit le DataFrame de base depuis le fichier Data Imperium.
    Ajoute les colonnes de normalisation nécessaires au matching.
    """
    col_date = find_column(df_imp, ["datep", "date"])
    col_sup  = find_column(df_imp, ["supportp", "support", "chaine", "station"])
    col_time = find_column(df_imp, ["heurep", "heure"])
    col_mar  = find_column(df_imp, ["marque"])

    if not all([col_date, col_sup, col_time, col_mar]):
        raise ValueError(
            "DATA IMPERIUM : colonnes minimales manquantes. "
            f"Trouvées : date={col_date}, support={col_sup}, heure={col_time}, marque={col_mar}"
        )

    df = df_imp.copy()
    df["datep"] = pd.to_datetime(df[col_date], errors="coerce")
    df = df[df["datep"].dt.date <= max_date].copy()

    out = pd.DataFrame(index=df.index)
    out["datep"]              = df["datep"]
    out["supportp"]           = df[col_sup].astype(str).str.strip()
    out["channel_norm"]       = out["supportp"].apply(normalize_channel)  # clé de matching
    out["heure de diffusion"] = df[col_time]
    out["Marque"]             = df[col_mar].astype(str).str.strip()
    out["Marque_norm"]        = out["Marque"].apply(normalize_brand)

    for alias, candidates in [
        ("Message",       ["message", "storyboard"]),
        ("Produit",       ["produit"]),
        ("RaisonSociale", ["raisonsociale", "raison sociale"]),
        ("FormatSec",     ["formatsec", "format"]),
        ("Avant",         ["avant"]),
        ("Apres",         ["apres", "apres", "après"]),
        ("rangE",         ["range", "rang"]),
        ("encombE",       ["encombe", "encombrement"]),
    ]:
        c = find_column(df_imp, candidates)
        out[alias] = df[c].values if c else None

    out["Code PM"]     = None
    out["Commentaire"] = None
    return out.reset_index(drop=True)

# ============================================================
# MATCHING MARQUE : PM → CLIENT IMPERIUM
# ============================================================

def match_pm_to_client(client_norm: str, pmv_all: pd.DataFrame) -> pd.DataFrame:
    """
    Retourne les lignes PM correspondant à un client Imperium.
    Matching multi-niveaux sur le nom normalisé de la marque (extrait du nom de fichier PM).

    Niveau 1 : égalité exacte normalisée
    Niveau 2 : inclusion (l'un contient l'autre, longueur ≥ 4)
    Niveau 3 : tokens communs (≥ 2 tokens de ≥ 3 chars)
    """
    if pmv_all.empty or not client_norm:
        return pd.DataFrame()

    # Niveau 1
    res = pmv_all[pmv_all["PM_FILE_BRAND_N"] == client_norm]
    if not res.empty:
        return res.copy()

    # Niveau 2
    if len(client_norm) >= 4:
        res = pmv_all[pmv_all["PM_FILE_BRAND_N"].apply(
            lambda x: len(x) >= 4 and (x in client_norm or client_norm in x)
        )]
        if not res.empty:
            return res.copy()

    # Niveau 3
    client_tokens = {t for t in client_norm.split() if len(t) >= 3}
    if client_tokens:
        def score(x):
            return len(client_tokens & {t for t in x.split() if len(t) >= 3})
        scores = pmv_all["PM_FILE_BRAND_N"].apply(score)
        res = pmv_all[scores >= 2]
        if not res.empty:
            return res.copy()

    return pd.DataFrame()

# ============================================================
# MATCHING CHAÎNE : PM ↔ IMPERIUM
# ============================================================

def match_channel(imp_channel_norm: str, pm_channels: list) -> str | None:
    """
    Trouve la chaîne PM la plus proche d'une chaîne Imperium.

    Stratégie :
      1. Égalité exacte normalisée
      2. L'un contient l'autre (robuste aux préfixes/suffixes mineurs)
      3. Score de caractères communs (pour les cas comme "2M" ↔ "2MFRANCE")

    Retourne le channel_norm PM matché, ou None si aucun match.
    """
    if not imp_channel_norm or not pm_channels:
        return None

    # 1. Exacte
    if imp_channel_norm in pm_channels:
        return imp_channel_norm

    # 2. Inclusion
    for ch in pm_channels:
        if ch in imp_channel_norm or imp_channel_norm in ch:
            return ch

    # 3. Préfixe commun (au moins 3 chars)
    best_ch, best_score = None, 0
    for ch in pm_channels:
        # longueur du préfixe commun
        common = 0
        for a, b in zip(imp_channel_norm, ch):
            if a == b:
                common += 1
            else:
                break
        if common >= 3 and common > best_score:
            best_score = common
            best_ch = ch

    return best_ch

# ============================================================
# CONSTRUCTION DES LIGNES
# ============================================================

def make_empty_row() -> dict:
    return {col: None for col in FINAL_COLUMNS}


def series_to_dict(s: pd.Series, sup_display: str) -> dict:
    """Convertit une ligne réalisée (Series) en dict FINAL_COLUMNS."""
    row = make_empty_row()
    for col in FINAL_COLUMNS:
        if col in s.index:
            row[col] = s[col]
    row["supportp"] = sup_display
    return row


def make_non_diffuse_row(d: date, sup_display: str, code_pm: str,
                          marque=None, produit=None, raison=None) -> dict:
    """
    Ligne 'Non diffusé' : remplit Date, Chaîne, Code PM, Commentaire,
    Marque, Produit, RaisonSociale. Le reste reste vide (spec).
    """
    row = make_empty_row()
    row["datep"]       = d
    row["supportp"]    = sup_display
    row["Code PM"]     = code_pm
    row["Commentaire"] = "Non diffusé"
    row["Marque"]      = marque
    row["Produit"]     = produit
    row["RaisonSociale"] = raison
    return row


def get_sort_key(row_dict: dict):
    """Clé de tri chronologique : heure réelle > heure PM > fin de journée."""
    t = to_excel_time(row_dict.get("heure de diffusion"))
    if t is not None:
        return t
    t2, _ = parse_codepm_time(row_dict.get("Code PM"))
    return t2 if t2 is not None else time(23, 59, 59)

# ============================================================
# CŒUR : REMPLISSAGE CODE PM + COMMENTAIRE
# ============================================================

def fill_codepm_per_client(
    df_client: pd.DataFrame,
    pm_client: pd.DataFrame,
    max_date: date,
    marque_display: str = None,
) -> pd.DataFrame:
    """
    Pour un client donné, remplit les colonnes 'Code PM' et 'Commentaire'
    en comparant les passages réalisés (df_client) avec le PM (pm_client).

    Logique par chaîne, par date :
      - real == pm  → matching chrono 1-to-1
      - real < pm   → matching closest, PM restants = Non diffusé
      - real > pm   → PM d'abord chrono, surplus = Compensation ou Passage supplémentaire
      - real == 0   → tout en Non diffusé
      - pm == 0     → aucun Code PM, pas de commentaire
    """
    df = df_client.copy()
    df["date_only"] = pd.to_datetime(df["datep"], errors="coerce").dt.date
    df["t_real"]    = df["heure de diffusion"].apply(to_excel_time)

    # Garantir que toutes les colonnes finales existent
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Valeurs à propager dans les lignes Non diffusé
    _marque  = marque_display or (str(df["Marque"].iloc[0]) if not df.empty else None)
    _produit = None
    _raison  = None
    if not df.empty:
        v = df["Produit"].iloc[0] if "Produit" in df.columns else None
        _produit = str(v) if v is not None and str(v) not in ("None", "nan", "") else None
        v = df["RaisonSociale"].iloc[0] if "RaisonSociale" in df.columns else None
        _raison  = str(v) if v is not None and str(v) not in ("None", "nan", "") else None

    # Pas de PM pour ce client → retourner tel quel
    if pm_client is None or pm_client.empty:
        return df.reindex(columns=FINAL_COLUMNS)

    pm = pm_client[pm_client["date_only"].notna()].copy()
    pm = pm[pm["date_only"] <= max_date]

    # ── Construire la correspondance chaîne Imperium → chaîne PM ────────────
    # On cherche pour chaque channel_norm Imperium quelle chaîne PM correspond
    pm_channels = list(pm["channel_norm"].dropna().unique())
    imp_channels = list(df["channel_norm"].dropna().unique())

    channel_map = {}   # imp_channel_norm → pm_channel_norm (ou None)
    for ich in imp_channels:
        channel_map[ich] = match_channel(ich, pm_channels)

    # Ajouter les chaînes PM sans correspondance Imperium (pour les Non diffusé)
    matched_pm_channels = set(v for v in channel_map.values() if v is not None)
    unmatched_pm_channels = [ch for ch in pm_channels if ch not in matched_pm_channels]

    # ── Backlog : Non diffusés non compensés, par pm_channel_norm ───────────
    backlog = {ch: 0 for ch in pm_channels}

    out_all = []

    # ── Traiter les chaînes qui ont des données réalisées ───────────────────
    for ich in imp_channels:
        pch = channel_map.get(ich)   # chaîne PM correspondante (peut être None)

        real_s = df[df["channel_norm"] == ich].copy()
        pm_s   = pm[pm["channel_norm"] == pch].copy() if pch else pd.DataFrame()

        sup_display = str(real_s.iloc[0]["supportp"]) if not real_s.empty else ich

        dates_real = set(real_s["date_only"].dropna().unique())
        dates_pm   = set(pm_s["date_only"].dropna().unique()) if not pm_s.empty else set()
        all_dates  = sorted(dates_real | dates_pm)

        for d in all_dates:
            if d > max_date:
                continue

            real_day = real_s[real_s["date_only"] == d].copy().sort_values("t_real", na_position="last")
            pm_day   = pm_s[pm_s["date_only"] == d].copy().sort_values("Heure_PM", na_position="last") \
                       if not pm_s.empty else pd.DataFrame()

            real_n = len(real_day)
            pm_n   = len(pm_day)

            day_rows = []

            # ── PM vide ce jour : passages sans Code PM ──────────────────────
            if pm_n == 0:
                for _, r in real_day.iterrows():
                    rd = series_to_dict(r, sup_display)
                    # Pas de Code PM, pas de commentaire (PM ne couvre pas ce jour)
                    day_rows.append(rd)

            # ── Aucun réalisé, PM prévu : Non diffusé ───────────────────────
            elif real_n == 0:
                for _, p in pm_day.iterrows():
                    day_rows.append(make_non_diffuse_row(
                        d, sup_display, p["Code PM"],
                        marque=_marque, produit=_produit, raison=_raison,
                    ))
                if pch:
                    backlog[pch] += pm_n

            # ── Égalité : matching chrono 1-to-1 ────────────────────────────
            elif real_n == pm_n:
                for i in range(real_n):
                    r   = real_day.iloc[i]
                    p   = pm_day.iloc[i]
                    rd  = series_to_dict(r, sup_display)
                    rd["Code PM"] = p["Code PM"]
                    diff = minutes_diff(r["t_real"], p["Heure_PM"])
                    overnight = bool(p.get("Overnight", False))
                    rd["Commentaire"] = (
                        "Décalage"
                        if not overnight and diff is not None and diff > DECALAGE_MINUTES
                        else None
                    )
                    day_rows.append(rd)

            # ── Moins diffusé que prévu : closest + Non diffusé ─────────────
            elif real_n < pm_n:
                used = set()
                for i in range(real_n):
                    r     = real_day.iloc[i]
                    avail = pm_day[~pm_day.index.isin(used)]
                    # Trouver le code PM le plus proche en horaire
                    if avail.empty:
                        rd = series_to_dict(r, sup_display)
                        rd["Commentaire"] = "Passage supplémentaire"
                        day_rows.append(rd)
                        continue
                    if r["t_real"] is None:
                        best_idx = avail.index[0]
                        diff     = None
                    else:
                        diffs    = avail["Heure_PM"].apply(
                            lambda x: minutes_diff(r["t_real"], x) if x else 999999
                        )
                        best_idx = diffs.idxmin()
                        diff     = float(diffs[best_idx])

                    used.add(best_idx)
                    p  = avail.loc[best_idx]
                    rd = series_to_dict(r, sup_display)
                    rd["Code PM"] = p["Code PM"]
                    overnight = bool(p.get("Overnight", False))
                    rd["Commentaire"] = (
                        "Décalage"
                        if not overnight and diff is not None and diff > DECALAGE_MINUTES
                        else None
                    )
                    day_rows.append(rd)

                # PM non utilisés → Non diffusé
                for _, p in pm_day[~pm_day.index.isin(used)].iterrows():
                    day_rows.append(make_non_diffuse_row(
                        d, sup_display, p["Code PM"],
                        marque=_marque, produit=_produit, raison=_raison,
                    ))
                if pch:
                    backlog[pch] += pm_n - real_n

            # ── Plus diffusé que prévu : PM d'abord, surplus après ──────────
            else:  # real_n > pm_n
                for i in range(real_n):
                    r  = real_day.iloc[i]
                    rd = series_to_dict(r, sup_display)
                    if i < pm_n:
                        p  = pm_day.iloc[i]
                        rd["Code PM"] = p["Code PM"]
                        diff      = minutes_diff(r["t_real"], p["Heure_PM"])
                        overnight = bool(p.get("Overnight", False))
                        rd["Commentaire"] = (
                            "Décalage"
                            if not overnight and diff is not None and diff > DECALAGE_MINUTES
                            else None
                        )
                    else:
                        rd["Code PM"] = None
                        if pch and backlog.get(pch, 0) > 0:
                            rd["Commentaire"] = "Compensation"
                            backlog[pch] -= 1
                        else:
                            rd["Commentaire"] = "Passage supplémentaire"
                    day_rows.append(rd)

            # Tri chronologique du jour et ajout
            if day_rows:
                day_rows_sorted = sorted(day_rows, key=get_sort_key)
                out_all.append(pd.DataFrame(day_rows_sorted, columns=FINAL_COLUMNS))

    # ── Traiter les chaînes PM sans correspondance Imperium ─────────────────
    # (chaîne prévue dans le PM mais zéro passage réalisé sur cette chaîne)
    for pch in unmatched_pm_channels:
        pm_s   = pm[pm["channel_norm"] == pch].copy()
        sup_pm = str(pm_s.iloc[0]["supportp_pm"]) if not pm_s.empty else pch

        for d in sorted(pm_s["date_only"].dropna().unique()):
            if d > max_date:
                continue
            pm_day = pm_s[pm_s["date_only"] == d].sort_values("Heure_PM", na_position="last")
            day_rows = []
            for _, p in pm_day.iterrows():
                day_rows.append(make_non_diffuse_row(
                    d, sup_pm, p["Code PM"],
                    marque=_marque, produit=_produit, raison=_raison,
                ))
            if day_rows:
                backlog[pch] += len(day_rows)
                out_all.append(pd.DataFrame(day_rows, columns=FINAL_COLUMNS))

    if out_all:
        return pd.concat(out_all, ignore_index=True).reindex(columns=FINAL_COLUMNS)
    return df.reindex(columns=FINAL_COLUMNS)

# ============================================================
# ÉCRITURE EXCEL (respecte la template)
# ============================================================

def build_client_workbook(
    template_wb: openpyxl.Workbook,
    client_name: str,
    df_client: pd.DataFrame,
) -> bytes:
    """
    Génère le fichier Excel de suivi en respectant exactement la template.
    - Ne modifie pas les lignes 1 à HEADER_ROW (mise en forme, logos, titres).
    - Lit les noms de colonnes depuis la ligne HEADER_ROW de la template.
    - Crée un onglet par chaîne (supportp).
    - Les lignes "Non diffusé" ne remplissent que jusqu'à Produit (spec).
    """
    wb          = template_wb
    tpl_ws      = wb.worksheets[0]
    max_ref_col = tpl_ws.max_column

    # ── Lire la map nom_colonne → index depuis la template ──────────────────
    header_map = {}
    for c in range(1, max_ref_col + 1):
        v = tpl_ws.cell(HEADER_ROW, c).value
        if v:
            header_map[norm_txt(str(v))] = c

    def find_col(*candidates):
        for cand in candidates:
            k = norm_txt(cand)
            # Exacte
            if k in header_map:
                return header_map[k]
            # Partielle
            for hk, hc in header_map.items():
                if k in hk or hk in k:
                    return hc
        return None

    # Mapping colonnes internes → colonnes template
    COL = {
        "date":        find_col("datep", "date"),
        "support":     find_col("supportp", "support", "chaine", "chaîne"),
        "heure":       find_col("heure de diffusion", "heurep", "heure"),
        "codepm":      find_col("code pm", "code ecran pm", "codepm"),
        "commentaire": find_col("commentaire", "comment"),
        "message":     find_col("message"),
        "produit":     find_col("produit"),
        "marque":      find_col("marque"),
        "raison":      find_col("raisonsociale", "raison sociale"),
        "format":      find_col("formatsec", "format"),
        "avant":       find_col("avant"),
        "apres":       find_col("apres", "après"),
        "rang":        find_col("range", "rang"),
        "encomb":      find_col("encombe", "encombrement"),
    }

    # ── Capturer le style de la ligne DATA_START_ROW comme référence ────────
    style_ref = [tpl_ws.cell(DATA_START_ROW, c) for c in range(1, max_ref_col + 1)]

    def apply_style(ws, r_idx):
        for c in range(1, max_ref_col + 1):
            src = style_ref[c - 1]
            dst = ws.cell(r_idx, c)
            dst._style       = pycopy(src._style)
            dst.number_format = src.number_format
            dst.font         = pycopy(src.font)
            dst.fill         = pycopy(src.fill)
            dst.border       = pycopy(src.border)
            dst.alignment    = pycopy(src.alignment)
            dst.protection   = pycopy(src.protection)

    def reset_data_zone(ws):
        """Supprime les lignes de données et recrée la ligne de référence vide."""
        if ws.max_row >= DATA_START_ROW:
            ws.delete_rows(DATA_START_ROW, ws.max_row - DATA_START_ROW + 1)
        apply_style(ws, DATA_START_ROW)

    reset_data_zone(tpl_ws)

    # ── Un onglet par chaîne ─────────────────────────────────────────────────
    supports = list(df_client["supportp"].dropna().unique())
    if not supports:
        supports = ["Support"]

    for sup in supports:
        ws = wb.copy_worksheet(tpl_ws)
        ws.title = safe_sheet_name(f"{client_name} - {str(sup).strip()}")
        ws.sheet_view.showGridLines = False
        reset_data_zone(ws)

        sub = df_client[df_client["supportp"] == sup].reset_index(drop=True)

        for i, row in sub.iterrows():
            r_idx = DATA_START_ROW + i
            if i > 0:
                ws.insert_rows(r_idx)
                apply_style(ws, r_idx)

            def w(col_key, val):
                ci = COL.get(col_key)
                if ci:
                    ws.cell(r_idx, ci).value = val

            is_nd = str(row.get("Commentaire", "")).strip().lower() == "non diffusé"

            # Colonnes communes à toutes les lignes
            w("date",        row.get("datep"))
            w("support",     row.get("supportp"))
            w("codepm",      row.get("Code PM"))
            w("commentaire", row.get("Commentaire"))

            if is_nd:
                # Non diffusé : seulement jusqu'à Produit (spec)
                w("marque",  row.get("Marque"))
                w("produit", row.get("Produit"))
                w("raison",  row.get("RaisonSociale"))
            else:
                # Ligne normale : toutes les colonnes
                w("heure",   to_excel_time(row.get("heure de diffusion")))
                w("message", row.get("Message"))
                w("produit", row.get("Produit"))
                w("marque",  row.get("Marque"))
                w("raison",  row.get("RaisonSociale"))
                w("format",  row.get("FormatSec"))
                w("avant",   row.get("Avant"))
                w("apres",   row.get("Apres"))
                w("rang",    row.get("rangE"))
                w("encomb",  row.get("encombE"))

    wb.remove(tpl_ws)
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

# ============================================================
# INTERFACE STREAMLIT
# ============================================================

st.title("📊 Suivi Pige — Automatisation")
st.caption(f"Template : {TEMPLATE_PATH}")

template_ok = False
try:
    load_template_workbook()
    template_ok = True
    st.success("Template chargée ✅")
except Exception as e:
    st.error(f"Template introuvable ❌ : {e}")

data_in  = st.file_uploader("① Data Imperium (réalisé)", type=["xlsx"])
pm_in    = st.file_uploader("② PM(s) validés — 1 ou plusieurs fichiers", type=["xlsx"],
                             accept_multiple_files=True)
max_date = st.date_input("③ Date max (N-1 par défaut)",
                          value=date.today() - timedelta(days=1))

if st.button("🚀 Lancer la génération", use_container_width=True, disabled=not template_ok):
    if not data_in:
        st.warning("Veuillez uploader le fichier Data Imperium.")
    elif not pm_in:
        st.warning("Veuillez uploader au moins un fichier PM.")
    else:
        try:
            with st.spinner("Génération en cours..."):

                # 1. Lire Imperium
                df_imp = pd.read_excel(data_in)
                df_all = build_final_df_from_imperium(df_imp, max_date=max_date)

                # 2. Lire tous les PM (toutes feuilles)
                pm_list = []
                pm_errors = []
                for f in pm_in:
                    try:
                        pmv = pm_grid_to_vertical_openpyxl(
                            f.getvalue(), getattr(f, "name", "PM.xlsx")
                        )
                        if not pmv.empty:
                            pm_list.append(pmv)
                    except Exception as e:
                        pm_errors.append(f"⚠️ {getattr(f, 'name', '?')} : {e}")

                pmv_all = pd.concat(pm_list, ignore_index=True) if pm_list else pd.DataFrame()

                for err in pm_errors:
                    st.warning(err)

                # 3. Générer un fichier par marque
                client_files = {}
                warnings_log = []

                for client_name in sorted(df_all["Marque"].dropna().unique()):
                    client_norm   = normalize_brand(client_name)
                    df_client_raw = df_all[df_all["Marque"] == client_name].copy()

                    pm_client = match_pm_to_client(client_norm, pmv_all)

                    if pm_client.empty and not pmv_all.empty:
                        warnings_log.append(
                            f"⚠️ Aucun PM trouvé pour **{client_name}** "
                            f"(normalisé : '{client_norm}')"
                        )

                    df_done = fill_codepm_per_client(
                        df_client_raw, pm_client,
                        max_date=max_date,
                        marque_display=client_name,
                    )

                    template_wb = load_template_workbook()
                    xlsx_bytes  = build_client_workbook(template_wb, client_name, df_done)
                    client_files[f"Suivi_{client_name}.xlsx"] = xlsx_bytes

                st.session_state.client_files  = client_files
                st.session_state.zip_bytes     = make_zip(client_files)
                st.session_state.last_run_info = f"✅ {len(client_files)} fichier(s) généré(s)"

                for w in warnings_log:
                    st.warning(w)

        except Exception as e:
            import traceback
            st.error(f"Erreur : {e}")
            st.code(traceback.format_exc())

# ── Téléchargements ──────────────────────────────────────────────────────────
if st.session_state.client_files:
    st.success(st.session_state.last_run_info)

    st.download_button(
        "📦 Télécharger tout (ZIP)",
        data=st.session_state.zip_bytes,
        file_name=f"Suivis_{max_date.isoformat()}.zip",
        mime="application/zip",
        use_container_width=True,
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
                use_container_width=True,
            )
