import io
import re
import zipfile
import unicodedata
from datetime import datetime, date, time, timedelta

import pandas as pd
import streamlit as st
import openpyxl
from copy import copy as pycopy

# =========================
# CONFIG
# =========================
TEMPLATE_PATH = "TEMPLATE_SUIVI_FINAL.xlsx"  # dans le repo
HEADER_ROW = 9
DATA_START_ROW = 10

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

DECALAGE_MINUTES = 45

st.set_page_config(page_title="Suivi Pige — Automatisation", page_icon="📊", layout="wide")

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
# STATE (pour ne pas perdre les résultats après download)
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

def safe_sheet_name(s: str) -> str:
    s = re.sub(r"[:\\/*?\[\]]", " ", str(s))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:31] if len(s) > 31 else s

def to_excel_time(val):
    """Convertit en time(): time/datetime/Timestamp/numpy datetime64/float Excel/string."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None

    if isinstance(val, time):
        return val
    if isinstance(val, datetime):
        return val.time()
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime().time()

    # numpy datetime64
    try:
        import numpy as np
        if isinstance(val, np.datetime64):
            t = pd.to_datetime(val, errors="coerce")
            if pd.notna(t):
                return t.to_pydatetime().time()
    except:
        pass

    # excel float hour
    if isinstance(val, (float, int)):
        seconds = int(round(float(val) * 86400))
        seconds = max(0, min(seconds, 86399))
        return time(seconds // 3600, (seconds % 3600) // 60, seconds % 60)

    # string
    try:
        s = str(val).strip().replace("h", ":").replace("H", ":")
        t = pd.to_datetime(s, errors="coerce")
        if pd.notna(t):
            return t.to_pydatetime().time()
    except:
        pass

    return None

def parse_hhmm_from_code(code_pm: str):
    """1600R -> time(16,00)"""
    if code_pm is None:
        return None
    s = str(code_pm).strip().upper()
    m = re.match(r"(\d{3,4})", s)
    if not m:
        return None
    hhmm = m.group(1)
    if len(hhmm) == 3:
        hh = int(hhmm[0])
        mm = int(hhmm[1:])
    else:
        hh = int(hhmm[:2])
        mm = int(hhmm[2:])
    if 0 <= hh <= 23 and 0 <= mm <= 59:
        return time(hh, mm, 0)
    return None

def minutes_diff(t1: time, t2: time):
    if t1 is None or t2 is None:
        return None
    a = t1.hour * 60 + t1.minute + t1.second / 60
    b = t2.hour * 60 + t2.minute + t2.second / 60
    return abs(a - b)

def make_zip(files: dict[str, bytes]) -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in files.items():
            z.writestr(name, data)
    return bio.getvalue()

# =========================
# Template
# =========================

def load_template_workbook() -> openpyxl.Workbook:
    return openpyxl.load_workbook(TEMPLATE_PATH)

# =========================
# PM parsing (grilles)
# =========================

def is_date_like(v):
    if isinstance(v, (datetime, date, pd.Timestamp)):
        return True
    s = str(v).strip()
    return bool(re.search(r"\d{4}-\d{2}-\d{2}", s)) or bool(re.search(r"\d{1,2}[/-]\d{1,2}", s))

def extract_marque_from_filename(fname: str) -> str:
    # PM GATO RAMADAN ... -> GATO
    base = fname.rsplit(".", 1)[0]
    base = base.replace("_", " ")
    base = re.sub(r"\s+", " ", base).strip()
    base = re.sub(r"^PM\s+", "", base, flags=re.IGNORECASE)
    parts = re.split(r"\bRAMADAN\b", base, flags=re.IGNORECASE)
    marque = parts[0].strip() if parts else base.strip()
    return re.sub(r"\s+", " ", marque).strip()

def pm_grid_to_vertical(df_raw: pd.DataFrame, pm_filename: str) -> pd.DataFrame:
    """
    PM grille -> vertical :
    Date | supportp | Marque | Code PM | Heure_PM
    """
    df = df_raw.copy()

    def row_has(i, keywords):
        row = df.iloc[i].tolist()
        row_norm = [norm_txt(x) for x in row]
        return any(any(k in cell for k in keywords) for cell in row_norm)

    # meta header row
    meta_header_row = None
    for i in range(min(len(df), 80)):
        if row_has(i, ["CHAINE"]) and row_has(i, ["ECRAN"]) and row_has(i, ["TRANCHE", "HORAIRE", "PROGRAMME", "AVANT", "APRES", "APRÈS"]):
            meta_header_row = i
            break
    if meta_header_row is None:
        raise ValueError(f"PM ({pm_filename}): ligne d’en-têtes introuvable.")

    # date row
    date_header_row = None
    for i in range(meta_header_row, min(len(df), meta_header_row + 40)):
        row_vals = df.iloc[i].tolist()
        if sum(is_date_like(x) for x in row_vals) >= 2:
            date_header_row = i
            break
    if date_header_row is None:
        raise ValueError(f"PM ({pm_filename}): ligne des dates introuvable.")

    meta_cols = df.iloc[meta_header_row].tolist()
    date_headers = df.iloc[date_header_row].tolist()

    date_cols_idx, date_map = [], {}
    for j, v in enumerate(date_headers):
        d = pd.to_datetime(v, errors="coerce") if is_date_like(v) else pd.NaT
        if pd.notna(d):
            date_cols_idx.append(j)
            date_map[j] = pd.to_datetime(d.date())

    def find_idx_contains(needle):
        n = norm_txt(needle)
        for j, v in enumerate(meta_cols):
            if n in norm_txt(v):
                return j
        return None

    idx_chaine = find_idx_contains("Chaine")
    idx_ecran = find_idx_contains("Ecran")
    if idx_chaine is None or idx_ecran is None:
        raise ValueError(f"PM ({pm_filename}): colonnes Chaine/Ecran introuvables.")

    marque = extract_marque_from_filename(pm_filename)
    data = df.iloc[date_header_row + 1:].copy().dropna(how="all")

    recs = []
    for _, r in data.iterrows():
        sup = r.iloc[idx_chaine]
        codepm = r.iloc[idx_ecran]
        if pd.isna(codepm) or str(codepm).strip() == "":
            continue

        for j in date_cols_idx:
            cell = r.iloc[j]
            if pd.isna(cell) or str(cell).strip() == "":
                continue
            recs.append({
                "Date": date_map[j],
                "supportp": str(sup).strip(),
                "Marque": marque,
                "Code PM": str(codepm).strip(),
                "Heure_PM": parse_hhmm_from_code(str(codepm)),
            })

    pmv = pd.DataFrame(recs)
    if pmv.empty:
        return pmv

    pmv["support_norm"] = pmv["supportp"].apply(norm_txt)
    pmv["marque_norm"] = pmv["Marque"].apply(norm_txt)
    pmv["date_only"] = pd.to_datetime(pmv["Date"], errors="coerce").dt.date
    return pmv

# =========================
# Data Imperium -> Final
# =========================

def find_column(df: pd.DataFrame, candidates: list[str]):
    cols = {c: norm_txt(c) for c in df.columns}
    for c, cn in cols.items():
        for cand in candidates:
            if norm_txt(cand) in cn:
                return c
    return None

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
    out["heure de diffusion"] = df[col_time]  # converti plus tard
    out["Marque"] = df[col_mar].astype(str).str.strip()

    # optionals
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

    return out[FINAL_COLUMNS]

# =========================
# PM vs Réalisé — moteur final
# =========================

def match_day(real_day: pd.DataFrame, pm_day: pd.DataFrame):
    """
    real_day: lignes réelles du jour (déjà triées)
    pm_day: lignes PM du jour (avec Code PM + Heure_PM)
    Retour:
      - df_real_filled (mêmes lignes réelles avec Code PM + Commentaire)
      - df_non_diffuse (lignes à insérer: Non diffusé)
      - backlog_non_diffuse_count (nb non diffusés restants pour compensation)
    """
    real = real_day.copy()
    pm = pm_day.copy()

    real["time_real"] = real["heure de diffusion"].apply(to_excel_time)
    pm = pm.sort_values("Heure_PM")

    used_pm = set()

    # Préparer sorties
    filled_rows = []
    non_diffuse_rows = []

    n_real = len(real)
    n_pm = len(pm)

    # Helper: meilleur pm par proximité (one-to-one)
    def pick_closest_pm(avail_pm: pd.DataFrame, t_real: time):
        if avail_pm.empty:
            return None, None
        if t_real is None:
            pick = avail_pm.iloc[0]
            return pick, None
        tmp = avail_pm.copy()
        tmp["diff"] = tmp["Heure_PM"].apply(lambda t: minutes_diff(t_real, t) if t else 999999)
        pick = tmp.sort_values("diff").iloc[0]
        return pick, float(pick["diff"]) if "diff" in pick else None

    # CASE 1: égal => ordre chrono strict
    if n_real == n_pm:
        pm_iter = pm.itertuples()
        for r, p in zip(real.itertuples(index=False), pm_iter):
            # r est un namedtuple; on reconstruit en dict depuis la ligne source
            row = real.loc[real.index[0]].copy()  # dummy
            row = real.loc[getattr(r, "Index", None)] if hasattr(r, "Index") else None

        # plus simple: boucle indexée
        pm_list = list(pm.itertuples(index=False))
        for i in range(n_real):
            rrow = real.iloc[i].copy()
            prow = pm_list[i]
            codepm = getattr(prow, "Code_PM".replace(" ", "_"), None)
            if codepm is None:
                codepm = getattr(prow, "Code PM", None) if hasattr(prow, "Code PM") else prow[pm.columns.get_loc("Code PM")]
            tpm = prow[pm.columns.get_loc("Heure_PM")] if "Heure_PM" in pm.columns else None
            treal = rrow["time_real"]

            # Commentaire: Décalage uniquement si diff > 45
            comment = None
            if treal is not None and tpm is not None:
                diff = minutes_diff(treal, tpm)
                if diff is not None and diff > DECALAGE_MINUTES:
                    comment = "Décalage"

            rrow["Code PM"] = pm.iloc[i]["Code PM"]
            rrow["Commentaire"] = comment
            filled_rows.append(rrow)

        return pd.DataFrame(filled_rows), pd.DataFrame(non_diffuse_rows), 0

    # CASE 2: moins de diffusions => plus proche + non diffusé
    if n_real < n_pm:
        for i in range(n_real):
            rrow = real.iloc[i].copy()
            treal = rrow["time_real"]

            avail = pm.loc[~pm.index.isin(used_pm)]
            pick, diff = pick_closest_pm(avail, treal)

            if pick is not None:
                used_pm.add(pick.name)
                rrow["Code PM"] = pick["Code PM"]

                # Décalage uniquement
                comment = None
                if diff is not None and diff > DECALAGE_MINUTES:
                    comment = "Décalage"
                rrow["Commentaire"] = comment
            else:
                rrow["Code PM"] = None
                rrow["Commentaire"] = "Passage supplémentaire"

            filled_rows.append(rrow)

        # PM restants => Non diffusé (ligne insérée)
        remaining = pm.loc[~pm.index.isin(used_pm)]
        for _, prow in remaining.iterrows():
            nd = {c: None for c in FINAL_COLUMNS}
            nd["datep"] = real_day.iloc[0]["datep"] if len(real_day) else prow["Date"]
            nd["supportp"] = real_day.iloc[0]["supportp"] if len(real_day) else prow["supportp"]
            nd["Marque"] = real_day.iloc[0]["Marque"] if len(real_day) else prow["Marque"]
            nd["Produit"] = real_day.iloc[0]["Produit"] if len(real_day) else None  # règle "jusqu’à Produit"
            nd["Code PM"] = prow["Code PM"]
            nd["Commentaire"] = "Non diffusé"
            non_diffuse_rows.append(nd)

        backlog = len(remaining)
        return pd.DataFrame(filled_rows), pd.DataFrame(non_diffuse_rows), backlog

    # CASE 3: plus de diffusions => assign d’abord pm chrono, puis extra = compensation ou passage sup
    # Compensation: on applique un backlog de Non diffusé venant d’avant (géré plus haut au niveau timeline)
    pm_list = list(pm.itertuples(index=False))
    for i in range(n_real):
        rrow = real.iloc[i].copy()
        if i < n_pm:
            # assign chrono
            rrow["Code PM"] = pm.iloc[i]["Code PM"]
            # Décalage?
            treal = rrow["time_real"]
            tpm = pm.iloc[i]["Heure_PM"]
            comment = None
            if treal is not None and tpm is not None:
                diff = minutes_diff(treal, tpm)
                if diff is not None and diff > DECALAGE_MINUTES:
                    comment = "Décalage"
            rrow["Commentaire"] = comment
        else:
            # extra
            rrow["Code PM"] = None
            # commentaire sera décidé plus tard avec backlog
            rrow["Commentaire"] = "__EXTRA__"
        filled_rows.append(rrow)

    return pd.DataFrame(filled_rows), pd.DataFrame(non_diffuse_rows), 0

def fill_codepm_commentaire_full(df_final: pd.DataFrame, pmv_all: pd.DataFrame, max_date: date):
    """
    Applique toutes les règles:
    - date <= max_date
    - matching par Marque+Support+Date
    - Non diffusé inséré
    - Compensation/Passage supplémentaire
    - Décalage >45
    """
    df = df_final.copy()
    df["marque_norm"] = df["Marque"].apply(norm_txt)
    df["support_norm"] = df["supportp"].apply(norm_txt)
    df["date_only"] = pd.to_datetime(df["datep"], errors="coerce").dt.date

    if pmv_all is None or pmv_all.empty:
        return df[FINAL_COLUMNS]

    pm = pmv_all.copy()
    pm["marque_norm"] = pm["marque_norm"] if "marque_norm" in pm.columns else pm["Marque"].apply(norm_txt)
    pm["support_norm"] = pm["support_norm"] if "support_norm" in pm.columns else pm["supportp"].apply(norm_txt)
    pm["date_only"] = pm["date_only"] if "date_only" in pm.columns else pd.to_datetime(pm["Date"], errors="coerce").dt.date

    out_all = []

    # backlog de non diffusé par (marque_norm, support_norm)
    backlog = {}

    # traiter groupe par marque+support sur toute la timeline (ordre date)
    for (mn, sn), g_ms in df.groupby(["marque_norm", "support_norm"], dropna=False):
        g_ms = g_ms.sort_values(["date_only", "heure de diffusion"], na_position="last").copy()
        key = (mn, sn)
        backlog.setdefault(key, 0)

        dates = sorted([d for d in g_ms["date_only"].dropna().unique() if d <= max_date])

        for d in dates:
            real_day = g_ms[g_ms["date_only"] == d].copy()

            pm_day = pm[(pm["marque_norm"] == mn) & (pm["support_norm"] == sn) & (pm["date_only"] == d)].copy()
            pm_day = pm_day.sort_values("Heure_PM")

            # tri réel (heure)
            real_day["_treal"] = real_day["heure de diffusion"].apply(to_excel_time)
            real_day = real_day.sort_values("_treal", na_position="last").drop(columns=["_treal"])

            filled, non_diff, day_backlog = match_day(real_day, pm_day)

            # gérer les lignes EXTRA (case n_real > n_pm) pour Compensation / Passage sup
            if not filled.empty and "Commentaire" in filled.columns:
                extra_mask = filled["Commentaire"].astype(str) == "__EXTRA__"
                if extra_mask.any():
                    # pour chaque extra: si backlog>0 => Compensation, sinon Passage supplémentaire
                    for idx in filled[extra_mask].index:
                        if backlog[key] > 0:
                            filled.at[idx, "Commentaire"] = "Compensation"
                            backlog[key] -= 1
                        else:
                            filled.at[idx, "Commentaire"] = "Passage supplémentaire"

            # si on a créé des Non diffusé aujourd’hui => augmenter backlog
            if day_backlog > 0:
                backlog[key] += day_backlog

            # output tri: ordre horaire (codes pm + diffusions)
            # - les lignes “Non diffusé” n’ont pas d’heure réelle, on les place après par défaut
            out_all.append(filled[FINAL_COLUMNS])
            if not non_diff.empty:
                out_all.append(non_diff[FINAL_COLUMNS])

        # on ignore les dates futures de g_ms automatiquement

    if not out_all:
        return df[FINAL_COLUMNS]

    out = pd.concat(out_all, ignore_index=True)

    # tri global léger: Marque, support, date, heure
    out["_d"] = pd.to_datetime(out["datep"], errors="coerce").dt.date
    out["_t"] = out["heure de diffusion"].apply(to_excel_time)
    out = out.sort_values(["Marque", "supportp", "_d", "_t"], na_position="last").drop(columns=["_d", "_t"])

    return out[FINAL_COLUMNS]

# =========================
# Build workbooks (feuille par support, template, no gridlines)
# =========================

def build_client_workbook_from_template(template_wb: openpyxl.Workbook, client_name: str, df_client: pd.DataFrame) -> bytes:
    wb = template_wb
    template_ws = wb.worksheets[0]  # modèle

    style_row_cells = [template_ws.cell(DATA_START_ROW, c) for c in range(1, len(FINAL_COLUMNS) + 1)]

    def reset_sheet(ws):
        if ws.max_row > DATA_START_ROW:
            ws.delete_rows(DATA_START_ROW + 1, ws.max_row - DATA_START_ROW)
        for c in range(1, len(FINAL_COLUMNS) + 1):
            ws.cell(DATA_START_ROW, c).value = None
        for c, col in enumerate(FINAL_COLUMNS, start=1):
            ws.cell(HEADER_ROW, c).value = col
        ws.sheet_view.showGridLines = False

    reset_sheet(template_ws)

    supports = list(df_client["supportp"].dropna().unique())
    if not supports:
        supports = ["Support"]

    for sup in supports:
        ws = wb.copy_worksheet(template_ws)
        ws.title = safe_sheet_name(f"{client_name} - {str(sup).strip()}")
        reset_sheet(ws)

        sub = df_client[df_client["supportp"] == sup].copy()

        # écriture SAFE (lecture par df)
        for i in range(len(sub)):
            r_idx = DATA_START_ROW + i
            if r_idx > DATA_START_ROW:
                ws.insert_rows(r_idx)
                for c in range(1, len(FINAL_COLUMNS) + 1):
                    src = style_row_cells[c - 1]
                    dst = ws.cell(r_idx, c)
                    dst._style = pycopy(src._style)
                    dst.number_format = src.number_format
                    dst.font = pycopy(src.font)
                    dst.fill = pycopy(src.fill)
                    dst.border = pycopy(src.border)
                    dst.alignment = pycopy(src.alignment)
                    dst.protection = pycopy(src.protection)

            for c, col in enumerate(FINAL_COLUMNS, start=1):
                val = sub.iloc[i][col] if col in sub.columns else None
                if col == "heure de diffusion":
                    val = to_excel_time(val)
                ws.cell(r_idx, c).value = val

    wb.remove(template_ws)

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

# =========================
# UI
# =========================

st.title("📊 Suivi Pige — Automatisation")
st.caption("Template chargé automatiquement depuis le repo : TEMPLATE_SUIVI_FINAL.xlsx")

tab1, tab2 = st.tabs(["Suivi Imperium", "Suivi Yumi (à brancher)"])

with tab1:
    st.subheader("Suivi Imperium — Data + PM (remplissage Code PM + Commentaire)")

    # Template check
    template_ok = False
    try:
        _ = load_template_workbook()
        template_ok = True
        st.success("Template OK ✅ (TEMPLATE_SUIVI_FINAL.xlsx trouvé dans le repo)")
    except Exception as e:
        st.error(
            "Template introuvable ❌\n\n"
            "➡️ Vérifie que le fichier s’appelle EXACTEMENT : TEMPLATE_SUIVI_FINAL.xlsx\n"
            f"Détail: {e}"
        )

    data_in = st.file_uploader("1) Uploader DATA IMPERIUM (filtré agence)", type=["xlsx"])
    pm_in = st.file_uploader("2) Uploader les PM validés (1 ou plusieurs)", type=["xlsx"], accept_multiple_files=True)

    # Date max par défaut = N-1
    default_max = date.today() - timedelta(days=1)
    max_date = st.date_input("3) Date max (N-1 par défaut)", value=default_max)

    if st.button("Lancer la génération (Imperium)", use_container_width=True, disabled=(not template_ok)):
        if not data_in:
            st.warning("Upload DATA IMPERIUM.")
        elif not pm_in:
            st.warning("Upload au moins 1 fichier PM.")
        else:
            try:
                with st.spinner("Génération en cours..."):
                    df_imp = pd.read_excel(data_in)
                    df_final = build_final_df_from_imperium(df_imp, max_date=max_date)

                    # PMs -> PM vertical unifié
                    pms = []
                    for f in pm_in:
                        df_pm_raw = pd.read_excel(f, header=None)
                        pmv = pm_grid_to_vertical(df_pm_raw, getattr(f, "name", "PM.xlsx"))
                        if not pmv.empty:
                            pms.append(pmv)
                    pmv_all = pd.concat(pms, ignore_index=True) if pms else pd.DataFrame()

                    # ✅ MOTEUR FINAL (Code PM + Commentaire + Non diffusé + Compensation)
                    df_final = fill_codepm_commentaire_full(df_final, pmv_all, max_date=max_date)

                    # 1 fichier par marque
                    client_files = {}
                    for marque in sorted(df_final["Marque"].dropna().unique()):
                        df_client = df_final[df_final["Marque"] == marque].copy()
                        df_client = df_client.sort_values(["datep", "supportp", "heure de diffusion"], na_position="last")

                        template_wb = load_template_workbook()
                        xlsx_bytes = build_client_workbook_from_template(template_wb, marque, df_client)
                        client_files[f"Suivi_{marque}.xlsx"] = xlsx_bytes

                    st.session_state.client_files = client_files
                    st.session_state.zip_bytes = make_zip(client_files)
                    st.session_state.last_run_info = f"{len(client_files)} fichiers générés (Code PM + Commentaire remplis)"

            except Exception as e:
                st.error(f"Erreur: {e}")

    # Résultats persistants
    if st.session_state.client_files:
        st.success(f"✅ {st.session_state.last_run_info}")
        st.download_button(
            "📦 Télécharger le ZIP (tous les clients)",
            data=st.session_state.zip_bytes,
            file_name=f"Suivis_Imperium_{max_date.isoformat()}.zip",
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

with tab2:
    st.subheader("Suivi Yumi (à brancher)")
    st.info("Dès que tu m’envoies un exemple DATA YUMI + son PM + template final, je branche la même logique.")
