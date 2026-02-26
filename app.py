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
TEMPLATE_PATH = "TEMPLATE_SUIVI_FINAL.xlsx"
HEADER_ROW = 9
DATA_START_ROW = 10
DECALAGE_MINUTES = 45

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

def normalize_support(sup: str) -> str:
    s = norm_txt(sup)
    s = s.replace(" ", "")
    s = re.sub(r"[^A-Z0-9]+", "", s)
    s = s.replace("TV", "")
    return s

def safe_sheet_name(s: str) -> str:
    s = re.sub(r"[:\\/*?\[\]]", " ", str(s))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:31] if len(s) > 31 else s

# ✅ FIX NaTType does not support time
def to_excel_time(val):
    if val is None:
        return None
    try:
        if pd.isna(val):  # attrape NaT/NaN
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

    # numpy datetime64
    try:
        import numpy as np
        if isinstance(val, np.datetime64):
            t = pd.to_datetime(val, errors="coerce")
            if pd.isna(t):
                return None
            return t.to_pydatetime().time()
    except:
        pass

    # Excel float time
    if isinstance(val, (float, int)):
        seconds = int(round(float(val) * 86400))
        seconds = max(0, min(seconds, 86399))
        return time(seconds // 3600, (seconds % 3600) // 60, seconds % 60)

    # string
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
    Retourne (time, overnight_bool)
    - 1600R -> 16:00, False
    - 2500R -> 01:00, True
    """
    if code_pm is None:
        return None, False
    s = str(code_pm).strip().upper()
    m = re.match(r"(\d{3,4})", s)
    if not m:
        return None, False

    hhmm = m.group(1)
    if len(hhmm) == 3:
        hh = int(hhmm[0])
        mm = int(hhmm[1:])
    else:
        hh = int(hhmm[:2])
        mm = int(hhmm[2:])

    if mm < 0 or mm > 59:
        return None, False

    overnight = False
    if hh >= 24:
        overnight = True
        hh = hh - 24

    if 0 <= hh <= 23:
        return time(hh, mm, 0), overnight
    return None, False

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
# PM parsing (merge-aware)
# =========================

def extract_marque_from_filename(fname: str) -> str:
    base = fname.rsplit(".", 1)[0]
    base = base.replace("_", " ")
    base = re.sub(r"\s+", " ", base).strip()
    base = re.sub(r"^PM\s+", "", base, flags=re.IGNORECASE)
    parts = re.split(r"\bRAMADAN\b", base, flags=re.IGNORECASE)
    marque = parts[0].strip() if parts else base.strip()
    return re.sub(r"\s+", " ", marque).strip()

def is_date_like_any(v):
    if isinstance(v, (datetime, date, pd.Timestamp)):
        return True
    s = str(v).strip()
    return bool(re.search(r"\d{4}-\d{2}-\d{2}", s)) or bool(re.search(r"\d{1,2}[/-]\d{1,2}", s))

def merged_value(ws, r, c):
    val = ws.cell(r, c).value
    if val is not None and str(val).strip() != "":
        return val
    for rng in ws.merged_cells.ranges:
        if (rng.min_row <= r <= rng.max_row) and (rng.min_col <= c <= rng.max_col):
            return ws.cell(rng.min_row, rng.min_col).value
    return val

def pm_grid_to_vertical_openpyxl(file_bytes: bytes, filename: str) -> pd.DataFrame:
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    ws = wb.active

    # meta row
    meta_row = None
    for r in range(1, min(ws.max_row, 120) + 1):
        row_vals = [norm_txt(ws.cell(r, c).value) for c in range(1, min(ws.max_column, 90) + 1)]
        if any("CHAINE" in v for v in row_vals) and any("ECRAN" in v for v in row_vals):
            meta_row = r
            break
    if meta_row is None:
        raise ValueError(f"PM ({filename}): ligne d’en-têtes introuvable.")

    # date row
    date_row = None
    for r in range(meta_row, min(ws.max_row, meta_row + 80) + 1):
        cnt = 0
        for c in range(1, min(ws.max_column, 240) + 1):
            if is_date_like_any(ws.cell(r, c).value):
                cnt += 1
        if cnt >= 2:
            date_row = r
            break
    if date_row is None:
        raise ValueError(f"PM ({filename}): ligne des dates introuvable.")

    # columns
    chaine_col = None
    ecran_col = None
    for c in range(1, min(ws.max_column, 90) + 1):
        v = norm_txt(ws.cell(meta_row, c).value)
        if "CHAINE" in v and chaine_col is None:
            chaine_col = c
        if "ECRAN" in v and ecran_col is None:
            ecran_col = c
    if chaine_col is None or ecran_col is None:
        raise ValueError(f"PM ({filename}): colonnes Chaine/Ecran introuvables.")

    # date columns
    date_cols, date_map = [], {}
    for c in range(1, min(ws.max_column, 240) + 1):
        v = ws.cell(date_row, c).value
        if is_date_like_any(v):
            d = pd.to_datetime(v, errors="coerce")
            if pd.notna(d):
                date_cols.append(c)
                date_map[c] = pd.to_datetime(d.date())

    pm_brand = extract_marque_from_filename(filename)

    recs = []
    last_sup = None
    for r in range(date_row + 1, ws.max_row + 1):
        sup = ws.cell(r, chaine_col).value
        codepm = ws.cell(r, ecran_col).value

        if norm_txt(sup).startswith("TOTAL"):
            break

        # forward fill chaine
        if sup is None or str(sup).strip() == "":
            sup = last_sup
        else:
            last_sup = sup

        if codepm is None or str(codepm).strip() == "":
            continue

        codepm_str = str(codepm).strip()
        t_pm, overnight = parse_codepm_time(codepm_str)

        for c in date_cols:
            cell_val = merged_value(ws, r, c)
            if cell_val is None:
                continue
            s = str(cell_val).strip().upper()
            if s in ("", "0", ".", "-", "OFF", "NAN", "NONE"):
                continue

            recs.append({
                "PM_FILE_BRAND": pm_brand,
                "PM_FILE_BRAND_N": normalize_brand(pm_brand),
                "Date": date_map[c],
                "date_only": pd.to_datetime(date_map[c], errors="coerce").date(),
                "supportp": str(sup).strip(),
                "support_norm": normalize_support(sup),
                "Code PM": codepm_str,
                "Heure_PM": t_pm,
                "Overnight": overnight,
            })

    return pd.DataFrame(recs)

# =========================
# Data Imperium -> DF suivi
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
    out["Marque_norm"] = out["Marque"].apply(normalize_brand)

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
    return out

# =========================
# Insert minimal rows
# =========================

def insert_minimal_row(d: date, sup_display: str, codepm: str, comment: str):
    row = {c: None for c in FINAL_COLUMNS}
    row["datep"] = d
    row["supportp"] = sup_display
    row["Code PM"] = codepm
    row["Commentaire"] = comment
    return row

# =========================
# Matching per client (PM forced)
# =========================

def fill_codepm_commentaire_per_client(df_client: pd.DataFrame, pm_client: pd.DataFrame, max_date: date):
    df = df_client.copy()
    df["date_only"] = pd.to_datetime(df["datep"], errors="coerce").dt.date
    df["t_real"] = df["heure de diffusion"].apply(to_excel_time)

    pm = pm_client.copy()
    pm = pm[pm["date_only"].notna()]
    pm = pm[pm["date_only"] <= max_date]

    out_all = []
    backlog = {}  # par support_norm

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

        def pick_closest(avail, t):
            if avail.empty:
                return None, None
            if t is None:
                pick = avail.iloc[0]
                return pick, None
            tmp = avail.copy()
            tmp["diff"] = tmp["Heure_PM"].apply(lambda x: minutes_diff(t, x) if x else 999999)
            pick = tmp.sort_values("diff").iloc[0]
            return pick, float(pick["diff"])

        for d in all_dates:
            if d > max_date:
                continue

            real_day = real_s[real_s["date_only"] == d].copy().sort_values("t_real", na_position="last")
            pm_day = pm_s[pm_s["date_only"] == d].copy().sort_values("Heure_PM", na_position="last")

            real_n = len(real_day)
            pm_n = len(pm_day)

            used = set()
            filled_rows = []
            inserted_rows = []

            # 0 réalisé, PM existe => Non diffusé
            if real_n == 0 and pm_n > 0:
                for _, p in pm_day.iterrows():
                    inserted_rows.append(insert_minimal_row(d, sup_display, p["Code PM"], "Non diffusé"))
                backlog[sn] += pm_n

            elif real_n == pm_n:
                for i in range(real_n):
                    r = real_day.iloc[i].copy()
                    p = pm_day.iloc[i]
                    r["Code PM"] = p["Code PM"]
                    diff = minutes_diff(r["t_real"], p["Heure_PM"]) if (r["t_real"] and p["Heure_PM"]) else None
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
                    inserted_rows.append(insert_minimal_row(d, sup_display, p["Code PM"], "Non diffusé"))
                backlog[sn] += len(remaining)

            else:
                for i in range(real_n):
                    r = real_day.iloc[i].copy()
                    if i < pm_n:
                        p = pm_day.iloc[i]
                        r["Code PM"] = p["Code PM"]
                        diff = minutes_diff(r["t_real"], p["Heure_PM"]) if (r["t_real"] and p["Heure_PM"]) else None
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
            df_insert = pd.DataFrame(inserted_rows) if inserted_rows else pd.DataFrame(columns=FINAL_COLUMNS)

            def sort_time(row):
                t = to_excel_time(row.get("heure de diffusion"))
                if t is not None:
                    return t
                t2, _ = parse_codepm_time(row.get("Code PM"))
                return t2

            if not df_filled.empty:
                df_filled["_sort_t"] = df_filled.apply(lambda r: sort_time(r), axis=1)
            if not df_insert.empty:
                df_insert["_sort_t"] = df_insert.apply(lambda r: sort_time(r), axis=1)

            out_day = []
            if not df_filled.empty:
                # forcer supportp affichage
                df_filled["supportp"] = sup_display
                out_day.append(df_filled)
            if not df_insert.empty:
                out_day.append(df_insert)

            out_day = pd.concat(out_day, ignore_index=True) if out_day else pd.DataFrame(columns=FINAL_COLUMNS)
            out_day = out_day.sort_values("_sort_t", na_position="last").drop(columns=["_sort_t"], errors="ignore")

            # garder colonnes template
            out_all.append(out_day[FINAL_COLUMNS])

    if not out_all:
        # fallback sans PM
        base = df_client.copy()
        for col in FINAL_COLUMNS:
            if col not in base.columns:
                base[col] = None
        return base[FINAL_COLUMNS]

    return pd.concat(out_all, ignore_index=True)[FINAL_COLUMNS]

# =========================
# Build workbooks
# =========================

def build_client_workbook_from_template(template_wb: openpyxl.Workbook, client_name: str, df_client: pd.DataFrame) -> bytes:
    wb = template_wb
    template_ws = wb.worksheets[0]

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
st.caption("Template chargé depuis le repo : TEMPLATE_SUIVI_FINAL.xlsx")

template_ok = False
try:
    _ = load_template_workbook()
    template_ok = True
    st.success("Template OK ✅")
except Exception as e:
    st.error(f"Template introuvable ❌ : {e}")

data_in = st.file_uploader("1) Uploader DATA IMPERIUM", type=["xlsx"])
pm_in = st.file_uploader("2) Uploader PM(s) validés (1 ou plusieurs)", type=["xlsx"], accept_multiple_files=True)
max_date = st.date_input("3) Date max (N-1 par défaut)", value=date.today() - timedelta(days=1))

if st.button("Lancer la génération", use_container_width=True, disabled=(not template_ok)):
    if not data_in:
        st.warning("Upload DATA IMPERIUM.")
    elif not pm_in:
        st.warning("Upload au moins 1 PM.")
    else:
        try:
            with st.spinner("Génération en cours..."):
                df_imp = pd.read_excel(data_in)
                df_all = build_final_df_from_imperium(df_imp, max_date=max_date)

                pm_list = []
                for f in pm_in:
                    pmv = pm_grid_to_vertical_openpyxl(f.getvalue(), getattr(f, "name", "PM.xlsx"))
                    if not pmv.empty:
                        pm_list.append(pmv)
                pmv_all = pd.concat(pm_list, ignore_index=True) if pm_list else pd.DataFrame()

                client_files = {}

                for client_name in sorted(df_all["Marque"].dropna().unique()):
                    df_client_raw = df_all[df_all["Marque"] == client_name].copy()

                    client_norm = normalize_brand(client_name)

                    # PM strict match
                    pm_client = pmv_all[pmv_all["PM_FILE_BRAND_N"] == client_norm].copy()

                    # fallback contains
                    if pm_client.empty and not pmv_all.empty:
                        pm_client = pmv_all[
                            pmv_all["PM_FILE_BRAND_N"].apply(lambda x: (x in client_norm) or (client_norm in x))
                        ].copy()

                    df_client_done = fill_codepm_commentaire_per_client(df_client_raw, pm_client, max_date=max_date)

                    template_wb = load_template_workbook()
                    xlsx_bytes = build_client_workbook_from_template(template_wb, client_name, df_client_done)
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
