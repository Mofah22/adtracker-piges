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
TV_DAY_CUTOFF_HOUR = 6  # 00:00-05:59 => fin journée TV => +1440

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

# =========================
# UI config
# =========================
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
    # uniformiser ALAOULA
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
# Template
# =========================

def load_template_workbook() -> openpyxl.Workbook:
    return openpyxl.load_workbook(TEMPLATE_PATH)

# =========================
# PM 2026.xlsx parsing (one workbook, many sheets)
# =========================

def parse_sheet_name(sheet_name: str, known_support_norms: set[str] | None = None):
    """
    Split sheet name into (brand, support_display, support_norm).
    We try last 1..3 tokens as support candidate.
    """
    tokens = sheet_name.strip().split()
    if not tokens:
        return None, None, None

    # support vocab: from known + common supports
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

    # fallback: last token support
    sup = tokens[-1]
    brand = " ".join(tokens[:-1]).strip() or sheet_name
    return brand, sup, normalize_support(sup)

def read_pm_2026_workbook(pm_bytes: bytes, known_support_norms: set[str]) -> pd.DataFrame:
    wb = openpyxl.load_workbook(io.BytesIO(pm_bytes), data_only=True)

    recs = []
    for sh in wb.sheetnames:
        ws = wb[sh]
        brand, sup_disp, sup_norm = parse_sheet_name(sh, known_support_norms)

        # expect headers in row 1: Date / Ecran
        for r in range(2, ws.max_row + 1):
            d = ws.cell(r, 1).value
            code = ws.cell(r, 2).value

            if d is None and code is None:
                continue
            if code is None or str(code).strip() == "":
                continue

            # parse date dayfirst
            d_parsed = pd.to_datetime(d, errors="coerce", dayfirst=True)
            if pd.isna(d_parsed):
                continue
            d_date = d_parsed.date()

            codepm = str(code).strip()
            _, overnight, tvm = parse_codepm_time(codepm)

            recs.append({
                "PM_FILE_BRAND": brand,
                "PM_FILE_BRAND_N": normalize_brand(brand),
                "Date": pd.to_datetime(d_date),
                "date_only": d_date,
                "supportp": sup_disp,
                "support_norm": sup_norm,
                "Code PM": codepm,
                "Overnight": overnight,
                "PM_TV_MIN": tvm,
            })

    pmv = pd.DataFrame(recs)
    return pmv

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
# Matching per client + tri TV-day + insert Non diffusé
# =========================

def fill_codepm_commentaire_per_client(df_client: pd.DataFrame, pm_client: pd.DataFrame, max_date: date):
    df = df_client.copy()
    df["date_only"] = pd.to_datetime(df["datep"], errors="coerce").dt.date
    df["t_real"] = df["heure de diffusion"].apply(to_excel_time)

    # no PM
    if pm_client is None or pm_client.empty:
        base = df.copy()
        for col in FINAL_COLUMNS:
            if col not in base.columns:
                base[col] = None
        return base[FINAL_COLUMNS]

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

        # closest by TV minutes
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

            # 0 réalisé => tout non diffusé
            if real_n == 0 and pm_n > 0:
                for _, p in pm_day.iterrows():
                    inserted_rows.append(insert_minimal_row(d, sup_display, p["Code PM"], "Non diffusé"))
                backlog[sn] += pm_n

            # equal => chrono
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

            # real < pm => closest + non diffusé
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

            # real > pm => chrono + extras
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
            df_insert = pd.DataFrame(inserted_rows) if inserted_rows else pd.DataFrame(columns=FINAL_COLUMNS)

            def sort_key_tv(row):
                t = to_excel_time(row.get("heure de diffusion"))
                if t is not None:
                    return real_tv_minutes(t)
                # non diffusé => tri par Code PM tv minutes
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

            out_day = pd.concat(out_day, ignore_index=True) if out_day else pd.DataFrame(columns=FINAL_COLUMNS)
            out_day = out_day.sort_values("_sort_t", na_position="last").drop(columns=["_sort_t"], errors="ignore")
            out_all.append(out_day[FINAL_COLUMNS])

    return pd.concat(out_all, ignore_index=True)[FINAL_COLUMNS] if out_all else df[FINAL_COLUMNS]

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

st.title("📊 Suivi Pige — Automatisation (PM 2026 unique)")
st.caption("PM unique : 1 feuille = 1 client + 1 chaîne | Col A=Date | Col B=Ecran")

template_ok = False
try:
    _ = load_template_workbook()
    template_ok = True
    st.success("Template OK ✅")
except Exception as e:
    st.error(f"Template introuvable ❌ : {e}")

data_in = st.file_uploader("1) Uploader DATA IMPERIUM", type=["xlsx"])
pm_file = st.file_uploader("2) Uploader PM 2026 (1 fichier)", type=["xlsx"])
max_date = st.date_input("3) Date max (N-1 par défaut)", value=date.today() - timedelta(days=1))

if st.button("Lancer la génération", use_container_width=True, disabled=(not template_ok)):
    if not data_in:
        st.warning("Upload DATA IMPERIUM.")
    elif not pm_file:
        st.warning("Upload PM 2026.xlsx.")
    else:
        try:
            with st.spinner("Génération en cours..."):
                df_imp = pd.read_excel(data_in)
                df_all = build_final_df_from_imperium(df_imp, max_date=max_date)

                known_supports = set(df_all["support_norm"].dropna().unique()) | {"2M", "MBC5", "ALAOULA"}
                pmv_all = read_pm_2026_workbook(pm_file.getvalue(), known_supports)

                client_files = {}

                for client_name in sorted(df_all["Marque"].dropna().unique()):
                    df_client_raw = df_all[df_all["Marque"] == client_name].copy()
                    client_norm = normalize_brand(client_name)

                    pm_client = pmv_all[pmv_all["PM_FILE_BRAND_N"] == client_norm].copy()
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
