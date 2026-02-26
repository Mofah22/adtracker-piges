import io
import re
import zipfile
import unicodedata
from datetime import datetime, date, time

import pandas as pd
import streamlit as st
import openpyxl
from copy import copy as pycopy

# =========================
# CONFIG
# =========================
TEMPLATE_PATH = "TEMPLATE_SUIVI_FINAL.xlsx"  # doit exister dans le repo
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
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, time):
        return val
    if isinstance(val, datetime):
        return val.time()
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime().time()
    try:
        t = pd.to_datetime(str(val), errors="coerce")
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
    base = fname.rsplit(".", 1)[0]
    base = base.replace("_", " ")
    base = re.sub(r"\s+", " ", base).strip()
    base = re.sub(r"^PM\s+", "", base, flags=re.IGNORECASE)
    parts = re.split(r"\bRAMADAN\b", base, flags=re.IGNORECASE)
    marque = parts[0].strip() if parts else base.strip()
    return re.sub(r"\s+", " ", marque).strip()

def pm_grid_to_vertical(df_raw: pd.DataFrame, pm_filename: str) -> pd.DataFrame:
    """
    PM grille -> vertical:
    Date | supportp | Marque | Code PM | Heure_PM
    """
    df = df_raw.copy()

    def row_has(i, keywords):
        row = df.iloc[i].tolist()
        row_norm = [norm_txt(x) for x in row]
        return any(any(k in cell for k in keywords) for cell in row_norm)

    meta_header_row = None
    for i in range(min(len(df), 80)):
        if row_has(i, ["CHAINE"]) and row_has(i, ["ECRAN"]) and row_has(i, ["TRANCHE", "HORAIRE", "PROGRAMME", "AVANT", "APRES", "APRÈS"]):
            meta_header_row = i
            break
    if meta_header_row is None:
        raise ValueError(f"PM ({pm_filename}): ligne d’en-têtes introuvable.")

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
# Build output workbooks
# =========================

def build_client_workbook_from_template(template_wb: openpyxl.Workbook, client_name: str, df_client: pd.DataFrame) -> bytes:
    wb = template_wb
    base_ws = wb.worksheets[0]

    style_row_cells = [base_ws.cell(DATA_START_ROW, c) for c in range(1, len(FINAL_COLUMNS) + 1)]

    if base_ws.max_row > DATA_START_ROW:
        base_ws.delete_rows(DATA_START_ROW + 1, base_ws.max_row - DATA_START_ROW)

    supports = list(df_client["supportp"].dropna().unique())
    if not supports:
        supports = ["Support"]

    for i, sup in enumerate(supports):
        ws = base_ws if i == 0 else wb.copy_worksheet(base_ws)
        ws.title = safe_sheet_name(f"{client_name} - {str(sup).strip()}")

        sub = df_client[df_client["supportp"] == sup].copy()

        for c in range(1, len(FINAL_COLUMNS) + 1):
            ws.cell(DATA_START_ROW, c).value = None

        for r_idx, row in enumerate(sub.itertuples(index=False), start=DATA_START_ROW):
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
                val = getattr(row, col.replace(" ", "_"), None)
                if col == "heure de diffusion":
                    val = to_excel_time(val)
                ws.cell(r_idx, c).value = val

        for c, col in enumerate(FINAL_COLUMNS, start=1):
            ws.cell(HEADER_ROW, c).value = col

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

# =========================
# Data Imperium -> Final + Matching PM
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
    out["heure de diffusion"] = df[col_time]
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

    out = out[FINAL_COLUMNS]
    return out

def fill_codepm_commentaire(df_final: pd.DataFrame, pmv_all: pd.DataFrame, decalage_min=45):
    df = df_final.copy()
    df["marque_norm"] = df["Marque"].apply(norm_txt)
    df["support_norm"] = df["supportp"].apply(norm_txt)
    df["date_only"] = pd.to_datetime(df["datep"], errors="coerce").dt.date
    df["time_real"] = df["heure de diffusion"].apply(to_excel_time)

    if pmv_all is None or pmv_all.empty:
        return df[FINAL_COLUMNS]

    out_rows = []
    for (mn, sn, d), g in df.groupby(["marque_norm", "support_norm", "date_only"], dropna=False):
        g = g.sort_values("time_real")
        pm_day = pmv_all[(pmv_all["marque_norm"] == mn) & (pmv_all["support_norm"] == sn) & (pmv_all["date_only"] == d)].copy()
        used = set()

        for _, row in g.iterrows():
            comment = ""
            codepm = ""

            if not pm_day.empty:
                avail = pm_day.loc[~pm_day.index.isin(used)]
                if not avail.empty:
                    if row["time_real"] is None:
                        pick = avail.iloc[0]
                        diff = None
                    else:
                        tmp = avail.copy()
                        tmp["diff"] = tmp["Heure_PM"].apply(lambda t: minutes_diff(row["time_real"], t) if t else 999999)
                        pick = tmp.sort_values("diff").iloc[0]
                        diff = pick["diff"]

                    used.add(pick.name)
                    codepm = pick.get("Code PM", "")

                    if diff is not None and diff > decalage_min:
                        comment = "Décalage"
                else:
                    comment = "Passage supplémentaire"
            else:
                comment = "Passage supplémentaire"

            new_row = row.copy()
            new_row["Code PM"] = codepm if codepm else None
            new_row["Commentaire"] = comment if comment else None
            out_rows.append(new_row)

    df_out = pd.DataFrame(out_rows)
    return df_out[FINAL_COLUMNS]

# =========================
# UI
# =========================

st.title("📊 Suivi Pige — Automatisation")
st.caption("Le template est chargé automatiquement depuis le repo (TEMPLATE_SUIVI_FINAL.xlsx).")

tab1, tab2 = st.tabs(["Suivi Imperium", "Suivi Yumi (à brancher)"])

with tab1:
    st.subheader("Suivi Imperium — 2 Uploads (Data + PM)")

    # Template check
    template_ok = False
    try:
        _ = load_template_workbook()
        template_ok = True
        st.success("Template OK ✅ (TEMPLATE_SUIVI_FINAL.xlsx trouvé dans le repo)")
    except Exception as e:
        st.error(
            "Template introuvable ❌\n\n"
            "➡️ Mets dans ton repo GitHub un fichier nommé exactement : **TEMPLATE_SUIVI_FINAL.xlsx**\n"
            "Tu peux prendre `SUIVI LE BERGER FINAL .xlsx` et le renommer.\n\n"
            f"Détail: {e}"
        )

    data_in = st.file_uploader("1) Uploader DATA IMPERIUM (filtré agence)", type=["xlsx"])
    pm_in = st.file_uploader("2) Uploader les PM validés (1 ou plusieurs)", type=["xlsx"], accept_multiple_files=True)
    max_date = st.date_input("3) Date max (pas de futur)", value=date.today())

    if st.button("Lancer la génération (Imperium)", use_container_width=True, disabled=(not template_ok)):
        if not data_in:
            st.warning("Upload DATA IMPERIUM.")
        elif not pm_in:
            st.warning("Upload au moins 1 fichier PM.")
        else:
            try:
                df_imp = pd.read_excel(data_in)
                df_final = build_final_df_from_imperium(df_imp, max_date=max_date)

                pms = []
                for f in pm_in:
                    df_pm_raw = pd.read_excel(f, header=None)
                    pmv = pm_grid_to_vertical(df_pm_raw, getattr(f, "name", "PM.xlsx"))
                    if not pmv.empty:
                        pms.append(pmv)

                pmv_all = pd.concat(pms, ignore_index=True) if pms else pd.DataFrame()
                df_final = fill_codepm_commentaire(df_final, pmv_all, decalage_min=45)

                client_files = {}
                for marque in sorted(df_final["Marque"].dropna().unique()):
                    df_client = df_final[df_final["Marque"] == marque].copy()
                    df_client = df_client.sort_values(["datep", "supportp", "heure de diffusion"], na_position="last")

                    # fresh template copy per client
                    template_wb_base = load_template_workbook()
                    xlsx_bytes = build_client_workbook_from_template(template_wb_base, marque, df_client)
                    client_files[f"Suivi_{marque}.xlsx"] = xlsx_bytes

                zip_bytes = make_zip(client_files)
                st.success(f"✅ Généré: {len(client_files)} fichiers (1 par marque)")

                st.download_button(
                    "📦 Télécharger le ZIP (tous les clients)",
                    data=zip_bytes,
                    file_name=f"Suivis_Imperium_{max_date.isoformat()}.zip",
                    mime="application/zip",
                    use_container_width=True
                )

                st.divider()
                cols = st.columns(3)
                for i, (fname, data) in enumerate(client_files.items()):
                    with cols[i % 3]:
                        st.download_button(
                            f"📥 {fname}",
                            data=data,
                            file_name=fname,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )

            except Exception as e:
                st.error(f"Erreur: {e}")

with tab2:
    st.subheader("Suivi Yumi (à brancher)")
    st.info("Dès que tu m’envoies un exemple DATA YUMI + son template final, je branche la même logique.")
