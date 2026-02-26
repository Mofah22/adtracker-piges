import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import unicodedata
from datetime import datetime, time, date
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

# Mets True si tu veux voir les traces/df head
DEBUG = False

st.set_page_config(page_title="AdTracker Pro - Maroc", page_icon="🇲🇦", layout="wide")

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
    """Normalise texte: strip, uppercase, sans accents, espaces propres."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_time_any(t):
    if pd.isna(t) or str(t).strip() == "":
        return None
    if isinstance(t, time):
        return t
    if isinstance(t, datetime):
        return t.time()
    if isinstance(t, (float, int)):
        seconds = int(round(float(t) * 86400))
        seconds = max(0, min(seconds, 86399))
        return time(seconds // 3600, (seconds % 3600) // 60, seconds % 60)
    t_str = str(t).strip().replace(" ", "").replace("h", ":").replace("H", ":")
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(t_str, fmt).time()
        except ValueError:
            continue
    return None

def is_date_like(v):
    if isinstance(v, (datetime, date, pd.Timestamp)):
        return True
    s = str(v).strip()
    return bool(re.search(r"\d{4}-\d{2}-\d{2}", s)) or bool(re.search(r"\d{1,2}[/-]\d{1,2}", s))

def extract_marque_from_filename(fname: str) -> str:
    """
    Ex: 'PM LE BERGER RAMADAN 2026 TV - V4.xlsx' -> 'LE BERGER'
        'PM GATO RAMADAN 2026 2M TV - VF 3.xlsx' -> 'GATO'
    """
    base = fname.rsplit(".", 1)[0]
    base = base.replace("_", " ")
    base = re.sub(r"\s+", " ", base).strip()

    # enlever "PM"
    base = re.sub(r"^PM\s+", "", base, flags=re.IGNORECASE)

    # couper sur RAMADAN si présent
    parts = re.split(r"\bRAMADAN\b", base, flags=re.IGNORECASE)
    marque = parts[0].strip() if parts else base.strip()

    marque = re.sub(r"\s+", " ", marque).strip()
    return marque

# =========================
# PM parser (template grille)
# =========================

def pm_grid_to_vertical(df_raw: pd.DataFrame, pm_filename: str) -> pd.DataFrame:
    """
    Transforme un PM 'grille' en table verticale:
    Date | Support | Marque | Code_Ecran | Duree_PM (optionnel)
    Compatible avec plusieurs templates (avec/ sans 'Période').
    """
    df = df_raw.copy()

    def row_has(i, keywords):
        row = df.iloc[i].tolist()
        row_norm = [norm_txt(x) for x in row]
        return any(any(k in cell for k in keywords) for cell in row_norm)

    # 1) trouver la ligne d'en-têtes meta (au min CHAINE + ECRAN + contexte)
    meta_header_row = None
    for i in range(min(len(df), 40)):
        has_chaine = row_has(i, ["CHAINE"])
        has_ecran  = row_has(i, ["ECRAN"])
        has_context = row_has(i, ["TRANCHE", "HORAIRE", "PROGRAMME", "AVANT", "APRES", "APRES"])
        if has_chaine and has_ecran and has_context:
            meta_header_row = i
            break

    if meta_header_row is None:
        raise ValueError("PM: impossible de trouver la ligne d’en-têtes (Chaine / Ecran / Tranche / Programme...).")

    # 2) trouver la ligne des dates (beaucoup de dates)
    date_header_row = None
    for i in range(meta_header_row, min(len(df), meta_header_row + 25)):
        row_vals = df.iloc[i].tolist()
        cnt_dt = sum(isinstance(x, (datetime, date, pd.Timestamp)) for x in row_vals)
        cnt_like = sum(is_date_like(x) for x in row_vals)
        if cnt_dt >= 2 or cnt_like >= 2:
            date_header_row = i
            break

    if date_header_row is None:
        raise ValueError("PM: impossible de trouver la ligne des dates.")

    meta_cols_names = df.iloc[meta_header_row].tolist()
    date_headers = df.iloc[date_header_row].tolist()

    # 3) index -> Date
    date_cols_idx = []
    date_map = {}
    for j, v in enumerate(date_headers):
        d = None
        if isinstance(v, (datetime, date, pd.Timestamp)):
            d = pd.to_datetime(v, errors="coerce")
        else:
            if is_date_like(v):
                d = pd.to_datetime(v, errors="coerce")
        if d is not None and pd.notna(d):
            date_cols_idx.append(j)
            date_map[j] = pd.to_datetime(d.date())

    if len(date_cols_idx) < 2:
        raise ValueError("PM: je n’ai pas identifié assez de colonnes dates.")

    # 4) data rows
    data_start = date_header_row + 1
    data = df.iloc[data_start:].copy()
    data = data.dropna(how="all")

    # 5) indices meta
    def find_idx_contains(needle):
        n = norm_txt(needle)
        for j, v in enumerate(meta_cols_names):
            if n in norm_txt(v):
                return j
        return None

    idx_chaine = find_idx_contains("Chaine")
    idx_ecran  = find_idx_contains("Ecran")

    if idx_chaine is None or idx_ecran is None:
        raise ValueError("PM: colonnes 'Chaine' ou 'Ecran' introuvables dans la ligne en-tête.")

    marque = extract_marque_from_filename(pm_filename)

    records = []
    for _, r in data.iterrows():
        support_val = r.iloc[idx_chaine]
        code_ecran  = r.iloc[idx_ecran]

        if pd.isna(code_ecran) or str(code_ecran).strip() == "":
            continue

        for j in date_cols_idx:
            cell = r.iloc[j]
            if pd.isna(cell) or str(cell).strip() == "":
                continue

            records.append({
                "Date": date_map[j],
                "Support": support_val,
                "Marque": marque,
                "Code_Ecran": str(code_ecran).strip(),
                "Duree_PM": str(cell).strip()
            })

    pmv = pd.DataFrame(records)
    if pmv.empty:
        return pmv

    pmv["Support_norm"] = pmv["Support"].apply(norm_txt)
    pmv["Marque_norm"] = pmv["Marque"].apply(norm_txt)
    pmv["Code_Ecran"] = pmv["Code_Ecran"].astype(str).str.strip()
    return pmv

# =========================
# Excel template
# =========================

def apply_template(writer, sheet_name, df):
    ws = writer.sheets[sheet_name]
    blue_fill = PatternFill(start_color='7289DA', end_color='7289DA', fill_type='solid')
    white_font = Font(color='FFFFFF', bold=True)
    border = Border(left=Side(style='thin'), right=Side(style='thin'),
                    top=Side(style='thin'), bottom=Side(style='thin'))

    ws["A4"], ws["B4"] = "DATE GÉNÉRATION :", datetime.now().strftime("%d/%m/%Y")
    ws["A5"], ws["B5"] = "CLIENT :", str(df['Marque'].iloc[0]) if 'Marque' in df.columns and not df.empty else "N/A"
    ws["A6"], ws["B6"] = "SUPPORT :", sheet_name

    for col_num, col_name in enumerate(df.columns, 1):
        cell = ws.cell(row=9, column=col_num, value=col_name)
        cell.fill = blue_fill
        cell.font = white_font
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = border

    for r_idx, row in enumerate(df.values, 10):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            cell.border = border

            col_label = str(df.columns[c_idx - 1]).upper()
            if "GRP" in col_label:
                cell.number_format = '0.0'
            if "DATE" in col_label:
                cell.number_format = 'DD/MM/YYYY'
            if "HEURE" in col_label:
                cell.number_format = 'HH:MM:SS'

    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 22

# =========================
# Reconcile
# =========================

def reconcile_all(df_brute, pmv):
    # Detect columns in brute
    cols = {c: norm_txt(c) for c in df_brute.columns}

    def find_col(keys):
        keys = [norm_txt(k) for k in keys]
        for c, cl in cols.items():
            if any(k in cl for k in keys):
                return c
        return None

    col_date = find_col(["date", "jour"])
    col_sup  = find_col(["chaine", "support", "station"])
    col_time = find_col(["h.debut", "heure", "horaire", "time"])
    col_mar  = find_col(["marque", "annonceur", "client"])
    col_code = find_col(["code ecran", "ecran", "code"])

    for name, col in [("Date", col_date), ("Support", col_sup), ("Marque", col_mar), ("Code", col_code)]:
        if col is None:
            raise ValueError(f"DATA BRUT: colonne '{name}' introuvable.")

    df_b = df_brute.copy()
    df_b["Date"] = pd.to_datetime(df_b[col_date], errors="coerce")
    df_b["Support"] = df_b[col_sup]
    df_b["Marque"] = df_b[col_mar]
    df_b["Code"] = df_b[col_code]
    df_b["Heure"] = df_b[col_time].apply(parse_time_any) if col_time else None

    df_b["Support_norm"] = df_b["Support"].apply(norm_txt)
    df_b["Marque_norm"] = df_b["Marque"].apply(norm_txt)

    output_files = {}

    for marque in sorted(df_b["Marque_norm"].dropna().unique()):
        b_m = df_b[df_b["Marque_norm"] == marque]
        p_m = pmv[pmv["Marque_norm"] == marque]
        if p_m.empty:
            continue

        client_results = []

        for sup in sorted(b_m["Support_norm"].dropna().unique()):
            s_b = b_m[b_m["Support_norm"] == sup].sort_values(["Date", "Heure"])
            s_p = p_m[p_m["Support_norm"] == sup].sort_values(["Date"])

            if s_p.empty:
                continue

            all_dates = sorted(set(s_b["Date"].dt.date.dropna().unique()) | set(s_p["Date"].dt.date.dropna().unique()))
            for d in all_dates:
                day_b = s_b[s_b["Date"].dt.date == d]
                day_p = s_p[s_p["Date"].dt.date == d]

                used = set()

                for _, r in day_b.iterrows():
                    avail = day_p.loc[~day_p.index.isin(used)]
                    row_data = r.to_dict()

                    if not avail.empty:
                        p_match = avail.iloc[0]
                        used.add(p_match.name)
                        row_data["Code Ecran PM"] = p_match.get("Code_Ecran", "")
                        row_data["Commentaire"] = ""
                    else:
                        row_data["Code Ecran PM"] = ""
                        row_data["Commentaire"] = "Passage supplémentaire"

                    client_results.append(row_data)

                remaining = day_p.loc[~day_p.index.isin(used)]
                for _, p in remaining.iterrows():
                    nd = {
                        "Date": p["Date"],
                        "Support": p["Support"],
                        "Marque": p["Marque"],
                        "Code Ecran PM": p.get("Code_Ecran", ""),
                        "Commentaire": "Non diffusé"
                    }
                    client_results.append(nd)

        if client_results:
            df_final = pd.DataFrame(client_results)

            marque_name = b_m["Marque"].dropna().iloc[0] if not b_m["Marque"].dropna().empty else marque

            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                for sup_val in df_final["Support"].dropna().unique():
                    dfs = df_final[df_final["Support"] == sup_val].copy()
                    sheet = str(sup_val)[:30] if str(sup_val).strip() else "Support"
                    dfs.to_excel(writer, index=False, sheet_name=sheet, startrow=8)
                    apply_template(writer, sheet, dfs)

            output_files[marque_name] = bio.getvalue()

    return output_files

# =========================
# UI
# =========================

st.title("🇲🇦 AdTracker Pro : Media Reconciler")
st.markdown("### Automatisation des rapports de suivi clients")

col_up1, col_up2 = st.columns(2)
with col_up1:
    pm_files = st.file_uploader("📁 Uploader les fichiers PM (Grilles)", accept_multiple_files=True)
with col_up2:
    brute_in = st.file_uploader("📊 Uploader la Data Brute (Pige cabinet)")

if st.button("LANCER LE TRAITEMENT", use_container_width=True):
    if pm_files and brute_in:
        try:
            with st.spinner("Analyse et réconciliation en cours..."):
                df_brute = pd.read_excel(brute_in, header=0)

                pms = []
                for f in pm_files:
                    df_raw = pd.read_excel(f, header=None)  # IMPORTANT: template grille
                    pmv = pm_grid_to_vertical(df_raw, getattr(f, "name", "PM.xlsx"))
                    if DEBUG:
                        st.write(f"PM '{getattr(f,'name','PM')}' -> lignes:", len(pmv))
                        st.write(pmv.head(10))
                    if not pmv.empty:
                        pms.append(pmv)

                if not pms:
                    st.warning("Aucun spot PM détecté dans les grilles. Vérifie le template PM.")
                else:
                    pmv_all = pd.concat(pms, ignore_index=True)

                    final_outputs = reconcile_all(df_brute, pmv_all)

                    if final_outputs:
                        st.success(f"✅ {len(final_outputs)} clients traités avec succès.")
                        st.divider()

                        grid = st.columns(3)
                        for i, (client_name, data) in enumerate(final_outputs.items()):
                            with grid[i % 3]:
                                st.download_button(
                                    label=f"📥 Suivi : {client_name}",
                                    data=data,
                                    file_name=f"Suivi_{client_name}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )
                    else:
                        st.warning("Aucune correspondance trouvée. Vérifie les noms Marque/Support entre PM (nom fichier + chaine) et Data Brute.")
        except Exception as e:
            st.error(f"Erreur: {e}")
            if DEBUG:
                import traceback
                st.code(traceback.format_exc())
    else:
        st.warning("Veuillez charger les fichiers nécessaires.")

st.divider()
st.caption("AdTracker Pro - PM Grille (Marque depuis nom fichier)")
