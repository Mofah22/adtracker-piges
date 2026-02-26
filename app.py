import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import unicodedata
from datetime import datetime, time, date
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

DEBUG = False  # True si tu veux voir les prints

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

    # nettoyer
    marque = re.sub(r"\s+", " ", marque).strip()
    return marque

# =========================
# PM parser (template grille)
# =========================

def pm_grid_to_vertical(df_raw: pd.DataFrame, pm_filename: str) -> pd.DataFrame:
    """
    Transforme ton PM 'grille' en table verticale:
    Date | Support | Marque | Code_Ecran | Duree_PM (optionnel)
    """
    # df_raw est lu avec header=None
    df = df_raw.copy()

    # 1) Trouver la ligne qui contient les headers méta (avec "Période", "Chaine", "Ecran"...)
    meta_header_row = None
    for i in range(min(len(df), 30)):
        row = df.iloc[i].astype(str).str.lower()
        if row.str.contains("periode").any() and row.str.contains("chaine").any() and row.str.contains("ecran").any():
            meta_header_row = i
            break

    if meta_header_row is None:
        raise ValueError("PM: impossible de trouver la ligne d’en-têtes (Période / Chaine / Ecran...).")

    # 2) Trouver la ligne des dates (beaucoup de datetime)
    date_header_row = None
    for i in range(meta_header_row, min(len(df), meta_header_row + 15)):
        cnt_dates = sum(isinstance(x, (datetime, date, pd.Timestamp)) for x in df.iloc[i].tolist())
        if cnt_dates >= 2:
            date_header_row = i
            break

    if date_header_row is None:
        # fallback: chercher des strings date
        for i in range(meta_header_row, min(len(df), meta_header_row + 20)):
            row_vals = df.iloc[i].tolist()
            cnt = sum(is_date_like(x) for x in row_vals)
            if cnt >= 2:
                date_header_row = i
                break

    if date_header_row is None:
        raise ValueError("PM: impossible de trouver la ligne des dates.")

    # 3) Construire noms de colonnes à partir de la meta header row
    meta_cols_names = df.iloc[meta_header_row].tolist()

    # 4) Identifier quelles colonnes sont des dates à partir de date_header_row
    date_headers = df.iloc[date_header_row].tolist()
    date_cols_idx = []
    date_map = {}  # idx -> datetime
    for j, v in enumerate(date_headers):
        if isinstance(v, (datetime, date, pd.Timestamp)):
            d = pd.to_datetime(v).date()
            date_cols_idx.append(j)
            date_map[j] = pd.to_datetime(d)

        else:
            # string date possible
            if is_date_like(v):
                d = pd.to_datetime(v, errors="coerce")
                if pd.notna(d):
                    date_cols_idx.append(j)
                    date_map[j] = pd.to_datetime(d.date())

    # On garde seulement les vraies colonnes dates (très important)
    if len(date_cols_idx) < 2:
        raise ValueError("PM: je n’ai pas identifié assez de colonnes dates.")

    # 5) Data rows commencent après la ligne des dates
    data_start = date_header_row + 1
    data = df.iloc[data_start:].copy()
    data = data.dropna(how="all")

    # 6) Trouver indices des champs clé dans la meta header row
    def find_idx(needle):
        for j, v in enumerate(meta_cols_names):
            if norm_txt(v).find(norm_txt(needle)) >= 0:
                return j
        return None

    idx_chaine = find_idx("Chaine")
    idx_ecran = find_idx("Ecran")

    if idx_chaine is None or idx_ecran is None:
        raise ValueError("PM: colonnes 'Chaine' ou 'Ecran' introuvables dans la ligne en-tête.")

    marque = extract_marque_from_filename(pm_filename)

    records = []
    for _, r in data.iterrows():
        support_val = r.iloc[idx_chaine]
        code_ecran = r.iloc[idx_ecran]

        # si pas d’écran -> on skip
        if pd.isna(code_ecran) or str(code_ecran).strip() == "":
            continue

        for j in date_cols_idx:
            cell = r.iloc[j]
            if pd.isna(cell) or str(cell).strip() == "":
                continue  # pas de spot ce jour-là

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

    # Normalisation matching
    pmv["Support_norm"] = pmv["Support"].apply(norm_txt)
    pmv["Marque_norm"] = pmv["Marque"].apply(norm_txt)
    pmv["Code_Ecran"] = pmv["Code_Ecran"].astype(str).str.strip()

    return pmv

# =========================
# Output Excel (style)
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
    """
    df_brute: ton fichier DATA BRUT (réalisé)
    pmv: PM vertical (Date, Support, Marque, Code_Ecran)
    """
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
            s_p = p_m[p_m["Support_norm"] == sup].sort_values(["Date"])  # pas d'heure dans PM

            if s_p.empty:
                continue

            all_dates = sorted(set(s_b["Date"].dt.date.dropna().unique()) | set(s_p["Date"].dt.date.dropna().unique()))
            for d in all_dates:
                day_b = s_b[s_b["Date"].dt.date == d]
                day_p = s_p[s_p["Date"].dt.date == d]

                used = set()

                # Match chronologique: on affecte le 1er écran PM dispo
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

                # Non diffusés
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

            # nom marque original (première valeur)
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

                # PM: concat vertical
                pms = []
                for f in pm_files:
                    df_raw = pd.read_excel(f, header=None)  # ⭐ important
                    pmv = pm_grid_to_vertical(df_raw, getattr(f, "name", "PM.xlsx"))
                    if DEBUG:
                        st.write(f"PM '{getattr(f,'name','PM')}' -> lignes:", len(pmv))
                        st.write(pmv.head(5))
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
                        st.warning("Aucune correspondance trouvée. Vérifie surtout les noms Marque/Support entre PM (nom fichier + chaine) et Data Brute.")
        except Exception as e:
            st.error(f"Erreur: {e}")
            if DEBUG:
                import traceback
                st.code(traceback.format_exc())
    else:
        st.warning("Veuillez charger les fichiers nécessaires.")

st.divider()
st.caption("AdTracker Pro - Version PM Grille (Marque depuis nom fichier)")
