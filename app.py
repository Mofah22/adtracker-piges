import streamlit as st
import pandas as pd
import numpy as np
import io
import re
from datetime import datetime, time, date
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

# ==========================
# MODE DEBUG (mets False après)
# ==========================
DEBUG = True

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="AdTracker Pro - Maroc",
    page_icon="🇲🇦",
    layout="wide"
)

# --- STYLES CSS POUR L'INTERFACE ---
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
        transition: all 0.3s;
    }
    .stButton>button:hover {
        background-color: #5b6eae;
        transform: translateY(-2px);
    }
    .stDownloadButton>button {
        width: 100%;
        border-radius: 8px;
        background-color: #43b581 !important;
        color: white !important;
    }
    </style>
    """, unsafe_allow_html=True)

# =========================================================
# --- OUTILS ROBUSTES (FIX dtype) ---
# =========================================================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie/flatten les colonnes (y compris MultiIndex) pour éviter bugs dtype."""
    df = df.copy()

    # Flatten MultiIndex si présent
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for tup in df.columns:
            parts = [str(x).strip() for x in tup if x is not None and str(x).strip().lower() != "nan"]
            col = " ".join(parts).strip() if parts else ""
            new_cols.append(col)
        df.columns = new_cols
    else:
        df.columns = [str(c).strip() for c in df.columns]

    # Nettoyage “Unnamed”
    df.columns = [re.sub(r"^Unnamed:.*$", "", c).strip() for c in df.columns]

    # Remplacer colonnes vides
    fixed = []
    for i, c in enumerate(df.columns):
        fixed.append(c if c else f"COL_{i}")
    df.columns = fixed

    return df

def get_series(df: pd.DataFrame, col: str):
    """
    Retourne toujours une Série.
    Si df[col] renvoie un DataFrame (doublons/MultiIndex), on prend la 1ère colonne.
    """
    if col not in df.columns:
        return None
    x = df[col]
    if isinstance(x, pd.DataFrame):
        return x.iloc[:, 0]
    return x

def parse_time(t):
    """Convertit divers formats d'heure (20h30, 20:30, float Excel) en objet time."""
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

def is_date_val(val):
    """Vérifie si une valeur ressemble à une date."""
    if isinstance(val, (datetime, date, pd.Timestamp)):
        return True
    s = str(val).strip()
    return bool(re.search(r'\d{1,2}[/-]\d{1,2}|\d{4}-\d{2}-\d{2}', s))

def debug_cols(title: str, df: pd.DataFrame):
    """Affiche colonnes + doublons + dtypes en mode DEBUG."""
    if not DEBUG:
        return
    df2 = normalize_columns(df)
    dups = pd.Index(df2.columns)[pd.Index(df2.columns).duplicated()].tolist()
    st.write(f"### {title}")
    st.write("Colonnes:", list(df2.columns))
    st.write("Doublons:", dups)
    st.write("dtypes:", df2.dtypes.astype(str))

# =========================================================
# --- STANDARDISATION PM ---
# =========================================================

def standardize_pm_columns(df):
    """Standardise PM (Date/Heure/Marque/Support/Code_Ecran) + conversions safe."""
    df = normalize_columns(df)

    def find_col(keys):
        for c in df.columns:
            cl = str(c).lower().strip()
            if any(k in cl for k in keys):
                return c
        return None

    col_date   = find_col(['date', 'jour'])
    col_heure  = find_col(['heure', 'horaire', 'time'])
    col_marque = find_col(['marque', 'annonceur', 'client'])
    col_sup    = find_col(['support', 'chaîne', 'chaine', 'station'])
    col_code   = find_col(['code', 'ecran', 'écran'])

    rename = {}
    if col_date:   rename[col_date] = 'Date'
    if col_heure:  rename[col_heure] = 'Heure'
    if col_marque: rename[col_marque] = 'Marque'
    if col_sup:    rename[col_sup] = 'Support'
    if col_code:   rename[col_code] = 'Code_Ecran'

    df2 = df.rename(columns=rename).copy()
    # Supprimer colonnes dupliquées (garder la 1ère)
    df2 = df2.loc[:, ~pd.Index(df2.columns).duplicated()].copy()

    # Conversions SAFE (Series garantie)
    s_date = get_series(df2, 'Date')
    if s_date is not None:
        df2['Date'] = pd.to_datetime(s_date, errors='coerce')

    s_heure = get_series(df2, 'Heure')
    if s_heure is not None:
        df2['Heure'] = s_heure.apply(parse_time)

    return df2

def transform_pm_horizontal(df):
    """Détecte PM horizontal -> vertical. Sinon retourne PM vertical standardisé."""
    df = normalize_columns(df)

    header_idx = -1
    for i in range(min(len(df), 25)):
        row = df.iloc[i]
        if sum(1 for x in row if is_date_val(x)) >= 2:
            header_idx = i
            break

    if header_idx == -1:
        return standardize_pm_columns(df)

    # Horizontal -> vertical
    df.columns = [c if str(c).strip() else f"Info_{i}" for i, c in enumerate(df.iloc[header_idx])]
    df = df.iloc[header_idx + 1:].reset_index(drop=True)
    df = normalize_columns(df)

    meta_cols = [c for c in df.columns if not is_date_val(c)]
    date_cols = [c for c in df.columns if is_date_val(c)]

    df_vert = df.melt(
        id_vars=meta_cols,
        value_vars=date_cols,
        var_name='Date',
        value_name='Code_Ecran'
    )

    df_vert = df_vert.dropna(subset=['Code_Ecran'])
    df_vert = df_vert[~df_vert['Code_Ecran'].astype(str).str.strip().isin(['0', '', 'nan', 'None'])]

    return standardize_pm_columns(df_vert)

# =========================================================
# --- EXCEL TEMPLATE ---
# =========================================================

def apply_template(writer, sheet_name, df):
    ws = writer.sheets[sheet_name]
    blue_fill = PatternFill(start_color='7289DA', end_color='7289DA', fill_type='solid')
    white_font = Font(color='FFFFFF', bold=True)
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

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

# =========================================================
# --- LOGIQUE DE RÉCONCILIATION ---
# =========================================================

def reconcile_all(df_brute, df_pm_total):
    df_brute = normalize_columns(df_brute)
    df_pm_total = standardize_pm_columns(df_pm_total)

    def find_col(df, keys):
        for c in df.columns:
            cl = str(c).lower().strip()
            if any(k in cl for k in keys):
                return c
        return None

    br_map = {
        'Date': find_col(df_brute, ['date', 'jour']),
        'Heure': find_col(df_brute, ['heure', 'time', 'horaire']),
        'Marque': find_col(df_brute, ['marque', 'client', 'annonceur']),
        'Support': find_col(df_brute, ['support', 'chaîne', 'chaine', 'station']),
        'Code': find_col(df_brute, ['code', 'ecran', 'écran'])
    }

    for k, v in br_map.items():
        if v is None:
            raise ValueError(f"La colonne '{k}' est introuvable dans la Data Brute.")

    df_b = df_brute.rename(columns={v: k for k, v in br_map.items() if v}).copy()
    df_b = df_b.loc[:, ~pd.Index(df_b.columns).duplicated()].copy()

    # conversions safe
    s_date = get_series(df_b, 'Date')
    df_b['Date'] = pd.to_datetime(s_date, errors='coerce') if s_date is not None else pd.NaT

    s_heure = get_series(df_b, 'Heure')
    df_b['Heure'] = s_heure.apply(parse_time) if s_heure is not None else None

    # checks
    for req in ['Date', 'Marque', 'Support']:
        if req not in df_pm_total.columns:
            raise ValueError(f"Le PM unifié ne contient pas '{req}' après standardisation.")

    output_files = {}
    marques = df_b['Marque'].dropna().unique()

    for m in marques:
        b_m = df_b[df_b['Marque'] == m]
        p_m = df_pm_total[df_pm_total['Marque'] == m] if 'Marque' in df_pm_total.columns else pd.DataFrame()
        if p_m.empty:
            continue

        client_results = []
        supports = b_m['Support'].dropna().unique()

        for s in supports:
            s_b = b_m[b_m['Support'] == s].sort_values(['Date', 'Heure'])
            s_p = p_m[p_m['Support'] == s].sort_values(['Date', 'Heure'])

            dates = sorted(list(
                set(s_b['Date'].dt.date.dropna().unique()) |
                set(s_p['Date'].dt.date.dropna().unique())
            ))

            for d in dates:
                day_b = s_b[s_b['Date'].dt.date == d]
                day_p = s_p[s_p['Date'].dt.date == d]
                used_p_idx = []

                for _, r in day_b.iterrows():
                    avail = day_p[~day_p.index.isin(used_p_idx)]
                    row_data = r.to_dict()

                    if not avail.empty:
                        p_match = avail.iloc[0]
                        used_p_idx.append(avail.index[0])

                        row_data['Code Ecran PM'] = p_match.get('Code_Ecran', '')

                        tr, tp = parse_time(r.get('Heure')), parse_time(p_match.get('Heure'))
                        if tr and tp:
                            dummy = datetime.today()
                            diff = abs((datetime.combine(dummy, tr) - datetime.combine(dummy, tp)).total_seconds() / 60)
                            row_data['Commentaire'] = "Décalage" if diff > 45 else ""
                        else:
                            row_data['Commentaire'] = ""
                    else:
                        row_data['Code Ecran PM'] = ""
                        row_data['Commentaire'] = "Passage supplémentaire"

                    client_results.append(row_data)

                remaining = day_p[~day_p.index.isin(used_p_idx)]
                for _, p in remaining.iterrows():
                    nd_row = {
                        'Date': d,
                        'Support': s,
                        'Marque': m,
                        'Code Ecran PM': p_match.get('Code_Ecran', '') if 'p_match' in locals() else p.get('Code_Ecran', ''),
                        'Commentaire': 'Non diffusé'
                    }
                    for col in df_b.columns:
                        if col not in nd_row:
                            nd_row[col] = ""
                    client_results.append(nd_row)

        if client_results:
            df_final_client = pd.DataFrame(client_results)

            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine='openpyxl') as writer:
                for sup in df_final_client['Support'].dropna().unique():
                    dfs = df_final_client[df_final_client['Support'] == sup].copy()
                    sheet = str(sup)[:30] if str(sup).strip() else "Support"
                    dfs.to_excel(writer, index=False, sheet_name=sheet, startrow=8)
                    apply_template(writer, sheet, dfs)

            output_files[m] = bio.getvalue()

    return output_files

# =========================================================
# --- INTERFACE STREAMLIT ---
# =========================================================

st.title("🇲🇦 AdTracker Pro : Media Reconciler")
st.markdown("### Automatisation des rapports de suivi clients")

col_up1, col_up2 = st.columns(2)
with col_up1:
    pm_in = st.file_uploader("📁 Uploader les fichiers PM (Grilles ou listes)", accept_multiple_files=True)
with col_up2:
    brute_in = st.file_uploader("📊 Uploader la Data Brute (Pige cabinet)")

if st.button("LANCER LE TRAITEMENT", use_container_width=True):
    if pm_in and brute_in:
        try:
            with st.spinner("Analyse et réconciliation en cours..."):

                # Lecture Data Brute
                df_brute_raw = pd.read_excel(brute_in, header=0)
                df_brute_raw = normalize_columns(df_brute_raw)
                debug_cols("DATA BRUTE (après lecture)", df_brute_raw)

                # Lecture + transformation PM
                pms_vertical = []
                for f in pm_in:
                    df_pm_raw = pd.read_excel(f, header=0)
                    df_pm_raw = normalize_columns(df_pm_raw)
                    debug_cols(f"PM RAW ({getattr(f, 'name', 'PM')})", df_pm_raw)

                    pmv = transform_pm_horizontal(df_pm_raw)
                    debug_cols(f"PM VERTICAL ({getattr(f, 'name', 'PM')})", pmv)
                    pms_vertical.append(pmv)

                df_pm_unified = pd.concat(pms_vertical, ignore_index=True)
                df_pm_unified = standardize_pm_columns(df_pm_unified)
                debug_cols("PM UNIFIÉ (standardisé)", df_pm_unified)

                final_outputs = reconcile_all(df_brute_raw, df_pm_unified)

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
                    st.warning("Aucune correspondance trouvée. Vérifiez que les noms des marques et supports sont identiques dans vos fichiers.")
        except Exception as e:
            st.error(f"Une erreur est survenue lors du traitement : {e}")
            if DEBUG:
                import traceback
                st.code(traceback.format_exc())
    else:
        st.warning("Veuillez charger les fichiers nécessaires.")

st.divider()
st.caption("AdTracker Pro v2.9 (DEBUG) - Outil interne d'agence média | Marché Maroc.")
