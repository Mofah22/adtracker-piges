import streamlit as st
import pandas as pd
import numpy as np
import io
import re
from datetime import datetime, time, date
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

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
# --- FONCTIONS DE PARSING / NORMALISATION ---
# =========================================================

def parse_time(t):
    """Convertit divers formats d'heure (20h30, 20:30, float Excel) en objet time."""
    if pd.isna(t) or str(t).strip() == "":
        return None
    if isinstance(t, time):
        return t
    if isinstance(t, datetime):
        return t.time()

    # Gestion des nombres décimaux Excel (ex: 0.85)
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

def standardize_pm_columns(df):
    """
    Standardise les colonnes d'un PM en évitant les doublons (Date, Heure, etc.).
    Fix principal pour l'erreur: 'DataFrame' object has no attribute 'dtype'
    (arrive quand df['Date'] renvoie un DataFrame à cause de colonnes dupliquées).
    """
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

    # ⚠️ Ne pas renommer vers un nom qui existe déjà => évite Date en double, etc.
    if col_date and 'Date' not in df.columns:
        rename[col_date] = 'Date'
    if col_heure and 'Heure' not in df.columns:
        rename[col_heure] = 'Heure'
    if col_marque and 'Marque' not in df.columns:
        rename[col_marque] = 'Marque'
    if col_sup and 'Support' not in df.columns:
        rename[col_sup] = 'Support'
    if col_code and 'Code_Ecran' not in df.columns:
        rename[col_code] = 'Code_Ecran'

    df2 = df.rename(columns=rename).copy()

    # ✅ Supprimer toutes colonnes dupliquées (garder la 1ère occurrence)
    df2 = df2.loc[:, ~df2.columns.duplicated()].copy()

    # Conversions safe
    if 'Date' in df2.columns:
        df2['Date'] = pd.to_datetime(df2['Date'], errors='coerce')
    if 'Heure' in df2.columns:
        df2['Heure'] = df2['Heure'].apply(parse_time)

    return df2

def transform_pm_horizontal(df):
    """
    Détecte et redresse un PM horizontal (dates en colonnes) en format vertical.
    Si déjà vertical, standardise juste les colonnes.
    """
    header_idx = -1
    for i in range(min(len(df), 25)):
        row = df.iloc[i]
        if sum(1 for x in row if is_date_val(x)) >= 2:
            header_idx = i
            break

    # --- Si déjà vertical : on standardise juste les colonnes
    if header_idx == -1:
        return standardize_pm_columns(df)

    # --- Sinon, logique “horizontal -> vertical”
    df.columns = [c if not pd.isna(c) else f"Info_{i}" for i, c in enumerate(df.iloc[header_idx])]
    df = df.iloc[header_idx + 1:].reset_index(drop=True)

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

    # Standardisation finale + anti-doublons
    df_vert = standardize_pm_columns(df_vert)

    if 'Code_Ecran' not in df_vert.columns:
        if 'Code Ecran' in df_vert.columns:
            df_vert = df_vert.rename(columns={'Code Ecran': 'Code_Ecran'})

    df_vert = df_vert.loc[:, ~df_vert.columns.duplicated()].copy()
    return df_vert

# =========================================================
# --- EXCEL TEMPLATE ---
# =========================================================

def apply_template(writer, sheet_name, df):
    """Applique le style Agence : En-têtes ligne 9 bleue, metadata en haut."""
    ws = writer.sheets[sheet_name]
    blue_fill = PatternFill(start_color='7289DA', end_color='7289DA', fill_type='solid')
    white_font = Font(color='FFFFFF', bold=True)
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    # Metadata (Lignes 4-6)
    ws["A4"], ws["B4"] = "DATE GÉNÉRATION :", datetime.now().strftime("%d/%m/%Y")
    ws["A5"], ws["B5"] = "CLIENT :", str(df['Marque'].iloc[0]) if 'Marque' in df.columns and not df.empty else "N/A"
    ws["A6"], ws["B6"] = "SUPPORT :", sheet_name

    # Header Ligne 9
    for col_num, col_name in enumerate(df.columns, 1):
        cell = ws.cell(row=9, column=col_num, value=col_name)
        cell.fill = blue_fill
        cell.font = white_font
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = border

    # Données à partir de la Ligne 10
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

    # Ajustement largeur colonnes
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 22

# =========================================================
# --- LOGIQUE DE RÉCONCILIATION ---
# =========================================================

def reconcile_all(df_brute, df_pm_total):

    def find_col(df, keys):
        for c in df.columns:
            col_low = str(c).lower().strip()
            if any(k in col_low for k in keys):
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
    # ✅ Anti-doublons côté Data Brute aussi
    df_b = df_b.loc[:, ~df_b.columns.duplicated()].copy()

    df_b['Date'] = pd.to_datetime(df_b['Date'], errors='coerce')
    df_b['Heure'] = df_b['Heure'].apply(parse_time)

    # Sécuriser le PM
    df_pm_total = standardize_pm_columns(df_pm_total)
    df_pm_total = df_pm_total.loc[:, ~df_pm_total.columns.duplicated()].copy()

    if 'Date' not in df_pm_total.columns:
        raise ValueError("Le PM unifié ne contient pas de colonne 'Date' après standardisation.")
    if 'Marque' not in df_pm_total.columns:
        raise ValueError("Le PM unifié ne contient pas de colonne 'Marque' après standardisation.")
    if 'Support' not in df_pm_total.columns:
        raise ValueError("Le PM unifié ne contient pas de colonne 'Support' après standardisation.")

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
                        'Code Ecran PM': p.get('Code_Ecran', ''),
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
                    sheet = str(sup)[:30] if str(sup).strip() != "" else "Support"
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
                df_brute_raw = pd.read_excel(brute_in)

                pms_vertical = []
                for f in pm_in:
                    df_pm_raw = pd.read_excel(f)
                    pms_vertical.append(transform_pm_horizontal(df_pm_raw))

                df_pm_unified = pd.concat(pms_vertical, ignore_index=True)
                df_pm_unified = df_pm_unified.loc[:, ~df_pm_unified.columns.duplicated()].copy()

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
    else:
        st.warning("Veuillez charger les fichiers nécessaires.")

st.divider()
st.caption("AdTracker Pro v2.7 - Outil interne d'agence média | Marché Maroc.")
