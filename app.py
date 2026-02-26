import streamlit as st
import pandas as pd
import io
from datetime import datetime
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

# --- CONFIGURATION ---
st.set_page_config(page_title="AdTracker Maroc - Réconciliation", layout="wide")

def parse_time(t):
    if pd.isna(t): return None
    if isinstance(t, datetime): return t.time()
    if isinstance(t, str):
        for fmt in ("%H:%M:%S", "%H:%M", "%Hh%M"):
            try: return datetime.strptime(t.replace(" ",""), fmt).time()
            except: continue
    return None

def time_diff_minutes(t1, t2):
    if not t1 or t2 is None: return 0
    dt1 = datetime.combine(datetime.today(), t1)
    dt2 = datetime.combine(datetime.today(), t2)
    return abs((dt1 - dt2).total_seconds() / 60)

def apply_excel_template(writer, sheet_name, df):
    workbook = writer.book
    ws = writer.sheets[sheet_name]
    blue_fill = PatternFill(start_color='7289DA', end_color='7289DA', fill_type='solid')
    white_font = Font(color='FFFFFF', bold=True)
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

    # Écriture des en-têtes à la ligne 9 (Standard Agence)
    for col_num, value in enumerate(df.columns, 1):
        cell = ws.cell(row=9, column=col_num, value=value)
        cell.fill = blue_fill
        cell.font = white_font
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = border

    # Données à partir de la ligne 10
    for r_idx, row in enumerate(df.values, 10):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            cell.border = border
            if "GRP" in df.columns[c_idx-1].upper(): cell.number_format = '0.0'

    # Infos en haut
    ws["A4"] = "RAPPORT DE DIFFUSION"
    ws["B4"] = datetime.now().strftime("%d/%m/%Y")
    ws["A5"] = "CLIENT :"
    ws["B5"] = str(df['Marque'].iloc[0]) if 'Marque' in df.columns and not df.empty else "N/A"
    
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 20

def process_reconciliation(df_brute, df_pm_all):
    df_brute['Date'] = pd.to_datetime(df_brute['Date'])
    df_pm_all['Date'] = pd.to_datetime(df_pm_all['Date'])
    
    results = {}
    marques = df_brute['Marque'].unique()

    for marque in marques:
        df_real_client = df_brute[df_brute['Marque'] == marque]
        df_pm_client = df_pm_all[df_pm_all['Marque'] == marque]
        if df_pm_client.empty: continue

        final_rows = []
        supports = df_real_client['Support'].unique()
        
        for support in supports:
            d_real = df_real_client[df_real_client['Support'] == support].sort_values(by=['Date', 'Heure'])
            d_pm = df_pm_client[df_pm_client['Support'] == support].sort_values(by=['Date', 'Heure'])
            dates = sorted(list(set(d_real['Date'].dt.date.unique()) | set(d_pm['Date'].dt.date.unique())))
            
            for date_val in dates:
                day_real = d_real[d_real['Date'].dt.date == date_val]
                day_pm = d_pm[d_pm['Date'].dt.date == date_val]
                matched_pm_indices = []
                
                for _, r_row in day_real.iterrows():
                    avail_pm = day_pm[~day_pm.index.isin(matched_pm_indices)]
                    row_res = r_row.to_dict()
                    if not avail_pm.empty:
                        pm_match = avail_pm.iloc[0]
                        matched_pm_indices.append(avail_pm.index[0])
                        row_res['Code Ecran PM'] = pm_match['Code Ecran']
                        row_res['Commentaire'] = "Décalage" if time_diff_minutes(parse_time(r_row['Heure']), parse_time(pm_match['Heure'])) > 45 else ""
                    else:
                        row_res['Code Ecran PM'] = ""
                        row_res['Commentaire'] = "Passage supplémentaire"
                    final_rows.append(row_res)
                
                # Non diffusés
                unmatched_pm = day_pm[~day_pm.index.isin(matched_pm_indices)]
                for _, p_row in unmatched_pm.iterrows():
                    nd_row = {'Date': date_val, 'Support': support, 'Marque': marque, 'Code Ecran PM': p_row['Code Ecran'], 'Commentaire': 'Non diffusé'}
                    for col in df_brute.columns:
                        if col not in nd_row: nd_row[col] = ""
                    final_rows.append(nd_row)

        if final_rows:
            df_res = pd.DataFrame(final_rows)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for s in df_res['Support'].unique():
                    df_s = df_res[df_res['Support'] == s]
                    df_s.to_excel(writer, index=False, sheet_name=str(s)[:30], startrow=8)
                    apply_excel_template(writer, str(s)[:30], df_s)
            results[marque] = output.getvalue()
    return results

# --- UI STREAMLIT ---
st.title("🇲🇦 AdTracker Pro : Automatisation Pige")
st.markdown("### 1. Préparation des données")
col1, col2 = st.columns(2)
with col1:
    pm_files = st.file_uploader("📁 Uploader les fichiers PM (Sources validées)", accept_multiple_files=True)
with col2:
    brute_file = st.file_uploader("📊 Uploader la Data Brute (Réalisé)")

if st.button("🚀 GÉNÉRER LES SUIVIS CLIENTS", use_container_width=True):
    if pm_files and brute_file:
        try:
            df_brute = pd.read_excel(brute_file)
            df_pm_all = pd.concat([pd.read_excel(f) for f in pm_files], ignore_index=True)
            files = process_reconciliation(df_brute, df_pm_all)
            st.success(f"✅ {len(files)} fichiers clients générés.")
            for client, data in files.items():
                st.download_button(label=f"📥 Télécharger Suivi {client}", data=data, file_name=f"Suivi_{client}.xlsx")
        except Exception as e:
            st.error(f"Erreur de traitement : {e}")
    else:
        st.warning("Veuillez charger les fichiers PM et la Data Brute.")
