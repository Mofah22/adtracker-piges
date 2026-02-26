import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import unicodedata
from datetime import datetime, time, date, timedelta
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

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
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def is_date_like(v):
    if isinstance(v, (datetime, date, pd.Timestamp)):
        return True
    s = str(v).strip()
    return bool(re.search(r"\d{4}-\d{2}-\d{2}", s)) or bool(re.search(r"\d{1,2}[/-]\d{1,2}", s))

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

def seconds_to_hms(sec):
    try:
        sec = int(float(sec))
    except:
        return None
    sec = max(0, sec)
    hh = sec // 3600
    mm = (sec % 3600) // 60
    ss = sec % 60
    return time(hh, mm, ss)

def extract_marque_from_filename(fname: str) -> str:
    base = fname.rsplit(".", 1)[0]
    base = base.replace("_", " ")
    base = re.sub(r"\s+", " ", base).strip()
    base = re.sub(r"^PM\s+", "", base, flags=re.IGNORECASE)
    parts = re.split(r"\bRAMADAN\b", base, flags=re.IGNORECASE)
    marque = parts[0].strip() if parts else base.strip()
    marque = re.sub(r"\s+", " ", marque).strip()
    return marque

def safe_sheet_name(s):
    s = re.sub(r"[:\\/*?\[\]]", " ", str(s))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:31] if len(s) > 31 else s

def code_to_time_hhmm(code_ecran_pm: str):
    """
    '1600R' -> time(16,00)
    '1825R' -> time(18,25)
    """
    if code_ecran_pm is None:
        return None
    s = str(code_ecran_pm).strip().upper()
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

def parse_pm_duration(cell):
    """
    '7s' / '10s' / '00:00:10' -> int seconds
    """
    if cell is None or (isinstance(cell, float) and np.isnan(cell)):
        return None
    s = str(cell).strip().lower()
    if s == "":
        return None
    m = re.search(r"(\d+)\s*s", s)
    if m:
        return int(m.group(1))
    # ex 00:00:10
    try:
        t = pd.to_datetime(s, errors="coerce")
        if pd.notna(t):
            return int(t.hour*3600 + t.minute*60 + t.second)
    except:
        pass
    # si juste "10"
    if s.isdigit():
        return int(s)
    return None

# =========================
# PM parser (grille)
# =========================

def pm_grid_to_vertical(df_raw: pd.DataFrame, pm_filename: str) -> pd.DataFrame:
    """
    Sortie PM:
    Date | Support | Marque | Code_Ecran_PM | Heure_PM | Duree_PM_sec
    """
    df = df_raw.copy()

    def row_has(i, keywords):
        row = df.iloc[i].tolist()
        row_norm = [norm_txt(x) for x in row]
        return any(any(k in cell for k in keywords) for cell in row_norm)

    # 1) trouver la ligne meta: CHAINE + ECRAN + contexte
    meta_header_row = None
    for i in range(min(len(df), 60)):
        has_chaine = row_has(i, ["CHAINE"])
        has_ecran  = row_has(i, ["ECRAN"])
        has_context = row_has(i, ["TRANCHE", "HORAIRE", "PROGRAMME", "AVANT", "APRES", "APRÈS"])
        if has_chaine and has_ecran and has_context:
            meta_header_row = i
            break
    if meta_header_row is None:
        raise ValueError("PM: impossible de trouver la ligne d’en-têtes (Chaine / Ecran / Tranche / Programme...).")

    # 2) trouver la ligne des dates
    date_header_row = None
    for i in range(meta_header_row, min(len(df), meta_header_row + 30)):
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

    # 3) map index -> date
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

    # 4) indices CHAINE et ECRAN
    def find_idx_contains(needle):
        n = norm_txt(needle)
        for j, v in enumerate(meta_cols_names):
            if n in norm_txt(v):
                return j
        return None

    idx_chaine = find_idx_contains("Chaine")
    idx_ecran  = find_idx_contains("Ecran")
    if idx_chaine is None or idx_ecran is None:
        raise ValueError("PM: colonnes 'Chaine' ou 'Ecran' introuvables.")

    # 5) data rows
    data = df.iloc[date_header_row + 1:].copy().dropna(how="all")
    marque = extract_marque_from_filename(pm_filename)

    records = []
    for _, r in data.iterrows():
        support_val = r.iloc[idx_chaine]
        code_pm = r.iloc[idx_ecran]

        if pd.isna(code_pm) or str(code_pm).strip() == "":
            continue

        heure_pm = code_to_time_hhmm(str(code_pm))
        for j in date_cols_idx:
            cell = r.iloc[j]
            if pd.isna(cell) or str(cell).strip() == "":
                continue
            dur_pm = parse_pm_duration(cell)

            records.append({
                "Date": date_map[j],
                "Support": support_val,
                "Marque": marque,
                "Code_Ecran_PM": str(code_pm).strip(),
                "Heure_PM": heure_pm,
                "Duree_PM_sec": dur_pm
            })

    pmv = pd.DataFrame(records)
    if pmv.empty:
        return pmv

    pmv["Support_norm"] = pmv["Support"].apply(norm_txt)
    pmv["Marque_norm"] = pmv["Marque"].apply(norm_txt)
    return pmv

# =========================
# Template Excel
# =========================

def apply_template(writer, sheet_name, df, annonceur, cible="15+"):
    ws = writer.sheets[sheet_name]
    blue_fill = PatternFill(start_color='7289DA', end_color='7289DA', fill_type='solid')
    white_font = Font(color='FFFFFF', bold=True)
    border = Border(left=Side(style='thin'), right=Side(style='thin'),
                    top=Side(style='thin'), bottom=Side(style='thin'))

    # Meta en haut (comme ton attendu)
    ws["A4"], ws["B4"] = "Date", datetime.now().strftime("%d/%m/%Y")
    ws["A5"], ws["B5"] = "Annonceur", annonceur
    ws["A6"], ws["B6"] = "Cible", cible

    # Header ligne 9
    for col_num, col_name in enumerate(df.columns, 1):
        cell = ws.cell(row=9, column=col_num, value=col_name)
        cell.fill = blue_fill
        cell.font = white_font
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = border

    # Données dès ligne 10
    for r_idx, row in enumerate(df.values, 10):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            cell.border = border

            header = str(df.columns[c_idx - 1]).upper()
            if header in ["DATE"]:
                cell.number_format = "DD/MM/YYYY"
            if header in ["H.DÉBUT", "H.DEBUT", "H.FIN"]:
                cell.number_format = "HH:MM:SS"
            if header == "DURÉE" or header == "DUREE":
                cell.number_format = "HH:MM:SS"

    # Largeur
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 22

# =========================
# Réconciliation (vrai matching)
# =========================

def reconcile_all(df_brute, pmv_all):
    """
    Produit des fichiers par annonceur (marque).
    """
    # mapping Data Brute (selon tes colonnes réelles)
    # datep / supportp / heurep / FormatSec / Avant / Apres / rangE / encombE / Message / Produit / Marque / TM%/TME (TM% = TM%? ici c'est TM% colonne TM% ?)
    cols = {c: norm_txt(c) for c in df_brute.columns}

    def find_col(keys):
        keys = [norm_txt(k) for k in keys]
        for c, cl in cols.items():
            if any(k in cl for k in keys):
                return c
        return None

    col_date = find_col(["datep", "date", "jour"])
    col_sup  = find_col(["supportp", "chaine", "support", "station"])
    col_time = find_col(["heurep", "h.debut", "heure", "horaire", "time"])
    col_mar  = find_col(["marque"])
    col_prod = find_col(["produit"])
    col_av   = find_col(["avant"])
    col_ap   = find_col(["apres"])
    col_pos  = find_col(["position"])  # si existe
    col_rang = find_col(["range", "rangE", "rang"])
    col_enc  = find_col(["encombE", "encombrement"])
    col_story= find_col(["storyboard", "message"])
    col_tm   = find_col(["tm%","tm"])
    col_tme  = find_col(["tme"])
    col_fmt  = find_col(["formatsec","duree","durée","format"])

    for name, col in [("Date", col_date), ("Support", col_sup), ("Marque", col_mar), ("Heure", col_time)]:
        if col is None:
            raise ValueError(f"DATA BRUT: colonne '{name}' introuvable.")

    df = df_brute.copy()
    df["Date"] = pd.to_datetime(df[col_date], errors="coerce")
    df["Chaîne"] = df[col_sup].astype(str).str.upper().str.strip()
    df["Marque"] = df[col_mar].astype(str).str.strip()
    df["Annonceur"] = df["Marque"]  # comme ton attendu
    df["Produit"] = df[col_prod] if col_prod else ""
    df["Programme avant"] = df[col_av] if col_av else ""
    df["Programme après"] = df[col_ap] if col_ap else ""
    df["Heure"] = df[col_time].apply(parse_time_any)

    # Durée réelle (FormatSec en secondes)
    if col_fmt:
        df["Durée_sec"] = pd.to_numeric(df[col_fmt], errors="coerce")
    else:
        df["Durée_sec"] = np.nan

    df["Durée"] = df["Durée_sec"].apply(lambda x: seconds_to_hms(x) if pd.notna(x) else None)

    # H.Début / H.Fin
    df["H.Début"] = df["Heure"]
    def compute_hfin(row):
        if row["H.Début"] is None or pd.isna(row["Durée_sec"]):
            return None
        base = datetime(2000,1,1,row["H.Début"].hour,row["H.Début"].minute,row["H.Début"].second)
        end = base + timedelta(seconds=int(row["Durée_sec"]))
        return end.time()
    df["H.Fin"] = df.apply(compute_hfin, axis=1)

    # Code Ecran (HHMM)
    def hhmm(row):
        if row["H.Début"] is None:
            return ""
        return f"{row['H.Début'].hour:02d}{row['H.Début'].minute:02d}"
    df["Code Ecran"] = df.apply(hhmm, axis=1)

    # Position / Rang / Encombrement / Storyboard / TM% / TME
    df["Position"] = df[col_pos] if col_pos else df.get("Position","")
    df["Rang"] = df[col_rang] if col_rang else ""
    df["Encombrement"] = df[col_enc] if col_enc else ""
    df["Storyboard"] = df[col_story] if col_story else ""
    df["TM%"] = df[col_tm] if col_tm else ""
    df["TME"] = df[col_tme] if col_tme else ""

    # Mois / Année
    df["N° Mois"] = df["Date"].dt.month
    df["Année"] = df["Date"].dt.year

    # normalisation matching
    df["Support_norm"] = df["Chaîne"].apply(norm_txt)
    df["Marque_norm"] = df["Marque"].apply(norm_txt)

    outputs = {}

    for marque_norm in sorted(df["Marque_norm"].dropna().unique()):
        d_m = df[df["Marque_norm"] == marque_norm].copy()
        p_m = pmv_all[pmv_all["Marque_norm"] == marque_norm].copy()

        if p_m.empty:
            continue

        rows_out = []

        for sup_norm in sorted(d_m["Support_norm"].dropna().unique()):
            d_s = d_m[d_m["Support_norm"] == sup_norm].sort_values(["Date","H.Début"])
            p_s = p_m[p_m["Support_norm"] == sup_norm].copy()

            if p_s.empty:
                continue

            # par date
            for dt in sorted(set(d_s["Date"].dt.date.dropna().unique()) | set(p_s["Date"].dt.date.dropna().unique())):
                real_day = d_s[d_s["Date"].dt.date == dt].copy()
                pm_day = p_s[p_s["Date"].dt.date == dt].copy()

                # préparer PM (Heure_PM peut être None si code non parseable)
                pm_day["Heure_PM"] = pm_day["Heure_PM"].apply(lambda x: x if isinstance(x, time) else None)
                pm_used = set()

                # match chaque diffusion par plus proche heure PM
                for _, r in real_day.iterrows():
                    comment = ""
                    code_pm = ""
                    dur_pm = None

                    if not pm_day.empty:
                        # candidats dispo
                        avail = pm_day.loc[~pm_day.index.isin(pm_used)]
                        if not avail.empty:
                            # si l'heure réelle manque, on prend le premier
                            if r["H.Début"] is None:
                                pick = avail.iloc[0]
                            else:
                                # distance minutes
                                def dist_minutes(pm_time):
                                    if pm_time is None:
                                        return 999999
                                    base = datetime(2000,1,1,r["H.Début"].hour,r["H.Début"].minute,r["H.Début"].second)
                                    pmv = datetime(2000,1,1,pm_time.hour,pm_time.minute,pm_time.second)
                                    return abs((base - pmv).total_seconds())/60.0
                                avail = avail.copy()
                                avail["dist"] = avail["Heure_PM"].apply(dist_minutes)
                                pick = avail.sort_values("dist").iloc[0]

                                # Décalage si >45min
                                if pick["dist"] > 45:
                                    comment = "Décalage"

                            pm_used.add(pick.name)
                            code_pm = pick.get("Code_Ecran_PM","")
                            dur_pm = pick.get("Duree_PM_sec", None)

                            # Durée mismatch
                            if pd.notna(r["Durée_sec"]) and dur_pm is not None and pd.notna(dur_pm):
                                try:
                                    dur_real = int(r["Durée_sec"])
                                    dur_pm_i = int(dur_pm)
                                    if dur_real != dur_pm_i:
                                        # ex "7s au lieu de 10s"
                                        extra = f"{dur_real}s au lieu de {dur_pm_i}s"
                                        comment = (comment + " | " + extra) if comment else extra
                                except:
                                    pass
                        else:
                            comment = "Passage supplémentaire"
                    else:
                        comment = "Passage supplémentaire"

                    out = {
                        "Date": r["Date"].date() if pd.notna(r["Date"]) else None,
                        "Chaîne": r["Chaîne"],
                        "N° Mois": r["N° Mois"],
                        "Année": r["Année"],
                        "Annonceur": r["Annonceur"],
                        "Marque": r["Marque"],
                        "Produit": r["Produit"],
                        "H.Début": r["H.Début"],
                        "H.Fin": r["H.Fin"],
                        "Durée": r["Durée"],
                        "Code Ecran": r["Code Ecran"],
                        "Code Ecran PM": code_pm,
                        "Commentaire": comment if comment else np.nan,
                        "Programme après": r["Programme après"],
                        "Programme avant": r["Programme avant"],
                        "Position": r["Position"],
                        "Rang": r["Rang"],
                        "Encombrement": r["Encombrement"],
                        "Storyboard": r["Storyboard"],
                        "TM%": r["TM%"],
                        "TME": r["TME"],
                    }
                    rows_out.append(out)

                # non diffusés (PM restants)
                remaining = pm_day.loc[~pm_day.index.isin(pm_used)]
                for _, p in remaining.iterrows():
                    out = {
                        "Date": p["Date"].date() if pd.notna(p["Date"]) else None,
                        "Chaîne": str(p["Support"]).upper().strip(),
                        "N° Mois": p["Date"].month if pd.notna(p["Date"]) else np.nan,
                        "Année": p["Date"].year if pd.notna(p["Date"]) else np.nan,
                        "Annonceur": p["Marque"],
                        "Marque": p["Marque"],
                        "Produit": np.nan,
                        "H.Début": np.nan,
                        "H.Fin": np.nan,
                        "Durée": np.nan,
                        "Code Ecran": np.nan,
                        "Code Ecran PM": p.get("Code_Ecran_PM",""),
                        "Commentaire": "Non diffusé",
                        "Programme après": np.nan,
                        "Programme avant": np.nan,
                        "Position": np.nan,
                        "Rang": np.nan,
                        "Encombrement": np.nan,
                        "Storyboard": np.nan,
                        "TM%": np.nan,
                        "TME": np.nan,
                    }
                    rows_out.append(out)

        if rows_out:
            df_out = pd.DataFrame(rows_out)

            # ordre exact des colonnes attendu
            cols_order = [
                "Date","Chaîne","N° Mois","Année","Annonceur","Marque","Produit",
                "H.Début","H.Fin","Durée","Code Ecran","Code Ecran PM","Commentaire",
                "Programme après","Programme avant","Position","Rang","Encombrement",
                "Storyboard","TM%","TME"
            ]
            df_out = df_out[cols_order]

            # Nom annonceur
            annonceur_name = d_m["Annonceur"].dropna().iloc[0] if not d_m["Annonceur"].dropna().empty else marque_norm

            # Excel multi-feuilles par chaine (mais ton exemple attend une seule feuille LE BERGER - 2M)
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                for chaine in df_out["Chaîne"].dropna().unique():
                    sub = df_out[df_out["Chaîne"] == chaine].copy()
                    sheet = safe_sheet_name(f"{annonceur_name} - {chaine}")
                    sub.to_excel(writer, index=False, sheet_name=sheet, startrow=8)
                    apply_template(writer, sheet, sub, annonceur=annonceur_name, cible="15+")

            outputs[annonceur_name] = bio.getvalue()

    return outputs

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
                    df_raw = pd.read_excel(f, header=None)
                    pmv = pm_grid_to_vertical(df_raw, getattr(f, "name", "PM.xlsx"))
                    if DEBUG:
                        st.write(getattr(f, "name", "PM"), "->", len(pmv))
                        st.write(pmv.head(10))
                    if not pmv.empty:
                        pms.append(pmv)

                if not pms:
                    st.warning("Aucun spot PM détecté dans les grilles.")
                else:
                    pmv_all = pd.concat(pms, ignore_index=True)

                    outputs = reconcile_all(df_brute, pmv_all)

                    if outputs:
                        st.success(f"✅ {len(outputs)} annonceurs traités avec succès.")
                        st.divider()
                        grid = st.columns(3)
                        for i, (client, data) in enumerate(outputs.items()):
                            with grid[i % 3]:
                                st.download_button(
                                    label=f"📥 Suivi : {client}",
                                    data=data,
                                    file_name=f"Suivi_{client}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )
                    else:
                        st.warning("Aucune correspondance trouvée. Vérifie Marque/Support dans la Data Brute vs noms fichiers PM.")
        except Exception as e:
            st.error(f"Erreur: {e}")
            if DEBUG:
                import traceback
                st.code(traceback.format_exc())
    else:
        st.warning("Veuillez charger les fichiers nécessaires.")

st.divider()
st.caption("AdTracker Pro - Version Template attendu (Code Ecran=HHMM, Décalage, Durée mismatch)")
