import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import unicodedata
from datetime import datetime, time, date, timedelta
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

DEBUG = False  # mets True si tu veux voir quelques prints

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

# =========================================================
# Utils
# =========================================================

def norm_txt(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
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

def seconds_to_time(sec):
    try:
        sec = int(float(sec))
    except:
        return None
    sec = max(0, sec)
    hh = sec // 3600
    mm = (sec % 3600) // 60
    ss = sec % 60
    return time(hh, mm, ss)

def compute_hfin(h_debut: time, sec: float):
    if h_debut is None or pd.isna(sec):
        return None
    base = datetime(2000, 1, 1, h_debut.hour, h_debut.minute, h_debut.second)
    end = base + timedelta(seconds=int(sec))
    return end.time()

def code_ecran_from_time(h: time):
    if h is None:
        return ""
    return f"{h.hour:02d}{h.minute:02d}"

def codepm_to_time(code_pm: str):
    """
    1600R -> 16:00
    825R  -> 08:25 (rare)
    """
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

def extract_marque_from_filename(fname: str) -> str:
    base = fname.rsplit(".", 1)[0]
    base = base.replace("_", " ")
    base = re.sub(r"\s+", " ", base).strip()
    base = re.sub(r"^PM\s+", "", base, flags=re.IGNORECASE)
    parts = re.split(r"\bRAMADAN\b", base, flags=re.IGNORECASE)
    marque = parts[0].strip() if parts else base.strip()
    marque = re.sub(r"\s+", " ", marque).strip()
    return marque

def parse_pm_duration(cell):
    """
    '7s' / '10s' / '00:00:10' -> secondes int
    """
    if cell is None or (isinstance(cell, float) and np.isnan(cell)):
        return None
    s = str(cell).strip().lower()
    if s == "":
        return None
    m = re.search(r"(\d+)\s*s", s)
    if m:
        return int(m.group(1))
    # 00:00:10
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.notna(dt):
            return int(dt.hour * 3600 + dt.minute * 60 + dt.second)
    except:
        pass
    if s.isdigit():
        return int(s)
    return None

def position_from_rang_encomb(rang, encomb):
    """
    Reproduit l’esprit “Premium vs Non premium” + libellés du fichier final :
    - First / Second / Before Last / Last Position sinon Any Other Position
    """
    try:
        r = int(rang)
        e = int(encomb)
    except:
        return "Any Other Position"
    if r == 1:
        return "First Position"
    if r == 2:
        return "Second Position"
    if e >= 2 and r == e - 1:
        return "Before Last"
    if r == e:
        return "Last Position"
    return "Any Other Position"

# =========================================================
# PM grille -> vertical
# =========================================================

def pm_grid_to_vertical(df_raw: pd.DataFrame, pm_filename: str) -> pd.DataFrame:
    """
    Sortie PM (vertical) :
    Date | Support | Marque | Code Ecran PM | Heure_PM | Duree_PM_sec
    """
    df = df_raw.copy()

    def row_has(i, keywords):
        row = df.iloc[i].tolist()
        row_norm = [norm_txt(x) for x in row]
        return any(any(k in cell for k in keywords) for cell in row_norm)

    # 1) ligne meta : doit contenir CHAINE + ECRAN + un peu de contexte (TRANCHE/PROGRAMME/AVANT/APRES)
    meta_header_row = None
    for i in range(min(len(df), 60)):
        has_chaine = row_has(i, ["CHAINE"])
        has_ecran = row_has(i, ["ECRAN"])
        has_context = row_has(i, ["TRANCHE", "HORAIRE", "PROGRAMME", "AVANT", "APRES", "APRÈS"])
        if has_chaine and has_ecran and has_context:
            meta_header_row = i
            break
    if meta_header_row is None:
        raise ValueError("PM: impossible de trouver la ligne d’en-têtes (Chaine / Ecran / Tranche / Programme...).")

    # 2) ligne dates
    date_header_row = None
    for i in range(meta_header_row, min(len(df), meta_header_row + 30)):
        row_vals = df.iloc[i].tolist()
        cnt_like = sum(is_date_like(x) for x in row_vals)
        if cnt_like >= 2:
            date_header_row = i
            break
    if date_header_row is None:
        raise ValueError("PM: impossible de trouver la ligne des dates.")

    meta_cols_names = df.iloc[meta_header_row].tolist()
    date_headers = df.iloc[date_header_row].tolist()

    # map idx -> date
    date_cols_idx = []
    date_map = {}
    for j, v in enumerate(date_headers):
        d = pd.to_datetime(v, errors="coerce") if is_date_like(v) else pd.NaT
        if pd.notna(d):
            date_cols_idx.append(j)
            date_map[j] = pd.to_datetime(d.date())

    if len(date_cols_idx) < 2:
        raise ValueError("PM: je n’ai pas identifié assez de colonnes dates.")

    def find_idx_contains(needle):
        n = norm_txt(needle)
        for j, v in enumerate(meta_cols_names):
            if n in norm_txt(v):
                return j
        return None

    idx_chaine = find_idx_contains("Chaine")
    idx_ecran = find_idx_contains("Ecran")
    if idx_chaine is None or idx_ecran is None:
        raise ValueError("PM: colonnes 'Chaine' ou 'Ecran' introuvables.")

    marque = extract_marque_from_filename(pm_filename)

    data = df.iloc[date_header_row + 1:].copy().dropna(how="all")

    recs = []
    for _, r in data.iterrows():
        support_val = r.iloc[idx_chaine]
        code_pm = r.iloc[idx_ecran]

        if pd.isna(code_pm) or str(code_pm).strip() == "":
            continue

        heure_pm = codepm_to_time(str(code_pm))

        for j in date_cols_idx:
            cell = r.iloc[j]
            if pd.isna(cell) or str(cell).strip() == "":
                continue
            dur_pm = parse_pm_duration(cell)

            recs.append({
                "Date": date_map[j],
                "Support": str(support_val).strip(),
                "Marque": marque,
                "Code Ecran PM": str(code_pm).strip(),
                "Heure_PM": heure_pm,
                "Duree_PM_sec": dur_pm
            })

    pmv = pd.DataFrame(recs)
    if pmv.empty:
        return pmv

    pmv["Support_norm"] = pmv["Support"].apply(norm_txt)
    pmv["Marque_norm"] = pmv["Marque"].apply(norm_txt)
    return pmv

# =========================================================
# Excel template (comme ton fichier final)
# =========================================================

def apply_template(writer, sheet_name, df, annonceur, cible="15+"):
    ws = writer.sheets[sheet_name]

    blue_fill = PatternFill(start_color='7289DA', end_color='7289DA', fill_type='solid')
    white_font = Font(color='FFFFFF', bold=True)
    border = Border(left=Side(style='thin'), right=Side(style='thin'),
                    top=Side(style='thin'), bottom=Side(style='thin'))

    # Meta
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

    # Data ligne 10+
    for r_idx, row in enumerate(df.values, 10):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            cell.border = border

            header = str(df.columns[c_idx - 1]).upper()
            if header == "DATE":
                cell.number_format = "DD/MM/YYYY"
            if header in ["H.DÉBUT", "H.DEBUT", "H.FIN", "DURÉE", "DUREE"]:
                cell.number_format = "HH:MM:SS"

    # largeurs
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 22

# =========================================================
# Reconcile (logique du résultat final)
# =========================================================

def reconcile_all(df_brute: pd.DataFrame, pmv_all: pd.DataFrame):
    # Colonnes Data Brute (format Imperium/Yumi)
    required_map = {
        "datep": ["DATEP", "DATE", "JOUR"],
        "supportp": ["SUPPORTP", "CHAINE", "SUPPORT", "STATION"],
        "heurep": ["HEUREP", "H.DEBUT", "H.DÉBUT", "HEURE", "HORAIRE", "TIME"],
        "marque": ["MARQUE", "ANNONCEUR", "CLIENT"],
        "produit": ["PRODUIT"],
        "formatsec": ["FORMATSEC", "DUREE", "DURÉE", "FORMAT"],
        "avant": ["AVANT"],
        "apres": ["APRES", "APRÈS"],
        "rangE": ["RANGE", "RANG"],
        "encombE": ["ENCOMBE", "ENCOMBREMENT"],
        "storyboard": ["STORYBOARD", "MESSAGE"],
        "tm": ["TM%","TM"],
        "tme": ["TME"],
    }

    # find helper
    col_norm = {c: norm_txt(c) for c in df_brute.columns}
    def find_col(keys):
        keys = [norm_txt(k) for k in keys]
        for c, cn in col_norm.items():
            if any(k in cn for k in keys):
                return c
        return None

    col_date = find_col(required_map["datep"])
    col_sup  = find_col(required_map["supportp"])
    col_time = find_col(required_map["heurep"])
    col_mar  = find_col(required_map["marque"])
    col_prod = find_col(required_map["produit"])
    col_fmt  = find_col(required_map["formatsec"])
    col_av   = find_col(required_map["avant"])
    col_ap   = find_col(required_map["apres"])
    col_rang = find_col(required_map["rangE"])
    col_enc  = find_col(required_map["encombE"])
    col_sto  = find_col(required_map["storyboard"])
    col_tm   = find_col(required_map["tm"])
    col_tme  = find_col(required_map["tme"])

    for name, col in [("datep", col_date), ("supportp", col_sup), ("heurep", col_time), ("marque", col_mar)]:
        if col is None:
            raise ValueError(f"DATA BRUT: colonne '{name}' introuvable.")

    df = df_brute.copy()
    df["Date_dt"] = pd.to_datetime(df[col_date], errors="coerce")
    df["Chaîne"] = df[col_sup].astype(str).str.strip()
    df["Marque"] = df[col_mar].astype(str).str.strip()
    df["Annonceur"] = df["Marque"]
    df["Produit"] = df[col_prod] if col_prod else np.nan
    df["H.Début"] = df[col_time].apply(parse_time_any)
    df["FormatSec"] = pd.to_numeric(df[col_fmt], errors="coerce") if col_fmt else np.nan
    df["Durée"] = df["FormatSec"].apply(lambda x: seconds_to_time(x) if pd.notna(x) else None)
    df["H.Fin"] = df.apply(lambda r: compute_hfin(r["H.Début"], r["FormatSec"]), axis=1)
    df["Code Ecran"] = df["H.Début"].apply(code_ecran_from_time)

    df["Programme avant"] = df[col_av] if col_av else np.nan
    df["Programme après"] = df[col_ap] if col_ap else np.nan

    df["Rang"] = df[col_rang] if col_rang else np.nan
    df["Encombrement"] = df[col_enc] if col_enc else np.nan
    df["Position"] = df.apply(lambda r: position_from_rang_encomb(r["Rang"], r["Encombrement"]), axis=1)

    df["Storyboard"] = df[col_sto] if col_sto else np.nan
    df["TM%"] = df[col_tm] if col_tm else np.nan
    df["TME"] = df[col_tme] if col_tme else np.nan

    df["N° Mois"] = df["Date_dt"].dt.month
    df["Année"] = df["Date_dt"].dt.year

    # Normalisation matching
    df["Support_norm"] = df["Chaîne"].apply(norm_txt)
    df["Marque_norm"] = df["Marque"].apply(norm_txt)

    outputs = {}

    # traiter uniquement marques présentes dans PM (sinon 0 match)
    marques_to_process = sorted(set(df["Marque_norm"].dropna().unique()) & set(pmv_all["Marque_norm"].dropna().unique()))

    for marque_norm in marques_to_process:
        d_m = df[df["Marque_norm"] == marque_norm].copy()
        p_m = pmv_all[pmv_all["Marque_norm"] == marque_norm].copy()

        if p_m.empty:
            continue

        rows_out = []

        for sup_norm in sorted(set(d_m["Support_norm"].dropna().unique()) | set(p_m["Support_norm"].dropna().unique())):
            d_s = d_m[d_m["Support_norm"] == sup_norm].sort_values(["Date_dt","H.Début"])
            p_s = p_m[p_m["Support_norm"] == sup_norm].copy()

            # dates union
            dates = sorted(set(d_s["Date_dt"].dt.date.dropna().unique()) | set(p_s["Date"].dt.date.dropna().unique()))
            for d in dates:
                real_day = d_s[d_s["Date_dt"].dt.date == d].copy()
                pm_day = p_s[p_s["Date"].dt.date == d].copy()

                used_pm = set()

                # Match chaque diffusion vers PM le plus proche (sinon “Passage supplémentaire”)
                for _, r in real_day.iterrows():
                    comment = ""
                    code_pm = ""

                    if not pm_day.empty:
                        avail = pm_day.loc[~pm_day.index.isin(used_pm)]
                        if not avail.empty:
                            # distance en minutes (si Heure_PM manquante => très loin)
                            def dist_minutes(pm_time):
                                if pm_time is None or not isinstance(pm_time, time) or r["H.Début"] is None:
                                    return 999999
                                base = datetime(2000,1,1,r["H.Début"].hour,r["H.Début"].minute,r["H.Début"].second)
                                pmv = datetime(2000,1,1,pm_time.hour,pm_time.minute,pm_time.second)
                                return abs((base - pmv).total_seconds()) / 60.0

                            tmp = avail.copy()
                            tmp["dist"] = tmp["Heure_PM"].apply(dist_minutes)
                            pick = tmp.sort_values("dist").iloc[0]

                            used_pm.add(pick.name)
                            code_pm = pick.get("Code Ecran PM", "")

                            if pick["dist"] > 45:
                                comment = "Décalage"
                        else:
                            comment = "Passage supplémentaire"
                    else:
                        comment = "Passage supplémentaire"

                    out = {
                        "Date": r["Date_dt"].date() if pd.notna(r["Date_dt"]) else np.nan,
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
                        "Commentaire": (comment if comment else np.nan),
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

                # Non diffusés (PM restants)
                remaining = pm_day.loc[~pm_day.index.isin(used_pm)]
                for _, p in remaining.iterrows():
                    out = {
                        "Date": p["Date"].date() if pd.notna(p["Date"]) else np.nan,
                        "Chaîne": p["Support"],
                        "N° Mois": p["Date"].month if pd.notna(p["Date"]) else np.nan,
                        "Année": p["Date"].year if pd.notna(p["Date"]) else np.nan,
                        "Annonceur": p["Marque"],
                        "Marque": p["Marque"],
                        "Produit": np.nan,
                        "H.Début": np.nan,
                        "H.Fin": np.nan,
                        "Durée": np.nan,
                        "Code Ecran": np.nan,
                        "Code Ecran PM": p.get("Code Ecran PM", ""),
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

            cols_order = [
                "Date","Chaîne","N° Mois","Année","Annonceur","Marque","Produit",
                "H.Début","H.Fin","Durée","Code Ecran","Code Ecran PM","Commentaire",
                "Programme après","Programme avant","Position","Rang","Encombrement",
                "Storyboard","TM%","TME"
            ]
            df_out = df_out[cols_order]

            annonceur_name = d_m["Annonceur"].dropna().iloc[0] if not d_m["Annonceur"].dropna().empty else marque_norm

            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                # une feuille par chaîne, nom = "MARQUE - CHAINE"
                for chaine in df_out["Chaîne"].dropna().unique():
                    sub = df_out[df_out["Chaîne"] == chaine].copy()
                    sheet = safe_sheet_name(f"{annonceur_name} - {str(chaine).strip()}")
                    sub.to_excel(writer, index=False, sheet_name=sheet, startrow=8)
                    apply_template(writer, sheet, sub, annonceur=annonceur_name, cible="15+")

            outputs[annonceur_name] = bio.getvalue()

    return outputs

# =========================================================
# UI
# =========================================================

st.title("🇲🇦 AdTracker Pro : Media Reconciler")
st.markdown("### Génération Suivi (logique du fichier final)")

col_up1, col_up2 = st.columns(2)
with col_up1:
    pm_files = st.file_uploader("📁 Uploader les fichiers PM (grilles)", accept_multiple_files=True)
with col_up2:
    brute_in = st.file_uploader("📊 Uploader la Data Brute (pige)")

if st.button("LANCER LE TRAITEMENT", use_container_width=True):
    if pm_files and brute_in:
        try:
            with st.spinner("Traitement en cours..."):
                df_brute = pd.read_excel(brute_in, header=0)

                pms = []
                for f in pm_files:
                    df_raw = pd.read_excel(f, header=None)  # PM grille
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
                        st.success(f"✅ {len(outputs)} annonceurs traités.")
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
                        st.warning("Aucun annonceur matché entre PM (nom fichier) et Data Brute (colonne Marque).")
        except Exception as e:
            st.error(f"Erreur: {e}")
            if DEBUG:
                import traceback
                st.code(traceback.format_exc())
    else:
        st.warning("Veuillez charger les fichiers nécessaires.")

st.divider()
st.caption("AdTracker Pro - Sortie au format du fichier final (21 colonnes, Décalage >45min, Non diffusé, Passage supplémentaire)")
