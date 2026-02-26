import io
import re
import zipfile
from datetime import date, datetime, time

import pandas as pd
import streamlit as st
import openpyxl
from openpyxl.utils import get_column_letter
from copy import copy as pycopy

# =========================
# CONFIG
# =========================
TEMPLATE_PATH = "TEMPLATE_SUIVI_FINAL.xlsx"  # <-- mets ce fichier dans ton repo
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

# Mapping Imperium -> Final
IMPERIUM_TO_FINAL = {
    "datep": "datep",
    "supportp": "supportp",
    "heurep": "heure de diffusion",
    "Message": "Message",
    "Produit": "Produit",
    "Marque": "Marque",
    "RaisonSociale": "RaisonSociale",
    "FormatSec": "FormatSec",
    "Avant": "Avant",
    "Apres": "Apres",
    "rangE": "rangE",
    "encombE": "encombE",
}

# =========================
# Helpers
# =========================

def safe_sheet_name(s: str) -> str:
    s = re.sub(r"[:\\/*?\[\]]", " ", str(s))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:31] if len(s) > 31 else s

def to_excel_time(val):
    """Convertit heurep (datetime / time / string) en time() pour Excel."""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, time):
        return val
    if isinstance(val, datetime):
        return val.time()
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime().time()
    # string "16:09:05" etc
    try:
        t = pd.to_datetime(str(val), errors="coerce")
        if pd.notna(t):
            return t.to_pydatetime().time()
    except:
        pass
    return None

def load_template_workbook():
    """Charge le template depuis le disque."""
    return openpyxl.load_workbook(TEMPLATE_PATH)

def build_client_workbook_from_template(client_name: str, client_df: pd.DataFrame) -> bytes:
    """
    Crée un workbook par client.
    Pour chaque support: 1 feuille "CLIENT - support" basée sur le template (mêmes styles / formules).
    """
    wb = load_template_workbook()

    # On prend la 1ère feuille comme feuille modèle
    base_ws = wb.worksheets[0]

    # On veut garder le style de la ligne DATA_START_ROW comme modèle de style
    style_row_cells = [base_ws.cell(DATA_START_ROW, c) for c in range(1, len(FINAL_COLUMNS) + 1)]

    # Supprimer les lignes de données au-delà de DATA_START_ROW (on garde la ligne modèle)
    if base_ws.max_row > DATA_START_ROW:
        base_ws.delete_rows(DATA_START_ROW + 1, base_ws.max_row - DATA_START_ROW)

    # Supports du client
    supports = list(client_df["supportp"].dropna().unique())
    if not supports:
        supports = ["Support"]

    # On va créer une feuille par support
    created_sheets = []
    for i, sup in enumerate(supports):
        if i == 0:
            ws = base_ws
        else:
            ws = wb.copy_worksheet(base_ws)

        sheet_name = safe_sheet_name(f"{client_name} - {str(sup).strip().lower()}")
        ws.title = sheet_name
        created_sheets.append(ws)

        # Filtrer les lignes client pour ce support
        sub = client_df[client_df["supportp"] == sup].copy()

        # Re-nettoyer les lignes existantes : vider DATA_START_ROW puis réécrire tout
        # (On garde les styles du template)
        # Vider ligne modèle
        for c in range(1, len(FINAL_COLUMNS) + 1):
            ws.cell(DATA_START_ROW, c).value = None

        # Écrire les lignes à partir de DATA_START_ROW
        for r_idx, row in enumerate(sub.itertuples(index=False), start=DATA_START_ROW):
            # si on dépasse la ligne modèle, insérer une nouvelle ligne et copier le style
            if r_idx > DATA_START_ROW:
                ws.insert_rows(r_idx)
                # copier style de la ligne modèle
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
                    dst.comment = None

            # set values
            for c, col_name in enumerate(FINAL_COLUMNS, start=1):
                val = getattr(row, col_name.replace(" ", "_"), None)

                # Heure de diffusion -> time
                if col_name == "heure de diffusion":
                    val = to_excel_time(val)

                ws.cell(r_idx, c).value = val

        # Après écriture, forcer les headers ligne 9 (au cas où)
        for c, col in enumerate(FINAL_COLUMNS, start=1):
            ws.cell(HEADER_ROW, c).value = col

        # Les 2 colonnes à laisser vides
        # Code PM (col 4) / Commentaire (col 5)
        # On ne remplit volontairement rien.

    # Export bytes
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()

def imperium_to_final_df(df: pd.DataFrame, max_date: date) -> pd.DataFrame:
    """Transforme DATA IMPERIUM -> DF final (14 colonnes) + filtre dates <= max_date."""
    # vérifier colonnes minimales
    for src in ["datep", "supportp", "heurep", "Marque"]:
        if src not in df.columns:
            raise ValueError(f"Colonne manquante dans Imperium: {src}")

    # filtrer dates <= max_date
    df = df.copy()
    df["datep"] = pd.to_datetime(df["datep"], errors="coerce")
    df = df[df["datep"].dt.date <= max_date]

    # construire DF final
    out = pd.DataFrame()

    # remplir les colonnes finales depuis Imperium
    for src, dst in IMPERIUM_TO_FINAL.items():
        if src in df.columns:
            out[dst] = df[src]
        else:
            out[dst] = None

    # ajouter les 2 colonnes vides
    out["Code PM"] = None
    out["Commentaire"] = None

    # ordre final
    out = out[FINAL_COLUMNS]

    # normaliser supportp (option: garder tel quel)
    out["supportp"] = out["supportp"].astype(str).str.strip()

    # s'assurer que Marque est string
    out["Marque"] = out["Marque"].astype(str).str.strip()

    return out

def make_zip(files: dict[str, bytes]) -> bytes:
    """files: filename -> bytes"""
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in files.items():
            z.writestr(name, data)
    return bio.getvalue()

# =========================
# UI
# =========================

st.title("Suivi Pige — Génération automatique")

st.markdown("""
Cette app génère **1 fichier Excel par client (Marque)** en respectant le **template FINAL**.  
Les colonnes **Code PM** et **Commentaire** restent vides (remplissage manuel).
""")

tab1, tab2 = st.tabs(["Suivi Imperium", "Suivi Yumi (à brancher)"])

with tab1:
    st.subheader("Génération Suivi Imperium")

    template_ok = False
    try:
        _ = load_template_workbook()
        template_ok = True
        st.success("Template détecté ✅ (TEMPLATE_SUIVI_FINAL.xlsx)")
    except Exception as e:
        st.error(
            "Template introuvable ou illisible ❌\n\n"
            "➡️ Mets ton fichier template dans le repo avec le nom : **TEMPLATE_SUIVI_FINAL.xlsx**\n"
            f"Détail: {e}"
        )

    imp_file = st.file_uploader("1) Uploader DATA IMPERIUM (filtré agence)", type=["xlsx"])
    max_date = st.date_input("2) Date max (pas de futur)", value=date.today())

    if st.button("Lancer la génération (Imperium)", use_container_width=True, disabled=(not template_ok)):
        if not imp_file:
            st.warning("Upload le fichier DATA IMPERIUM.")
        else:
            try:
                df_imp = pd.read_excel(imp_file)
                df_final = imperium_to_final_df(df_imp, max_date=max_date)

                if df_final.empty:
                    st.warning("Aucune ligne après filtre date. Vérifie la Date max.")
                else:
                    # group by Marque
                    client_files = {}
                    for marque in sorted(df_final["Marque"].dropna().unique()):
                        df_client = df_final[df_final["Marque"] == marque].copy()

                        # trier (comme un suivi)
                        df_client = df_client.sort_values(["datep", "supportp", "heure de diffusion"], na_position="last")

                        # générer workbook client
                        xlsx_bytes = build_client_workbook_from_template(marque, df_client)
                        filename = f"Suivi_{marque}.xlsx"
                        client_files[filename] = xlsx_bytes

                    # ZIP
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
                    st.caption("Téléchargements individuels")
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
    st.subheader("Génération Suivi Yumi (à brancher)")
    st.info(
        "Quand tu m’envoies un exemple **DATA YUMI brute** + un exemple de **fichier final YUMI**, "
        "je branche la même logique (1 fichier par marque, feuilles par chaîne) avec son mapping."
    )
