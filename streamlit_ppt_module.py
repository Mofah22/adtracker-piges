"""
Module Streamlit — Générateur Media Review PPT
===============================================
À intégrer dans la plateforme existante (même fichier app.py ou import séparé).

Usage standalone:
    streamlit run streamlit_ppt_module.py

Usage intégré dans app.py existant:
    from streamlit_ppt_module import render_ppt_module
    render_ppt_module()
"""

import io
import os
from pathlib import Path
from datetime import date

import pandas as pd
import streamlit as st

# Import du moteur
try:
    from ppt_engine import MediaCalculator
except ImportError:
    st.error("ppt_engine.py introuvable. Placez-le dans le même répertoire.")
    st.stop()

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
APP_DIR = Path(__file__).resolve().parent

# Chercher les templates automatiquement dans le dossier courant
def find_templates() -> dict[str, str]:
    templates = {}
    for f in APP_DIR.glob("*.pptx"):
        templates[f.name] = str(f)
    return templates

# ─────────────────────────────────────────────
# RENDER FUNCTION (intégrable)
# ─────────────────────────────────────────────

def render_ppt_module():
    """Affiche le module PPT. Appelable depuis app.py."""

    st.header("📊 Générateur Media Review PPT")
    st.caption("Uploadez votre DATA brute et obtenez un PPT finalisé avec graphiques et commentaires IA.")

    # ── Clé API ──────────────────────────────────────────────────────
    with st.expander("🔑 Configuration API", expanded=False):
        api_key = st.text_input(
            "Clé API Anthropic (pour les commentaires IA)",
            type="password",
            value=os.environ.get("ANTHROPIC_API_KEY", ""),
            help="Obtenez votre clé sur console.anthropic.com. Si vide, les commentaires seront générés automatiquement sans IA.",
        )

    st.divider()

    # ── Étape 1 : Upload DATA ─────────────────────────────────────────
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("**1) Uploader la DATA brute (Excel)**")
        data_file = st.file_uploader(
            "Fichier Excel Imperium",
            type=["xlsx", "csv"],
            key="ppt_data_upload",
            label_visibility="collapsed",
        )
    with col2:
        st.markdown("**2) Template PPT**")
        template_file = st.file_uploader(
            "Template .pptx",
            type=["pptx"],
            key="ppt_template_upload",
            label_visibility="collapsed",
        )

    # ── Chargement DATA ───────────────────────────────────────────────
    df_raw = None
    if data_file:
        try:
            if data_file.name.endswith(".csv"):
                df_raw = pd.read_csv(data_file)
            else:
                df_raw = pd.read_excel(data_file)
            st.success(f"✅ DATA chargée : {len(df_raw):,} lignes | Colonnes : {list(df_raw.columns[:6])}…")
        except Exception as e:
            st.error(f"Erreur chargement DATA : {e}")

    # ── Paramètres de filtrage ─────────────────────────────────────────
    if df_raw is not None:
        st.divider()
        st.markdown("**3) Paramètres de génération**")

        col_a, col_b, col_c = st.columns(3)

        with col_a:
            secteurs = sorted(df_raw["Secteur"].dropna().unique().tolist()) if "Secteur" in df_raw.columns else []
            secteur_sel = st.selectbox("Secteur", secteurs, key="ppt_secteur")

        with col_b:
            if "SousSecteur" in df_raw.columns and secteur_sel:
                df_sect = df_raw[df_raw["Secteur"] == secteur_sel]
                sous_secteurs = ["(Secteur entier)"] + sorted(df_sect["SousSecteur"].dropna().unique().tolist())
            else:
                sous_secteurs = ["(Secteur entier)"]
            sous_sel = st.selectbox("Sous-secteur", sous_secteurs, key="ppt_sous_secteur")
            sous_secteur_val = None if sous_sel == "(Secteur entier)" else sous_sel

        with col_c:
            # Années disponibles (preview)
            if secteur_sel:
                df_preview = df_raw[df_raw["Secteur"] == secteur_sel]
                if sous_secteur_val:
                    df_preview = df_preview[df_preview["SousSecteur"] == sous_secteur_val]
                years_available = sorted(df_preview["Anp"].dropna().unique().tolist()) if "Anp" in df_preview.columns else []
                st.info(f"📅 Années détectées : {', '.join(str(y) for y in years_available)}")

        # Preview stats rapides
        if secteur_sel:
            try:
                calc_preview = MediaCalculator(df_raw, secteur_sel, sous_secteur_val)
                totals = calc_preview.total_by_year()
                if totals:
                    st.markdown("**Aperçu investissements :**")
                    cols_prev = st.columns(len(totals))
                    for i, (y, v) in enumerate(totals.items()):
                        with cols_prev[i]:
                            st.metric(str(int(y)), f"{v/1e6:.1f} M MAD")
            except Exception as e:
                st.warning(f"Aperçu non disponible : {e}")

        # ── Template ──────────────────────────────────────────────────
        template_path = None
        if template_file:
            # Sauvegarder temp
            tmp_path = APP_DIR / f"_tmp_template_{template_file.name}"
            with open(tmp_path, "wb") as f:
                f.write(template_file.read())
            template_path = str(tmp_path)
            st.success(f"✅ Template : {template_file.name}")
        else:
            # Chercher un template par défaut dans le dossier
            templates = find_templates()
            if templates:
                default_tpl = st.selectbox(
                    "Ou choisir un template existant",
                    list(templates.keys()),
                    key="ppt_tpl_select"
                )
                template_path = templates[default_tpl]
                st.info(f"Template sélectionné : {default_tpl}")
            else:
                st.warning("Aucun template .pptx trouvé. Uploadez-en un ci-dessus.")

        # ── Bouton Génération ─────────────────────────────────────────
        st.divider()
        if st.button("🚀 Générer le Media Review PPT", use_container_width=True, type="primary",
                      disabled=(template_path is None)):

            if not secteur_sel:
                st.warning("Sélectionnez un secteur.")
                return

            with st.spinner("Génération en cours… Calculs + Commentaires IA + Injection PPT"):
                progress = st.progress(0, text="Calcul des agrégats...")

                try:
                    # Calculs
                    progress.progress(20, text="📊 Calcul des agrégats...")
                    calc = MediaCalculator(df_raw, secteur_sel, sous_secteur_val)
                    if not calc.years:
                        st.error("Aucune donnée pour ce filtre.")
                        return
                    stats = calc.summary_stats()

                    # Commentaires IA
                    progress.progress(50, text="🤖 Génération des commentaires IA...")
                    from ppt_engine import generate_comments_via_claude
                    label = sous_secteur_val or secteur_sel
                    effective_api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
                    comments = generate_comments_via_claude(stats, secteur_sel, label, effective_api_key)

                    # Injection PPT
                    progress.progress(75, text="💉 Injection dans le template PPT...")
                    from ppt_engine import PPTInjector
                    injector = PPTInjector(template_path)
                    pptx_bytes = injector.generate(calc, comments, secteur_sel, label)

                    progress.progress(100, text="✅ Terminé !")

                    # Nettoyer temp
                    if template_file and Path(str(template_path)).name.startswith("_tmp_"):
                        try:
                            os.remove(template_path)
                        except:
                            pass

                    # ── Résultat ─────────────────────────────────────
                    st.success("✅ Media Review généré avec succès !")

                    filename = f"Media_Review_{label.replace(' ', '_')}_{calc.year_last}.pptx"
                    st.download_button(
                        label=f"📥 Télécharger {filename}",
                        data=pptx_bytes,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        use_container_width=True,
                    )

                    # Aperçu commentaires générés
                    with st.expander("💬 Commentaires générés par IA", expanded=True):
                        for key, val in comments.items():
                            slide_label = {
                                "slide2_headline": "Slide 2 — Headline",
                                "slide2_global": "Slide 2 — Commentaire global",
                                "slide3_annonceurs": "Slide 3 — Annonceurs",
                                "slide4_ooh": "Slide 4 — OOH/Affichage",
                                "slide5_tv": "Slide 5 — TV",
                                "slide6_rd": "Slide 6 — Radio",
                            }.get(key, key)
                            st.markdown(f"**{slide_label}**")
                            st.write(val)
                            st.divider()

                except Exception as e:
                    st.error(f"Erreur lors de la génération : {e}")
                    import traceback
                    st.code(traceback.format_exc())

    elif data_file is None:
        st.info("👆 Commencez par uploader votre fichier DATA Excel.")


# ─────────────────────────────────────────────
# MODE STANDALONE
# ─────────────────────────────────────────────
if __name__ == "__main__":
    st.set_page_config(
        page_title="Media Review PPT Generator",
        page_icon="📊",
        layout="wide"
    )
    st.markdown("""
    <style>
    .main { background-color: #f8fafc; }
    .stButton>button {
        border-radius: 8px;
        font-weight: bold;
    }
    .stDownloadButton>button {
        background-color: #43b581 !important;
        color: white !important;
        border-radius: 8px;
    }
    </style>
    """, unsafe_allow_html=True)

    render_ppt_module()
