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

# Import du moteur — fait au niveau module mais sans st.stop() au top level
_ppt_engine_ok = False
try:
    from ppt_engine import MediaCalculator
    _ppt_engine_ok = True
except ImportError:
    pass

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
# FONCTIONS CACHÉES — évitent les recalculs
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _load_dataframe(file_bytes: bytes, file_name: str) -> pd.DataFrame:
    """Charge le DataFrame une seule fois — mis en cache par contenu du fichier."""
    if file_name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes))
    return pd.read_excel(io.BytesIO(file_bytes))


@st.cache_data(show_spinner=False)
def _load_template(template_path: str) -> bytes:
    """Lit le template PPT une seule fois — mis en cache par chemin."""
    with open(template_path, "rb") as f:
        return f.read()


@st.cache_data(show_spinner=False)
def _compute_stats(
    file_bytes: bytes, file_name: str,
    secteur: str, sous_secteurs: tuple  # tuple pour être hashable
) -> dict:
    """Calcule les stats agrégées — recalcul uniquement si les filtres changent."""
    from ppt_engine import MediaCalculator
    df = _load_dataframe(file_bytes, file_name)
    ss = list(sous_secteurs) if sous_secteurs else None
    calc = MediaCalculator(df, secteur, ss)
    return calc.summary_stats()


@st.cache_data(show_spinner=False)
def _get_medias_and_totals(
    file_bytes: bytes, file_name: str,
    secteur: str, sous_secteurs: tuple
) -> dict:
    """Retourne totaux par année + médias présents pour l'aperçu."""
    from ppt_engine import MediaCalculator
    df = _load_dataframe(file_bytes, file_name)
    ss = list(sous_secteurs) if sous_secteurs else None
    calc = MediaCalculator(df, secteur, ss)
    return {
        "totals": calc.total_by_year(),
        "years": calc.years,
        "medias": calc.medias_present,
    }

# ─────────────────────────────────────────────
# RENDER FUNCTION (intégrable)
# ─────────────────────────────────────────────

def render_ppt_module():
    """Affiche le module PPT. Appelable depuis app.py."""
    if not _ppt_engine_ok:
        st.error("ppt_engine.py introuvable. Vérifiez que le fichier est bien dans le repo GitHub.")
        return

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
    st.markdown("**1) Uploader la DATA brute (Excel)**")
    data_file = st.file_uploader(
        "Fichier Excel Imperium",
        type=["xlsx", "csv"],
        key="ppt_data_upload",
        label_visibility="collapsed",
    )
    template_file = None  # template auto-détecté depuis le repo

    # ── Chargement DATA ───────────────────────────────────────────────
    df_raw = None
    file_bytes = None
    if data_file:
        try:
            file_bytes = data_file.read()
            df_raw = _load_dataframe(file_bytes, data_file.name)
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
                sous_secteurs_available = sorted(df_sect["SousSecteur"].dropna().unique().tolist())
            else:
                sous_secteurs_available = []
            sous_sel = st.multiselect(
                "Sous-secteur(s)",
                sous_secteurs_available,
                placeholder="Laisser vide = secteur entier",
                key="ppt_sous_secteur"
            )
            # Liste vide = secteur entier ; sinon liste de sous-secteurs sélectionnés
            sous_secteur_val = sous_sel if sous_sel else None

        with col_c:
            # Années disponibles (preview)
            if secteur_sel:
                df_preview = df_raw[df_raw["Secteur"] == secteur_sel]
                if sous_secteur_val:
                    df_preview = df_preview[df_preview["SousSecteur"].isin(sous_secteur_val)]
                years_available = sorted(df_preview["Anp"].dropna().unique().tolist()) if "Anp" in df_preview.columns else []
                st.info(f"📅 Années détectées : {', '.join(str(y) for y in years_available)}")

        # Preview stats rapides — via cache (pas de recalcul si filtres inchangés)
        if secteur_sel and file_bytes is not None:
            try:
                ss_tuple = tuple(sous_secteur_val) if sous_secteur_val else ()
                preview = _get_medias_and_totals(file_bytes, data_file.name, secteur_sel, ss_tuple)
                totals = preview["totals"]
                if totals:
                    st.markdown("**Aperçu investissements :**")
                    cols_prev = st.columns(len(totals))
                    for i, (y, v) in enumerate(totals.items()):
                        with cols_prev[i]:
                            st.metric(str(int(y)), f"{v/1e6:.1f} M MAD")
            except Exception as e:
                st.warning(f"Aperçu non disponible : {e}")

        # ── Template — auto-détection depuis le repo ─────────────────
        template_path = None
        templates = find_templates()
        if templates:
            # Prendre le premier template trouvé (ou laisser choisir si plusieurs)
            if len(templates) == 1:
                template_path = list(templates.values())[0]
            else:
                default_tpl = st.selectbox(
                    "Template PPT",
                    list(templates.keys()),
                    key="ppt_tpl_select"
                )
                template_path = templates[default_tpl]
        else:
            st.warning("⚠️ Aucun template .pptx trouvé dans le repo. Ajoutez-en un sur GitHub.")

        # ── Bouton Génération ─────────────────────────────────────────
        st.divider()
        if st.button("🚀 Générer le Media Review PPT", use_container_width=True, type="primary",
                      disabled=(template_path is None)):

            if not secteur_sel:
                st.warning("Sélectionnez un secteur.")
                return

            with st.spinner("Génération en cours… Commentaires IA + Injection PPT"):
                progress = st.progress(0, text="Calcul des agrégats...")

                try:
                    # Calculs — via cache si déjà fait avec ce filtre
                    progress.progress(15, text="📊 Calcul des agrégats...")
                    from ppt_engine import MediaCalculator as _MC, generate_comments_via_claude, PPTInjector
                    ss_val = list(sous_secteur_val) if sous_secteur_val else None
                    calc = _MC(df_raw, secteur_sel, ss_val)
                    if not calc.years:
                        st.error("Aucune donnée pour ce filtre.")
                        return
                    stats = calc.summary_stats()

                    # Label
                    if sous_secteur_val and len(sous_secteur_val) == 1:
                        label = sous_secteur_val[0]
                    elif sous_secteur_val and len(sous_secteur_val) > 1:
                        label = " + ".join(sous_secteur_val)
                    else:
                        label = secteur_sel

                    # Commentaires IA
                    progress.progress(40, text="🤖 Génération des commentaires IA...")
                    effective_api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
                    comments = generate_comments_via_claude(stats, secteur_sel, label, effective_api_key)

                    # Template — chargé depuis cache (lecture disque évitée si déjà lu)
                    progress.progress(70, text="💉 Injection dans le template PPT...")
                    tpl_bytes = _load_template(template_path)
                    injector = PPTInjector.__new__(PPTInjector)
                    injector.template_path = template_path
                    injector.template_raw = tpl_bytes
                    pptx_bytes = injector.generate(calc, comments, secteur_sel, label)

                    progress.progress(100, text="✅ Terminé !")

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
