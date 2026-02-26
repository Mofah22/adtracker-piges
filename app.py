def pm_grid_to_vertical(df_raw: pd.DataFrame, pm_filename: str) -> pd.DataFrame:
    """
    Transforme ton PM 'grille' en table verticale:
    Date | Support | Marque | Code_Ecran | Duree_PM (optionnel)
    Compatible avec 2 templates:
    - Template A: Période / Chaine / Tranche Horaire / Ecran / Programme...
    - Template B: Chaine / Ecran / Tranche horaire / Programme ... (sans Période)
    """
    df = df_raw.copy()

    # --- helper: vérifier si une ligne contient certains mots (accent-insensitive)
    def row_has(i, keywords):
        row = df.iloc[i].tolist()
        row_norm = [norm_txt(x) for x in row]
        return any(any(k in cell for k in keywords) for cell in row_norm)

    # --- 1) trouver la ligne d'en-têtes meta
    meta_header_row = None
    for i in range(min(len(df), 40)):
        # on veut au minimum "CHAINE" et "ECRAN"
        has_chaine = row_has(i, ["CHAINE"])
        has_ecran  = row_has(i, ["ECRAN"])
        # et idéalement "TRANCHE" ou "HORAIRE" ou "PROGRAMME"
        has_context = row_has(i, ["TRANCHE", "HORAIRE", "PROGRAMME", "AVANT", "APRES", "APRÈS"])

        if has_chaine and has_ecran and has_context:
            meta_header_row = i
            break

    if meta_header_row is None:
        raise ValueError("PM: impossible de trouver la ligne d’en-têtes (Chaine / Ecran / Tranche / Programme...).")

    # --- 2) trouver la ligne des dates (beaucoup de dates)
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

    # --- 3) noms de colonnes meta
    meta_cols_names = df.iloc[meta_header_row].tolist()

    # --- 4) map index -> Date
    date_headers = df.iloc[date_header_row].tolist()
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

    # --- 5) data rows commencent après la ligne date
    data_start = date_header_row + 1
    data = df.iloc[data_start:].copy()
    data = data.dropna(how="all")

    # --- 6) trouver index de CHAINE et ECRAN dans la meta header row
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

    # --- 7) construire la table verticale
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
