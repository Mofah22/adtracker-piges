def fill_codeecranpm_commentaire_per_client_yumi(df_client: pd.DataFrame, pm_client: pd.DataFrame, max_date: date):
    df = df_client.copy()
    df["date_only"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["t_real"] = df["H.Début"].apply(to_excel_time)

    if pm_client is None or pm_client.empty:
        base = df.copy()
        for col in FINAL_COLUMNS_YUMI:
            if col not in base.columns:
                base[col] = None
        return base[FINAL_COLUMNS_YUMI]

    pm = pm_client.copy()
    pm = pm[pm["date_only"].notna()]
    pm = pm[pm["date_only"] <= max_date]

    out_all = []
    backlog = {}

    supports_real = set(df["support_norm"].dropna().unique())
    supports_pm = set(pm["support_norm"].dropna().unique())
    all_supports = sorted(list(supports_real | supports_pm))

    def pm_tv_min_safe(code_pm, fallback):
        # 1) utiliser fallback si numérique
        try:
            if fallback is not None and pd.notna(fallback):
                return int(fallback)
        except:
            pass
        # 2) sinon recalcul depuis code_pm
        _, _, tvm = parse_codepm_time(code_pm)
        return tvm if tvm is not None else 10**9

    def insert_minimal_row_yumi(dte, chaine, codepm):
        row = {c: None for c in FINAL_COLUMNS_YUMI}
        dts = pd.to_datetime(dte)
        row["Date"] = dts
        row["Chaîne"] = chaine
        row["N° Mois"] = int(dts.month) if not pd.isna(dts) else None
        row["Année"] = int(dts.year) if not pd.isna(dts) else None
        row["Code Ecran PM"] = codepm
        row["Commentaire"] = "Non diffusé"
        return row

    def sort_key_tv_yumi(row):
        t = to_excel_time(row.get("H.Début"))
        if t is not None:
            return real_tv_minutes(t)
        _, _, tvm = parse_codepm_time(row.get("Code Ecran PM"))
        return tvm if tvm is not None else 10**9

    # pour Anticipé : on prend le 1er PM du lendemain (le plus tôt)
    def pop_first_pm(pm_next_df):
        if pm_next_df is None or pm_next_df.empty:
            return None, pm_next_df
        first = pm_next_df.iloc[0]
        pm_next_df = pm_next_df.drop(index=first.name, errors="ignore")
        return first, pm_next_df

    for sn in all_supports:
        backlog.setdefault(sn, 0)

        real_s = df[df["support_norm"] == sn].copy()
        pm_s = pm[pm["support_norm"] == sn].copy()

        if not real_s.empty:
            sup_display = str(real_s.iloc[0]["Chaîne"])
        elif not pm_s.empty:
            sup_display = str(pm_s.iloc[0]["supportp"])
        else:
            sup_display = str(sn)

        dates_real = set(real_s["date_only"].dropna().unique())
        dates_pm = set(pm_s["date_only"].dropna().unique())
        all_dates = sorted(list(dates_real | dates_pm))

        # dictionnaire PM par date
        pm_by_date = {}
        for dd in all_dates:
            pdd = pm_s[pm_s["date_only"] == dd].copy()
            if not pdd.empty:
                # tri STRICT par heure PM (safe)
                pdd["_PM_TV_MIN_SAFE"] = pdd.apply(
                    lambda r: pm_tv_min_safe(r.get("Code PM"), r.get("PM_TV_MIN")),
                    axis=1
                )
                pdd = pdd.sort_values("_PM_TV_MIN_SAFE", na_position="last").drop(columns=["_PM_TV_MIN_SAFE"], errors="ignore")
            pm_by_date[dd] = pdd

        for d in all_dates:
            if d > max_date:
                continue

            # réels triés par heure
            real_day = real_s[real_s["date_only"] == d].copy()
            real_day["_rt"] = real_day["t_real"].apply(lambda t: real_tv_minutes(t))
            real_day = real_day.sort_values("_rt", na_position="last").drop(columns=["_rt"], errors="ignore")

            # PM triés par heure (safe)
            pm_day = pm_by_date.get(d, pd.DataFrame()).copy()
            if not pm_day.empty:
                pm_day["_PM_TV_MIN_SAFE"] = pm_day.apply(
                    lambda r: pm_tv_min_safe(r.get("Code PM"), r.get("PM_TV_MIN")),
                    axis=1
                )
                pm_day = pm_day.sort_values("_PM_TV_MIN_SAFE", na_position="last").drop(columns=["_PM_TV_MIN_SAFE"], errors="ignore")

            real_n = len(real_day)
            pm_n = len(pm_day)

            filled_rows = []
            inserted_rows = []

            # Cas aucun réel mais PM => Non diffusé (sur D)
            if real_n == 0 and pm_n > 0:
                for _, pm_row in pm_day.iterrows():
                    inserted_rows.append(insert_minimal_row_yumi(d, sup_display, pm_row["Code PM"]))
                backlog[sn] += pm_n

            else:
                # 1) MATCH STRICT PAR ORDRE CHRONO
                n_match = min(real_n, pm_n)
                for i in range(n_match):
                    r = real_day.iloc[i].copy()
                    pm_row = pm_day.iloc[i]

                    r["Code Ecran PM"] = pm_row["Code PM"]

                    # Décalage (on garde la règle: si > 45min)
                    diff = None
                    if r["t_real"] is not None:
                        rt = real_tv_minutes(r["t_real"])
                        # recalcul safe PM minute
                        pm_min = pm_tv_min_safe(pm_row.get("Code PM"), pm_row.get("PM_TV_MIN"))
                        if rt is not None and pm_min is not None and pm_min < 10**9:
                            diff = abs(rt - pm_min)

                    overnight_flag = bool(pm_row.get("Overnight", False))
                    if (not overnight_flag) and diff is not None and diff > DECALAGE_MINUTES:
                        r["Commentaire"] = "Décalage"
                    else:
                        r["Commentaire"] = None

                    filled_rows.append(r)

                # 2) Réels restants = extras => Anticipé (D+1) sinon Compensation / Passage supplémentaire
                if real_n > pm_n:
                    next_d = d + timedelta(days=1)
                    pm_next = pm_by_date.get(next_d, pd.DataFrame()).copy()
                    if not pm_next.empty:
                        pm_next["_PM_TV_MIN_SAFE"] = pm_next.apply(
                            lambda r: pm_tv_min_safe(r.get("Code PM"), r.get("PM_TV_MIN")),
                            axis=1
                        )
                        pm_next = pm_next.sort_values("_PM_TV_MIN_SAFE", na_position="last").drop(columns=["_PM_TV_MIN_SAFE"], errors="ignore")

                    for i in range(n_match, real_n):
                        r = real_day.iloc[i].copy()
                        r["Code Ecran PM"] = None
                        r["Commentaire"] = None

                        pm_first, pm_next = pop_first_pm(pm_next)
                        if pm_first is not None:
                            # consomme le PM du lendemain = Anticipé
                            r["Code Ecran PM"] = pm_first["Code PM"]
                            r["Commentaire"] = "Anticipé"
                            # retirer du dictionnaire pour éviter Non diffusé demain
                            pm_by_date[next_d] = pm_next.copy()
                        else:
                            # pas d'anticipé => logique normale
                            if backlog[sn] > 0:
                                r["Commentaire"] = "Compensation"
                                backlog[sn] -= 1
                            else:
                                r["Commentaire"] = "Passage supplémentaire"

                        filled_rows.append(r)

                # 3) PM restants = Non diffusé
                if pm_n > real_n:
                    remaining_pm = pm_day.iloc[real_n:].copy()
                    for _, pm_row in remaining_pm.iterrows():
                        inserted_rows.append(insert_minimal_row_yumi(d, sup_display, pm_row["Code PM"]))
                    backlog[sn] += (pm_n - real_n)

            df_filled = pd.DataFrame(filled_rows) if filled_rows else pd.DataFrame()
            df_insert = pd.DataFrame(inserted_rows) if inserted_rows else pd.DataFrame(columns=FINAL_COLUMNS_YUMI)

            if not df_filled.empty:
                df_filled["_sort_t"] = df_filled.apply(lambda rr: sort_key_tv_yumi(rr), axis=1)
                df_filled["Chaîne"] = sup_display
            if not df_insert.empty:
                df_insert["_sort_t"] = df_insert.apply(lambda rr: sort_key_tv_yumi(rr), axis=1)

            out_day = pd.concat([x for x in [df_filled, df_insert] if not x.empty], ignore_index=True) \
                     if (not df_filled.empty or not df_insert.empty) else pd.DataFrame(columns=FINAL_COLUMNS_YUMI)

            out_day = out_day.sort_values("_sort_t", na_position="last").drop(columns=["_sort_t"], errors="ignore")
            out_all.append(out_day[FINAL_COLUMNS_YUMI])

    out_df = pd.concat(out_all, ignore_index=True) if out_all else df.copy()
    return out_df[FINAL_COLUMNS_YUMI]
