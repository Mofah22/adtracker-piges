def match_day_exact_then_order_swap(rt_minutes, pm_minutes, real_codes, pm_codes):
    """
    Version FIX: Exact-match verrouillé + matching optimal (DP) qui peut:
    - sauter un PM (=> Non diffusé) au lieu de forcer l'ordre
    - éviter les mauvais matchs qui créent des Décalage faux
    Return: assign list len(rt): assign[i] = j index in pm list or None
    """
    n = len(rt_minutes)
    m = len(pm_minutes)

    assign = [None] * n
    used_pm = set()
    locked_real = set()

    # ---- 0) exact match first (digits hhmm), choix 1 = premier PM dispo
    if real_codes and pm_codes and len(real_codes) == n and len(pm_codes) == m:
        for i in range(n):
            rc = real_codes[i]
            if not rc:
                continue
            for j in range(m):
                if j in used_pm:
                    continue
                if pm_codes[j] == rc:
                    assign[i] = j
                    used_pm.add(j)
                    locked_real.add(i)
                    break

    # indices restants
    real_rem = [i for i in range(n) if i not in locked_real]
    pm_rem = [j for j in range(m) if j not in used_pm]

    if not USE_OPTIMAL_MATCHING:
        # fallback: ton ancien comportement ordre+swap (sans casser exact match)
        remaining_real = [i for i in range(n) if assign[i] is None]
        k = min(len(remaining_real), len(pm_rem))
        for idx in range(k):
            assign[remaining_real[idx]] = pm_rem[idx]
        return assign

    # ---- 1) DP (sequence alignment) sur les restants
    # dp[a][b] = coût minimal pour traiter real_rem[:a] et pm_rem[:b]
    # moves:
    # - match (a-1, b-1)
    # - skip pm (a, b-1) => Non diffusé
    # - skip real (a-1, b) => real sans PM
    INF = 10**15
    A = len(real_rem)
    B = len(pm_rem)

    dp = [[INF] * (B + 1) for _ in range(A + 1)]
    prev = [[None] * (B + 1) for _ in range(A + 1)]
    dp[0][0] = 0

    # init: skip pm only
    for b in range(1, B + 1):
        dp[0][b] = dp[0][b - 1] + PM_SKIP_PENALTY
        prev[0][b] = ("SKIP_PM", 0, b - 1)

    # init: skip real only
    for a in range(1, A + 1):
        dp[a][0] = dp[a - 1][0] + REAL_SKIP_PENALTY
        prev[a][0] = ("SKIP_REAL", a - 1, 0)

    def match_cost(rt, pm):
        if rt is None or pm is None or rt >= 10**9 or pm >= 10**9:
            return HARD_MAX_MATCH * 10  # très cher
        d = abs(rt - pm)
        # au-delà d'un seuil, on décourage fortement
        if d > HARD_MAX_MATCH:
            return d + 5000
        return d

    for a in range(1, A + 1):
        i = real_rem[a - 1]
        rt = rt_minutes[i]
        for b in range(1, B + 1):
            j = pm_rem[b - 1]
            pmv = pm_minutes[j]

            # 1) match
            c_match = dp[a - 1][b - 1] + match_cost(rt, pmv)
            best = c_match
            best_prev = ("MATCH", a - 1, b - 1)

            # 2) skip pm
            c_spm = dp[a][b - 1] + PM_SKIP_PENALTY
            if c_spm < best:
                best = c_spm
                best_prev = ("SKIP_PM", a, b - 1)

            # 3) skip real
            c_sreal = dp[a - 1][b] + REAL_SKIP_PENALTY
            if c_sreal < best:
                best = c_sreal
                best_prev = ("SKIP_REAL", a - 1, b)

            dp[a][b] = best
            prev[a][b] = best_prev

    # backtrack
    a, b = A, B
    chosen_pairs = []
    while a > 0 or b > 0:
        step, pa, pb = prev[a][b]
        if step == "MATCH":
            i = real_rem[a - 1]
            j = pm_rem[b - 1]
            chosen_pairs.append((i, j))
        a, b = pa, pb

    # appliquer pairs
    for i, j in chosen_pairs:
        assign[i] = j

    return assign
