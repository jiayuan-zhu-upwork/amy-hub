WITH a AS (
    SELECT
        DATE_TRUNC('week', CURRENT_DATE)::date    AS ws,
        DATE_TRUNC('month', CURRENT_DATE)::date   AS ms,
        DATE_TRUNC('quarter', CURRENT_DATE)::date AS qs,
        DATE_TRUNC('year', CURRENT_DATE)::date    AS ys
)

, base as 
(
SELECT 
        user_uid,
        visit_date,
        is_client,
        is_freelancer,
        a.ws,
        a.ms,
        a.qs,
        a.ys
FROM sdc_user.amyzhu.new_wbr_login_stg
CROSS JOIN a
)

, breakdowns as (
-- Add new breakdowns here as additional UNION ALL blocks.
-- Each row needs: breakdown label, contract_key, hire_date, ws, ms, qs, ys.
    SELECT 'Login Client' as breakdown,      user_uid, visit_date, ws, ms, qs, ys FROM base WHERE is_client
    UNION ALL
    SELECT 'Login Talent',                   user_uid, visit_date, ws, ms, qs, ys FROM base WHERE is_freelancer
    
)

SELECT
    breakdown,

    -- Weekly
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 84 AND ws - 78, user_uid, NULL)) AS "W-11",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 77 AND ws - 71, user_uid, NULL)) AS "W-10",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 70 AND ws - 64, user_uid, NULL)) AS "W-9",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 63 AND ws - 57, user_uid, NULL)) AS "W-8",

    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 56 AND ws - 50, user_uid, NULL)) AS "W-7",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 49 AND ws - 43, user_uid, NULL)) AS "W-6",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 42 AND ws - 36, user_uid, NULL)) AS "W-5",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 35 AND ws - 29, user_uid, NULL)) AS "W-4",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 28 AND ws - 22, user_uid, NULL)) AS "W-3",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 21 AND ws - 15, user_uid, NULL)) AS "W-2",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, user_uid, NULL)) AS "W-1",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, user_uid, NULL)) AS "W-0",

    -- Previous 4W Avg (W-4 through W-1)
    (
        COUNT(DISTINCT IFF(visit_date BETWEEN ws - 35 AND ws - 29, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 28 AND ws - 22, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 21 AND ws - 15, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, user_uid, NULL))
    ) / 4.0 AS "Previous 4W Avg",

    -- T4W Avg (W-0 through W-3)
    (
        COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 21 AND ws - 15, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 28 AND ws - 22, user_uid, NULL))
    ) / 4.0 AS "T4W Avg",

    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, user_uid, NULL))
    ) - 1 AS "W-0 WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date BETWEEN ws -   7 AND ws -   1, user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date BETWEEN ws - 371 AND ws - 365, user_uid, NULL))
    ) - 1 AS "W-0 YoY(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 21 AND ws - 15, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 28 AND ws - 22, user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date BETWEEN ws - 35 AND ws - 29, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 42 AND ws - 36, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 49 AND ws - 43, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 56 AND ws - 50, user_uid, NULL))
    ) - 1 AS "T4W WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date BETWEEN ws -   7 AND ws -   1, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws -  14 AND ws -   8, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws -  21 AND ws -  15, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws -  28 AND ws -  22, user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date BETWEEN ws - 371 AND ws - 365, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 378 AND ws - 372, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 385 AND ws - 379, user_uid, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 392 AND ws - 386, user_uid, NULL))
    ) - 1 AS "T4W YoY(%)",

    -- Monthly
    COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -4, ms) AND visit_date < DATEADD('month', -3, ms), user_uid, NULL)) AS "M-3",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -3, ms) AND visit_date < DATEADD('month', -2, ms), user_uid, NULL)) AS "M-2",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms), user_uid, NULL)) AS "M-1",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -1, ms) AND visit_date < ms,                       user_uid, NULL)) AS "M-0",
    COUNT(DISTINCT IFF(visit_date >= ms                        AND visit_date < CURRENT_DATE,             user_uid, NULL)) AS "MtD",

    -- M-2 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms), user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -14, ms) AND visit_date < DATEADD('month', -13, ms), user_uid, NULL))
    ) - 1 AS "M-2 YoY(%)",

    -- M-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -1, ms) AND visit_date < ms, user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -13, ms) AND visit_date < DATEADD('month', -12, ms), user_uid, NULL))
    ) - 1 AS "M-1 YoY(%)",

    -- M-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= ms AND visit_date < DATEADD('month', 1, ms), user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -12, ms) AND visit_date < DATEADD('month', -11, ms), user_uid, NULL))
    ) - 1 AS "M-0 YoY(%)",

    -- MtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= ms AND visit_date < CURRENT_DATE, user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ms) AND visit_date < DATEADD('year', -1, CURRENT_DATE), user_uid, NULL))
    ) - 1 AS "MtD YoY(%)",

    -- Quarterly
    COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -4, qs) AND visit_date < DATEADD('quarter', -3, qs), user_uid, NULL)) AS "Q-3",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -3, qs) AND visit_date < DATEADD('quarter', -2, qs), user_uid, NULL)) AS "Q-2",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs), user_uid, NULL)) AS "Q-1",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs,                         user_uid, NULL)) AS "Q-0",
    COUNT(DISTINCT IFF(visit_date >= qs                          AND visit_date < CURRENT_DATE,               user_uid, NULL)) AS "QtD",

    -- Q-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs), user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -6, qs) AND visit_date < DATEADD('quarter', -5, qs), user_uid, NULL))
    ) - 1 AS "Q-1 YoY(%)",

    -- Q-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs, user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -5, qs) AND visit_date < DATEADD('quarter', -4, qs), user_uid, NULL))
    ) - 1 AS "Q-0 YoY(%)",

    -- QtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= qs AND visit_date < CURRENT_DATE, user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, qs) AND visit_date < DATEADD('year', -1, CURRENT_DATE), user_uid, NULL))
    ) - 1 AS "QtD YoY(%)",

    -- Yearly
    COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys), user_uid, NULL)) AS "Y-1",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < ys,                      user_uid, NULL)) AS "Y-0",
    COUNT(DISTINCT IFF(visit_date >= ys                       AND visit_date < CURRENT_DATE,            user_uid, NULL)) AS "YtD",

    -- Y-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < ys, user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys), user_uid, NULL))
    ) - 1 AS "Y-0 YoY(%)",

    -- YtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= ys AND visit_date < CURRENT_DATE, user_uid, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < DATEADD('year', -1, CURRENT_DATE), user_uid, NULL))
    ) - 1 AS "YtD YoY(%)"

FROM breakdowns
GROUP BY breakdown
ORDER BY
    CASE breakdown
        WHEN 'Login Client'     THEN 1
        WHEN 'Login Talent'     THEN 2
        ELSE 99
    END
;