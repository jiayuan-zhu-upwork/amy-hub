

---------- CL Reg to High-Intent CSW Output -------------
WITH a AS (
    SELECT
        DATE_TRUNC('week', CURRENT_DATE)::date    AS ws,
        DATE_TRUNC('month', CURRENT_DATE)::date   AS ms,
        DATE_TRUNC('quarter', CURRENT_DATE)::date AS qs,
        DATE_TRUNC('year', CURRENT_DATE)::date    AS ys
)
, registration as 
(
SELECT 
    cl_fl_key,
    sherlock_registration_fact.registration_date AS reg_date,
    cl_fl_type,
    max(case when csw.activity_date between sherlock_registration_fact.registration_date and sherlock_registration_fact.registration_date + 7 then true else false end) is_high_intent_csw_7d,
    max(case when cdv.first_spend_date between sherlock_registration_fact.registration_date and sherlock_registration_fact.registration_date + 7 then true else false end) is_cl_start_7d,
    max(case when cdv.first_spend_date between sherlock_registration_fact.registration_date and sherlock_registration_fact.registration_date + 28 then true else false end) is_cl_start_28d
FROM dm_iom.sherlock_registration_fact_vw AS sherlock_registration_fact
LEFT JOIN "SHASTA_SDC_PUBLISHED"."DM_IOM"."CSW_CLIENT_SEEKING_WORK_FACT_VW" csw
    on sherlock_registration_fact.cl_fl_key = csw.client_key
    and sherlock_registration_fact.cl_fl_type = 'client'
    and activity_type in ('messaged_without_linked_post_or_project','posted_job_post')
    and activity_date >= sherlock_registration_fact.registration_date
LEFT JOIN "SHASTA_SDC_PUBLISHED"."SHERLOCK"."CLIENT_DIM_VW" cdv
    on sherlock_registration_fact.cl_fl_key = cdv.client_key
WHERE (NOT (CASE WHEN sherlock_registration_fact.cl_fl_type = 'client' THEN sherlock_registration_fact.cl_fl_is_bad_actor ELSE NULL END) OR (CASE WHEN sherlock_registration_fact.cl_fl_type = 'client' THEN sherlock_registration_fact.cl_fl_is_bad_actor ELSE NULL END) IS NULL) 
    AND (NOT (CASE WHEN sherlock_registration_fact.cl_fl_type = 'freelancer' THEN sherlock_registration_fact.cl_fl_is_bad_actor ELSE NULL END) OR (CASE WHEN sherlock_registration_fact.cl_fl_type = 'freelancer' THEN sherlock_registration_fact.cl_fl_is_bad_actor ELSE NULL END) IS NULL) 
    AND (NOT (sherlock_registration_fact.user_is_bad_actor ) OR (sherlock_registration_fact.user_is_bad_actor ) IS NULL)
    AND sherlock_registration_fact.registration_date::date >= DATEADD('year', -3, date_trunc('year', current_date()))
    AND sherlock_registration_fact.registration_date::date < CURRENT_DATE()
GROUP BY ALL
)

, base as 
(
SELECT cl_fl_key,
        reg_date,
        cl_fl_type,
        is_high_intent_csw_7d,
        is_cl_start_7d,
        is_cl_start_28d,
        a.ws,
        a.ms,
        a.qs,
        a.ys
FROM registration
CROSS JOIN a
)

-- Add new breakdowns here as additional UNION ALL blocks.
-- Each row needs: breakdown label, contract_key, hire_date, ws, ms, qs, ys.
, breakdowns AS (
    SELECT 'CL Reg to High-Intent CSW' AS breakdown, cl_fl_key, is_high_intent_csw_7d, reg_date, ws, ms, qs, ys FROM base WHERE cl_fl_type = 'client'
    -- UNION ALL
    -- SELECT 'CL Reg to CL Start' AS breakdown, cl_fl_key, is_cl_start_7d, reg_date, ws, ms, qs, ys FROM base WHERE cl_fl_type = 'client'
)

SELECT
    breakdown,

    -- Weekly
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  84 AND ws -  78) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 84 AND ws - 78, cl_fl_key, NULL)) AS "W-11",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  77 AND ws -  71) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 77 AND ws - 71, cl_fl_key, NULL)) AS "W-10",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  70 AND ws -  64) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 70 AND ws - 64, cl_fl_key, NULL)) AS "W-9",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  63 AND ws -  57) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 63 AND ws - 57, cl_fl_key, NULL)) AS "W-8",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  56 AND ws -  50) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 56 AND ws - 50, cl_fl_key, NULL)) AS "W-7",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  49 AND ws -  43) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 49 AND ws - 43, cl_fl_key, NULL)) AS "W-6",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  42 AND ws -  36) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 42 AND ws - 36, cl_fl_key, NULL)) AS "W-5",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  35 AND ws -  29) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 35 AND ws - 29, cl_fl_key, NULL)) AS "W-4",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  28 AND ws -  22) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 28 AND ws - 22, cl_fl_key, NULL)) AS "W-3",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  21 AND ws -  15) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 21 AND ws - 15, cl_fl_key, NULL)) AS "W-2",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  14 AND ws -  8) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 14 AND ws -  8, cl_fl_key, NULL)) AS "W-1",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  7 AND ws -  1) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  7 AND ws -  1, cl_fl_key, NULL)) AS "W-0",

    -- Previous 4W Avg (W-4 through W-1)
    (
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  35 AND ws -  8) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  35 AND ws -  8, cl_fl_key, NULL))
    ) AS "Previous 4W Avg",

    -- T4W Avg (W-0 through W-3)
    (
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  28 AND ws -  1) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  28 AND ws -  1, cl_fl_key, NULL))
    ) AS "T4W Avg",

    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  7 AND ws -  1) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  7 AND ws -  1, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  14 AND ws -  8) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 14 AND ws -  8, cl_fl_key, NULL))
    ) - 1 AS "W-0 WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  7 AND ws -  1) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  7 AND ws -  1, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date BETWEEN ws - 371 AND ws - 365) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  371 AND ws -  365, cl_fl_key, NULL))
    ) - 1 AS "W-0 YoY(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  28 AND ws -  1) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  28 AND ws -  1, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  56 AND ws -  29) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  56 AND ws -  29, cl_fl_key, NULL))
    ) - 1 AS "T4W WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  28 AND ws -  1) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  28 AND ws -  1, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  392 AND ws -  365) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  392 AND ws -  365, cl_fl_key, NULL))
    ) - 1 AS "T4W YoY(%)",

    -- Monthly
    COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -4, ms) AND reg_date < DATEADD('month', -3, ms)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -4, ms) AND reg_date < DATEADD('month', -3, ms), cl_fl_key, NULL)) AS "M-3",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -3, ms) AND reg_date < DATEADD('month', -2, ms)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -3, ms) AND reg_date < DATEADD('month', -2, ms), cl_fl_key, NULL)) AS "M-2",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -2, ms) AND reg_date < DATEADD('month', -1, ms)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -2, ms) AND reg_date < DATEADD('month', -1, ms), cl_fl_key, NULL)) AS "M-1",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -1, ms) AND reg_date < ms) AND (is_high_intent_csw_7d),                       cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -1, ms) AND reg_date < ms,                       cl_fl_key, NULL)) AS "M-0",
    COUNT(DISTINCT IFF((reg_date >= ms                        AND reg_date < DATEADD('day', 28, CURRENT_DATE)) AND (is_high_intent_csw_7d),             cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= ms                        AND reg_date < DATEADD('day', 28, CURRENT_DATE),             cl_fl_key, NULL)) AS "MtD",

    -- M-2 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -2, ms) AND reg_date < DATEADD('month', -1, ms)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -2, ms) AND reg_date < DATEADD('month', -1, ms), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -14, ms) AND reg_date < DATEADD('month', -13, ms)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -14, ms) AND reg_date < DATEADD('month', -13, ms), cl_fl_key, NULL))
    ) - 1 AS "M-2 YoY(%)",

    -- M-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -1, ms) AND reg_date < ms) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -1, ms) AND reg_date < ms, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -13, ms) AND reg_date < DATEADD('month', -12, ms)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -13, ms) AND reg_date < DATEADD('month', -12, ms), cl_fl_key, NULL))
    ) - 1 AS "M-1 YoY(%)",

    -- M-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= ms AND reg_date < DATEADD('month', 1, ms)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= ms AND reg_date < DATEADD('month', 1, ms), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -12, ms) AND reg_date < DATEADD('month', -11, ms)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -12, ms) AND reg_date < DATEADD('month', -11, ms), cl_fl_key, NULL))
    ) - 1 AS "M-0 YoY(%)",

    -- MtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= ms AND reg_date < DATEADD('day', 28, CURRENT_DATE)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= ms AND reg_date < DATEADD('day', 28, CURRENT_DATE), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -1, ms) AND reg_date < DATEADD('year', -1, DATEADD('day', 28, CURRENT_DATE))) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -1, ms) AND reg_date < DATEADD('year', -1, DATEADD('day', 28, CURRENT_DATE)), cl_fl_key, NULL))
    ) - 1 AS "MtD YoY(%)",

    -- Quarterly
    COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -4, qs) AND reg_date < DATEADD('quarter', -3, qs)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -4, qs) AND reg_date < DATEADD('quarter', -3, qs), cl_fl_key, NULL)) AS "Q-3",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -3, qs) AND reg_date < DATEADD('quarter', -2, qs)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -3, qs) AND reg_date < DATEADD('quarter', -2, qs), cl_fl_key, NULL)) AS "Q-2",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -2, qs) AND reg_date < DATEADD('quarter', -1, qs)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -2, qs) AND reg_date < DATEADD('quarter', -1, qs), cl_fl_key, NULL)) AS "Q-1",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -1, qs) AND reg_date < qs) AND (is_high_intent_csw_7d),                         cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -1, qs) AND reg_date < qs,                         cl_fl_key, NULL)) AS "Q-0",
    COUNT(DISTINCT IFF((reg_date >= qs                          AND reg_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_high_intent_csw_7d),               cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= qs                          AND reg_date < DATEADD('day', -28, CURRENT_DATE),               cl_fl_key, NULL)) AS "QtD",

    -- Q-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -2, qs) AND reg_date < DATEADD('quarter', -1, qs)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -2, qs) AND reg_date < DATEADD('quarter', -1, qs), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -6, qs) AND reg_date < DATEADD('quarter', -5, qs)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -6, qs) AND reg_date < DATEADD('quarter', -5, qs), cl_fl_key, NULL))
    ) - 1 AS "Q-1 YoY(%)",

    -- Q-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -1, qs) AND reg_date < qs) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -1, qs) AND reg_date < qs, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -5, qs) AND reg_date < DATEADD('quarter', -4, qs)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -5, qs) AND reg_date < DATEADD('quarter', -4, qs), cl_fl_key, NULL))
    ) - 1 AS "Q-0 YoY(%)",

    -- QtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= qs AND reg_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= qs AND reg_date < DATEADD('day', -28, CURRENT_DATE), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -1, qs) AND reg_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE))) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -1, qs) AND reg_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE)), cl_fl_key, NULL))
    ) - 1 AS "QtD YoY(%)",

    -- Yearly
    COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -2, ys) AND reg_date < DATEADD('year', -1, ys)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -2, ys) AND reg_date < DATEADD('year', -1, ys), cl_fl_key, NULL)) AS "Y-1",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -1, ys) AND reg_date < ys) AND (is_high_intent_csw_7d),                      cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -1, ys) AND reg_date < ys,                      cl_fl_key, NULL)) AS "Y-0",
    COUNT(DISTINCT IFF((reg_date >= ys                       AND reg_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_high_intent_csw_7d),            cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= ys                       AND reg_date < DATEADD('day', -28, CURRENT_DATE),            cl_fl_key, NULL)) AS "YtD",

    -- Y-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -1, ys) AND reg_date < ys) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -1, ys) AND reg_date < ys, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -2, ys) AND reg_date < DATEADD('year', -1, ys)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -2, ys) AND reg_date < DATEADD('year', -1, ys), cl_fl_key, NULL))
    ) - 1 AS "Y-0 YoY(%)",

    -- YtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= ys AND reg_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= ys AND reg_date < DATEADD('day', -28, CURRENT_DATE), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -1, ys) AND reg_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE))) AND (is_high_intent_csw_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -1, ys) AND reg_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE)), cl_fl_key, NULL))
    ) - 1 AS "YtD YoY(%)"

FROM breakdowns
GROUP BY breakdown
;


---------- CL Reg to CL Start Output -------------
WITH a AS (
    SELECT
        DATE_TRUNC('week', CURRENT_DATE)::date    AS ws,
        DATE_TRUNC('month', CURRENT_DATE)::date   AS ms,
        DATE_TRUNC('quarter', CURRENT_DATE)::date AS qs,
        DATE_TRUNC('year', CURRENT_DATE)::date    AS ys
)
, registration as 
(
SELECT 
    cl_fl_key,
    sherlock_registration_fact.registration_date AS reg_date,
    cl_fl_type,
    max(case when csw.activity_date between sherlock_registration_fact.registration_date and sherlock_registration_fact.registration_date + 7 then true else false end) is_high_intent_csw_7d,
    max(case when cdv.first_spend_date between sherlock_registration_fact.registration_date and sherlock_registration_fact.registration_date + 7 then true else false end) is_cl_start_7d,
    max(case when cdv.first_spend_date between sherlock_registration_fact.registration_date and sherlock_registration_fact.registration_date + 28 then true else false end) is_cl_start_28d
FROM dm_iom.sherlock_registration_fact_vw AS sherlock_registration_fact
LEFT JOIN "SHASTA_SDC_PUBLISHED"."DM_IOM"."CSW_CLIENT_SEEKING_WORK_FACT_VW" csw
    on sherlock_registration_fact.cl_fl_key = csw.client_key
    and sherlock_registration_fact.cl_fl_type = 'client'
    and activity_type in ('messaged_without_linked_post_or_project','posted_job_post')
    and activity_date >= sherlock_registration_fact.registration_date
LEFT JOIN "SHASTA_SDC_PUBLISHED"."SHERLOCK"."CLIENT_DIM_VW" cdv
    on sherlock_registration_fact.cl_fl_key = cdv.client_key
WHERE (NOT (CASE WHEN sherlock_registration_fact.cl_fl_type = 'client' THEN sherlock_registration_fact.cl_fl_is_bad_actor ELSE NULL END) OR (CASE WHEN sherlock_registration_fact.cl_fl_type = 'client' THEN sherlock_registration_fact.cl_fl_is_bad_actor ELSE NULL END) IS NULL) 
    AND (NOT (CASE WHEN sherlock_registration_fact.cl_fl_type = 'freelancer' THEN sherlock_registration_fact.cl_fl_is_bad_actor ELSE NULL END) OR (CASE WHEN sherlock_registration_fact.cl_fl_type = 'freelancer' THEN sherlock_registration_fact.cl_fl_is_bad_actor ELSE NULL END) IS NULL) 
    AND (NOT (sherlock_registration_fact.user_is_bad_actor ) OR (sherlock_registration_fact.user_is_bad_actor ) IS NULL)
    AND sherlock_registration_fact.registration_date::date >= DATEADD('year', -3, date_trunc('year', current_date()))
    AND sherlock_registration_fact.registration_date::date < CURRENT_DATE() - 7
GROUP BY ALL
)

, base as 
(
SELECT cl_fl_key,
        reg_date,
        cl_fl_type,
        is_high_intent_csw_7d,
        is_cl_start_7d,
        is_cl_start_28d,
        a.ws,
        a.ms,
        a.qs,
        a.ys
FROM registration
CROSS JOIN a
)

-- Add new breakdowns here as additional UNION ALL blocks.
-- Each row needs: breakdown label, contract_key, hire_date, ws, ms, qs, ys.
, breakdowns AS (
    SELECT 'CL Reg to CL Start' AS breakdown, cl_fl_key, is_cl_start_7d, reg_date, ws, ms, qs, ys FROM base WHERE cl_fl_type = 'client'
)

SELECT
    breakdown,

    -- Weekly
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  84 AND ws -  78) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 84 AND ws - 78, cl_fl_key, NULL)) AS "W-11",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  77 AND ws -  71) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 77 AND ws - 71, cl_fl_key, NULL)) AS "W-10",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  70 AND ws -  64) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 70 AND ws - 64, cl_fl_key, NULL)) AS "W-9",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  63 AND ws -  57) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 63 AND ws - 57, cl_fl_key, NULL)) AS "W-8",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  56 AND ws -  50) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 56 AND ws - 50, cl_fl_key, NULL)) AS "W-7",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  49 AND ws -  43) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 49 AND ws - 43, cl_fl_key, NULL)) AS "W-6",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  42 AND ws -  36) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 42 AND ws - 36, cl_fl_key, NULL)) AS "W-5",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  35 AND ws -  29) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 35 AND ws - 29, cl_fl_key, NULL)) AS "W-4",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  28 AND ws -  22) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 28 AND ws - 22, cl_fl_key, NULL)) AS "W-3",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  21 AND ws -  15) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 21 AND ws - 15, cl_fl_key, NULL)) AS "W-2",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  14 AND ws -  8) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 14 AND ws -  8, cl_fl_key, NULL)) AS "W-1",
    COUNT(DISTINCT IFF((reg_date BETWEEN ws -  7 AND ws -  1) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  7 AND ws -  1, cl_fl_key, NULL)) AS "W-0",

    -- Previous 4W Avg (W-4 through W-1)
    (
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  35 AND ws -  8) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  35 AND ws -  8, cl_fl_key, NULL))
    ) AS "Previous 4W Avg",

    -- T4W Avg (W-0 through W-3)
    (
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  28 AND ws -  1) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  28 AND ws -  1, cl_fl_key, NULL))
    ) AS "T4W Avg",

    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  7 AND ws -  1) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  7 AND ws -  1, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  14 AND ws -  8) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws - 14 AND ws -  8, cl_fl_key, NULL))
    ) - 1 AS "W-0 WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  7 AND ws -  1) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  7 AND ws -  1, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date BETWEEN ws - 371 AND ws - 365) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  371 AND ws -  365, cl_fl_key, NULL))
    ) - 1 AS "W-0 YoY(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  28 AND ws -  1) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  28 AND ws -  1, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  56 AND ws -  29) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  56 AND ws -  29, cl_fl_key, NULL))
    ) - 1 AS "T4W WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  28 AND ws -  1) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  28 AND ws -  1, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date BETWEEN ws -  392 AND ws -  365) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date BETWEEN ws -  392 AND ws -  365, cl_fl_key, NULL))
    ) - 1 AS "T4W YoY(%)",

    -- Monthly
    COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -4, ms) AND reg_date < DATEADD('month', -3, ms)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -4, ms) AND reg_date < DATEADD('month', -3, ms), cl_fl_key, NULL)) AS "M-3",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -3, ms) AND reg_date < DATEADD('month', -2, ms)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -3, ms) AND reg_date < DATEADD('month', -2, ms), cl_fl_key, NULL)) AS "M-2",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -2, ms) AND reg_date < DATEADD('month', -1, ms)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -2, ms) AND reg_date < DATEADD('month', -1, ms), cl_fl_key, NULL)) AS "M-1",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -1, ms) AND reg_date < ms) AND (is_cl_start_7d),                       cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -1, ms) AND reg_date < ms,                       cl_fl_key, NULL)) AS "M-0",
    COUNT(DISTINCT IFF((reg_date >= ms                        AND reg_date < DATEADD('day', 28, CURRENT_DATE)) AND (is_cl_start_7d),             cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= ms                        AND reg_date < DATEADD('day', 28, CURRENT_DATE),             cl_fl_key, NULL)) AS "MtD",

    -- M-2 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -2, ms) AND reg_date < DATEADD('month', -1, ms)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -2, ms) AND reg_date < DATEADD('month', -1, ms), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -14, ms) AND reg_date < DATEADD('month', -13, ms)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -14, ms) AND reg_date < DATEADD('month', -13, ms), cl_fl_key, NULL))
    ) - 1 AS "M-2 YoY(%)",

    -- M-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -1, ms) AND reg_date < ms) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -1, ms) AND reg_date < ms, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -13, ms) AND reg_date < DATEADD('month', -12, ms)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -13, ms) AND reg_date < DATEADD('month', -12, ms), cl_fl_key, NULL))
    ) - 1 AS "M-1 YoY(%)",

    -- M-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= ms AND reg_date < DATEADD('month', 1, ms)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= ms AND reg_date < DATEADD('month', 1, ms), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('month', -12, ms) AND reg_date < DATEADD('month', -11, ms)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('month', -12, ms) AND reg_date < DATEADD('month', -11, ms), cl_fl_key, NULL))
    ) - 1 AS "M-0 YoY(%)",

    -- MtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= ms AND reg_date < DATEADD('day', 28, CURRENT_DATE)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= ms AND reg_date < DATEADD('day', 28, CURRENT_DATE), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -1, ms) AND reg_date < DATEADD('year', -1, DATEADD('day', 28, CURRENT_DATE))) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -1, ms) AND reg_date < DATEADD('year', -1, DATEADD('day', 28, CURRENT_DATE)), cl_fl_key, NULL))
    ) - 1 AS "MtD YoY(%)",

    -- Quarterly
    COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -4, qs) AND reg_date < DATEADD('quarter', -3, qs)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -4, qs) AND reg_date < DATEADD('quarter', -3, qs), cl_fl_key, NULL)) AS "Q-3",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -3, qs) AND reg_date < DATEADD('quarter', -2, qs)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -3, qs) AND reg_date < DATEADD('quarter', -2, qs), cl_fl_key, NULL)) AS "Q-2",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -2, qs) AND reg_date < DATEADD('quarter', -1, qs)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -2, qs) AND reg_date < DATEADD('quarter', -1, qs), cl_fl_key, NULL)) AS "Q-1",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -1, qs) AND reg_date < qs) AND (is_cl_start_7d),                         cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -1, qs) AND reg_date < qs,                         cl_fl_key, NULL)) AS "Q-0",
    COUNT(DISTINCT IFF((reg_date >= qs                          AND reg_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_cl_start_7d),               cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= qs                          AND reg_date < DATEADD('day', -28, CURRENT_DATE),               cl_fl_key, NULL)) AS "QtD",

    -- Q-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -2, qs) AND reg_date < DATEADD('quarter', -1, qs)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -2, qs) AND reg_date < DATEADD('quarter', -1, qs), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -6, qs) AND reg_date < DATEADD('quarter', -5, qs)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -6, qs) AND reg_date < DATEADD('quarter', -5, qs), cl_fl_key, NULL))
    ) - 1 AS "Q-1 YoY(%)",

    -- Q-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -1, qs) AND reg_date < qs) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -1, qs) AND reg_date < qs, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('quarter', -5, qs) AND reg_date < DATEADD('quarter', -4, qs)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('quarter', -5, qs) AND reg_date < DATEADD('quarter', -4, qs), cl_fl_key, NULL))
    ) - 1 AS "Q-0 YoY(%)",

    -- QtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= qs AND reg_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= qs AND reg_date < DATEADD('day', -28, CURRENT_DATE), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -1, qs) AND reg_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE))) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -1, qs) AND reg_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE)), cl_fl_key, NULL))
    ) - 1 AS "QtD YoY(%)",

    -- Yearly
    COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -2, ys) AND reg_date < DATEADD('year', -1, ys)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -2, ys) AND reg_date < DATEADD('year', -1, ys), cl_fl_key, NULL)) AS "Y-1",
    COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -1, ys) AND reg_date < ys) AND (is_cl_start_7d),                      cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -1, ys) AND reg_date < ys,                      cl_fl_key, NULL)) AS "Y-0",
    COUNT(DISTINCT IFF((reg_date >= ys                       AND reg_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_cl_start_7d),            cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= ys                       AND reg_date < DATEADD('day', -28, CURRENT_DATE),            cl_fl_key, NULL)) AS "YtD",

    -- Y-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -1, ys) AND reg_date < ys) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -1, ys) AND reg_date < ys, cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -2, ys) AND reg_date < DATEADD('year', -1, ys)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -2, ys) AND reg_date < DATEADD('year', -1, ys), cl_fl_key, NULL))
    ) - 1 AS "Y-0 YoY(%)",

    -- YtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((reg_date >= ys AND reg_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= ys AND reg_date < DATEADD('day', -28, CURRENT_DATE), cl_fl_key, NULL)),
        COUNT(DISTINCT IFF((reg_date >= DATEADD('year', -1, ys) AND reg_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE))) AND (is_cl_start_7d), cl_fl_key, NULL)) / COUNT(DISTINCT IFF(reg_date >= DATEADD('year', -1, ys) AND reg_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE)), cl_fl_key, NULL))
    ) - 1 AS "YtD YoY(%)"

FROM breakdowns
GROUP BY breakdown
;