
create or replace table sdc_user.amyzhu.new_wbr_traffic_stg as 
WITH traffic_stg as (
SELECT 
    suit_entry_page_performance_fact.visitor_id,
    suit_entry_page_performance_fact.session_date AS visit_date,
    case when suit_entry_page_performance_fact.is_exit and not suit_entry_page_performance_fact.is_click then true else false end is_bounce,
    case when (suit_entry_page_performance_fact.is_logged_out_visitor ) AND NOT (suit_entry_page_performance_fact.is_recognized_visitor ) then true else false end as is_net_new_visitor,
    case when suit_entry_page_performance_fact.is_logged_out_visit_to_cl_registration_1d then true else false end is_cl_reg_1d,
    case when suit_entry_page_performance_fact.is_logged_out_visit_to_fl_registration_1d then true else false end is_fl_reg_1d
FROM dm_iom.suit_entry_page_performance_fact_vw  AS suit_entry_page_performance_fact
WHERE (NOT (suit_entry_page_performance_fact.reg_client_is_bad_actor ) OR (suit_entry_page_performance_fact.reg_client_is_bad_actor ) IS NULL) 
    AND (NOT (suit_entry_page_performance_fact.reg_freelancer_is_bad_actor ) OR (suit_entry_page_performance_fact.reg_freelancer_is_bad_actor ) IS NULL) 
    -- AND (suit_entry_page_performance_fact.is_logged_out_visitor ) AND (suit_entry_page_performance_fact.is_recognized_visitor )
    AND suit_entry_page_performance_fact.session_date::date >= DATEADD('year', -3, date_trunc('year', current_date()))
    AND suit_entry_page_performance_fact.session_date::date < CURRENT_DATE()
GROUP BY ALL
)

SELECT visitor_id,
        visit_date,
        max(is_net_new_visitor) as is_net_new_visitor,
        max(is_bounce) as is_bounce,
        max(is_cl_reg_1d) as is_cl_reg_1d,
        max(is_fl_reg_1d) as is_fl_reg_1d

FROM traffic_stg
GROUP BY 1, 2
;


---------------- All Visitors & Bounce --------------
create or replace table sdc_user.amyzhu.new_wbr_traffic_input_dataset as 
WITH a AS (
    SELECT
        DATE_TRUNC('week', CURRENT_DATE)::date    AS ws,
        DATE_TRUNC('month', CURRENT_DATE)::date   AS ms,
        DATE_TRUNC('quarter', CURRENT_DATE)::date AS qs,
        DATE_TRUNC('year', CURRENT_DATE)::date    AS ys
)

-- , base as (
select visitor_id,
        visit_date,
        is_bounce,
        is_cl_reg_1d,
        is_fl_reg_1d,
        is_net_new_visitor,
        a.ws,
        a.ms,
        a.qs,
        a.ys
from sdc_user.amyzhu.new_wbr_traffic_stg
CROSS JOIN a
-- )
;

WITH breakdowns as (
-- Add new breakdowns here as additional UNION ALL blocks.
-- Each row needs: breakdown label, contract_key, hire_date, ws, ms, qs, ys.
    SELECT 'All Visitor'      AS breakdown, visitor_id, visit_date, ws, ms, qs, ys FROM sdc_user.amyzhu.new_wbr_traffic_input_dataset 
    UNION ALL
    SELECT 'Bounce',                   visitor_id, visit_date, ws, ms, qs, ys FROM sdc_user.amyzhu.new_wbr_traffic_input_dataset WHERE is_bounce
)

SELECT
    breakdown,

    -- Weekly
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 84 AND ws - 78, visitor_id, NULL)) AS "W-11",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 77 AND ws - 71, visitor_id, NULL)) AS "W-10",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 70 AND ws - 64, visitor_id, NULL)) AS "W-9",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 63 AND ws - 57, visitor_id, NULL)) AS "W-8",

    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 56 AND ws - 50, visitor_id, NULL)) AS "W-7",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 49 AND ws - 43, visitor_id, NULL)) AS "W-6",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 42 AND ws - 36, visitor_id, NULL)) AS "W-5",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 35 AND ws - 29, visitor_id, NULL)) AS "W-4",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 28 AND ws - 22, visitor_id, NULL)) AS "W-3",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 21 AND ws - 15, visitor_id, NULL)) AS "W-2",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, visitor_id, NULL)) AS "W-1",
    COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, visitor_id, NULL)) AS "W-0",

    -- Previous 4W Avg (W-4 through W-1)
    (
        COUNT(DISTINCT IFF(visit_date BETWEEN ws - 35 AND ws - 29, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 28 AND ws - 22, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 21 AND ws - 15, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, visitor_id, NULL))
    ) / 4.0 AS "Previous 4W Avg",

    -- T4W Avg (W-0 through W-3)
    (
        COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 21 AND ws - 15, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 28 AND ws - 22, visitor_id, NULL))
    ) / 4.0 AS "T4W Avg",

    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, visitor_id, NULL))
    ) - 1 AS "W-0 WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date BETWEEN ws -   7 AND ws -   1, visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date BETWEEN ws - 371 AND ws - 365, visitor_id, NULL))
    ) - 1 AS "W-0 YoY(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 21 AND ws - 15, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 28 AND ws - 22, visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date BETWEEN ws - 35 AND ws - 29, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 42 AND ws - 36, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 49 AND ws - 43, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 56 AND ws - 50, visitor_id, NULL))
    ) - 1 AS "T4W WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date BETWEEN ws -   7 AND ws -   1, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws -  14 AND ws -   8, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws -  21 AND ws -  15, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws -  28 AND ws -  22, visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date BETWEEN ws - 371 AND ws - 365, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 378 AND ws - 372, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 385 AND ws - 379, visitor_id, NULL))
      + COUNT(DISTINCT IFF(visit_date BETWEEN ws - 392 AND ws - 386, visitor_id, NULL))
    ) - 1 AS "T4W YoY(%)",

    -- Monthly
    COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -4, ms) AND visit_date < DATEADD('month', -3, ms), visitor_id, NULL)) AS "M-3",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -3, ms) AND visit_date < DATEADD('month', -2, ms), visitor_id, NULL)) AS "M-2",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms), visitor_id, NULL)) AS "M-1",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -1, ms) AND visit_date < ms,                       visitor_id, NULL)) AS "M-0",
    COUNT(DISTINCT IFF(visit_date >= ms                        AND visit_date < CURRENT_DATE,             visitor_id, NULL)) AS "MtD",

    -- M-2 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms), visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -14, ms) AND visit_date < DATEADD('month', -13, ms), visitor_id, NULL))
    ) - 1 AS "M-2 YoY(%)",

    -- M-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -1, ms) AND visit_date < ms, visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -13, ms) AND visit_date < DATEADD('month', -12, ms), visitor_id, NULL))
    ) - 1 AS "M-1 YoY(%)",

    -- M-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= ms AND visit_date < DATEADD('month', 1, ms), visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -12, ms) AND visit_date < DATEADD('month', -11, ms), visitor_id, NULL))
    ) - 1 AS "M-0 YoY(%)",

    -- MtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= ms AND visit_date < CURRENT_DATE, visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ms) AND visit_date < DATEADD('year', -1, CURRENT_DATE), visitor_id, NULL))
    ) - 1 AS "MtD YoY(%)",

    -- Quarterly
    COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -4, qs) AND visit_date < DATEADD('quarter', -3, qs), visitor_id, NULL)) AS "Q-3",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -3, qs) AND visit_date < DATEADD('quarter', -2, qs), visitor_id, NULL)) AS "Q-2",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs), visitor_id, NULL)) AS "Q-1",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs,                         visitor_id, NULL)) AS "Q-0",
    COUNT(DISTINCT IFF(visit_date >= qs                          AND visit_date < CURRENT_DATE,               visitor_id, NULL)) AS "QtD",

    -- Q-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs), visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -6, qs) AND visit_date < DATEADD('quarter', -5, qs), visitor_id, NULL))
    ) - 1 AS "Q-1 YoY(%)",

    -- Q-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs, visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -5, qs) AND visit_date < DATEADD('quarter', -4, qs), visitor_id, NULL))
    ) - 1 AS "Q-0 YoY(%)",

    -- QtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= qs AND visit_date < CURRENT_DATE, visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, qs) AND visit_date < DATEADD('year', -1, CURRENT_DATE), visitor_id, NULL))
    ) - 1 AS "QtD YoY(%)",

    -- Yearly
    COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys), visitor_id, NULL)) AS "Y-1",
    COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < ys,                      visitor_id, NULL)) AS "Y-0",
    COUNT(DISTINCT IFF(visit_date >= ys                       AND visit_date < CURRENT_DATE,            visitor_id, NULL)) AS "YtD",

    -- Y-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < ys, visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys), visitor_id, NULL))
    ) - 1 AS "Y-0 YoY(%)",

    -- YtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(visit_date >= ys AND visit_date < CURRENT_DATE, visitor_id, NULL)),
        COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < DATEADD('year', -1, CURRENT_DATE), visitor_id, NULL))
    ) - 1 AS "YtD YoY(%)"

FROM breakdowns
GROUP BY breakdown
ORDER BY
    CASE breakdown
        WHEN 'All Visitor'     THEN 1
        WHEN 'Bounce'     THEN 2
        ELSE 99
    END
;



---------- Visit to CL Reg -------------
WITH breakdowns as (
-- Add new breakdowns here as additional UNION ALL blocks.
-- Each row needs: breakdown label, contract_key, hire_date, ws, ms, qs, ys.
    SELECT 'Visit to CL Reg'      AS breakdown, visitor_id, is_cl_reg_1d, visit_date, ws, ms, qs, ys FROM sdc_user.amyzhu.new_wbr_traffic_input_dataset WHERE is_net_new_visitor
)


SELECT
    breakdown,

    -- Weekly
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  84 AND ws -  78) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 84 AND ws - 78, visitor_id, NULL)) AS "W-11",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  77 AND ws -  71) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 77 AND ws - 71, visitor_id, NULL)) AS "W-10",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  70 AND ws -  64) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 70 AND ws - 64, visitor_id, NULL)) AS "W-9",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  63 AND ws -  57) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 63 AND ws - 57, visitor_id, NULL)) AS "W-8",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  56 AND ws -  50) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 56 AND ws - 50, visitor_id, NULL)) AS "W-7",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  49 AND ws -  43) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 49 AND ws - 43, visitor_id, NULL)) AS "W-6",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  42 AND ws -  36) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 42 AND ws - 36, visitor_id, NULL)) AS "W-5",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  35 AND ws -  29) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 35 AND ws - 29, visitor_id, NULL)) AS "W-4",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  28 AND ws -  22) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 28 AND ws - 22, visitor_id, NULL)) AS "W-3",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  21 AND ws -  15) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 21 AND ws - 15, visitor_id, NULL)) AS "W-2",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  14 AND ws -  8) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, visitor_id, NULL)) AS "W-1",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  7 AND ws -  1) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, visitor_id, NULL)) AS "W-0",

    -- Previous 4W Avg (W-4 through W-1)
    (
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  35 AND ws -  8) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  35 AND ws -  8, visitor_id, NULL))
    ) AS "Previous 4W Avg",

    -- T4W Avg (W-0 through W-3)
    (
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  28 AND ws -  1) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  28 AND ws -  1, visitor_id, NULL))
    ) AS "T4W Avg",

    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  7 AND ws -  1) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  14 AND ws -  8) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, visitor_id, NULL))
    ) - 1 AS "W-0 WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  7 AND ws -  1) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date BETWEEN ws - 371 AND ws - 365) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  371 AND ws -  365, visitor_id, NULL))
    ) - 1 AS "W-0 YoY(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  28 AND ws -  1) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  28 AND ws -  1, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  56 AND ws -  29) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  56 AND ws -  29, visitor_id, NULL))
    ) - 1 AS "T4W WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  28 AND ws -  1) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  28 AND ws -  1, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  392 AND ws -  365) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  392 AND ws -  365, visitor_id, NULL))
    ) - 1 AS "T4W YoY(%)",

    -- Monthly
    COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -4, ms) AND visit_date < DATEADD('month', -3, ms)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -4, ms) AND visit_date < DATEADD('month', -3, ms), visitor_id, NULL)) AS "M-3",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -3, ms) AND visit_date < DATEADD('month', -2, ms)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -3, ms) AND visit_date < DATEADD('month', -2, ms), visitor_id, NULL)) AS "M-2",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms), visitor_id, NULL)) AS "M-1",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -1, ms) AND visit_date < ms) AND (is_cl_reg_1d),                       visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -1, ms) AND visit_date < ms,                       visitor_id, NULL)) AS "M-0",
    COUNT(DISTINCT IFF((visit_date >= ms                        AND visit_date < DATEADD('day', 28, CURRENT_DATE)) AND (is_cl_reg_1d),             visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= ms                        AND visit_date < DATEADD('day', 28, CURRENT_DATE),             visitor_id, NULL)) AS "MtD",

    -- M-2 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -14, ms) AND visit_date < DATEADD('month', -13, ms)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -14, ms) AND visit_date < DATEADD('month', -13, ms), visitor_id, NULL))
    ) - 1 AS "M-2 YoY(%)",

    -- M-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -1, ms) AND visit_date < ms) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -1, ms) AND visit_date < ms, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -13, ms) AND visit_date < DATEADD('month', -12, ms)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -13, ms) AND visit_date < DATEADD('month', -12, ms), visitor_id, NULL))
    ) - 1 AS "M-1 YoY(%)",

    -- M-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= ms AND visit_date < DATEADD('month', 1, ms)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= ms AND visit_date < DATEADD('month', 1, ms), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -12, ms) AND visit_date < DATEADD('month', -11, ms)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -12, ms) AND visit_date < DATEADD('month', -11, ms), visitor_id, NULL))
    ) - 1 AS "M-0 YoY(%)",

    -- MtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= ms AND visit_date < DATEADD('day', 28, CURRENT_DATE)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= ms AND visit_date < DATEADD('day', 28, CURRENT_DATE), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -1, ms) AND visit_date < DATEADD('year', -1, DATEADD('day', 28, CURRENT_DATE))) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ms) AND visit_date < DATEADD('year', -1, DATEADD('day', 28, CURRENT_DATE)), visitor_id, NULL))
    ) - 1 AS "MtD YoY(%)",

    -- Quarterly
    COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -4, qs) AND visit_date < DATEADD('quarter', -3, qs)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -4, qs) AND visit_date < DATEADD('quarter', -3, qs), visitor_id, NULL)) AS "Q-3",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -3, qs) AND visit_date < DATEADD('quarter', -2, qs)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -3, qs) AND visit_date < DATEADD('quarter', -2, qs), visitor_id, NULL)) AS "Q-2",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs), visitor_id, NULL)) AS "Q-1",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs) AND (is_cl_reg_1d),                         visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs,                         visitor_id, NULL)) AS "Q-0",
    COUNT(DISTINCT IFF((visit_date >= qs                          AND visit_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_cl_reg_1d),               visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= qs                          AND visit_date < DATEADD('day', -28, CURRENT_DATE),               visitor_id, NULL)) AS "QtD",

    -- Q-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -6, qs) AND visit_date < DATEADD('quarter', -5, qs)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -6, qs) AND visit_date < DATEADD('quarter', -5, qs), visitor_id, NULL))
    ) - 1 AS "Q-1 YoY(%)",

    -- Q-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -5, qs) AND visit_date < DATEADD('quarter', -4, qs)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -5, qs) AND visit_date < DATEADD('quarter', -4, qs), visitor_id, NULL))
    ) - 1 AS "Q-0 YoY(%)",

    -- QtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= qs AND visit_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= qs AND visit_date < DATEADD('day', -28, CURRENT_DATE), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -1, qs) AND visit_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE))) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, qs) AND visit_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE)), visitor_id, NULL))
    ) - 1 AS "QtD YoY(%)",

    -- Yearly
    COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys), visitor_id, NULL)) AS "Y-1",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -1, ys) AND visit_date < ys) AND (is_cl_reg_1d),                      visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < ys,                      visitor_id, NULL)) AS "Y-0",
    COUNT(DISTINCT IFF((visit_date >= ys                       AND visit_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_cl_reg_1d),            visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= ys                       AND visit_date < DATEADD('day', -28, CURRENT_DATE),            visitor_id, NULL)) AS "YtD",

    -- Y-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -1, ys) AND visit_date < ys) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < ys, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys), visitor_id, NULL))
    ) - 1 AS "Y-0 YoY(%)",

    -- YtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= ys AND visit_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= ys AND visit_date < DATEADD('day', -28, CURRENT_DATE), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -1, ys) AND visit_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE))) AND (is_cl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE)), visitor_id, NULL))
    ) - 1 AS "YtD YoY(%)"

FROM breakdowns
GROUP BY breakdown
;

---------- Visit to FL Reg -------------
WITH breakdowns as (
-- Add new breakdowns here as additional UNION ALL blocks.
-- Each row needs: breakdown label, contract_key, hire_date, ws, ms, qs, ys.
    SELECT 'Visit to CL Reg'      AS breakdown, visitor_id, is_fl_reg_1d, visit_date, ws, ms, qs, ys FROM sdc_user.amyzhu.new_wbr_traffic_input_dataset WHERE is_net_new_visitor
)

SELECT
    breakdown,

    -- Weekly
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  84 AND ws -  78) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 84 AND ws - 78, visitor_id, NULL)) AS "W-11",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  77 AND ws -  71) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 77 AND ws - 71, visitor_id, NULL)) AS "W-10",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  70 AND ws -  64) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 70 AND ws - 64, visitor_id, NULL)) AS "W-9",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  63 AND ws -  57) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 63 AND ws - 57, visitor_id, NULL)) AS "W-8",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  56 AND ws -  50) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 56 AND ws - 50, visitor_id, NULL)) AS "W-7",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  49 AND ws -  43) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 49 AND ws - 43, visitor_id, NULL)) AS "W-6",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  42 AND ws -  36) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 42 AND ws - 36, visitor_id, NULL)) AS "W-5",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  35 AND ws -  29) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 35 AND ws - 29, visitor_id, NULL)) AS "W-4",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  28 AND ws -  22) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 28 AND ws - 22, visitor_id, NULL)) AS "W-3",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  21 AND ws -  15) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 21 AND ws - 15, visitor_id, NULL)) AS "W-2",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  14 AND ws -  8) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, visitor_id, NULL)) AS "W-1",
    COUNT(DISTINCT IFF((visit_date BETWEEN ws -  7 AND ws -  1) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, visitor_id, NULL)) AS "W-0",

    -- Previous 4W Avg (W-4 through W-1)
    (
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  35 AND ws -  8) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  35 AND ws -  8, visitor_id, NULL))
    ) AS "Previous 4W Avg",

    -- T4W Avg (W-0 through W-3)
    (
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  28 AND ws -  1) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  28 AND ws -  1, visitor_id, NULL))
    ) AS "T4W Avg",

    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  7 AND ws -  1) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  14 AND ws -  8) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws - 14 AND ws -  8, visitor_id, NULL))
    ) - 1 AS "W-0 WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  7 AND ws -  1) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  7 AND ws -  1, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date BETWEEN ws - 371 AND ws - 365) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  371 AND ws -  365, visitor_id, NULL))
    ) - 1 AS "W-0 YoY(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  28 AND ws -  1) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  28 AND ws -  1, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  56 AND ws -  29) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  56 AND ws -  29, visitor_id, NULL))
    ) - 1 AS "T4W WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  28 AND ws -  1) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  28 AND ws -  1, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date BETWEEN ws -  392 AND ws -  365) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date BETWEEN ws -  392 AND ws -  365, visitor_id, NULL))
    ) - 1 AS "T4W YoY(%)",

    -- Monthly
    COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -4, ms) AND visit_date < DATEADD('month', -3, ms)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -4, ms) AND visit_date < DATEADD('month', -3, ms), visitor_id, NULL)) AS "M-3",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -3, ms) AND visit_date < DATEADD('month', -2, ms)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -3, ms) AND visit_date < DATEADD('month', -2, ms), visitor_id, NULL)) AS "M-2",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms), visitor_id, NULL)) AS "M-1",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -1, ms) AND visit_date < ms) AND (is_fl_reg_1d),                       visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -1, ms) AND visit_date < ms,                       visitor_id, NULL)) AS "M-0",
    COUNT(DISTINCT IFF((visit_date >= ms                        AND visit_date < DATEADD('day', 28, CURRENT_DATE)) AND (is_fl_reg_1d),             visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= ms                        AND visit_date < DATEADD('day', 28, CURRENT_DATE),             visitor_id, NULL)) AS "MtD",

    -- M-2 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -2, ms) AND visit_date < DATEADD('month', -1, ms), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -14, ms) AND visit_date < DATEADD('month', -13, ms)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -14, ms) AND visit_date < DATEADD('month', -13, ms), visitor_id, NULL))
    ) - 1 AS "M-2 YoY(%)",

    -- M-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -1, ms) AND visit_date < ms) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -1, ms) AND visit_date < ms, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -13, ms) AND visit_date < DATEADD('month', -12, ms)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -13, ms) AND visit_date < DATEADD('month', -12, ms), visitor_id, NULL))
    ) - 1 AS "M-1 YoY(%)",

    -- M-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= ms AND visit_date < DATEADD('month', 1, ms)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= ms AND visit_date < DATEADD('month', 1, ms), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('month', -12, ms) AND visit_date < DATEADD('month', -11, ms)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('month', -12, ms) AND visit_date < DATEADD('month', -11, ms), visitor_id, NULL))
    ) - 1 AS "M-0 YoY(%)",

    -- MtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= ms AND visit_date < DATEADD('day', 28, CURRENT_DATE)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= ms AND visit_date < DATEADD('day', 28, CURRENT_DATE), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -1, ms) AND visit_date < DATEADD('year', -1, DATEADD('day', 28, CURRENT_DATE))) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ms) AND visit_date < DATEADD('year', -1, DATEADD('day', 28, CURRENT_DATE)), visitor_id, NULL))
    ) - 1 AS "MtD YoY(%)",

    -- Quarterly
    COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -4, qs) AND visit_date < DATEADD('quarter', -3, qs)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -4, qs) AND visit_date < DATEADD('quarter', -3, qs), visitor_id, NULL)) AS "Q-3",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -3, qs) AND visit_date < DATEADD('quarter', -2, qs)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -3, qs) AND visit_date < DATEADD('quarter', -2, qs), visitor_id, NULL)) AS "Q-2",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs), visitor_id, NULL)) AS "Q-1",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs) AND (is_fl_reg_1d),                         visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs,                         visitor_id, NULL)) AS "Q-0",
    COUNT(DISTINCT IFF((visit_date >= qs                          AND visit_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_fl_reg_1d),               visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= qs                          AND visit_date < DATEADD('day', -28, CURRENT_DATE),               visitor_id, NULL)) AS "QtD",

    -- Q-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -2, qs) AND visit_date < DATEADD('quarter', -1, qs), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -6, qs) AND visit_date < DATEADD('quarter', -5, qs)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -6, qs) AND visit_date < DATEADD('quarter', -5, qs), visitor_id, NULL))
    ) - 1 AS "Q-1 YoY(%)",

    -- Q-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -1, qs) AND visit_date < qs, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('quarter', -5, qs) AND visit_date < DATEADD('quarter', -4, qs)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('quarter', -5, qs) AND visit_date < DATEADD('quarter', -4, qs), visitor_id, NULL))
    ) - 1 AS "Q-0 YoY(%)",

    -- QtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= qs AND visit_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= qs AND visit_date < DATEADD('day', -28, CURRENT_DATE), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -1, qs) AND visit_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE))) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, qs) AND visit_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE)), visitor_id, NULL))
    ) - 1 AS "QtD YoY(%)",

    -- Yearly
    COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys), visitor_id, NULL)) AS "Y-1",
    COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -1, ys) AND visit_date < ys) AND (is_fl_reg_1d),                      visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < ys,                      visitor_id, NULL)) AS "Y-0",
    COUNT(DISTINCT IFF((visit_date >= ys                       AND visit_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_fl_reg_1d),            visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= ys                       AND visit_date < DATEADD('day', -28, CURRENT_DATE),            visitor_id, NULL)) AS "YtD",

    -- Y-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -1, ys) AND visit_date < ys) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < ys, visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -2, ys) AND visit_date < DATEADD('year', -1, ys), visitor_id, NULL))
    ) - 1 AS "Y-0 YoY(%)",

    -- YtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF((visit_date >= ys AND visit_date < DATEADD('day', -28, CURRENT_DATE)) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= ys AND visit_date < DATEADD('day', -28, CURRENT_DATE), visitor_id, NULL)),
        COUNT(DISTINCT IFF((visit_date >= DATEADD('year', -1, ys) AND visit_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE))) AND (is_fl_reg_1d), visitor_id, NULL)) / COUNT(DISTINCT IFF(visit_date >= DATEADD('year', -1, ys) AND visit_date < DATEADD('year', -1, DATEADD('day', -28, CURRENT_DATE)), visitor_id, NULL))
    ) - 1 AS "YtD YoY(%)"

FROM breakdowns
GROUP BY breakdown
;

create or replace table sdc_user.amyzhu.new_wbr_login_stg as 
SELECT 
    suit_all_session_fact.user_uid as user_uid,
    suit_all_session_fact.session_date AS visit_date,
    suit_all_session_fact.is_client,
    suit_all_session_fact.is_freelancer
FROM dm_iom.suit_all_session_fact_vw  AS suit_all_session_fact
WHERE (NOT (suit_all_session_fact.client_is_bad_actor ) OR (suit_all_session_fact.client_is_bad_actor ) IS NULL) 
    AND (NOT (suit_all_session_fact.freelancer_is_bad_actor ) OR (suit_all_session_fact.freelancer_is_bad_actor ) IS NULL)
    AND suit_all_session_fact.session_date::date >= DATEADD('year', -3, date_trunc('year', current_date()))
    AND suit_all_session_fact.session_date::date < CURRENT_DATE()
AND is_logged_in_user
GROUP BY ALL
;


