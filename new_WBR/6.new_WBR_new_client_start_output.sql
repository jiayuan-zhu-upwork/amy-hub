WITH a AS (
    SELECT
        DATE_TRUNC('week', CURRENT_DATE)::date    AS ws,
        DATE_TRUNC('month', CURRENT_DATE)::date   AS ms,
        DATE_TRUNC('quarter', CURRENT_DATE)::date AS qs,
        DATE_TRUNC('year', CURRENT_DATE)::date    AS ys
)
, client_start as 
(
SELECT
    clients_extends.client_id as client_id,
    clients_extends.first_gsv_spend_date as client_start_date,
    attr_biz_kpi_client_acquisition_level.attributed_marketing_channel_group as marketing_channel_group,
    attr_biz_kpi_client_acquisition_level.attributed_marketing_channel as marketing_channel,
    clients_extends.is_qualified_client as is_qualified_client,
    clients_extends.is_bad_actor as is_bad_actor
FROM sherlock.allocated_payment_gsv_fact  AS payments
LEFT JOIN sherlock.client_dim_vw  AS clients_extends ON clients_extends.client_key::bigint = payments.client_key::bigint
LEFT JOIN dm_marketing_attr.attr_biz_kpi_client_acquisition_level  AS attr_biz_kpi_client_acquisition_level ON clients_extends.client_key::bigint = attr_biz_kpi_client_acquisition_level.client_key::bigint
LEFT JOIN sherlock.gsv_type_dim  AS gsv_type ON gsv_type.gsv_type_key::BIGINT = payments.gsv_type_key::BIGINT
WHERE (gsv_type.is_gsv ) 
AND clients_extends.first_gsv_spend_date::date >= DATEADD('year', -3, date_trunc('year', current_date()))
AND clients_extends.first_gsv_spend_date::date < CURRENT_DATE()
GROUP BY ALL
)

, base as 
(
SELECT client_id,
        client_start_date,
        marketing_channel_group,
        marketing_channel,
        is_qualified_client,
        is_bad_actor,
        a.ws,
        a.ms,
        a.qs,
        a.ys
FROM client_start
CROSS JOIN a
)

, breakdowns as (
-- Add new breakdowns here as additional UNION ALL blocks.
-- Each row needs: breakdown label, contract_key, hire_date, ws, ms, qs, ys.
    SELECT 'All-Actor New Client Start'      AS breakdown, client_id, client_start_date, ws, ms, qs, ys FROM base
    UNION ALL
    SELECT 'Good-Actor New Client Start',                   client_id, client_start_date, ws, ms, qs, ys FROM base WHERE not is_bad_actor
)

SELECT
    breakdown,

COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 84 AND ws - 78, client_id, NULL)) AS "W-11",
    COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 77 AND ws - 71, client_id, NULL)) AS "W-10",
    COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 70 AND ws - 64, client_id, NULL)) AS "W-9",
    COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 63 AND ws - 57, client_id, NULL)) AS "W-8",

    COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 56 AND ws - 50, client_id, NULL)) AS "W-7",
    COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 49 AND ws - 43, client_id, NULL)) AS "W-6",
    COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 42 AND ws - 36, client_id, NULL)) AS "W-5",
    COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 35 AND ws - 29, client_id, NULL)) AS "W-4",
    COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 28 AND ws - 22, client_id, NULL)) AS "W-3",
    COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 21 AND ws - 15, client_id, NULL)) AS "W-2",
    COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 14 AND ws -  8, client_id, NULL)) AS "W-1",
    COUNT(DISTINCT IFF(client_start_date BETWEEN ws -  7 AND ws -  1, client_id, NULL)) AS "W-0",

    -- Previous 4W Avg (W-4 through W-1)
    (
        COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 35 AND ws - 29, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 28 AND ws - 22, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 21 AND ws - 15, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 14 AND ws -  8, client_id, NULL))
    ) / 4.0 AS "Previous 4W Avg",

    -- T4W Avg (W-0 through W-3)
    (
        COUNT(DISTINCT IFF(client_start_date BETWEEN ws -  7 AND ws -  1, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 14 AND ws -  8, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 21 AND ws - 15, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 28 AND ws - 22, client_id, NULL))
    ) / 4.0 AS "T4W Avg",

    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date BETWEEN ws -  7 AND ws -  1, client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 14 AND ws -  8, client_id, NULL))
    ) - 1 AS "W-0 WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date BETWEEN ws -   7 AND ws -   1, client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 371 AND ws - 365, client_id, NULL))
    ) - 1 AS "W-0 YoY(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date BETWEEN ws -  7 AND ws -  1, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 14 AND ws -  8, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 21 AND ws - 15, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 28 AND ws - 22, client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 35 AND ws - 29, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 42 AND ws - 36, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 49 AND ws - 43, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 56 AND ws - 50, client_id, NULL))
    ) - 1 AS "T4W WoW(%)",

    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date BETWEEN ws -   7 AND ws -   1, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws -  14 AND ws -   8, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws -  21 AND ws -  15, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws -  28 AND ws -  22, client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 371 AND ws - 365, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 378 AND ws - 372, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 385 AND ws - 379, client_id, NULL))
      + COUNT(DISTINCT IFF(client_start_date BETWEEN ws - 392 AND ws - 386, client_id, NULL))
    ) - 1 AS "T4W YoY(%)",

    -- Monthly
    COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -4, ms) AND client_start_date < DATEADD('month', -3, ms), client_id, NULL)) AS "M-3",
    COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -3, ms) AND client_start_date < DATEADD('month', -2, ms), client_id, NULL)) AS "M-2",
    COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -2, ms) AND client_start_date < DATEADD('month', -1, ms), client_id, NULL)) AS "M-1",
    COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -1, ms) AND client_start_date < ms,                       client_id, NULL)) AS "M-0",
    COUNT(DISTINCT IFF(client_start_date >= ms                        AND client_start_date < CURRENT_DATE,             client_id, NULL)) AS "MtD",
    
    -- M-3 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -3, ms) AND client_start_date < DATEADD('month', -1, ms), client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -15, ms) AND client_start_date < DATEADD('month', -13, ms), client_id, NULL))
    ) - 1 AS "M-2 YoY(%)",
    
    -- M-2 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -2, ms) AND client_start_date < DATEADD('month', -1, ms), client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -14, ms) AND client_start_date < DATEADD('month', -13, ms), client_id, NULL))
    ) - 1 AS "M-2 YoY(%)",

    -- M-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -1, ms) AND client_start_date < ms, client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -13, ms) AND client_start_date < DATEADD('month', -12, ms), client_id, NULL))
    ) - 1 AS "M-1 YoY(%)",

    -- -- M-0 YoY(%)
    -- DIV0NULL(
    --     COUNT(DISTINCT IFF(client_start_date >= ms AND client_start_date < DATEADD('month', 1, ms), client_id, NULL)),
    --     COUNT(DISTINCT IFF(client_start_date >= DATEADD('month', -12, ms) AND client_start_date < DATEADD('month', -11, ms), client_id, NULL))
    -- ) - 1 AS "M-0 YoY(%)",

    -- MtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date >= ms AND client_start_date < CURRENT_DATE, client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('year', -1, ms) AND client_start_date < DATEADD('year', -1, CURRENT_DATE), client_id, NULL))
    ) - 1 AS "MtD YoY(%)",

    -- Quarterly
    COUNT(DISTINCT IFF(client_start_date >= DATEADD('quarter', -4, qs) AND client_start_date < DATEADD('quarter', -3, qs), client_id, NULL)) AS "Q-3",
    COUNT(DISTINCT IFF(client_start_date >= DATEADD('quarter', -3, qs) AND client_start_date < DATEADD('quarter', -2, qs), client_id, NULL)) AS "Q-2",
    COUNT(DISTINCT IFF(client_start_date >= DATEADD('quarter', -2, qs) AND client_start_date < DATEADD('quarter', -1, qs), client_id, NULL)) AS "Q-1",
    COUNT(DISTINCT IFF(client_start_date >= DATEADD('quarter', -1, qs) AND client_start_date < qs,                         client_id, NULL)) AS "Q-0",
    COUNT(DISTINCT IFF(client_start_date >= qs                          AND client_start_date < CURRENT_DATE,               client_id, NULL)) AS "QtD",

    -- Q-1 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('quarter', -2, qs) AND client_start_date < DATEADD('quarter', -1, qs), client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('quarter', -6, qs) AND client_start_date < DATEADD('quarter', -5, qs), client_id, NULL))
    ) - 1 AS "Q-1 YoY(%)",

    -- Q-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('quarter', -1, qs) AND client_start_date < qs, client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('quarter', -5, qs) AND client_start_date < DATEADD('quarter', -4, qs), client_id, NULL))
    ) - 1 AS "Q-0 YoY(%)",

    -- QtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date >= qs AND client_start_date < CURRENT_DATE, client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('year', -1, qs) AND client_start_date < DATEADD('year', -1, CURRENT_DATE), client_id, NULL))
    ) - 1 AS "QtD YoY(%)",

    -- Yearly
    COUNT(DISTINCT IFF(client_start_date >= DATEADD('year', -2, ys) AND client_start_date < DATEADD('year', -1, ys), client_id, NULL)) AS "Y-1",
    COUNT(DISTINCT IFF(client_start_date >= DATEADD('year', -1, ys) AND client_start_date < ys,                      client_id, NULL)) AS "Y-0",
    COUNT(DISTINCT IFF(client_start_date >= ys                       AND client_start_date < CURRENT_DATE,            client_id, NULL)) AS "YtD",

    -- Y-0 YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('year', -1, ys) AND client_start_date < ys, client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('year', -2, ys) AND client_start_date < DATEADD('year', -1, ys), client_id, NULL))
    ) - 1 AS "Y-0 YoY(%)",

    -- YtD YoY(%)
    DIV0NULL(
        COUNT(DISTINCT IFF(client_start_date >= ys AND client_start_date < CURRENT_DATE, client_id, NULL)),
        COUNT(DISTINCT IFF(client_start_date >= DATEADD('year', -1, ys) AND client_start_date < DATEADD('year', -1, CURRENT_DATE), client_id, NULL))
    ) - 1 AS "YtD YoY(%)"

FROM breakdowns
GROUP BY breakdown
ORDER BY
    CASE breakdown
        WHEN 'All-Actor New Client Start'     THEN 1
        WHEN 'Good-Actor New Client Start'     THEN 2
        ELSE 99
    END
;