WITH weekly_traffic AS (
    SELECT
        uvs.first_upwork_location AS upwork_location,
        DATE_TRUNC(week, uvs.session_start_date) AS event_week,
        COUNT(DISTINCT uvs.visitor_id) AS total_traffic
    FROM SHASTA_SDC_PUBLISHED.DM_CSI.USER_SESSION AS uvs
    JOIN shasta_sdc_published.dm_iom.suit_all_session_fact sasf
        ON uvs.domain_session_id = sasf.domain_session_id
    WHERE
        -- Scope to the two weeks needed
        uvs.session_start_date >= '2025-03-30'
        AND uvs.session_start_date < '2025-04-13'
        -- AND suit_entry_page_performance_fact.marketing_channel = 'Direct'
        AND (NOT sasf.reg_client_is_bad_actor OR sasf.reg_client_is_bad_actor IS NULL)
        AND (NOT sasf.reg_freelancer_is_bad_actor OR sasf.reg_freelancer_is_bad_actor IS NULL)
        AND sasf.is_logged_out_visitor
        AND NOT sasf.is_recognized_visitor
        AND uvs.first_upwork_location is not null
    GROUP BY 1, 2
),

current_week AS (
    SELECT upwork_location, total_traffic AS traffic_4_6
    FROM weekly_traffic
    WHERE event_week = '2025-04-07'
),

prior_week AS (
    SELECT upwork_location, total_traffic AS traffic_3_30
    FROM weekly_traffic
    WHERE event_week = '2025-03-31'
)

SELECT
    COALESCE(c.upwork_location, p.upwork_location)   AS upwork_location,
    COALESCE(c.traffic_4_6, 0)                        AS traffic_wk_4_6,
    COALESCE(p.traffic_3_30, 0)                       AS traffic_wk_3_30,
    COALESCE(c.traffic_4_6, 0) - COALESCE(p.traffic_3_30, 0)
                                                       AS traffic_diff,
    ROUND(
        100.0 * (COALESCE(c.traffic_4_6, 0) - COALESCE(p.traffic_3_30, 0))
        / NULLIF(p.traffic_3_30, 0),
        2
    )                                                  AS traffic_pct_change
FROM current_week c
FULL OUTER JOIN prior_week p
    ON c.upwork_location = p.upwork_location

-- Top 10 by absolute increase in traffic volume
ORDER BY traffic_diff DESC
LIMIT 100;