---- Adding 7 metrics:
---- 1. Entry Page Visitors (#)
---- 2. % of Visitors from SEM Marketing Channel (%)
---- 3. % of Visitors from SEO Marketing Channel (%)
---- 4. Visit to Offered Hire conversion 1D (%)
---- 5. Visit to First Hire conversion 1D (%)
---- 6. Bplus Signup (#)
---- 7. SMB Signup (#)
WITH base AS (
    SELECT
        date_trunc('week', uvs.session_start_date)                                                                   AS week_
        , count(distinct uvs.visitor_id)                                                                              AS visitors
        , count(distinct case when sasf.marketing_channel_group = 'SEM' then uvs.visitor_id end)                      AS sem_visitors
        , count(distinct case when sasf.marketing_channel_group = 'SEO' then uvs.visitor_id end)                      AS seo_visitors
        , count(distinct case when sasf.is_logged_out_visit_to_cl_registration_1d then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_cl_reg_conversion_1d
        , count(distinct case when bp.client_id is not null then uvs.visitor_id end)                                   AS bp_signup
        , bp_signup
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_bp_signup_conversion_1d
        , count(distinct case when cd.client_id is not null
                                   and uppd.payment_plan_name not in ('Business Plus', 'Business Plus Net 30')
                                   and cd.registration_date between uvs.session_start_date and uvs.session_start_date + 1
                              then uvs.visitor_id end)                                                                 AS basic_signup
        , basic_signup / visitors                                                                                      AS visit_to_basic_signup_conversion_1d
        , count(distinct case when cd.client_id is not null
                                   and cd.business_entity_size_segment = 'SMB'
                                   and cd.registration_date between uvs.session_start_date and uvs.session_start_date + 1
                              then uvs.visitor_id end)                                                                 AS smb_signup
        , smb_signup / visitors                                                                                        AS visit_to_smb_signup_conversion_1d
        , count(distinct case when csw.activity_type in ('messaged_without_linked_post_or_project', 'posted_job_post')
                              then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_high_intent_csw_1d
        , count(distinct case when sasf.is_logged_out_visit_to_cl_fjp_1d then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_cl_fjp_conversion_1d
        , count(distinct case when sasf.is_logged_out_visit_to_cl_start_1d then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_cl_start_conversion_1d
        , count(distinct case when offer.client_id is not null then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_offered_hire_conversion_1d
        , count(distinct case when cd.first_hire_date between uvs.session_start_date and uvs.session_start_date + 1 then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_first_hire_conversion_1d
        , sem_visitors / visitors                                                                                       AS pct_sem
        , seo_visitors / visitors                                                                                       AS pct_seo
    FROM SHASTA_SDC_PUBLISHED.DM_CSI.USER_SESSION uvs
    JOIN shasta_sdc_published.dm_iom.suit_all_session_fact sasf
        ON uvs.domain_session_id = sasf.domain_session_id
    LEFT JOIN SHASTA_SDC_UPWORK.REGISTRATION.REGISTRATIONS reg
        ON sasf.visitor_id::varchar = reg.visitor_id::varchar
    LEFT JOIN shasta_sdc_published.sherlock.client_dim_vw cd
        ON cd.username = reg.username
    LEFT JOIN shasta_sdc_published.sales.business_plus_reg_attr AS bp
        ON cd.client_id = bp.client_id
        AND uvs.session_start_date BETWEEN bp.sub_create_ts::date - interval '2 day' AND bp.sub_create_ts::date
    LEFT JOIN sherlock.user_payment_plan_dim uppd
        ON uppd.client_id = cd.client_id
        AND cd.registration_date BETWEEN uppd.start_dt AND uppd.end_dt
    LEFT JOIN shasta_sdc_published.dm_iom.csw_client_seeking_work_fact csw
        ON csw.client_key = sasf.reg_client_key
        AND csw.activity_date BETWEEN uvs.session_start_date AND uvs.session_start_date + 1
    LEFT JOIN SHASTA_SDC_PUBLISHED.DM_IOM.SHERLOCK_OFFER_FACT_VW offer
        ON offer.client_key = sasf.reg_client_key
        AND offer.offer_date BETWEEN uvs.session_start_date AND uvs.session_start_date + 1
    WHERE NOT uvs.is_bot
        AND uvs.IS_NEW_VISITOR AND NOT uvs.IS_RECOGNIZED_VISITOR AND uvs.IS_LOGGED_OUT_VISITOR
        AND date_trunc('week', uvs.session_start_date) >= date_trunc('week', current_date()) - interval '55 week'
        AND date_trunc('week', uvs.session_start_date) < date_trunc('week', current_date())
        AND NOT (sasf.reg_freelancer_is_upwork_internal OR sasf.reg_client_is_test_account OR sasf.reg_freelancer_is_test_account)
        AND NOT (sasf.reg_client_is_bad_actor OR sasf.reg_freelancer_is_bad_actor)
    GROUP BY ALL
),

windowed AS (
    SELECT
        week_
        , visitors
        , bp_signup
        , smb_signup
        , pct_sem
        , pct_seo
        , visit_to_cl_reg_conversion_1d
        , visit_to_smb_signup_conversion_1d
        , visit_to_basic_signup_conversion_1d
        , visit_to_bp_signup_conversion_1d
        , visit_to_high_intent_csw_1d
        , visit_to_cl_fjp_conversion_1d
        , visit_to_offered_hire_conversion_1d
        , visit_to_first_hire_conversion_1d
        , visit_to_cl_start_conversion_1d

        -- 4-week rolling avg
        , AVG(visitors)                               OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS visitors_4wk_avg
        , AVG(bp_signup)                              OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS bp_signup_4wk_avg
        , AVG(smb_signup)                             OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS smb_signup_4wk_avg
        , AVG(pct_sem)                                OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS pct_sem_4wk_avg
        , AVG(pct_seo)                                OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS pct_seo_4wk_avg
        , AVG(visit_to_cl_reg_conversion_1d)          OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS cl_reg_4wk_avg
        , AVG(visit_to_smb_signup_conversion_1d)      OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS smb_reg_4wk_avg
        , AVG(visit_to_basic_signup_conversion_1d)    OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS basic_reg_4wk_avg
        , AVG(visit_to_bp_signup_conversion_1d)       OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS bp_reg_4wk_avg
        , AVG(visit_to_high_intent_csw_1d)            OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS csw_4wk_avg
        , AVG(visit_to_cl_fjp_conversion_1d)          OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS fjp_4wk_avg
        , AVG(visit_to_offered_hire_conversion_1d)    OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS offered_hire_4wk_avg
        , AVG(visit_to_first_hire_conversion_1d)      OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS first_hire_4wk_avg
        , AVG(visit_to_cl_start_conversion_1d)        OVER (ORDER BY week_ ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)  AS offer_4wk_avg

        -- prior 4-week avg (weeks 5–8 back)
        , AVG(visitors)                               OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS visitors_prior_4wk_avg
        , AVG(bp_signup)                              OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS bp_signup_prior_4wk_avg
        , AVG(smb_signup)                             OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS smb_signup_prior_4wk_avg
        , AVG(pct_sem)                                OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS pct_sem_prior_4wk_avg
        , AVG(pct_seo)                                OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS pct_seo_prior_4wk_avg
        , AVG(visit_to_cl_reg_conversion_1d)          OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS cl_reg_prior_4wk_avg
        , AVG(visit_to_smb_signup_conversion_1d)      OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS smb_reg_prior_4wk_avg
        , AVG(visit_to_basic_signup_conversion_1d)    OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS basic_reg_prior_4wk_avg
        , AVG(visit_to_bp_signup_conversion_1d)       OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS bp_reg_prior_4wk_avg
        , AVG(visit_to_high_intent_csw_1d)            OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS csw_prior_4wk_avg
        , AVG(visit_to_cl_fjp_conversion_1d)          OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS fjp_prior_4wk_avg
        , AVG(visit_to_offered_hire_conversion_1d)    OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS offered_hire_prior_4wk_avg
        , AVG(visit_to_first_hire_conversion_1d)      OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS first_hire_prior_4wk_avg
        , AVG(visit_to_cl_start_conversion_1d)        OVER (ORDER BY week_ ASC ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)  AS offer_prior_4wk_avg

        -- YoY same 4-week avg (52 weeks back)
        , AVG(visitors)                               OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS visitors_yoy_4wk_avg
        , AVG(bp_signup)                              OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS bp_signup_yoy_4wk_avg
        , AVG(smb_signup)                             OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS smb_signup_yoy_4wk_avg
        , AVG(pct_sem)                                OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS pct_sem_yoy_4wk_avg
        , AVG(pct_seo)                                OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS pct_seo_yoy_4wk_avg
        , AVG(visit_to_cl_reg_conversion_1d)          OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS cl_reg_yoy_4wk_avg
        , AVG(visit_to_smb_signup_conversion_1d)      OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS smb_reg_yoy_4wk_avg
        , AVG(visit_to_basic_signup_conversion_1d)    OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS basic_reg_yoy_4wk_avg
        , AVG(visit_to_bp_signup_conversion_1d)       OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS bp_reg_yoy_4wk_avg
        , AVG(visit_to_high_intent_csw_1d)            OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS csw_yoy_4wk_avg
        , AVG(visit_to_cl_fjp_conversion_1d)          OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS fjp_yoy_4wk_avg
        , AVG(visit_to_offered_hire_conversion_1d)    OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS offered_hire_yoy_4wk_avg
        , AVG(visit_to_first_hire_conversion_1d)      OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS first_hire_yoy_4wk_avg
        , AVG(visit_to_cl_start_conversion_1d)        OVER (ORDER BY week_ ASC ROWS BETWEEN 55 PRECEDING AND 52 PRECEDING) AS offer_yoy_4wk_avg

        -- lag 1 week
        , LAG(visitors, 1)                               OVER (ORDER BY week_ ASC)  AS visitors_last_wk
        , LAG(bp_signup, 1)                              OVER (ORDER BY week_ ASC)  AS bp_signup_last_wk
        , LAG(smb_signup, 1)                             OVER (ORDER BY week_ ASC)  AS smb_signup_last_wk
        , LAG(pct_sem, 1)                                OVER (ORDER BY week_ ASC)  AS pct_sem_last_wk
        , LAG(pct_seo, 1)                                OVER (ORDER BY week_ ASC)  AS pct_seo_last_wk
        , LAG(visit_to_cl_reg_conversion_1d, 1)          OVER (ORDER BY week_ ASC)  AS cl_reg_last_wk
        , LAG(visit_to_smb_signup_conversion_1d, 1)      OVER (ORDER BY week_ ASC)  AS smb_reg_last_wk
        , LAG(visit_to_basic_signup_conversion_1d, 1)    OVER (ORDER BY week_ ASC)  AS basic_reg_last_wk
        , LAG(visit_to_bp_signup_conversion_1d, 1)       OVER (ORDER BY week_ ASC)  AS bp_reg_last_wk
        , LAG(visit_to_high_intent_csw_1d, 1)            OVER (ORDER BY week_ ASC)  AS csw_last_wk
        , LAG(visit_to_cl_fjp_conversion_1d, 1)          OVER (ORDER BY week_ ASC)  AS fjp_last_wk
        , LAG(visit_to_offered_hire_conversion_1d, 1)    OVER (ORDER BY week_ ASC)  AS offered_hire_last_wk
        , LAG(visit_to_first_hire_conversion_1d, 1)      OVER (ORDER BY week_ ASC)  AS first_hire_last_wk
        , LAG(visit_to_cl_start_conversion_1d, 1)        OVER (ORDER BY week_ ASC)  AS offer_last_wk

    FROM base
),

latest AS (
    SELECT * FROM windowed
    WHERE week_ = (SELECT MAX(week_) FROM windowed)
),

unpivoted AS (
    SELECT 1 AS metric_order, 'Entry Page Visitors' AS metric
        , visitors               AS this_wk
        , visitors_last_wk       AS last_wk
        , visitors_4wk_avg       AS avg_4wk
        , visitors_prior_4wk_avg AS prior_4wk_avg
        , visitors_yoy_4wk_avg   AS yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 2, '% Entry Page Visitors from SEM'
        , pct_sem,                pct_sem_last_wk
        , pct_sem_4wk_avg,        pct_sem_prior_4wk_avg
        , pct_sem_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 3, '% Entry Page Visitors from SEO'
        , pct_seo,                pct_seo_last_wk
        , pct_seo_4wk_avg,        pct_seo_prior_4wk_avg
        , pct_seo_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 4, 'Visit → CL Reg 1D'
        , visit_to_cl_reg_conversion_1d,       cl_reg_last_wk
        , cl_reg_4wk_avg,                      cl_reg_prior_4wk_avg
        , cl_reg_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 5, 'Visit → SMB Reg 1D'
        , visit_to_smb_signup_conversion_1d,   smb_reg_last_wk
        , smb_reg_4wk_avg,                     smb_reg_prior_4wk_avg
        , smb_reg_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 6, 'SMB Signups'
        , smb_signup,              smb_signup_last_wk
        , smb_signup_4wk_avg,      smb_signup_prior_4wk_avg
        , smb_signup_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 7, 'Visit → Basic Reg 1D'
        , visit_to_basic_signup_conversion_1d, basic_reg_last_wk
        , basic_reg_4wk_avg,                   basic_reg_prior_4wk_avg
        , basic_reg_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 8, 'Visit → B+ Reg 1D'
        , visit_to_bp_signup_conversion_1d,    bp_reg_last_wk
        , bp_reg_4wk_avg,                      bp_reg_prior_4wk_avg
        , bp_reg_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 9, 'B+ Signups'
        , bp_signup,               bp_signup_last_wk
        , bp_signup_4wk_avg,       bp_signup_prior_4wk_avg
        , bp_signup_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 10, 'Visit → High-Intent CSW 1D'
        , visit_to_high_intent_csw_1d,         csw_last_wk
        , csw_4wk_avg,                         csw_prior_4wk_avg
        , csw_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 11, 'Visit → FJP 1D'
        , visit_to_cl_fjp_conversion_1d,       fjp_last_wk
        , fjp_4wk_avg,                         fjp_prior_4wk_avg
        , fjp_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 12, 'Visit → Offer Hire 1D'
        , visit_to_offered_hire_conversion_1d, offered_hire_last_wk
        , offered_hire_4wk_avg,                offered_hire_prior_4wk_avg
        , offered_hire_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 13, 'Visit → First Hire 1D'
        , visit_to_first_hire_conversion_1d,   first_hire_last_wk
        , first_hire_4wk_avg,                  first_hire_prior_4wk_avg
        , first_hire_yoy_4wk_avg
    FROM latest
    UNION ALL
    SELECT 14, 'Visit → Offer 1D'
        , visit_to_cl_start_conversion_1d,     offer_last_wk
        , offer_4wk_avg,                       offer_prior_4wk_avg
        , offer_yoy_4wk_avg
    FROM latest
)

SELECT
    metric_order
    , metric
    , this_wk
    , last_wk
    , this_wk / NULLIF(last_wk, 0) - 1                     AS wow_delta
    , avg_4wk
    , avg_4wk / NULLIF(yoy_4wk_avg, 0) - 1                 AS four_wk_yoy_delta
    , prior_4wk_avg
    , avg_4wk / NULLIF(prior_4wk_avg, 0) - 1               AS four_wo4w_delta
FROM unpivoted
ORDER BY metric_order
;