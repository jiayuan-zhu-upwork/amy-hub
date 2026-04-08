
-- ctr
-- visit to cl reg 
-- visit to smb reg
-- visit to b+ reg
-- hero ctr - need definition by page
-- median duration 

;
WITH base AS (
    -- your existing base query unchanged
    SELECT
            case when uvs.first_upwork_location in ('homepage','pricing', 'i_how-it-works') then uvs.first_upwork_location 
            when uvs.first_upwork_location = 'hire' then
            case when sasf.marketing_channel_group = 'SEO' then 'hire_seo'
                when sasf.marketing_channel_group = 'SEM' then 'hire_sem'
                else 'hire_other' end
            else 'other'
        end as first_upwork_location_group
        , date_trunc('week', uvs.session_start_date)                                                        AS week_
        , count(distinct uvs.visitor_id)                                                                    AS visitors
        , count(distinct case when uvs.total_click_cnt > 0 then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                               AS page_ctr
        , median(uvs.session_duration_seconds)                                                              AS median_duration_seconds
        , count(distinct case when sasf.is_logged_out_visit_to_cl_registration_1d then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                               AS visit_to_cl_reg_conversion_1d
        , count(distinct case when bp.client_id is not null then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                               AS visit_to_bp_signup_conversion_1d
        , count(distinct case when cd.client_id is not null
                                   and cd.business_entity_size_segment = 'SMB'
                                   and cd.registration_date between uvs.session_start_date and uvs.session_start_date + 1
                              then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                               AS visit_to_smb_signup_conversion_1d
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
    WHERE uvs.first_upwork_location IN ('homepage', 'hire', 'hire_landing_skill', 'pricing', 'i_how-it-works')
        AND NOT uvs.is_bot
        AND uvs.IS_NEW_VISITOR AND NOT uvs.IS_RECOGNIZED_VISITOR AND uvs.IS_LOGGED_OUT_VISITOR
        AND date_trunc('week', uvs.session_start_date) >= date_trunc('week', current_date()) - interval '12 week'
        AND date_trunc('week', uvs.session_start_date) < date_trunc('week', current_date())
        AND NOT (sasf.reg_freelancer_is_upwork_internal OR sasf.reg_client_is_test_account OR sasf.reg_freelancer_is_test_account)
        AND NOT (sasf.reg_client_is_bad_actor OR sasf.reg_freelancer_is_bad_actor)
    GROUP BY ALL
),

final AS (
    SELECT
    first_upwork_location_group
        , week_
, visitors
, visitors / NULLIF(LAG(visitors, 1) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1  AS visitors_wow_pct
, visitors / NULLIF(LAG(visitors, 4) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1  AS visitors_vs_last_mo_pct

        , page_ctr
        , page_ctr / NULLIF(LAG(page_ctr, 1) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1   AS ctr_wow_pct
        , page_ctr / NULLIF(LAG(page_ctr, 4) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1   AS ctr_vs_last_mo_pct

        , visit_to_cl_reg_conversion_1d
        , visit_to_cl_reg_conversion_1d / NULLIF(LAG(visit_to_cl_reg_conversion_1d, 1) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1  AS cl_reg_wow_pct
        , visit_to_cl_reg_conversion_1d / NULLIF(LAG(visit_to_cl_reg_conversion_1d, 4) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1  AS cl_reg_vs_last_mo_pct

        , visit_to_smb_signup_conversion_1d
        , visit_to_smb_signup_conversion_1d / NULLIF(LAG(visit_to_smb_signup_conversion_1d, 1) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1  AS smb_reg_wow_pct
        , visit_to_smb_signup_conversion_1d / NULLIF(LAG(visit_to_smb_signup_conversion_1d, 4) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1  AS smb_reg_vs_last_mo_pct

        , visit_to_bp_signup_conversion_1d
        , visit_to_bp_signup_conversion_1d / NULLIF(LAG(visit_to_bp_signup_conversion_1d, 1) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1   AS bp_reg_wow_pct
        , visit_to_bp_signup_conversion_1d / NULLIF(LAG(visit_to_bp_signup_conversion_1d, 4) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1   AS bp_reg_vs_last_mo_pct

        , median_duration_seconds
        , median_duration_seconds / NULLIF(LAG(median_duration_seconds, 1) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1  AS dur_wow_pct
        , median_duration_seconds / NULLIF(LAG(median_duration_seconds, 4) OVER (PARTITION BY first_upwork_location_group ORDER BY week_ ASC), 0) - 1  AS dur_vs_last_mo_pct

    FROM base
),

-- Step 1: filter to latest week only
latest AS (
    SELECT * FROM final
    WHERE week_ = (SELECT MAX(week_) FROM final)
),

-- Step 2: unpivot metrics into rows
-- Each metric becomes 3 rows: wk value, wow, vs last mo
unpivoted AS (
SELECT 1 AS metric_order, 'Visits' AS metric, first_upwork_location_group, visitors AS wk_val, visitors_wow_pct AS wow, visitors_vs_last_mo_pct AS vs_mo FROM latest
    UNION ALL
    SELECT 2,                 'CTR',              first_upwork_location_group, page_ctr,                          ctr_wow_pct,               ctr_vs_last_mo_pct                           FROM latest
    UNION ALL
    SELECT 3,                 'CL Reg %',         first_upwork_location_group, visit_to_cl_reg_conversion_1d,    cl_reg_wow_pct,            cl_reg_vs_last_mo_pct                        FROM latest
    UNION ALL
    SELECT 4,                 'SMB Reg %',        first_upwork_location_group, visit_to_smb_signup_conversion_1d, smb_reg_wow_pct,          smb_reg_vs_last_mo_pct                       FROM latest
    UNION ALL
    SELECT 5,                 'B+ Reg %',         first_upwork_location_group, visit_to_bp_signup_conversion_1d,  bp_reg_wow_pct,           bp_reg_vs_last_mo_pct                        FROM latest
    UNION ALL
    SELECT 6,                 'Median session (s)', first_upwork_location_group, median_duration_seconds,         dur_wow_pct,               dur_vs_last_mo_pct                           FROM latest
)

-- Step 3: pivot surfaces into columns
SELECT
    metric_order
    , metric

    -- Homepage
    , MAX(CASE WHEN first_upwork_location_group = 'homepage'          THEN wk_val END)  AS hp_wk
    , MAX(CASE WHEN first_upwork_location_group = 'homepage'          THEN wow    END)  AS hp_wow
    , MAX(CASE WHEN first_upwork_location_group = 'homepage'          THEN vs_mo  END)  AS hp_vs_mo

    -- Hire SEO
    , MAX(CASE WHEN first_upwork_location_group = 'hire_seo'              THEN wk_val END)  AS hire_wk
    , MAX(CASE WHEN first_upwork_location_group = 'hire_seo'              THEN wow    END)  AS hire_wow
    , MAX(CASE WHEN first_upwork_location_group = 'hire_seo'              THEN vs_mo  END)  AS hire_vs_mo

    -- Hire SEM
    , MAX(CASE WHEN first_upwork_location_group = 'hire_sem' THEN wk_val END) AS hls_wk
    , MAX(CASE WHEN first_upwork_location_group = 'hire_sem' THEN wow    END) AS hls_wow
    , MAX(CASE WHEN first_upwork_location_group = 'hire_sem' THEN vs_mo  END) AS hls_vs_mo

    -- Hire Other
    , MAX(CASE WHEN first_upwork_location_group = 'hire_other' THEN wk_val END) AS hire_other_wk
    , MAX(CASE WHEN first_upwork_location_group = 'hire_other' THEN wow    END) AS hire_other_wow
    , MAX(CASE WHEN first_upwork_location_group = 'hire_other' THEN vs_mo  END) AS hire_other_vs_mo

    -- Pricing
    , MAX(CASE WHEN first_upwork_location_group = 'pricing'           THEN wk_val END)  AS pricing_wk
    , MAX(CASE WHEN first_upwork_location_group = 'pricing'           THEN wow    END)  AS pricing_wow
    , MAX(CASE WHEN first_upwork_location_group = 'pricing'           THEN vs_mo  END)  AS pricing_vs_mo

    -- How It Works
    , MAX(CASE WHEN first_upwork_location_group = 'i_how-it-works'   THEN wk_val END)   AS hiw_wk
    , MAX(CASE WHEN first_upwork_location_group = 'i_how-it-works'   THEN wow    END)   AS hiw_wow
    , MAX(CASE WHEN first_upwork_location_group = 'i_how-it-works'   THEN vs_mo  END)   AS hiw_vs_mo

FROM unpivoted
GROUP BY metric_order, metric
ORDER BY metric_order
;


select uvs.first_upwork_location
, date_trunc('week', uvs.session_start_date) as week_
, count(distinct uvs.visitor_id) as visitors
, count(distinct case when uvs.total_click_cnt > 0 then uvs.visitor_id end) / count(distinct uvs.visitor_id) as page_ctr
, count(distinct case when events.upwork_event_key in ('click.search_input_click.homepage.cl_gp_hero',
'click.browse_jobs_click.homepage.cl_gp_hero',
'click.open_category.homepage.dynamic-l1-categories',
'click.iterative_skills_hero_cta.hire.iterative-skills-hero',
'click.see_profile_button_click.hire.fl_tile'.
'click.tile_click.hire.fl_tile',
'click.click_hero_mkpl_cta.pricing_client.hero',
'click.click_hero_bp_cta.pricing_client.hero',
'click.click_compare_mkpl_cta.pricing_client.compare',
'click.click_compare_bp_cta.pricing_client.compare',
'click.click_hero_cta.i_how-it-works_client.hero',
'click.click_bplus_cta.i_how-it-works_client.bplus') then uvs.visitor_id end) / count(distinct uvs.visitor_id) as high_intent_zone_ctr
, median(uvs.session_duration_seconds) as median_duration_seconds
, count(distinct case when sasf.is_logged_out_visit_to_cl_registration_1d then uvs.visitor_id end) / count(distinct uvs.visitor_id) as visit_to_cl_reg_conversion_1d
, count(distinct case when bp.client_id is not null then uvs.visitor_id end) as bp_signup
, count(distinct case when bp.client_id is not null then uvs.visitor_id end) / count(distinct uvs.visitor_id) as visit_to_bp_signup_conversion_1d
, count(distinct case when cd.client_id is not null and uppd.payment_plan_name not in ('Business Plus', 'Business Plus Net 30') and cd.registration_date between uvs.session_start_date and uvs.session_start_date + 1 then reg.visitor_id end) basic_signup
, basic_signup / visitors as visit_to_basic_signup_conversion_1d
, count(distinct case when cd.client_id is not null and cd.business_entity_size_segment = 'SMB' and cd.registration_date between uvs.session_start_date and uvs.session_start_date + 1 then reg.visitor_id end) smb_signup
, smb_signup / visitors as visit_to_smb_signup_conversion_1d
FROM SHASTA_SDC_PUBLISHED.DM_CSI.USER_SESSION uvs
join shasta_sdc_published.dm_iom.suit_all_session_fact sasf
    on uvs.domain_session_id = sasf.domain_session_id
join SHASTA_SDC_PUBLISHED.DM_CSI.USER_VISIT_SESSION events
    on uvs.domain_session_id = events.domain_session_id
left join SHASTA_SDC_UPWORK.REGISTRATION.REGISTRATIONS reg
    on sasf.visitor_id::varchar = reg.visitor_id::varchar
left join shasta_sdc_published.sherlock.client_dim_vw cd 
    on cd.username = reg.username  
left join shasta_sdc_published.sales.business_plus_reg_attr as bp
    on cd.client_id = bp.client_id
    and uvs.session_start_date between bp.sub_create_ts::date - interval '2 day' and bp.sub_create_ts::date
left join sherlock.user_payment_plan_dim uppd
  on uppd.client_id = cd.client_id
  and cd.registration_date between uppd.start_dt and uppd.end_dt
-- left join shasta_sdc_published.dm_iom.csw_client_seeking_work_fact csw
--     on csw.client_key = sasf.reg_client_key
-- left join sherlock.contract_fact_vw cf
--     on hmd.hiring_manager_key = cf.hiring_manager_key
where uvs.first_upwork_location in ('homepage', 'hire', 'hire_landing_skill','pricing','i_how-it-works')
    and not uvs.is_bot
    and uvs.IS_NEW_VISITOR AND not uvs.IS_RECOGNIZED_VISITOR AND uvs.IS_LOGGED_OUT_VISITOR
    and date_trunc('week', uvs.session_start_date) >= date_trunc('week', current_date()) - interval '12 week'
    and date_trunc('week', uvs.session_start_date) < date_trunc('week', current_date())
    and not (sasf.reg_freelancer_is_upwork_internal or sasf.reg_client_is_test_account or sasf.reg_freelancer_is_test_account)
    and not (sasf.reg_client_is_bad_actor or sasf.reg_freelancer_is_bad_actor)
Group by all
order by 1, 2 desc
;


    SELECT
            case when uvs.first_upwork_location in ('homepage','pricing', 'i_how-it-works') then uvs.first_upwork_location 
            when uvs.first_upwork_location = 'hire' then
            case when sasf.marketing_channel = 'SEO' then 'hire_seo'
                when sasf.marketing_channel = 'SEM' then 'hire_sem'
                else 'hire_other' end
            else 'other'
        end as first_upwork_location_group
        , date_trunc('week', uvs.session_start_date)                                                        AS week_
        , count(distinct uvs.visitor_id)                                                                    AS visitors
        , count(distinct case when uvs.total_click_cnt > 0 then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                               AS page_ctr
        , median(uvs.session_duration_seconds)                                                              AS median_duration_seconds
        , count(distinct case when sasf.is_logged_out_visit_to_cl_registration_1d then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                               AS visit_to_cl_reg_conversion_1d
        , count(distinct case when bp.client_id is not null then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                               AS visit_to_bp_signup_conversion_1d
        , count(distinct case when cd.client_id is not null
                                   and cd.business_entity_size_segment = 'SMB'
                                   and cd.registration_date between uvs.session_start_date and uvs.session_start_date + 1
                              then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                               AS visit_to_smb_signup_conversion_1d
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
    WHERE uvs.first_upwork_location IN ('homepage', 'hire', 'hire_landing_skill', 'pricing', 'i_how-it-works')
        AND NOT uvs.is_bot
        AND uvs.IS_NEW_VISITOR AND NOT uvs.IS_RECOGNIZED_VISITOR AND uvs.IS_LOGGED_OUT_VISITOR
        AND date_trunc('week', uvs.session_start_date) >= date_trunc('week', current_date()) - interval '12 week'
        AND date_trunc('week', uvs.session_start_date) < date_trunc('week', current_date())
        AND NOT (sasf.reg_freelancer_is_upwork_internal OR sasf.reg_client_is_test_account OR sasf.reg_freelancer_is_test_account)
        AND NOT (sasf.reg_client_is_bad_actor OR sasf.reg_freelancer_is_bad_actor)
    GROUP BY ALL
    ;