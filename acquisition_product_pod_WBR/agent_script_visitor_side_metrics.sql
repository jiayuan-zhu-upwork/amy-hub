-- Metric Aggregation Formula
-- Entry Page Visitors: visitors
--  ↳ % from SEM: sem_visitors / visitors
--  ↳ % from SEO: seo_visitors / visitors
-- Visit → CL Reg 1D: cnt_cl_reg_conversion_1d / visitors
--  ↳ Visit → SMB Reg 1D: cnt_smb_signup / visitors
--  ↳ SMB Reg (#): cnt_smb_signup
--  ↳ Visit → B+ Reg 1D: cnt_bp_signup / visitors
--  ↳ B+ Reg (#): cnt_bp_signup
-- Visit → DM or JP 1D: cnt_high_intent_csw_1d / visitors
-- Visit → FJP 1D: cnt_cl_fjp_1d / visitors
-- Visit → Offer 1D: cnt_cl_send_offer_1d / visitors
-- Visit → CL Hire 1D: cnt_cl_hire_1d / visitors
-- Visit → CL Start 1D: cnt_cl_start_1d / visitors

-- Surface Level:
-- homepage: upwork_surface = 'homepage'
-- hire_seo: upwork_surface = 'hire' and marketing_channel in ('SEO Brand', 'SEO Non-Brand')
-- hire_sem: upwork_surface = 'hire' and marketing_channel in ('SEM Brand', 'SEM Non-Brand', 'SEM Unknown')
-- pricing: upwork_surface = 'pricing'
-- how it works: upwork_surface = 'i_how-it-works'
-- Metric Needed:
-- CTR: cnt_visitors_w_click / visitors
-- Median session: meadian(median_duration_seconds)
-- Visit → CL Reg 1D: cnt_cl_reg_conversion_1d / visitors
--  ↳ Visit → SMB Reg 1D: cnt_smb_signup / visitors
--  ↳ SMB Reg (#): cnt_smb_signup
--  ↳ Visit → B+ Reg 1D: cnt_bp_signup / visitors
--  ↳ B+ Reg (#): cnt_bp_signup

    SELECT
        date_trunc('week', uvs.session_start_date)                                                                   AS week_
        , sasf.marketing_channel_group                                                                                      AS marketing_channel
        , sasf.geo_country_name                                                                                         AS country
        , sasf.first_upwork_location                                                                                     AS upwork_surface
        , count(distinct uvs.visitor_id)                                                                              AS visitors
        , count(distinct case when sasf.marketing_channel_group = 'SEM' then uvs.visitor_id end)                      AS sem_visitors
        , count(distinct case when sasf.marketing_channel_group = 'SEO' then uvs.visitor_id end)                      AS seo_visitors
        , count(distinct case when uvs.total_click_cnt > 0 then uvs.visitor_id end)                                   AS cnt_visitors_w_click
        , median(uvs.session_duration_seconds)                                                                        AS median_duration_seconds
        , count(distinct case when sasf.is_logged_out_visit_to_cl_registration_1d then uvs.visitor_id end)            AS cnt_cl_reg_conversion_1d
        , count(distinct case when sasf.is_logged_out_visit_to_fl_registration_1d then uvs.visitor_id end)            AS cnt_fl_reg_conversion_1d
        , count(distinct case when cd.client_id is not null
                                   and cd.business_entity_size_segment = 'SMB'
                                   and cd.registration_date between uvs.session_start_date and uvs.session_start_date + 1
                              then uvs.visitor_id end)                                                                 AS cnt_smb_signup
        , count(distinct case when csw.activity_type in ('messaged_without_linked_post_or_project', 'posted_job_post') then uvs.visitor_id end) AS cnt_high_intent_csw_1d
        , count(distinct case when sasf.is_logged_out_visit_to_cl_fjp_1d then uvs.visitor_id end)                      AS cnt_cl_fjp_1d
        , count(distinct case when offer.client_id is not null then uvs.visitor_id end)                                AS cnt_cl_send_offer_1d
        , count(distinct case when cd.first_hire_date between uvs.session_start_date and uvs.session_start_date + 1 then uvs.visitor_id end)  AS cnt_cl_hire_1d
        , count(distinct case when sasf.is_logged_out_visit_to_cl_start_1d then uvs.visitor_id end)                    AS cnt_cl_start_1d

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
;
