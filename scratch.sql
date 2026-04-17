    SELECT
        date_trunc('quarter', uvs.session_start_date)                                                                   AS quarter_
        , count(distinct uvs.visitor_id)                                                                              AS visitors
        , count(distinct case when cd.client_id is not null
                                   and cd.business_entity_size_segment = 'SMB'
                                   and cd.registration_date between uvs.session_start_date and uvs.session_start_date + 1
                              then uvs.visitor_id end)                                                                 AS smb_signup
        , smb_signup / visitors                                                                                        AS visit_to_smb_signup_conversion_1d
    FROM SHASTA_SDC_PUBLISHED.DM_CSI.USER_SESSION uvs
    JOIN shasta_sdc_published.dm_iom.suit_all_session_fact sasf
        ON uvs.domain_session_id = sasf.domain_session_id
    LEFT JOIN SHASTA_SDC_UPWORK.REGISTRATION.REGISTRATIONS reg
        ON sasf.visitor_id::varchar = reg.visitor_id::varchar
    LEFT JOIN shasta_sdc_published.sherlock.client_dim_vw cd
        ON cd.client_id = sasf.client_id
    WHERE uvs.session_start_date >= '2025-01-01'
    AND NOT uvs.is_bot
        AND uvs.IS_NEW_VISITOR AND NOT uvs.IS_RECOGNIZED_VISITOR AND uvs.IS_LOGGED_OUT_VISITOR
        AND NOT (sasf.reg_freelancer_is_upwork_internal OR sasf.reg_client_is_test_account OR sasf.reg_freelancer_is_test_account)
        AND NOT (sasf.reg_client_is_bad_actor OR sasf.reg_freelancer_is_bad_actor)
    GROUP BY 1 ORDER BY 1 DESC;

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
                                   and cd.business_entity_size_segment = 'SMB'
                                   and cd.registration_date between uvs.session_start_date and uvs.session_start_date + 1
                              then uvs.visitor_id end)                                                                 AS smb_signup
        , median(case when cd.client_id is not null
                                   and cd.business_entity_size_segment = 'SMB'
                                   then datediff(minute, uvs.session_start_ts, cd.registration_ts) end)                        AS smb_signup_median_time_mins
, percentile_cont(0.80) within group (order by 
    case when cd.client_id is not null
         and cd.business_entity_size_segment = 'SMB'
         then datediff(minute, uvs.session_start_ts, cd.registration_ts) end)  AS smb_signup_p80_time_mins

, percentile_cont(0.90) within group (order by 
    case when cd.client_id is not null
         and cd.business_entity_size_segment = 'SMB'
         then datediff(minute, uvs.session_start_ts, cd.registration_ts) end)  AS smb_signup_p90_time_mins
        , max(case when cd.client_id is not null
                                   and cd.business_entity_size_segment = 'SMB'
                                   then datediff(minute, uvs.session_start_ts, cd.registration_ts) end)                        AS smb_signup_max_time_mins

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
    GROUP BY ALL;