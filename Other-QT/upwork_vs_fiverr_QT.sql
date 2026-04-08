select date_trunc('week', uvs.session_start_date) as week_
--- allocation point to get traffic
        , count(distinct uvs.visitor_id) as land_on_upwork_vs_fiverr_page
--- business metrics baseline for power calculation
        , count(distinct case when uvs.click_cnt>0 then uvs.visitor_id end) / count(distinct uvs.visitor_id) AS click_through_rate
        , count(distinct case when sasf.is_logged_out_visit_to_cl_registration_1d then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_cl_reg_conversion_1d
        , count(distinct case when bp.client_id is not null then uvs.visitor_id end)                                   AS bp_signup
        , bp_signup
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_bp_signup_conversion_1d
        , count(distinct case when cd.client_id is not null
                                   and cd.business_entity_size_segment = 'SMB'
                                   and cd.registration_date between uvs.session_start_date and uvs.session_start_date + 1
                              then uvs.visitor_id end)                                                                 AS smb_signup
        , smb_signup / count(distinct uvs.visitor_id)                                                                  AS visit_to_smb_signup_conversion_1d
        , count(distinct case when csw.activity_type in ('messaged_without_linked_post_or_project', 'posted_job_post')
                              then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_dm_or_jp_1d
        , count(distinct case when sasf.is_logged_out_visit_to_cl_fjp_1d then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_cl_fjp_conversion_1d
        , count(distinct case when offer.client_id is not null then uvs.visitor_id end) /  count(distinct uvs.visitor_id) AS visit_to_offer_1d
        , count(distinct case when sasf.is_logged_out_visit_to_cl_start_1d then uvs.visitor_id end)
              / count(distinct uvs.visitor_id)                                                                         AS visit_to_cl_start_conversion_1d


from SHASTA_SDC_PUBLISHED.DM_CSI.USER_VISIT_SESSION uvs
left join SHASTA_SDC_PUBLISHED.DM_IOM.SUIT_ALL_SESSION_FACT sasf
    on uvs.domain_session_id = sasf.domain_session_id
LEFT JOIN SHASTA_SDC_UPWORK.REGISTRATION.REGISTRATIONS reg
    ON sasf.visitor_id::varchar = reg.visitor_id::varchar
LEFT JOIN shasta_sdc_published.sherlock.client_dim_vw cd
    ON cd.username = reg.username
LEFT JOIN shasta_sdc_published.sales.business_plus_reg_attr AS bp
    ON cd.client_id = bp.client_id
    AND uvs.session_start_date BETWEEN bp.sub_create_ts::date - interval '2 day' AND bp.sub_create_ts::date
LEFT JOIN shasta_sdc_published.dm_iom.csw_client_seeking_work_fact csw
    ON csw.client_key = sasf.reg_client_key
    AND csw.activity_date BETWEEN uvs.session_start_date AND uvs.session_start_date + 1
LEFT JOIN SHASTA_SDC_PUBLISHED.DM_IOM.SHERLOCK_OFFER_FACT_VW offer
    ON offer.client_key = sasf.reg_client_key
    AND offer.offer_date BETWEEN uvs.session_start_date AND uvs.session_start_date + 1
where date_trunc('month', uvs.session_start_date) = '2026-03-01'
and uvs.upwork_location = 'resources'
and upwork_event_key ilike '%resources_upwork-vs-fiverr%'
and sasf.is_logged_out_visitor
and not sasf.is_recognized_visitor
group by all
order by week_
;