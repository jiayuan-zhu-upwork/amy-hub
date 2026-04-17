
-- Visit to SMB Registration and SMB Registration 1D (top of Efficiency tab under Visit to CL Reg)
SELECT
   DATE_TRUNC('week', f.session_date)::date                  AS week_start   -- Monday
--    , f.geo_country_name
--    , f.marketing_channel_group
--    , f.marketing_channel
--    , f.first_upwork_location                                  AS upwork_surface
   , COUNT(DISTINCT CASE
         WHEN f.is_logged_out_visitor
          AND NOT f.is_recognized_visitor
         THEN f.visitor_id END)                               AS net_new_visitors
   , COUNT(DISTINCT CASE
         WHEN f.is_logged_out_visitor
          AND NOT f.is_recognized_visitor
          AND f.is_logged_out_visit_to_cl_registration_1d
         THEN f.visitor_id END)                               AS cl_reg_1d_visitors
 
   , COUNT(DISTINCT CASE
         WHEN f.is_logged_out_visitor
          AND NOT f.is_recognized_visitor
          AND f.is_logged_out_visit_to_fl_registration_1d
         THEN f.visitor_id END)                               AS fl_reg_1d_visitors
   , DIV0NULL(
         COUNT(DISTINCT CASE
             WHEN f.is_logged_out_visitor
              AND NOT f.is_recognized_visitor
              AND f.is_logged_out_visit_to_cl_registration_1d
             THEN f.visitor_id END),
         COUNT(DISTINCT CASE
             WHEN f.is_logged_out_visitor
              AND NOT f.is_recognized_visitor
             THEN f.visitor_id END)
     )                                                        AS visit_to_cl_reg_rate
   , DIV0NULL(
         COUNT(DISTINCT CASE
             WHEN f.is_logged_out_visitor
              AND NOT f.is_recognized_visitor
              AND f.is_logged_out_visit_to_fl_registration_1d
             THEN f.visitor_id END),
         COUNT(DISTINCT CASE
             WHEN f.is_logged_out_visitor
              AND NOT f.is_recognized_visitor
             THEN f.visitor_id END)
     )                                                        AS visit_to_fl_reg_rate
    
    , COUNT(DISTINCT CASE
         WHEN f.is_logged_out_visitor
          AND NOT f.is_recognized_visitor
          AND bp.client_id is not null
         THEN f.visitor_id END)                               AS bp_enroll_1d_visitors

    , COUNT(DISTINCT CASE
         WHEN f.is_logged_out_visitor
          AND NOT f.is_recognized_visitor
          AND f.is_logged_out_visit_to_cl_registration_1d
          AND reg.signup_flow = 66
         THEN f.visitor_id END)                               AS bp_reg_1d_visitors

    , COUNT(DISTINCT CASE
         WHEN f.is_logged_out_visitor
          AND NOT f.is_recognized_visitor
          AND f.is_logged_out_visit_to_cl_registration_1d
          AND cd.business_entity_size_segment = 'SMB'
         THEN f.visitor_id END)                               AS smb_cl_reg_1d_visitors

   , DIV0NULL(
        COUNT(DISTINCT CASE
            WHEN f.is_logged_out_visitor
            AND NOT f.is_recognized_visitor
            AND bp.client_id is not null
            THEN f.visitor_id END),
         COUNT(DISTINCT CASE
             WHEN f.is_logged_out_visitor
              AND NOT f.is_recognized_visitor
             THEN f.visitor_id END)
     )                                                        AS visit_to_bp_enrollment_rate

   , DIV0NULL(
        COUNT(DISTINCT CASE
         WHEN f.is_logged_out_visitor
          AND NOT f.is_recognized_visitor
          AND f.is_logged_out_visit_to_cl_registration_1d
          AND reg.signup_flow = 66
         THEN f.visitor_id END),
         COUNT(DISTINCT CASE
             WHEN f.is_logged_out_visitor
              AND NOT f.is_recognized_visitor
             THEN f.visitor_id END)
     )                                                        AS visit_to_bp_reg_rate

    , DIV0NULL(
        COUNT(DISTINCT CASE
         WHEN f.is_logged_out_visitor
          AND NOT f.is_recognized_visitor
          AND f.is_logged_out_visit_to_cl_registration_1d
          AND cd.business_entity_size_segment = 'SMB'
         THEN f.visitor_id END),
         COUNT(DISTINCT CASE
             WHEN f.is_logged_out_visitor
              AND NOT f.is_recognized_visitor
             THEN f.visitor_id END)
     )                                                        AS visit_to_smb_cl_reg_rate

FROM dm_iom.suit_entry_page_performance_fact_vw  AS f
LEFT JOIN SHASTA_SDC_UPWORK.REGISTRATION.REGISTRATIONS reg
    ON f.visitor_id::varchar = reg.visitor_id::varchar
LEFT JOIN shasta_sdc_published.sherlock.client_dim_vw cd
    ON cd.username = reg.username
LEFT JOIN shasta_sdc_published.sales.business_plus_reg_attr AS bp
    ON cd.client_id = bp.client_id
    AND f.session_date BETWEEN bp.sub_create_ts::date - interval '1 day' AND bp.sub_create_ts::date
WHERE   (NOT f.reg_client_is_bad_actor     OR f.reg_client_is_bad_actor     IS NULL)
   AND (NOT f.reg_freelancer_is_bad_actor OR f.reg_freelancer_is_bad_actor IS NULL)
   AND f.session_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY ALL
;
