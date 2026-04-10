-- SEO NB Total Job Posts (based on JP influenced channel)
SELECT
    (TO_CHAR(DATE_TRUNC('month', CAST(DATE_TRUNC('quarter', job_posts_extends.post_ts ) AS DATE)), 'YYYY-MM')) AS "job_posts_extends.job_post_quarter",
    COUNT(DISTINCT job_posts_extends.post_key ) AS "job_posts_extends.post_count"
FROM sherlock.post_dim_vw  AS job_posts_extends 
LEFT JOIN sherlock.client_dim_vw  AS clients_extends ON clients_extends.client_id::bigint = job_posts_extends.client_id::bigint
LEFT JOIN sherlock.user_dim_vw  AS client_users ON client_users.user_id::BIGINT = clients_extends.user_id::BIGINT
LEFT JOIN shasta_sdc_published.dm_marketing_attr.attr_biz_kpi  AS attr_biz_kpi_client_acquisition_level ON attr_biz_kpi_client_acquisition_level.biz_kpi_id = job_posts_extends.agora_post_id
WHERE (clients_extends.is_qualified_client AND client_users.is_qualified_user) 
AND UPPER(attr_biz_kpi_client_acquisition_level.attributed_marketing_channel) = UPPER('SEO Non-Brand')
AND job_posts_extends.is_qualified_post
AND job_posts_extends.post_ts >= DATEADD('month', -18, CAST(DATE_TRUNC('quarter', CAST(DATE_TRUNC('quarter', CURRENT_DATE()) AS DATE)) AS DATE))
AND job_posts_extends.post_ts < DATEADD('month', 18, CAST(DATE_TRUNC('quarter', DATEADD('month', -18, CAST(DATE_TRUNC('quarter', CAST(DATE_TRUNC('quarter', CURRENT_DATE()) AS DATE)) AS DATE))) AS DATE))
GROUP BY
    1
ORDER BY
    1 DESC
  ;


--- SEO NB GSV (based on JP influenced channel)
--- Looker View: https://looker.upwork.com/looks/32872?toggle=pik; switched channel filter
SELECT
    (TO_CHAR(DATE_TRUNC('month', CAST(DATE_TRUNC('quarter', job_posts_extends.post_ts ) AS DATE)), 'YYYY-MM')) AS "job_posts_extends.job_post_quarter",
    COUNT(DISTINCT assignments_extends.contract_id ) AS "assignments_extends.count",
    COALESCE(SUM(CASE WHEN gsv_type.is_gsv  THEN payments.gl_amount  ELSE NULL END), 0) AS "payments.sum_total_gsv",
    COUNT(DISTINCT clients_extends.client_id) AS "clients_extends.client_count",
    COUNT(DISTINCT job_posts_extends.post_key ) AS "job_posts_extends.post_count"
FROM sherlock.post_dim_vw  AS job_posts_extends 
left join sherlock.allocated_payment_gsv_fact  AS payments ON payments.post_key::BIGINT = job_posts_extends.post_key::BIGINT
LEFT JOIN sherlock.contract_fact_vw  AS assignments_extends ON payments.contract_id::BIGINT = assignments_extends.contract_id::BIGINT
LEFT JOIN sherlock.client_dim_vw  AS clients_extends ON clients_extends.client_key::bigint = payments.client_key::bigint
LEFT JOIN sherlock.user_dim_vw  AS client_users ON client_users.user_id::BIGINT = clients_extends.user_id::BIGINT
LEFT JOIN sherlock.freelancer_dim_vw  AS freelancers_extends ON freelancers_extends.freelancer_key::BIGINT = assignments_extends.freelancer_key::BIGINT
LEFT JOIN shasta_sdc_published.dm_marketing_attr.attr_biz_kpi  AS attr_biz_kpi_client_acquisition_level ON attr_biz_kpi_client_acquisition_level.biz_kpi_id = job_posts_extends.agora_post_id
LEFT JOIN sherlock.gsv_type_dim  AS gsv_type ON gsv_type.gsv_type_key::BIGINT = payments.gsv_type_key::BIGINT

WHERE (NOT (case when gsv_type.je_source not in ('UPWK Platform', 'UPWK UCAD') then true end ) OR (case when gsv_type.je_source not in ('UPWK Platform', 'UPWK UCAD') then true end ) IS NULL) 
AND (NOT (case when not assignments_extends.is_qualified_contract ) then true end ) OR (case when not assignments_extends.is_qualified_contract  then true end ) IS NULL)
AND ((clients_extends.is_qualified_client ) AND (client_users.is_qualified_user )) 
AND (freelancers_extends.is_qualified_freelancer ) 
AND ((UPPER(( attr_biz_kpi_client_acquisition_level.attributed_marketing_channel  )) = UPPER('SEO Non-Brand'))) 
AND (job_posts_extends.is_qualified_post ) 
AND ( job_posts_extends.post_ts  ) >= ((DATEADD('month', -18, CAST(DATE_TRUNC('quarter', CAST(DATE_TRUNC('quarter', CURRENT_DATE()) AS DATE)) AS DATE)))) 
AND ( job_posts_extends.post_ts  ) < ((DATEADD('month', 18, CAST(DATE_TRUNC('quarter', DATEADD('month', -18, CAST(DATE_TRUNC('quarter', CAST(DATE_TRUNC('quarter', CURRENT_DATE()) AS DATE)) AS DATE))) AS DATE))))
AND ((CAST(DATE_TRUNC('quarter', job_posts_extends.post_ts ) AS DATE))  =  (CAST(DATE_TRUNC('quarter', payments.gl_date ) AS DATE)))
GROUP BY
    1
ORDER BY
    1 DESC
;

--- Jared's Query
--// In-Quarter GSV per Post
with base as (
    SELECT
        date_trunc(quarter, pdv.post_date) post_qtr
        ,pdv.post_id
        ,cdv.country_code = 'US' as is_us
        ,abk.attributed_marketing_channel channel
        ,SUM(case when gsv_type.is_gsv then p.gl_amount else 0 end) AS total_gsv
    from shasta_sdc_published.sherlock.post_dim_vw AS pdv
    left join shasta_sdc_published.sherlock.contract_fact_vw  AS con 
        ON pdv.post_id = con.post_id
        and con.is_qualified_contract
    join shasta_sdc_published.sherlock.client_dim_vw AS cdv 
        on cdv.client_id = pdv.client_id
    left join shasta_sdc_published.sherlock.allocated_payment_gsv_fact AS p
        on p.contract_id = con.contract_id
        -- Only want GSV within the same quarter as the post date ---------
        and date_trunc(quarter, p.gl_date) = date_trunc(quarter, pdv.post_date)
        -------------------------------------------------------------------
    left join shasta_sdc_published.sherlock.gsv_type_dim AS gsv_type 
        on gsv_type.gsv_type_key = p.gsv_type_key
        and gsv_type.is_gsv
    join shasta_sdc_published.dm_marketing_attr.attr_biz_kpi abk
        on abk.biz_kpi_id = pdv.agora_post_id
    where true
        -- Default Filters
        and (gsv_type.je_source in ('UPWK Platform', 'UPWK UCAD') or gsv_type.je_source is null)
        and (upper(cdv.client_market_segment) <> 'ENTERPRISE' or cdv.client_market_segment is null)
        and (gsv_type.gl_account_desc not in (
                'Managed Services GSV~UDC Service Rev'
                ,'Managed Services GSV~Embedded Checkout'
                ,'Managed Services GSV~Default'
            ) or gsv_type.gl_account_desc is null)
        and cdv.is_qualified_client
        and not cdv.is_guest_client
        and not cdv.is_test_account
        and not cdv.is_bad_actor
        and pdv.is_qualified_post
        -- Custom Filters
        and abk.attributed_marketing_channel = 'SEO Non-Brand'
    group by all
)
select 
    post_qtr
    ,count(distinct post_id) post_cnt
    ,sum(total_gsv) in_qtr_gsv
    ,in_qtr_gsv / post_cnt  in_qtr_gsv_per_post
from base
where true
    and post_qtr in ('2025-01-01', '2026-01-01')
group by all
order by 1,2
;