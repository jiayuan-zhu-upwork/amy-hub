


select date_trunc('month',pdv.post_date) as post_month,
    count(distinct case when pp_ops.profile_posting_ceiling then pdv.post_id end) as profile_posting_count,
    count(distinct case when pdv.post_status = 'MQ' and not pdv.is_ghost_post then pdv.post_id end) as mq_post_count,
    count(distinct case when pdv.is_qualified_post and not pdv.is_ghost_post then pdv.post_id end) as qualified_post_count,
    count(distinct case when not pdv.is_ghost_post then pdv.post_id end) as marketplace_post_count,
    profile_posting_count / marketplace_post_count as pct_profile_posting,
    mq_post_count / marketplace_post_count as pct_mq_posting
from shasta_sdc_published.sherlock.post_dim_vw as pdv
left join shasta_sdc_published.tns_analytics.profile_posting pp_ops --hardcoded DB
  on pp_ops.post_key = pdv.post_key
--   and pp_ops.profile_posting_ceiling --truth is somewhere in the middle between floor & ceiling, but it's better to overreport than underreport
where pdv.post_date >= date_trunc('month', current_date - interval '12' month) --looking at 12 months to get enough data points for trend analysis
group by 1
order by 1;


select * from shasta_sdc_published.tns_analytics.profile_posting limit 101;