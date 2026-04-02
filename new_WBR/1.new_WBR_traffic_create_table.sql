---------------- Base Table for Visitors --------------
create or replace table sdc_user.amyzhu.new_wbr_traffic_stg as 
WITH traffic_stg as (
SELECT 
    suit_entry_page_performance_fact.visitor_id,
    suit_entry_page_performance_fact.session_date AS visit_date,
    case when suit_entry_page_performance_fact.is_exit and not suit_entry_page_performance_fact.is_click then true else false end is_bounce,
    case when (suit_entry_page_performance_fact.is_logged_out_visitor ) AND NOT (suit_entry_page_performance_fact.is_recognized_visitor ) then true else false end as is_net_new_visitor,
    case when suit_entry_page_performance_fact.is_logged_out_visit_to_cl_registration_1d then true else false end is_cl_reg_1d,
    case when suit_entry_page_performance_fact.is_logged_out_visit_to_fl_registration_1d then true else false end is_fl_reg_1d
FROM dm_iom.suit_entry_page_performance_fact_vw  AS suit_entry_page_performance_fact
WHERE (NOT (suit_entry_page_performance_fact.reg_client_is_bad_actor ) OR (suit_entry_page_performance_fact.reg_client_is_bad_actor ) IS NULL) 
    AND (NOT (suit_entry_page_performance_fact.reg_freelancer_is_bad_actor ) OR (suit_entry_page_performance_fact.reg_freelancer_is_bad_actor ) IS NULL) 
    -- AND (suit_entry_page_performance_fact.is_logged_out_visitor ) AND (suit_entry_page_performance_fact.is_recognized_visitor )
    AND suit_entry_page_performance_fact.session_date::date >= DATEADD('year', -3, date_trunc('year', current_date()))
    AND suit_entry_page_performance_fact.session_date::date < CURRENT_DATE()
GROUP BY ALL
)

SELECT visitor_id,
        visit_date,
        max(is_net_new_visitor) as is_net_new_visitor,
        max(is_bounce) as is_bounce,
        max(is_cl_reg_1d) as is_cl_reg_1d,
        max(is_fl_reg_1d) as is_fl_reg_1d

FROM traffic_stg
GROUP BY 1, 2
;


---------------- All Visitors & Bounce --------------
create or replace table sdc_user.amyzhu.new_wbr_traffic_input_dataset as 
WITH a AS (
    SELECT
        DATE_TRUNC('week', CURRENT_DATE)::date    AS ws,
        DATE_TRUNC('month', CURRENT_DATE)::date   AS ms,
        DATE_TRUNC('quarter', CURRENT_DATE)::date AS qs,
        DATE_TRUNC('year', CURRENT_DATE)::date    AS ys
)

-- , base as (
select visitor_id,
        visit_date,
        is_bounce,
        is_cl_reg_1d,
        is_fl_reg_1d,
        is_net_new_visitor,
        a.ws,
        a.ms,
        a.qs,
        a.ys
from sdc_user.amyzhu.new_wbr_traffic_stg
CROSS JOIN a
;


---------------- Base Table for Login --------------
create or replace table sdc_user.amyzhu.new_wbr_login_stg as 
SELECT 
    suit_all_session_fact.user_uid as user_uid,
    suit_all_session_fact.session_date AS visit_date,
    suit_all_session_fact.is_client,
    suit_all_session_fact.is_freelancer
FROM dm_iom.suit_all_session_fact_vw  AS suit_all_session_fact
WHERE (NOT (suit_all_session_fact.client_is_bad_actor ) OR (suit_all_session_fact.client_is_bad_actor ) IS NULL) 
    AND (NOT (suit_all_session_fact.freelancer_is_bad_actor ) OR (suit_all_session_fact.freelancer_is_bad_actor ) IS NULL)
    AND suit_all_session_fact.session_date::date >= DATEADD('year', -3, date_trunc('year', current_date()))
    AND suit_all_session_fact.session_date::date < CURRENT_DATE()
AND is_logged_in_user
GROUP BY ALL
;