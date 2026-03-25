select distinct upwork_event_key, upwork_sublocation
from SHASTA_SDC_PUBLISHED.DM_CSI.USER_VISIT_SESSION uvs
join SHASTA_SDC_PUBLISHED.DM_CSI.USER_SESSION us 
    on uvs.domain_session_id = us.domain_session_id
where uvs.session_start_date between '2026-02-10' and '2026-02-17'
and upwork_location = 'reg-glide-path'
and not us.is_recognized_visitor
and is_logged_out_visitor;