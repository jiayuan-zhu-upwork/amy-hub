select count(distinct domain_session_id)
from SHASTA_SDC_PUBLISHED.DM_CSI.USER_SESSION
where session_start_date >= '2026-01-01'