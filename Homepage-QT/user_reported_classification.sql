
----- current user classfication service
select registration_date AS day,
count(DISTINCT client_id) as total_reg,
count(DISTINCT CASE WHEN business_entity_size_segment = 'SMB' THEN client_id END) AS "Very Small Business",
"Very Small Business" / total_reg * 100 AS smb_pct,
count(DISTINCT CASE WHEN business_entity_employee_count>1 THEN client_id END) AS cl_with_employee_count,
cl_with_employee_count / total_reg * 100 AS employee_count_coverage_pct
from sherlock.client_dim_vw
where registration_date >= CURRENT_DATE - INTERVAL '7' DAY
group by 1
order by 1;

----- user self-reported size classification
SELECT
    DATE(stsg.allocated_date) AS day,
    COUNT(DISTINCT stsg.organizationid) AS total_allocated,
    COUNT(DISTINCT CASE WHEN co_profiles.company_size BETWEEN 10 AND 499 THEN stsg.organizationid END) AS "Small & Medium Business",
    "Small & Medium Business" / NULLIF(total_allocated, 0) * 100 AS smb_pct,
    COUNT(DISTINCT CASE WHEN co_profiles.company_size>=1 THEN stsg.organizationid END) AS cl_with_company_size,
    cl_with_company_size / NULLIF(total_allocated, 0) * 100 AS company_size_coverage_pct
FROM (
    SELECT
        experiment_id,
        group_name,
        organizationid,
        TIMESTAMP::date as allocated_date
    FROM shasta_sdc_published.anl_statsig.exposures
    WHERE experiment_id = 'cg125_enrichment_reg_flow'
      AND group_name = 'Treatment'
) AS stsg
LEFT JOIN shasta_sdc_upwork.company_profiles.company_profiles AS co_profiles
    ON co_profiles.company_uid = stsg.organizationid
LEFT JOIN shasta_sdc_upwork.directory.organizations AS orgs
    ON orgs.uid = stsg.organizationid
WHERE stsg.allocated_date >= CURRENT_DATE - INTERVAL '7' DAY
AND stsg.organizationid IS NOT NULL
GROUP BY DATE(stsg.allocated_date)
ORDER BY day DESC;

------ check current classification thresholds
select
min(business_entity_employee_count), max(business_entity_employee_count) from sherlock.client_dim_vw
where business_entity_size_segment = 'SMB'
and registration_date >= CURRENT_DATE - INTERVAL '7' DAY
;


-- company size coverage improved from 9% to about 65%
-- PCT registration that can be classified as SMB increased from 4% to 10%