--- company 
SELECT 
    stsg.experiment_id,
    stsg.group_name,
    co_profiles.company_uid,
    co_profiles.*
FROM shasta_sdc_published.anl_statsig.exposures AS stsg
JOIN shasta_sdc_upwork.company_profiles.company_profiles AS co_profiles
    ON stsg.organizationid = co_profiles.company_uid
WHERE stsg.experiment_id = 'cg125_enrichment_reg_flow'
  AND stsg.group_name = 'Control'
  ;

  SELECT
    stsg.experiment_id,
    stsg.group_name,
    co_profiles.company_uid,
    orgs.name,
    co_profiles.company_size,
    co_profiles.url
FROM (
    SELECT
        experiment_id,
        group_name,
        organizationid
    FROM shasta_sdc_published.anl_statsig.exposures
    WHERE experiment_id = 'cg125_enrichment_reg_flow'
      AND group_name != 'Control'
) AS stsg
JOIN shasta_sdc_upwork.company_profiles.company_profiles AS co_profiles
    ON co_profiles.company_uid = stsg.organizationid
JOIN shasta_sdc_upwork.directory.organizations AS orgs
    ON orgs.uid = stsg.organizationid;