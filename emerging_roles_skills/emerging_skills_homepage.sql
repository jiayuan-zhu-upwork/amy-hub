WITH task_w AS (
  SELECT post_uid, label as task_label
  FROM sdc_umami_published.umami_sqlmesh.post_task_umr_inference_latest_v1
  WHERE rank = 1
  GROUP BY post_uid, label
),
role_w AS (
  SELECT post_uid,
    label as role_label
  FROM sdc_umami_published.umami_sqlmesh.post_role_umr_inference_latest_v1
  WHERE rank = 1 
  GROUP BY post_uid, label
),
l3_w AS (
  SELECT post_uid,
    label as l3_label
  FROM sdc_umami_published.umami_sqlmesh.post_l3_umr_inference_latest_v1
  WHERE rank = 1
  GROUP BY post_uid, label
),
skill_w AS (
  SELECT post_uid,
    Label as skill_label
  FROM sdc_umami_published.umami_sqlmesh.post_skill_umr_inference_latest_v1
  WHERE rank = 1
),
l2_w AS (
  SELECT post_uid,
    Label as l2_label
  FROM sdc_umami_published.umami_sqlmesh.post_l2_umr_inference_latest_v1
  WHERE rank = 1
  GROUP BY post_uid, label
)

SELECT
  b.agora_post_id, b.post_id, b.post_key,
  task.task_label,
  R.role_label,
  l3.l3_label,
  Sk.skill_label,
  l2.l2_label,
  is_ai.predicted_classification,
  is_ai.ai_category
FROM sherlock.post_dim_vw b
LEFT JOIN task_w task ON b.agora_post_id = task.post_uid
LEFT JOIN role_w r ON b.agora_post_id = r.post_uid
LEFT JOIN l3_w l3 ON b.agora_post_id = l3.post_uid
LEFT JOIN skill_w sk ON b.agora_post_id = sk.post_uid
LEFT JOIN l2_w l2 ON b.agora_post_id = l2.post_uid
LEFT JOIN SDC_UMAMI_PUBLISHED.MDS_PRYSMA.AI_JOB_POSTS_CLASSIFICATION is_ai 
  ON is_ai.post_id = b.post_id
WHERE b.post_date >= current_date - interval '120' day
AND b.is_qualified_post = true
;