WITH

-- ── Step 1: Time window boundaries ───────────────────────────
dates AS (
  SELECT
    CURRENT_DATE - 2               AS today,
    CURRENT_DATE - 3               AS yesterday,
    CURRENT_DATE - 7               AS this_week_start,
    CURRENT_DATE - 17              AS last_week_start,
    '2026-01-01'::DATE             AS ai_window_start,
    '2026-03-28'::DATE             AS ai_window_end
),

-- ── Step 2: Skill-level AI flag from historical window ────────
skill_ai_flag AS (
  SELECT
    t.label                                           AS task_label,

    ROUND(
      SUM(CASE
        WHEN ai.predicted_classification = 'AI-related'
        THEN 1 ELSE 0
      END) / COUNT(DISTINCT b.post_id) * 100, 1)     AS ai_share_pct,

    CASE
      WHEN ROUND(
        SUM(CASE
          WHEN ai.predicted_classification = 'AI-related'
          THEN 1 ELSE 0
        END) / COUNT(DISTINCT b.post_id) * 100, 1) >= 40
      THEN 'AI-related'
      ELSE 'Non-AI-related'
    END                                               AS is_ai_skill

  FROM sherlock.post_dim_vw b
  INNER JOIN sdc_umami_published.umami_sqlmesh.post_task_umr_inference_latest_v1 t
          ON b.agora_post_id = t.post_uid
         AND t.rank <= 5
         AND t.score >= 0.6
  INNER JOIN sdc_umami_published.mds_prysma.ai_job_posts_classification ai
          ON ai.post_id = b.post_id
  CROSS JOIN dates d
  WHERE b.post_date >= d.ai_window_start
    AND b.post_date <= d.ai_window_end
    AND b.is_qualified_post = TRUE
    AND t.label IS NOT NULL
  GROUP BY t.label
),

-- ── Step 3: Raw posts — covers daily + weekly windows ─────────
raw_posts AS (
  SELECT
    b.post_id,
    b.post_date,
    t.label  AS task_label,
    r.label  AS role_label       -- for top_role_label context
  FROM sherlock.post_dim_vw b
  INNER JOIN sdc_umami_published.umami_sqlmesh.post_task_umr_inference_latest_v1 t
          ON b.agora_post_id = t.post_uid
         AND t.rank = 1
  LEFT JOIN sdc_umami_published.umami_sqlmesh.post_role_umr_inference_latest_v1 r
         ON b.agora_post_id = r.post_uid
        AND r.rank = 1
  CROSS JOIN dates d
  WHERE b.post_date >= d.last_week_start
    AND b.post_date <= d.today
    AND b.is_qualified_post = TRUE
    AND t.label IS NOT NULL
),

-- ── Step 4: Top role per skill today (context column) ─────────
top_role_per_skill AS (
  SELECT
    task_label,
    MODE(role_label) AS top_role_label
  FROM raw_posts p
  CROSS JOIN dates d
  WHERE p.post_date = d.today
    AND p.role_label IS NOT NULL
  GROUP BY task_label
),

-- ── Step 5: Aggregate counts per skill per window ─────────────
skill_counts AS (
  SELECT
    p.task_label,

    COUNT(DISTINCT CASE
      WHEN p.post_date = d.today
      THEN p.post_id END)                             AS today_count,

    COUNT(DISTINCT CASE
      WHEN p.post_date = d.yesterday
      THEN p.post_id END)                             AS yesterday_count,

    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.this_week_start
      THEN p.post_id END)                             AS this_week_count,

    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.last_week_start
       AND p.post_date <  d.this_week_start
      THEN p.post_id END)                             AS last_week_count

  FROM raw_posts p
  CROSS JOIN dates d
  GROUP BY p.task_label
),

-- ── Step 6: Daily percentile ranks ────────────────────────────
today_ranked AS (
  SELECT
    task_label,
    today_count,
    ROUND(CUME_DIST() OVER (ORDER BY today_count) * 100, 2) AS today_rank_pct
  FROM skill_counts
  WHERE today_count > 0
),

yesterday_ranked AS (
  SELECT
    task_label,
    yesterday_count,
    ROUND(CUME_DIST() OVER (ORDER BY yesterday_count) * 100, 2) AS yesterday_rank_pct
  FROM skill_counts
  WHERE yesterday_count > 0
),

-- ── Step 7: Weekly percentile ranks ───────────────────────────
this_week_ranked AS (
  SELECT
    task_label,
    this_week_count,
    ROUND(CUME_DIST() OVER (ORDER BY this_week_count) * 100, 2) AS this_week_rank_pct
  FROM skill_counts
  WHERE this_week_count > 0
),

last_week_ranked AS (
  SELECT
    task_label,
    last_week_count,
    ROUND(CUME_DIST() OVER (ORDER BY last_week_count) * 100, 2) AS last_week_rank_pct
  FROM skill_counts
  WHERE last_week_count > 0
),

-- ── Step 8: Combine all rank comparisons ──────────────────────
rank_comparison AS (
  SELECT
    t.task_label,

    t.today_count,
    COALESCE(y.yesterday_count,     0)               AS yesterday_count,
    t.today_rank_pct,
    COALESCE(y.yesterday_rank_pct,  0)               AS yesterday_rank_pct,

    ROUND(
      t.today_rank_pct - COALESCE(y.yesterday_rank_pct, 0)
    , 2)                                             AS rank_shift_pp,

    COALESCE(tw.this_week_count,    0)               AS this_week_count,
    COALESCE(lw.last_week_count,    0)               AS last_week_count,
    COALESCE(tw.this_week_rank_pct, 0)               AS this_week_rank_pct,
    COALESCE(lw.last_week_rank_pct, 0)               AS last_week_rank_pct,

    ROUND(
      COALESCE(tw.this_week_rank_pct, 0)
      - COALESCE(lw.last_week_rank_pct, 0)
    , 2)                                             AS wow_rank_shift_pp

  FROM today_ranked t
  LEFT JOIN yesterday_ranked  y  ON y.task_label  = t.task_label
  LEFT JOIN this_week_ranked  tw ON tw.task_label = t.task_label
  LEFT JOIN last_week_ranked  lw ON lw.task_label = t.task_label
),

-- ── Step 9: Classify signal ───────────────────────────────────
skill_signals AS (
  SELECT
    r.task_label,

    COALESCE(ai.is_ai_skill,  'Non-AI-related')      AS is_ai_skill,
    COALESCE(ai.ai_share_pct, 0)                     AS ai_share_pct,
    tr.top_role_label,

    r.today_count,
    r.yesterday_count,
    r.today_rank_pct,
    r.yesterday_rank_pct,
    r.rank_shift_pp,

    r.this_week_count,
    r.last_week_count,
    r.this_week_rank_pct,
    r.last_week_rank_pct,
    r.wow_rank_shift_pp,

    CASE
      WHEN r.today_count < 5
        THEN 'Filtered'
      WHEN r.yesterday_count = 0 AND r.today_count >= 5
        THEN 'Emerging'
      WHEN r.rank_shift_pp > 10
        THEN 'Emerging'
      WHEN r.rank_shift_pp > 3
        THEN 'Growing'
      WHEN r.rank_shift_pp < -3
        THEN 'Declining'
      ELSE 'Stable'
    END                                               AS signal

  FROM rank_comparison r
  LEFT JOIN skill_ai_flag ai ON ai.task_label = r.task_label
  LEFT JOIN top_role_per_skill tr ON tr.task_label = r.task_label
)

-- ── Step 10: Final output ─────────────────────────────────────
SELECT
  task_label,
  is_ai_skill,
  ai_share_pct,
  top_role_label,
  -- Daily (signal driver)
  today_count,
  yesterday_count,
  today_rank_pct,
  yesterday_rank_pct,
  rank_shift_pp,
  -- Weekly (context)
  this_week_count,
  last_week_count,
  this_week_rank_pct,
  last_week_rank_pct,
  wow_rank_shift_pp,
  -- Signal
  signal
FROM skill_signals
-- WHERE signal != 'Filtered'
ORDER BY rank_shift_pp DESC NULLS LAST, today_count DESC
;