WITH

-- ── Step 1: Define time window boundaries ────────────────────
dates AS (
  SELECT
    CURRENT_DATE                          AS today,
    CURRENT_DATE - 7                      AS this_week_start,
    CURRENT_DATE - 14                     AS last_week_start,
    CURRENT_DATE - 28                     AS this_4w_start,
    CURRENT_DATE - 56                     AS prior_4w_start,
    '2026-01-01'::DATE                    AS ai_window_start,
    '2026-03-28'::DATE                    AS ai_window_end
),

-- ── Step 2: Build role-level AI flag from historical window ───
role_ai_flag AS (
  SELECT
    r.label                                         AS role_label,
    ROUND(
      SUM(CASE
        WHEN ai.predicted_classification = 'AI-related'
        THEN 1 ELSE 0
      END) / COUNT(DISTINCT b.post_id) * 100, 1)   AS ai_share_pct,
    MODE(
      CASE
        WHEN ai.predicted_classification = 'AI-related'
        THEN ai.ai_category END)                    AS top_ai_category,
    CASE
      WHEN ROUND(
        SUM(CASE
          WHEN ai.predicted_classification = 'AI-related'
          THEN 1 ELSE 0
        END) / COUNT(DISTINCT b.post_id) * 100, 1) >= 40
      THEN 'AI-related'
      ELSE 'Non-AI-related'
    END                                             AS is_ai_role
  FROM sherlock.post_dim_vw b
  INNER JOIN sdc_umami_published.umami_sqlmesh.post_role_umr_inference_latest_v1 r
          ON b.agora_post_id = r.post_uid
         AND r.rank <= 5
         AND r.score >= 0.6
  INNER JOIN sdc_umami_published.mds_prysma.ai_job_posts_classification ai
          ON ai.post_id = b.post_id
  CROSS JOIN dates d
  WHERE b.post_date >= d.ai_window_start
    AND b.post_date <= d.ai_window_end
    AND b.is_qualified_post = TRUE
    AND r.label IS NOT NULL
  GROUP BY r.label
),

-- ── Step 3: Pull current posts for frequency calculation ──────
raw_posts AS (
  SELECT
    b.post_id,
    b.post_date,
    r.label                               AS role_label
  FROM sherlock.post_dim_vw b
  INNER JOIN sdc_umami_published.umami_sqlmesh.post_role_umr_inference_latest_v1 r
          ON b.agora_post_id = r.post_uid
         AND r.rank = 1
  CROSS JOIN dates d
  WHERE b.post_date >= d.prior_4w_start
    AND b.post_date <  d.today
    AND b.is_qualified_post = TRUE
    AND r.label IS NOT NULL
),

-- ── Step 4: Aggregate post counts per role per window ─────────
role_window_counts AS (
  SELECT
    p.role_label,
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.this_week_start
      THEN p.post_id END)                           AS this_week_count,
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.last_week_start
       AND p.post_date <  d.this_week_start
      THEN p.post_id END)                           AS last_week_count,
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.this_4w_start
      THEN p.post_id END)                           AS this_4w_count,
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.prior_4w_start
       AND p.post_date <  d.this_4w_start
      THEN p.post_id END)                           AS prior_4w_count
  FROM raw_posts p
  CROSS JOIN dates d
  GROUP BY p.role_label
),

-- ── Step 5: Compute derived metrics ──────────────────────────
role_metrics AS (
  SELECT
    w.role_label,
    w.this_week_count,
    w.last_week_count,
    w.this_4w_count,
    w.prior_4w_count,
    CASE
      WHEN w.last_week_count = 0 THEN NULL
      ELSE ROUND(
        (w.this_week_count - w.last_week_count)
        / w.last_week_count * 100, 1)
    END                                             AS wow_pct,
    ROUND(w.this_4w_count / 4.0, 1)                AS this_4w_avg,
    ROUND(w.prior_4w_count / 4.0, 1)               AS prior_4w_avg,
    CASE
      WHEN w.prior_4w_count = 0 THEN NULL
      ELSE ROUND(
        (w.this_4w_count - w.prior_4w_count)
        / w.prior_4w_count * 100, 1)
    END                                             AS t4wo4w_pct
  FROM role_window_counts w
),

-- ── Step 5b: 4-week percentile ranks (mirroring daily pattern) ─
-- Ranked independently per window — self-normalizing
this_4w_ranked AS (
  SELECT
    role_label,
    this_4w_count,
    ROUND(CUME_DIST() OVER (ORDER BY this_4w_count) * 100, 2) AS this_4w_rank_pct
  FROM role_metrics
  WHERE this_4w_count > 0
),

prior_4w_ranked AS (
  SELECT
    role_label,
    prior_4w_count,
    ROUND(CUME_DIST() OVER (ORDER BY prior_4w_count) * 100, 2) AS prior_4w_rank_pct
  FROM role_metrics
  WHERE prior_4w_count > 0
),

-- ── Step 5c: Join 4-week ranks and compute rank shift ─────────
rank_4w_comparison AS (
  SELECT
    t.role_label,
    t.this_4w_rank_pct,
    COALESCE(p.prior_4w_rank_pct, 0)               AS prior_4w_rank_pct,
    ROUND(
      t.this_4w_rank_pct - COALESCE(p.prior_4w_rank_pct, 0)
    , 2)                                            AS t4wo4w_rank_shift_pp
  FROM this_4w_ranked t
  LEFT JOIN prior_4w_ranked p ON p.role_label = t.role_label
),

-- ── Step 6: Role-level fill & completion metrics ──────────────
role_contract_metrics AS (
  SELECT
    r.label                                         AS role_label,
    COUNT(DISTINCT CASE
      WHEN b.first_hire_ts IS NOT NULL
       AND YEAR(b.post_date) = 2026
      THEN b.post_id END)                           AS jobs_filled_2026,
    COUNT(DISTINCT CASE
      WHEN b.first_hire_ts IS NOT NULL
      THEN b.post_id END)                           AS jobs_filled_ever,
    COUNT(DISTINCT CASE
      WHEN COALESCE(c.end_date, c.extended_end_date) < CURRENT_DATE
       AND YEAR(b.post_date) = 2026
      THEN c.post_id END)                           AS jobs_completed_2026,
    COUNT(DISTINCT CASE
      WHEN COALESCE(c.end_date, c.extended_end_date) < CURRENT_DATE
      THEN c.post_id END)                           AS jobs_completed_ever
  FROM shasta_sdc_published.sherlock.contract_fact_vw c
  INNER JOIN sherlock.post_dim_vw b
          ON b.post_id = c.post_id
  INNER JOIN sdc_umami_published.umami_sqlmesh.post_role_umr_inference_latest_v1 r
          ON b.agora_post_id = r.post_uid
         AND r.rank <= 5
         AND r.score >= 0.6
         AND r.label IS NOT NULL
  GROUP BY r.label
),

-- ── Step 7: Classify signal (WoW only, priority order) ────────
role_signals AS (
  SELECT
    m.role_label,
    COALESCE(ai.is_ai_role, 'Non-AI-related')      AS is_ai_role,
    COALESCE(ai.ai_share_pct, 0)                   AS ai_share_pct,
    ai.top_ai_category,
    m.this_week_count,
    m.last_week_count,
    m.wow_pct,
    m.this_4w_avg,
    m.prior_4w_avg,
    m.t4wo4w_pct,

    -- 4W percentile ranks (context columns, do not drive signal)
    COALESCE(r4.this_4w_rank_pct,       0)         AS this_4w_rank_pct,
    COALESCE(r4.prior_4w_rank_pct,      0)         AS prior_4w_rank_pct,
    COALESCE(r4.t4wo4w_rank_shift_pp,   0)         AS t4wo4w_rank_shift_pp,

    COALESCE(cm.jobs_filled_2026,    0)            AS jobs_filled_2026,
    COALESCE(cm.jobs_filled_ever,    0)            AS jobs_filled_ever,
    COALESCE(cm.jobs_completed_2026, 0)            AS jobs_completed_2026,
    COALESCE(cm.jobs_completed_ever, 0)            AS jobs_completed_ever,

    CASE
      WHEN m.this_week_count < 10
        THEN 'Filtered'
      WHEN m.last_week_count = 0 AND m.this_week_count >= 10
        THEN 'Emerging'
      WHEN m.wow_pct > 20
        THEN 'Emerging'
      WHEN m.wow_pct > 5
        THEN 'Growing'
      WHEN m.wow_pct < -5
        THEN 'Declining'
      ELSE 'Stable'
    END                                             AS signal

  FROM role_metrics m
  LEFT JOIN role_ai_flag ai
         ON ai.role_label = m.role_label
  LEFT JOIN rank_4w_comparison r4
         ON r4.role_label = m.role_label
  LEFT JOIN role_contract_metrics cm
         ON cm.role_label = m.role_label
)

-- ── Step 8: Final output ──────────────────────────────────────
SELECT
  role_label,
  is_ai_role,
  ai_share_pct,
  top_ai_category,
  this_week_count,
  last_week_count,
  wow_pct,
  this_4w_avg,
  prior_4w_avg,
  t4wo4w_pct,
  -- 4W percentile ranks (context)
  this_4w_rank_pct,
  prior_4w_rank_pct,
  t4wo4w_rank_shift_pp,
  jobs_filled_2026,
  jobs_filled_ever,
  jobs_completed_2026,
  jobs_completed_ever,
  signal
FROM role_signals
-- WHERE signal != 'Filtered'
ORDER BY wow_pct DESC NULLS LAST, this_week_count DESC
;