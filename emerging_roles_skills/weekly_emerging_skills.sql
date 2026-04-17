WITH
 
-- ── Step 1: Time window boundaries ───────────────────────────────────────────
dates AS (
  SELECT
    CURRENT_DATE                AS today,
    CURRENT_DATE - 7            AS this_week_start,   -- last 7 days
    CURRENT_DATE - 14           AS last_week_start,   -- 8-14 days ago
    CURRENT_DATE - 28           AS this_4w_start,     -- last 28 days
    CURRENT_DATE - 56           AS prior_4w_start,    -- 29-56 days ago
    '2026-01-01'::DATE   AS ai_window_start,
    '2026-03-28'::DATE     AS ai_window_end
),
 
-- ── Step 2: Skill-level AI flag from historical classification window ──────────
-- Uses 2026-01-01 to 2026-03-28 where AI classification data is available.
-- Computes % of posts per skill classified as AI-related.
-- Skills with >= {AI_THRESHOLD}% AI posts → labeled 'AI-related'.
-- This label is then applied to ALL current posts for that skill.
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
         AND t.rank = 1
  INNER JOIN sdc_umami_published.mds_prysma.ai_job_posts_classification ai
          ON ai.post_id = b.post_id
  CROSS JOIN dates d
  WHERE b.post_date >= d.ai_window_start
    AND b.post_date <= d.ai_window_end
    AND b.is_qualified_post = TRUE
    AND t.label IS NOT NULL
  GROUP BY t.label
),
 
-- ── Step 3: Current posts for frequency calculation ───────────────────────────
-- Joins both task and role tables so we can compute top_role_label per skill
raw_posts AS (
  SELECT
    b.post_id,
    b.post_date,
    t.label  AS task_label,
    r.label  AS role_label       -- for top_role_label computation in Step 6
  FROM sherlock.post_dim_vw b
  INNER JOIN sdc_umami_published.umami_sqlmesh.post_task_umr_inference_latest_v1 t
          ON b.agora_post_id = t.post_uid
         AND t.rank = 1
  LEFT JOIN sdc_umami_published.umami_sqlmesh.post_role_umr_inference_latest_v1 r
         ON b.agora_post_id = r.post_uid
        AND r.rank = 1
  CROSS JOIN dates d
  WHERE b.post_date >= d.prior_4w_start
    AND b.post_date <  d.today
    AND b.is_qualified_post = TRUE
    AND t.label IS NOT NULL
),
 
-- ── Step 4: Aggregate post counts per skill per window ────────────────────────
skill_window_counts AS (
  SELECT
    p.task_label,
 
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.this_week_start
      THEN p.post_id END)                             AS this_week_count,
 
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.last_week_start
       AND p.post_date <  d.this_week_start
      THEN p.post_id END)                             AS last_week_count,
 
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.this_4w_start
      THEN p.post_id END)                             AS this_4w_count,
 
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.prior_4w_start
       AND p.post_date <  d.this_4w_start
      THEN p.post_id END)                             AS prior_4w_count
 
  FROM raw_posts p
  CROSS JOIN dates d
  GROUP BY p.task_label
),
 
-- ── Step 5: Top role per skill this week ──────────────────────────────────────
-- Most common role_label appearing alongside each skill in this week's posts
top_role_per_skill AS (
  SELECT
    task_label,
    MODE(role_label) AS top_role_label
  FROM raw_posts p
  CROSS JOIN dates d
  WHERE p.post_date >= d.this_week_start
    AND p.role_label IS NOT NULL
  GROUP BY task_label
),
 
-- ── Step 6: Compute WoW%, 4W averages, T4Wo4W% ───────────────────────────────
skill_metrics AS (
  SELECT
    w.task_label,
    w.this_week_count,
    w.last_week_count,
    w.this_4w_count,
    w.prior_4w_count,
 
    -- WoW %: this week vs last week
    CASE
      WHEN w.last_week_count = 0 THEN NULL   -- new skill, handled in signal step
      ELSE ROUND(
        (w.this_week_count - w.last_week_count)
        / w.last_week_count * 100, 1)
    END                                               AS wow_pct,
 
    -- This 4W weekly average
    ROUND(w.this_4w_count / 4.0, 1)                  AS this_4w_avg,
 
    -- Prior 4W weekly average
    ROUND(w.prior_4w_count / 4.0, 1)                 AS prior_4w_avg,
 
    -- T4Wo4W %: rolling 28d vs prior 28d
    CASE
      WHEN w.prior_4w_count = 0 THEN NULL
      ELSE ROUND(
        (w.this_4w_count - w.prior_4w_count)
        / w.prior_4w_count * 100, 1)
    END                                               AS t4wo4w_pct
 
  FROM skill_window_counts w
),
 
-- ── Step 7: Classify signal (WoW-based, priority order) ──────────────────────
-- All skills kept — no volume filter here.
-- Claude analyzer excludes signal = 'Filtered' from narrative and HTML.
skill_signals AS (
  SELECT
    m.task_label,
 
    -- AI flag from historical window (defaults to Non-AI-related if not in window)
    COALESCE(ai.is_ai_skill,   'Non-AI-related')     AS is_ai_skill,
    COALESCE(ai.ai_share_pct,  0)                    AS ai_share_pct,
 
    -- Top role associated with this skill this week
    tr.top_role_label,
 
    m.this_week_count,
    m.last_week_count,
    m.wow_pct,
    m.this_4w_avg,
    m.prior_4w_avg,
    m.t4wo4w_pct,
 
    CASE
      -- Filtered: fewer than 10 posts this week (too noisy for narrative)
      WHEN m.this_week_count < 10
        THEN 'Filtered'
      -- New skill: no posts last week, >= 10 this week
      WHEN m.last_week_count = 0 AND m.this_week_count >= 10
        THEN 'Emerging'
      -- Emerging: WoW > +20%
      WHEN m.wow_pct > 20
        THEN 'Emerging'
      -- Growing: WoW +5% to +20%
      WHEN m.wow_pct > 5
        THEN 'Growing'
      -- Declining: WoW < -5%
      WHEN m.wow_pct < -5
        THEN 'Declining'
      -- Stable: catch-all (-5% to +5%)
      ELSE 'Stable'
    END                                               AS signal
 
  FROM skill_metrics m
  LEFT JOIN skill_ai_flag  ai ON ai.task_label  = m.task_label
  LEFT JOIN top_role_per_skill tr ON tr.task_label = m.task_label
)
 
-- ── Step 8: Final output ──────────────────────────────────────────────────────
SELECT
  task_label,
  is_ai_skill,
  ai_share_pct,
  top_role_label,
  this_week_count,
  last_week_count,
  wow_pct,
  this_4w_avg,
  prior_4w_avg,
  t4wo4w_pct,
  signal
FROM skill_signals
ORDER BY wow_pct DESC NULLS LAST, this_week_count DESC
;



WITH
 
-- ── Step 1: Time window boundaries ───────────────────────────────────────────
dates AS (
  SELECT
    CURRENT_DATE                AS today,
    CURRENT_DATE - 7            AS this_week_start,
    CURRENT_DATE - 14           AS last_week_start,
    CURRENT_DATE - 28           AS this_4w_start,
    CURRENT_DATE - 56           AS prior_4w_start,
    '2026-01-01'::DATE          AS ai_window_start,
    '2026-03-28'::DATE          AS ai_window_end
),
 
-- ── Step 2: Skill-level AI flag from historical classification window ─────────
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
         AND t.rank = 1
  INNER JOIN sdc_umami_published.mds_prysma.ai_job_posts_classification ai
          ON ai.post_id = b.post_id
  CROSS JOIN dates d
  WHERE b.post_date >= d.ai_window_start
    AND b.post_date <= d.ai_window_end
    AND b.is_qualified_post = TRUE
    AND t.label IS NOT NULL
  GROUP BY t.label
),
 
-- ── Step 3: Current posts for frequency calculation ───────────────────────────
raw_posts AS (
  SELECT
    b.post_id,
    b.post_date,
    t.label  AS task_label,
    r.label  AS role_label
  FROM sherlock.post_dim_vw b
  INNER JOIN sdc_umami_published.umami_sqlmesh.post_task_umr_inference_latest_v1 t
          ON b.agora_post_id = t.post_uid
         AND t.rank = 1
  LEFT JOIN sdc_umami_published.umami_sqlmesh.post_role_umr_inference_latest_v1 r
         ON b.agora_post_id = r.post_uid
        AND r.rank <= 5
        AND r.score >= 0.6
  CROSS JOIN dates d
  WHERE b.post_date >= d.prior_4w_start
    AND b.post_date <  d.today
    AND b.is_qualified_post = TRUE
    AND t.label IS NOT NULL
),
 
-- ── Step 4: Aggregate post counts per skill per window ────────────────────────
skill_window_counts AS (
  SELECT
    p.task_label,
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.this_week_start
      THEN p.post_id END)                             AS this_week_count,
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.last_week_start
       AND p.post_date <  d.this_week_start
      THEN p.post_id END)                             AS last_week_count,
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.this_4w_start
      THEN p.post_id END)                             AS this_4w_count,
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.prior_4w_start
       AND p.post_date <  d.this_4w_start
      THEN p.post_id END)                             AS prior_4w_count
  FROM raw_posts p
  CROSS JOIN dates d
  GROUP BY p.task_label
),
 
-- ── Step 5: Top role per skill this week ──────────────────────────────────────
top_role_per_skill AS (
  SELECT
    task_label,
    MODE(role_label) AS top_role_label
  FROM raw_posts p
  CROSS JOIN dates d
  WHERE p.post_date >= d.this_week_start
    AND p.role_label IS NOT NULL
  GROUP BY task_label
),
 
-- ── Step 6: Compute WoW%, 4W averages, T4Wo4W% ───────────────────────────────
skill_metrics AS (
  SELECT
    w.task_label,
    w.this_week_count,
    w.last_week_count,
    w.this_4w_count,
    w.prior_4w_count,
    CASE
      WHEN w.last_week_count = 0 THEN NULL
      ELSE ROUND(
        (w.this_week_count - w.last_week_count)
        / w.last_week_count * 100, 1)
    END                                               AS wow_pct,
    ROUND(w.this_4w_count / 4.0, 1)                  AS this_4w_avg,
    ROUND(w.prior_4w_count / 4.0, 1)                 AS prior_4w_avg,
    CASE
      WHEN w.prior_4w_count = 0 THEN NULL
      ELSE ROUND(
        (w.this_4w_count - w.prior_4w_count)
        / w.prior_4w_count * 100, 1)
    END                                               AS t4wo4w_pct
  FROM skill_window_counts w
),

-- ── Step 7: NEW — Skill-level fill & completion metrics ───────────────────────
-- Bridges contract_fact_vw → post_dim_vw → task inference to get skill labels.
-- Fill:      FIRST_HIRE_TS IS NOT NULL
-- Completed: COALESCE(extended_end_date, end_date) < CURRENT_DATE
-- 2026 filter applied to the hire/end date itself, not post_date.
skill_contract_metrics AS (
  SELECT
    t.label                                           AS task_label,

    -- # jobs filled in 2026
    COUNT(DISTINCT CASE
      WHEN b.first_hire_ts IS NOT NULL
       AND YEAR(b.post_date) = 2026
      THEN b.post_id END)                             AS jobs_filled_2026,

    -- # jobs filled ever
    COUNT(DISTINCT CASE
      WHEN b.first_hire_ts IS NOT NULL
      THEN b.post_id END)                             AS jobs_filled_ever,

    -- # jobs completed in 2026
    COUNT(DISTINCT CASE
      WHEN COALESCE(c.end_date, c.extended_end_date) < CURRENT_DATE
       AND YEAR(b.post_date) = 2026
      THEN b.post_id END)                             AS jobs_completed_2026,

    -- # jobs completed ever
    COUNT(DISTINCT CASE
      WHEN COALESCE(c.end_date, c.extended_end_date) < CURRENT_DATE
      THEN b.post_id END)                             AS jobs_completed_ever

  FROM shasta_sdc_published.sherlock.contract_fact_vw c
  INNER JOIN sherlock.post_dim_vw b
          ON b.post_id = c.post_id
  INNER JOIN sdc_umami_published.umami_sqlmesh.post_task_umr_inference_latest_v1 t
          ON b.agora_post_id = t.post_uid
          AND t.rank <= 5
          AND t.score >= 0.6
         AND t.label IS NOT NULL
  GROUP BY t.label
),
 
-- ── Step 8: Classify signal ───────────────────────────────────────────────────
skill_signals AS (
  SELECT
    m.task_label,
    COALESCE(ai.is_ai_skill,  'Non-AI-related')      AS is_ai_skill,
    COALESCE(ai.ai_share_pct, 0)                     AS ai_share_pct,
    tr.top_role_label,
    m.this_week_count,
    m.last_week_count,
    m.wow_pct,
    m.this_4w_avg,
    m.prior_4w_avg,
    m.t4wo4w_pct,

    -- New metrics (0 if skill has posts but no matching contracts)
    COALESCE(cm.jobs_filled_2026,    0)              AS jobs_filled_2026,
    COALESCE(cm.jobs_filled_ever,    0)              AS jobs_filled_ever,
    COALESCE(cm.jobs_completed_2026, 0)              AS jobs_completed_2026,
    COALESCE(cm.jobs_completed_ever, 0)              AS jobs_completed_ever,

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
    END                                               AS signal
  FROM skill_metrics m
  LEFT JOIN skill_ai_flag ai
         ON ai.task_label = m.task_label
  LEFT JOIN top_role_per_skill tr
         ON tr.task_label = m.task_label
  LEFT JOIN skill_contract_metrics cm               -- LEFT so skills with no contracts still appear
         ON cm.task_label = m.task_label
)
 
-- ── Step 9: Final output ──────────────────────────────────────────────────────
SELECT
  task_label,
  is_ai_skill,
  ai_share_pct,
  top_role_label,
  this_week_count,
  last_week_count,
  wow_pct,
  this_4w_avg,
  prior_4w_avg,
  t4wo4w_pct,
  jobs_filled_2026,
  jobs_filled_ever,
  jobs_completed_2026,
  jobs_completed_ever,
  signal
FROM skill_signals
-- WHERE signal != 'Filtered'
ORDER BY wow_pct DESC NULLS LAST, this_week_count DESC
;