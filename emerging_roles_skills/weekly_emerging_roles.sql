-- ============================================================
-- Emerging Roles & Skills — Role Frequency + Signal + AI Flag
-- Version 3: Role-level AI flag derived from historical window
--
-- AI classification logic:
--   Use posts from 2026-01-01 to 2026-03-28 (where AI data exists)
--   Compute % of posts per role classified as AI-related
--   If >= 50% → role is labeled AI-related
--   Apply that role label to ALL current posts (no direct join needed)
--
-- Output columns:
--   role_label, is_ai_role, ai_share_pct, top_ai_category,
--   this_week_count, last_week_count, wow_pct,
--   this_4w_avg, prior_4w_avg, t4wo4w_pct, signal
-- ============================================================

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
-- Uses only posts where AI classification data is available
-- Computes AI share per role, then applies 50% threshold
role_ai_flag AS (
  SELECT
    r.label                                         AS role_label,

    -- % of posts for this role classified as AI-related
    ROUND(
      SUM(CASE
        WHEN ai.predicted_classification = 'AI-related'
        THEN 1 ELSE 0
      END) / COUNT(DISTINCT b.post_id) * 100, 1)   AS ai_share_pct,

    -- Most common AI category for this role in the window
    MODE(
      CASE
        WHEN ai.predicted_classification = 'AI-related'
        THEN ai.ai_category END)                    AS top_ai_category,

    -- Role-level AI label based on 50% threshold
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
-- No AI join needed here — role label carries the AI flag
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

    -- This week: last 7 days
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.this_week_start
      THEN p.post_id END)                           AS this_week_count,

    -- Last week: 8-14 days ago
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.last_week_start
       AND p.post_date <  d.this_week_start
      THEN p.post_id END)                           AS last_week_count,

    -- This 4W: last 28 days
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.this_4w_start
      THEN p.post_id END)                           AS this_4w_count,

    -- Prior 4W: 29-56 days ago
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

    -- WoW %
    CASE
      WHEN w.last_week_count = 0 THEN NULL
      ELSE ROUND(
        (w.this_week_count - w.last_week_count)
        / w.last_week_count * 100, 1)
    END                                             AS wow_pct,

    -- This 4W weekly average
    ROUND(w.this_4w_count / 4.0, 1)                AS this_4w_avg,

    -- Prior 4W weekly average
    ROUND(w.prior_4w_count / 4.0, 1)               AS prior_4w_avg,

    -- T4Wo4W %
    CASE
      WHEN w.prior_4w_count = 0 THEN NULL
      ELSE ROUND(
        (w.this_4w_count - w.prior_4w_count)
        / w.prior_4w_count * 100, 1)
    END                                             AS t4wo4w_pct

  FROM role_window_counts w
),

-- ── Step 6: Classify signal (WoW only, priority order) ───────
role_signals AS (
  SELECT
    m.role_label,

    -- AI dimensions from historical window (NULL if role not in AI window)
    COALESCE(ai.is_ai_role, 'Non-AI-related')      AS is_ai_role,
    COALESCE(ai.ai_share_pct, 0)                   AS ai_share_pct,
    ai.top_ai_category,

    m.this_week_count,
    m.last_week_count,
    m.wow_pct,
    m.this_4w_avg,
    m.prior_4w_avg,
    m.t4wo4w_pct,

    CASE
      -- Filter: fewer than 10 posts this week
      WHEN m.this_week_count < 10
        THEN 'Filtered'
      -- New role: no posts last week, >= 10 this week
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
      -- Stable: catch-all
      ELSE 'Stable'
    END                                             AS signal

  FROM role_metrics m
  LEFT JOIN role_ai_flag ai
         ON ai.role_label = m.role_label
)

-- ── Step 7: Final output ──────────────────────────────────────
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
  signal
FROM role_signals
-- WHERE signal != 'Filtered'
ORDER BY wow_pct DESC NULLS LAST, this_week_count DESC
;

-- ── QC checks ─────────────────────────────────────────────────

-- 1. Validate AI flag coverage
--    How many roles were found in the AI classification window?
--    Roles not found default to Non-AI-related
-- SELECT
--   is_ai_role,
--   COUNT(*)            AS role_count,
--   SUM(this_week_count) AS total_posts_this_week
-- FROM role_signals
-- WHERE signal != 'Filtered'
-- GROUP BY is_ai_role;

-- 2. Signal distribution
-- SELECT signal, COUNT(*) AS role_count
-- FROM role_signals
-- WHERE signal != 'Filtered'
-- GROUP BY signal
-- ORDER BY role_count DESC;

-- 3. AI vs non-AI signal breakdown
-- SELECT is_ai_role, signal, COUNT(*) AS role_count
-- FROM role_signals
-- WHERE signal != 'Filtered'
-- GROUP BY is_ai_role, signal
-- ORDER BY is_ai_role DESC, role_count DESC;

-- 4. Top AI-related emerging roles
-- SELECT role_label, ai_share_pct, top_ai_category,
--        this_week_count, wow_pct, signal
-- FROM role_signals
-- WHERE is_ai_role = 'AI-related'
--   AND signal = 'Emerging'
-- ORDER BY wow_pct DESC;

-- 5. AI share distribution — validate 50% threshold
-- SELECT
--   CASE
--     WHEN ai_share_pct = 0   THEN '0%'
--     WHEN ai_share_pct < 25  THEN '1-24%'
--     WHEN ai_share_pct < 50  THEN '25-49%'
--     WHEN ai_share_pct < 75  THEN '50-74%'
--     ELSE '75-100%'
--   END                       AS ai_share_bucket,
--   COUNT(*)                  AS role_count
-- FROM role_signals
-- WHERE signal != 'Filtered'
-- GROUP BY 1
-- ORDER BY 1;

-- 6. Spot check: roles in AI window vs roles in current period
--    Any roles in current week that weren't in the AI window?
-- SELECT
--   COUNT(DISTINCT m.role_label)                    AS roles_this_week,
--   COUNT(DISTINCT ai.role_label)                   AS roles_in_ai_window,
--   COUNT(DISTINCT CASE
--     WHEN ai.role_label IS NULL
--     THEN m.role_label END)                        AS roles_missing_ai_flag
-- FROM role_metrics m
-- LEFT JOIN role_ai_flag ai ON ai.role_label = m.role_label
-- WHERE m.this_week_count >= 10;



select min(post_date), max(post_date)from sdc_umami_published.mds_prysma.ai_job_posts_classification limit 101;

select min(b.post_date), max(b.post_date), count(distinct label)  
from sherlock.post_dim_vw b
join sdc_umami_published.umami_sqlmesh.post_role_umr_inference_latest_v1 r
    on b.agora_post_id = r.post_uid
         AND r.rank = 1
where b.post_date >= '2026-03-01' and b.post_date <= '2026-03-28'
and b.is_qualified_post;


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

-- ── Step 6: NEW — Role-level fill & completion metrics ────────
-- Joins contract data back through post_dim_vw to get role labels.
-- Fill:      FIRST_HIRE_TS IS NOT NULL
-- Completed: COALESCE(extended_end_date, end_date) < CURRENT_DATE
role_contract_metrics AS (
  SELECT
    r.label                                         AS role_label,

    -- # jobs filled in 2026 (FIRST_HIRE_TS not null, hired in 2026)
    COUNT(DISTINCT CASE
      WHEN b.first_hire_ts IS NOT NULL
       AND YEAR(b.post_date) = 2026
      THEN b.post_id END)                           AS jobs_filled_2026,

    -- # jobs filled ever (FIRST_HIRE_TS not null)
    COUNT(DISTINCT CASE
      WHEN b.first_hire_ts IS NOT NULL
      THEN b.post_id END)                           AS jobs_filled_ever,

    -- # jobs completed in 2026
    -- Completed = end or extended_end is in the past, and it happened in 2026
    COUNT(DISTINCT CASE
      WHEN COALESCE(c.end_date, c.extended_end_date) < CURRENT_DATE
       AND YEAR(b.post_date) = 2026
      THEN c.post_id END)                           AS jobs_completed_2026,

    -- # jobs completed ever
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

-- ── Step 7: Classify signal (WoW only, priority order) ───────
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

    -- New metrics (NULL-safe if a role has posts but no contracts)
    COALESCE(cm.jobs_filled_2026, 0)               AS jobs_filled_2026,
    COALESCE(cm.jobs_filled_ever, 0)               AS jobs_filled_ever,
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
  LEFT JOIN role_contract_metrics cm              -- LEFT so roles with no contracts still appear
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
  jobs_filled_2026,
  jobs_filled_ever,
  jobs_completed_2026,
  jobs_completed_ever,
  signal
FROM role_signals
-- WHERE signal != 'Filtered'
ORDER BY wow_pct DESC NULLS LAST, this_week_count DESC
;

select distinct OUTCOME from SHASTA_SDC_PUBLISHED.SHERLOCK.CONTRACT_FACT_VW; -- good, bad, null