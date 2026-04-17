-- ============================================================
-- Emerging Roles & Skills — Daily Role Signal
-- Method: Percentile rank shift (today vs yesterday)
--
-- Signal driver:  daily rank shift (today vs yesterday)
-- Context column: WoW rank shift (this week vs last week)
--
-- Self-normalizing — eliminates weekday/weekend volume bias.
-- A role moving from 60th → 80th percentile = +20pp regardless
-- of whether today is Monday or Sunday.
--
-- Output columns:
--   role_label, is_ai_role, ai_share_pct, top_ai_category
--   today_count, yesterday_count
--   today_rank_pct, yesterday_rank_pct, rank_shift_pp   ← signal driver
--   this_week_count, last_week_count
--   this_week_rank_pct, last_week_rank_pct, wow_rank_shift_pp  ← context
--   signal
-- ============================================================

WITH

-- ── Step 1: Time window boundaries ───────────────────────────
dates AS (
  SELECT
    CURRENT_DATE - 2               AS today,
    CURRENT_DATE - 3            AS yesterday,
    CURRENT_DATE - 7            AS this_week_start,   -- last 7 days
    CURRENT_DATE - 17           AS last_week_start,   -- 8-14 days ago
    '2026-01-01'::DATE          AS ai_window_start,
    '2026-03-28'::DATE          AS ai_window_end
),

-- ── Step 2: Role-level AI flag from historical window ─────────
role_ai_flag AS (
  SELECT
    r.label                                           AS role_label,

    ROUND(
      SUM(CASE
        WHEN ai.predicted_classification = 'AI-related'
        THEN 1 ELSE 0
      END) / COUNT(DISTINCT b.post_id) * 100, 1)     AS ai_share_pct,

    MODE(CASE
      WHEN ai.predicted_classification = 'AI-related'
      THEN ai.ai_category END)                        AS top_ai_category,

    CASE
      WHEN ROUND(
        SUM(CASE
          WHEN ai.predicted_classification = 'AI-related'
          THEN 1 ELSE 0
        END) / COUNT(DISTINCT b.post_id) * 100, 1) >= 40
      THEN 'AI-related'
      ELSE 'Non-AI-related'
    END                                               AS is_ai_role

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

-- ── Step 3: Raw posts — covers daily + weekly windows ─────────
-- 14-day lookback covers today, yesterday, this week, last week
raw_posts AS (
  SELECT
    b.post_id,
    b.post_date,
    r.label AS role_label
  FROM sherlock.post_dim_vw b
  INNER JOIN sdc_umami_published.umami_sqlmesh.post_role_umr_inference_latest_v1 r
          ON b.agora_post_id = r.post_uid
         AND r.rank = 1
  CROSS JOIN dates d
  WHERE b.post_date >= d.last_week_start
    AND b.post_date <=  d.today
    AND b.is_qualified_post = TRUE
    AND r.label IS NOT NULL
),

-- ── Step 4: Aggregate counts per role per window ──────────────
role_counts AS (
  SELECT
    p.role_label,

    -- Daily windows
    COUNT(DISTINCT CASE
      WHEN p.post_date = d.today
      THEN p.post_id END)                             AS today_count,

    COUNT(DISTINCT CASE
      WHEN p.post_date = d.yesterday
      THEN p.post_id END)                             AS yesterday_count,

    -- Weekly windows
    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.this_week_start
      THEN p.post_id END)                             AS this_week_count,

    COUNT(DISTINCT CASE
      WHEN p.post_date >= d.last_week_start
       AND p.post_date <  d.this_week_start
      THEN p.post_id END)                             AS last_week_count

  FROM raw_posts p
  CROSS JOIN dates d
  GROUP BY p.role_label
),

-- ── Step 5: Daily percentile ranks ───────────────────────────
-- Ranked independently per day — self-normalizing
today_ranked AS (
  SELECT
    role_label,
    today_count,
    ROUND(CUME_DIST() OVER (ORDER BY today_count) * 100, 2) AS today_rank_pct
  FROM role_counts
  WHERE today_count > 0
),

yesterday_ranked AS (
  SELECT
    role_label,
    yesterday_count,
    ROUND(CUME_DIST() OVER (ORDER BY yesterday_count) * 100, 2) AS yesterday_rank_pct
  FROM role_counts
  WHERE yesterday_count > 0
),

-- ── Step 6: Weekly percentile ranks ───────────────────────────
-- Ranked independently per week — self-normalizing
this_week_ranked AS (
  SELECT
    role_label,
    this_week_count,
    ROUND(CUME_DIST() OVER (ORDER BY this_week_count) * 100, 2) AS this_week_rank_pct
  FROM role_counts
  WHERE this_week_count > 0
),

last_week_ranked AS (
  SELECT
    role_label,
    last_week_count,
    ROUND(CUME_DIST() OVER (ORDER BY last_week_count) * 100, 2) AS last_week_rank_pct
  FROM role_counts
  WHERE last_week_count > 0
),

-- ── Step 7: Combine all rank comparisons ─────────────────────
-- LEFT JOIN from today → all other windows
-- Roles with no prior data get COALESCE(0)
rank_comparison AS (
  SELECT
    t.role_label,

    -- Daily counts and ranks
    t.today_count,
    COALESCE(y.yesterday_count,    0)                AS yesterday_count,
    t.today_rank_pct,
    COALESCE(y.yesterday_rank_pct, 0)                AS yesterday_rank_pct,

    -- Daily rank shift — PRIMARY SIGNAL DRIVER
    ROUND(
      t.today_rank_pct - COALESCE(y.yesterday_rank_pct, 0)
    , 2)                                             AS rank_shift_pp,

    -- Weekly counts and ranks — CONTEXT ONLY
    COALESCE(tw.this_week_count,   0)                AS this_week_count,
    COALESCE(lw.last_week_count,   0)                AS last_week_count,
    COALESCE(tw.this_week_rank_pct, 0)               AS this_week_rank_pct,
    COALESCE(lw.last_week_rank_pct, 0)               AS last_week_rank_pct,

    -- WoW rank shift — CONTEXT ONLY (does not drive signal)
    ROUND(
      COALESCE(tw.this_week_rank_pct, 0)
      - COALESCE(lw.last_week_rank_pct, 0)
    , 2)                                             AS wow_rank_shift_pp

  FROM today_ranked t
  LEFT JOIN yesterday_ranked  y  ON y.role_label  = t.role_label
  LEFT JOIN this_week_ranked  tw ON tw.role_label = t.role_label
  LEFT JOIN last_week_ranked  lw ON lw.role_label = t.role_label
),

-- ── Step 8: Classify signal (daily rank shift only) ───────────
-- WoW rank shift is a display column — does NOT affect signal.
-- Volume filter uses today_count for stability.
role_signals AS (
  SELECT
    r.role_label,

    COALESCE(ai.is_ai_role,   'Non-AI-related')      AS is_ai_role,
    COALESCE(ai.ai_share_pct, 0)                     AS ai_share_pct,
    ai.top_ai_category,

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
      -- Filtered: fewer than 5 posts today
      WHEN r.today_count < 5
        THEN 'Filtered'
      -- New role: no posts yesterday, >= 5 today
      WHEN r.yesterday_count = 0 AND r.today_count >= 5
        THEN 'Emerging'
      -- Emerging: daily rank shifted up > +10pp
      WHEN r.rank_shift_pp > 10
        THEN 'Emerging'
      -- Growing: daily rank shifted up +3pp to +10pp
      WHEN r.rank_shift_pp > 3
        THEN 'Growing'
      -- Declining: daily rank shifted down > -3pp
      WHEN r.rank_shift_pp < -3
        THEN 'Declining'
      -- Stable: catch-all (-3pp to +3pp)
      ELSE 'Stable'
    END                                               AS signal

  FROM rank_comparison r
  LEFT JOIN role_ai_flag ai ON ai.role_label = r.role_label
)

-- ── Step 9: Final output ──────────────────────────────────────
SELECT
  role_label,
  is_ai_role,
  ai_share_pct,
  top_ai_category,
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
FROM role_signals
-- WHERE signal != 'Filtered'
ORDER BY rank_shift_pp DESC NULLS LAST, today_count DESC
;

-- ── QC checks ─────────────────────────────────────────────────

-- 1. Signal distribution
-- SELECT signal, COUNT(*) AS role_count
-- FROM role_signals
-- WHERE signal != 'Filtered'
-- GROUP BY signal
-- ORDER BY role_count DESC;

-- 2. Daily vs WoW rank shift agreement
-- How often do daily and weekly signals agree?
-- SELECT
--   CASE
--     WHEN rank_shift_pp > 10  THEN 'Emerging'
--     WHEN rank_shift_pp > 3   THEN 'Growing'
--     WHEN rank_shift_pp < -3  THEN 'Declining'
--     ELSE 'Stable'
--   END AS daily_signal,
--   CASE
--     WHEN wow_rank_shift_pp > 10  THEN 'Emerging'
--     WHEN wow_rank_shift_pp > 3   THEN 'Growing'
--     WHEN wow_rank_shift_pp < -3  THEN 'Declining'
--     ELSE 'Stable'
--   END AS wow_signal,
--   COUNT(*) AS role_count
-- FROM role_signals
-- WHERE signal != 'Filtered'
-- GROUP BY 1, 2
-- ORDER BY role_count DESC;

-- 3. Top Emerging today + WoW context
-- SELECT role_label, is_ai_role,
--        today_count, rank_shift_pp,
--        wow_rank_shift_pp, signal
-- FROM role_signals
-- WHERE signal = 'Emerging'
-- ORDER BY rank_shift_pp DESC
-- LIMIT 20;

-- 4. One-day spikes vs sustained: Emerging daily but Declining WoW
-- SELECT role_label, today_count,
--        rank_shift_pp, wow_rank_shift_pp
-- FROM role_signals
-- WHERE signal = 'Emerging'
--   AND wow_rank_shift_pp < -3
-- ORDER BY rank_shift_pp DESC;

-- 5. AI vs non-AI signal breakdown
-- SELECT is_ai_role, signal, COUNT(*) AS role_count
-- FROM role_signals
-- WHERE signal != 'Filtered'
-- GROUP BY is_ai_role, signal
-- ORDER BY is_ai_role DESC, role_count DESC;