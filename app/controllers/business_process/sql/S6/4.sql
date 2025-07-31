WITH agg AS (
  SELECT 
    date_trunc('{{time_grain}}', from_unixtime((create_time + 28800000) / 1000)) AS dt,
    currency_type_name AS currency,
    promotion_purpose_type_name AS purpose,
    SUM(bonus) AS bonus_amt,
    COUNT(bonus) AS bonus_count,
    COUNT(DISTINCT account_user_id) AS unique_players
  FROM ads_mcd_s6_account_bonus_turnover
  WHERE bonus <> 0 AND bonus_title <> 'Test Bonus'
  GROUP BY 1, 2, 3
),

ranked AS (
  SELECT 
    date_trunc('{{time_grain}}', from_unixtime((create_time + 28800000) / 1000)) AS dt,
    currency_type_name,
    promotion_purpose_type_name,
    bonus_title,
    RANK() OVER (
      PARTITION BY currency_type_name, promotion_purpose_type_name, 
                   date_trunc('{{time_grain}}', from_unixtime((create_time + 28800000) / 1000))
      ORDER BY COUNT(*) DESC
    ) AS rnk
  FROM ads_mcd_s6_account_bonus_turnover
  WHERE bonus <> 0 AND bonus_title <> 'Test Bonus'
  GROUP BY 1, 2, 3, bonus_title
),

final AS (
  SELECT 
    a.dt,
    a.purpose,
    a.currency,
    a.bonus_amt,
    a.bonus_count,
    a.unique_players,
    MAX(CASE WHEN r.rnk = 1 THEN r.bonus_title END) AS r1,
    MAX(CASE WHEN r.rnk = 2 THEN r.bonus_title END) AS r2,
    MAX(CASE WHEN r.rnk = 3 THEN r.bonus_title END) AS r3,
    MAX(CASE WHEN r.rnk = 4 THEN r.bonus_title END) AS r4,
    MAX(CASE WHEN r.rnk = 5 THEN r.bonus_title END) AS r5,
    MAX(CASE WHEN r.rnk = 6 THEN r.bonus_title END) AS r6,
    MAX(CASE WHEN r.rnk = 7 THEN r.bonus_title END) AS r7,
    MAX(CASE WHEN r.rnk = 8 THEN r.bonus_title END) AS r8,
    MAX(CASE WHEN r.rnk = 9 THEN r.bonus_title END) AS r9,
    MAX(CASE WHEN r.rnk = 10 THEN r.bonus_title END) AS r10
  FROM agg a
  LEFT JOIN ranked r 
    ON a.dt = r.dt AND a.currency = r.currency_type_name 
    AND a.purpose = r.promotion_purpose_type_name
  GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT 
  dt AS "Date",
  purpose AS "PMT Purpose",
  COALESCE(NULLIF(r1, 'N/A'), '-') AS "Rank_1",
  COALESCE(NULLIF(r2, 'N/A'), '-') AS "Rank_2",
  COALESCE(NULLIF(r3, 'N/A'), '-') AS "Rank_3",
  COALESCE(NULLIF(r4, 'N/A'), '-') AS "Rank_4",
  COALESCE(NULLIF(r5, 'N/A'), '-') AS "Rank_5",
  COALESCE(NULLIF(r6, 'N/A'), '-') AS "Rank_6",
  COALESCE(NULLIF(r7, 'N/A'), '-') AS "Rank_7",
  COALESCE(NULLIF(r8, 'N/A'), '-') AS "Rank_8",
  COALESCE(NULLIF(r9, 'N/A'), '-') AS "Rank_9",
  COALESCE(NULLIF(r10, 'N/A'), '-') AS "Rank_10",
  bonus_amt AS "PMT Bonus Cost",
  bonus_count AS "PMT Total Claimed",
  unique_players AS "PMT Total Unique Player Claimed"
FROM final
WHERE dt >= TIMESTAMP '{{start_date}}' AND dt < TIMESTAMP '{{end_date}}' AND currency = '{{currency}}'
ORDER BY purpose, dt
LIMIT 1000;
