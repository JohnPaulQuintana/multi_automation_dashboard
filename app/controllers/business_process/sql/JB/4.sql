SELECT 
  date_trunc('{{time_grain}}', CAST("Date" AS TIMESTAMP)) AS "Date",
  Purpose AS "PMT Purpose",
  COALESCE(rk1_bonus_title, '-') AS "Rank_1",
  COALESCE(rk2_bonus_title, '-') AS "Rank_2",
  COALESCE(rk3_bonus_title, '-') AS "Rank_3",
  COALESCE(rk4_bonus_title, '-') AS "Rank_4",
  COALESCE(rk5_bonus_title, '-') AS "Rank_5",
  COALESCE(rk6_bonus_title, '-') AS "Rank_6",
  COALESCE(rk7_bonus_title, '-') AS "Rank_7",
  COALESCE(rk8_bonus_title, '-') AS "Rank_8",
  COALESCE(rk9_bonus_title, '-') AS "Rank_9",
  COALESCE(rk10_bonus_title, '-') AS "Rank_10",
  Bonus_Amount AS "PMT Bonus Cost",
  Bonus_Count AS "PMT Total Claimed",
  Uni_Depositors AS "PMT Total Unique Player Claimed"
FROM (
  WITH aggregated_data AS (
    SELECT 
      date_trunc('{{time_grain}}', FROM_UNIXTIME((create_time + 28800000) / 1000)) AS create_time,
      currency_type_name AS Currency,
      promotion_purpose_type_name AS Purpose,
      SUM(bonus) AS Bonus_Amount,
      COUNT(bonus) AS Bonus_Count,
      COUNT(DISTINCT account_user_id) AS Uni_Depositors
    FROM ads_mcd_jb_account_bonus_turnover
    WHERE bonus <> 0 AND bonus_title <> 'Test Bonus'
    GROUP BY 1, 2, 3
  ),
  ranked_bonus_titles AS (
    SELECT 
      date_trunc('{{time_grain}}', FROM_UNIXTIME((create_time + 28800000) / 1000)) AS create_time,
      currency_type_name,
      promotion_purpose_type_name,
      bonus_title,
      RANK() OVER (
        PARTITION BY promotion_purpose_type_name, currency_type_name, date_trunc('{{time_grain}}', FROM_UNIXTIME((create_time + 28800000) / 1000))
        ORDER BY COUNT(*) DESC
      ) AS bonus_title_rank
    FROM ads_mcd_jb_account_bonus_turnover
    WHERE bonus <> 0 AND bonus_title <> 'Test Bonus'
    GROUP BY 1, 2, 3, bonus_title
  )
  SELECT 
    ad.create_time AS "Date",
    ad.Currency,
    ad.Purpose,
    ad.Bonus_Amount,
    ad.Bonus_Count,
    ad.Uni_Depositors,
    MAX(CASE WHEN rb.bonus_title_rank = 1 THEN rb.bonus_title END) AS rk1_bonus_title,
    MAX(CASE WHEN rb.bonus_title_rank = 2 THEN rb.bonus_title END) AS rk2_bonus_title,
    MAX(CASE WHEN rb.bonus_title_rank = 3 THEN rb.bonus_title END) AS rk3_bonus_title,
    MAX(CASE WHEN rb.bonus_title_rank = 4 THEN rb.bonus_title END) AS rk4_bonus_title,
    MAX(CASE WHEN rb.bonus_title_rank = 5 THEN rb.bonus_title END) AS rk5_bonus_title,
    MAX(CASE WHEN rb.bonus_title_rank = 6 THEN rb.bonus_title END) AS rk6_bonus_title,
    MAX(CASE WHEN rb.bonus_title_rank = 7 THEN rb.bonus_title END) AS rk7_bonus_title,
    MAX(CASE WHEN rb.bonus_title_rank = 8 THEN rb.bonus_title END) AS rk8_bonus_title,
    MAX(CASE WHEN rb.bonus_title_rank = 9 THEN rb.bonus_title END) AS rk9_bonus_title,
    MAX(CASE WHEN rb.bonus_title_rank = 10 THEN rb.bonus_title END) AS rk10_bonus_title
  FROM aggregated_data ad
  LEFT JOIN ranked_bonus_titles rb 
    ON ad.create_time = rb.create_time 
    AND ad.Currency = rb.currency_type_name 
    AND ad.Purpose = rb.promotion_purpose_type_name
  GROUP BY 
    ad.create_time, ad.Currency, ad.Purpose, ad.Bonus_Amount, ad.Bonus_Count, ad.Uni_Depositors
) AS virtual_table
WHERE "Date" >= TIMESTAMP '{{start_date}}'
  AND "Date" < TIMESTAMP '{{end_date}}'
  AND Currency = '{{currency}}'
