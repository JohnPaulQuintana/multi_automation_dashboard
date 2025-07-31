SELECT
  virtual_table."Date",
  virtual_table."Purpose" AS "PMT Purpose",
  COALESCE(virtual_table.rk1, '-') AS "Rank_1",
  COALESCE(virtual_table.rk2, '-') AS "Rank_2",
  COALESCE(virtual_table.rk3, '-') AS "Rank_3",
  COALESCE(virtual_table.rk4, '-') AS "Rank_4",
  COALESCE(virtual_table.rk5, '-') AS "Rank_5",
  COALESCE(virtual_table.rk6, '-') AS "Rank_6",
  COALESCE(virtual_table.rk7, '-') AS "Rank_7",
  COALESCE(virtual_table.rk8, '-') AS "Rank_8",
  COALESCE(virtual_table.rk9, '-') AS "Rank_9",
  COALESCE(virtual_table.rk10, '-') AS "Rank_10",
  SUM(virtual_table.Bonus_Amount) AS "PMT Bonus Cost",
  SUM(virtual_table.Bonus_Count) AS "PMT Total Claimed",
  SUM(virtual_table.Uni_Depositors) AS "PMT Total Unique Player Claimed"
FROM (
  WITH aggregated_data AS (
    SELECT 
      CAST(date_trunc('{{time_grain}}', FROM_UNIXTIME((create_time + 28800000) / 1000)) AS TIMESTAMP) AS create_time,
      currency_type_name AS Currency,
      promotion_purpose_type_name AS Purpose,
      SUM(bonus) AS Bonus_Amount,
      COUNT(*) AS Bonus_Count,
      COUNT(DISTINCT account_user_id) AS Uni_Depositors
    FROM ads_mcd_ctn_account_bonus_turnover
    WHERE bonus <> 0 AND bonus_title <> 'Test Bonus'
    GROUP BY 1, 2, 3
  ),
  ranked_bonus_titles AS (
    SELECT 
      CAST(date_trunc('{{time_grain}}', FROM_UNIXTIME((create_time + 28800000) / 1000)) AS TIMESTAMP) AS create_time,
      currency_type_name,
      promotion_purpose_type_name,
      bonus_title,
      RANK() OVER (
        PARTITION BY 
          CAST(date_trunc('{{time_grain}}', FROM_UNIXTIME((create_time + 28800000) / 1000)) AS TIMESTAMP),
          currency_type_name,
          promotion_purpose_type_name
        ORDER BY COUNT(*) DESC
      ) AS bonus_title_rank
    FROM ads_mcd_ctn_account_bonus_turnover
    WHERE bonus <> 0 AND bonus_title <> 'Test Bonus'
    GROUP BY 1, 2, 3, 4
  )
  SELECT 
    ad.create_time AS "Date",
    ad.Purpose,
    ad.Currency,
    ad.Bonus_Amount,
    ad.Bonus_Count,
    ad.Uni_Depositors,
    MAX(CASE WHEN rb.bonus_title_rank = 1 THEN rb.bonus_title END) AS rk1,
    MAX(CASE WHEN rb.bonus_title_rank = 2 THEN rb.bonus_title END) AS rk2,
    MAX(CASE WHEN rb.bonus_title_rank = 3 THEN rb.bonus_title END) AS rk3,
    MAX(CASE WHEN rb.bonus_title_rank = 4 THEN rb.bonus_title END) AS rk4,
    MAX(CASE WHEN rb.bonus_title_rank = 5 THEN rb.bonus_title END) AS rk5,
    MAX(CASE WHEN rb.bonus_title_rank = 6 THEN rb.bonus_title END) AS rk6,
    MAX(CASE WHEN rb.bonus_title_rank = 7 THEN rb.bonus_title END) AS rk7,
    MAX(CASE WHEN rb.bonus_title_rank = 8 THEN rb.bonus_title END) AS rk8,
    MAX(CASE WHEN rb.bonus_title_rank = 9 THEN rb.bonus_title END) AS rk9,
    MAX(CASE WHEN rb.bonus_title_rank = 10 THEN rb.bonus_title END) AS rk10
  FROM aggregated_data ad
  LEFT JOIN ranked_bonus_titles rb
    ON ad.create_time = rb.create_time
    AND ad.Currency = rb.currency_type_name
    AND ad.Purpose = rb.promotion_purpose_type_name
  GROUP BY 1, 2, 3, 4, 5, 6
) AS virtual_table
WHERE virtual_table."Date" >= TIMESTAMP '{{start_date}}'
  AND virtual_table."Date" < TIMESTAMP '{{end_date}}'
  AND virtual_table.Currency IN ('{{currency}}')
GROUP BY virtual_table."Date", virtual_table."Purpose",
         virtual_table.rk1, virtual_table.rk2, virtual_table.rk3,
         virtual_table.rk4, virtual_table.rk5, virtual_table.rk6,
         virtual_table.rk7, virtual_table.rk8, virtual_table.rk9, virtual_table.rk10
