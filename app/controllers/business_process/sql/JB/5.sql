WITH bonus_data AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(create_date_id AS TIMESTAMP)) AS month,
    SUM(bonus) AS bc_total_bonus_cost,
    COUNT(bonus) AS bc_total_claimed,
    COUNT(DISTINCT account_user_id) AS bc_total_unique_player_claimed
  FROM default.ads_mcd_jb_account_bonus_turnover
  WHERE CAST(create_date_id AS DATE) >= DATE '{{start_date}}'
    AND CAST(create_date_id AS DATE) < DATE '{{end_date}}'
    AND bonus != 0
    AND bonus_title != 'Test Bonus'
    AND currency_type_name = '{{currency}}'
  GROUP BY 1
),

vip_data AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(create_date_id AS TIMESTAMP)) AS month,
    COUNT(*) AS vip_count,
    SUM(balance) AS vip_cash
  FROM ads_mcd_jb_vip_point_exchange_balance_record
  WHERE CAST(create_date_id AS DATE) >= DATE '{{start_date}}'
    AND CAST(create_date_id AS DATE) < DATE '{{end_date}}'
    AND currency_type_name = '{{currency}}'
  GROUP BY 1
),

raf_data AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(received_date_id AS TIMESTAMP)) AS month,
    COUNT(*) AS raf_total_count,
    SUM(real_amount) AS raf_commission
  FROM ads_mcd_jb_semi_auto_raf_commission
  WHERE CAST(received_date_id AS DATE) >= DATE '{{start_date}}'
    AND CAST(received_date_id AS DATE) < DATE '{{end_date}}'
    AND currency_type_name = '{{currency}}'
  GROUP BY 1
)

SELECT 
  b.month AS "Date",
  b.bc_total_bonus_cost AS "BC Total Bonus Cost",
  b.bc_total_claimed AS "BC Total Claimed",
  b.bc_total_unique_player_claimed AS "BC Total Unique Player Claimed",
  v.vip_count AS "VIP Count",
  v.vip_cash AS "VIP Cash",
  r.raf_total_count AS "RAF Count",
  r.raf_commission AS "RAF Commission"
FROM bonus_data b
LEFT JOIN vip_data v ON b.month = v.month
LEFT JOIN raf_data r ON b.month = r.month
ORDER BY b.month DESC
LIMIT 1000;
