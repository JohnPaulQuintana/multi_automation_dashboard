SELECT 
  COALESCE(b.create_date_id__, v.create_date_id__, r.received_date_id) AS "Date",
  COALESCE(b."Total Bonus Cost", 0) AS "BC Total Bonus Cost",
  COALESCE(b."Total Claimed", 0) AS "BC Total Claimed",
  COALESCE(b."Total Unique Player Claimed", 0) AS "BC Total Unique Player Claimed",
  COALESCE(v."VIP Cash", 0) AS "VIP Cash",
  COALESCE(v."Count", 0) AS "VIP Count",
  COALESCE(r.count, 0) AS "RAF Count",
  COALESCE(r."RAF Commission", 0) AS "RAF Commission"
  
FROM (
  SELECT 
    date_trunc('{{time_grain}}', CAST(create_date_id AS TIMESTAMP)) AS create_date_id__,
    SUM(bonus) AS "Total Bonus Cost",
    COUNT(bonus) AS "Total Claimed",
    COUNT(DISTINCT account_user_id) AS "Total Unique Player Claimed"
  FROM default.ads_mcd_ctn_account_bonus_turnover
  WHERE create_date_id >= '{{start_date}}'
    AND create_date_id < '{{end_date}}'
    AND bonus != 0
    AND bonus_title != 'Test Bonus'
    AND currency_type_name IN ('{{currency}}')
  GROUP BY 1
) b
FULL OUTER JOIN (
  SELECT 
    date_trunc('{{time_grain}}', CAST(create_date_id AS TIMESTAMP)) AS create_date_id__,
    COUNT(id) AS "Count",
    SUM(balance) AS "VIP Cash"
  FROM default.ads_mcd_ctn_vip_point_exchange_balance_record
  WHERE create_date_id >= '{{start_date}}'
    AND create_date_id < '{{end_date}}'
    AND currency_type_name IN ('{{currency}}')
  GROUP BY 1
) v ON b.create_date_id__ = v.create_date_id__
FULL OUTER JOIN (
  SELECT 
    date_trunc('{{time_grain}}', CAST(received_date_id AS TIMESTAMP)) AS received_date_id,
    COUNT(*) AS count,
    SUM(real_amount) AS "RAF Commission"
  FROM default.ads_mcd_ctn_semi_auto_raf_commission
  WHERE received_date_id >= '{{start_date}}'
    AND received_date_id < '{{end_date}}'
    AND currency_type_name IN ('{{currency}}')
  GROUP BY 1
) r ON COALESCE(b.create_date_id__, v.create_date_id__) = r.received_date_id
