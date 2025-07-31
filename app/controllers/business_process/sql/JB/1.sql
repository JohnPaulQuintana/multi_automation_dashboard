WITH 
-- Deposit
deposit AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(from_unixtime(((approved_time + 28800000) / 1000)) AS TIMESTAMP)) AS "Date",
    currency_type_name,
    COUNT(DISTINCT CASE WHEN status_name = 'Approved' THEN account_user_id END) AS Unique_Depositors,
    COUNT(CASE WHEN status_name = 'Approved' THEN account_user_id END) AS Deposit_Count,
    SUM(CASE WHEN status_name = 'Approved' THEN amount END) AS Deposit_amount
  FROM default.ads_mcd_jb_deposit_transaction
  WHERE status_name = 'Approved'
  GROUP BY 1, currency_type_name
),

-- Withdrawal
withdrawal AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(from_unixtime(((approved_time + 28800000) / 1000)) AS TIMESTAMP)) AS "Date",
    currency_type_name,
    COUNT(DISTINCT CASE WHEN status_name = 'Approved' THEN account_user_id END) AS Unique_Withdrawer,
    COUNT(CASE WHEN status_name = 'Approved' THEN account_user_id END) AS Withdrawal_Count,
    SUM(CASE WHEN status_name = 'Approved' THEN amount END) AS Withdrawal_amount
  FROM ads_mcd_jb_withdraw_transaction
  WHERE status_name = 'Approved'
  GROUP BY 1, currency_type_name
),

-- Sign Ups
sign_up AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(from_unixtime(((sign_up_time + 28800000)/1000)) AS TIMESTAMP)) AS "Date",
    currency_type_name,
    COUNT(*) AS sign_up_count
  FROM default.ads_mcd_jb_account_anon
  WHERE (sign_up_time + 28800000) >= 1577808000000
  GROUP BY 1, currency_type_name
),

-- First Deposits
first_deposit AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(from_unixtime(((first_deposit_time + 28800000)/1000)) AS TIMESTAMP)) AS "Date",
    currency_type_name,
    COUNT(*) AS first_deposit_count
  FROM default.ads_mcd_jb_account_anon
  WHERE (first_deposit_time + 28800000) >= 1577808000000
  GROUP BY 1, currency_type_name
)

SELECT 
  d."Date",
  SUM(s.sign_up_count) AS "NSU",
  SUM(fd.first_deposit_count) AS "FTD",
  ROUND(CAST(SUM(fd.first_deposit_count) AS DECIMAL(10, 3)) / NULLIF(SUM(s.sign_up_count), 0), 4) AS "Conversion Rate",

  SUM(d.Unique_Depositors) AS "UDW Unique Depositors",
  SUM(d.Deposit_Count) AS "UDW Deposit Count",
  SUM(d.Deposit_amount) AS "UDW Deposit Amount",

  SUM(w.Unique_Withdrawer) AS "UDW Unique Withdrawer",
  SUM(w.Withdrawal_Count) AS "UDW Withdrawal Count",
  SUM(w.Withdrawal_amount) AS "UDW Withdrawal Amount",

  SUM(d.Deposit_amount) - SUM(w.Withdrawal_amount) AS "UDW Cash Inflow/(Outflow)",

  SUM(d.Unique_Depositors) - SUM(fd.first_deposit_count) AS "UDW No. Unique Players W/ Returning Deposits",
  SUM(d.Deposit_Count) - SUM(fd.first_deposit_count) AS "UDW Returning Deposits Count"

FROM deposit d
LEFT JOIN withdrawal w 
  ON d."Date" = w."Date" AND d.currency_type_name = w.currency_type_name
LEFT JOIN sign_up s 
  ON d."Date" = s."Date" AND d.currency_type_name = s.currency_type_name
LEFT JOIN first_deposit fd 
  ON d."Date" = fd."Date" AND d.currency_type_name = fd.currency_type_name

WHERE d."Date" >= TIMESTAMP '{{start_date}}'
  AND d."Date" < TIMESTAMP '{{end_date}}'
  AND d.currency_type_name = '{{currency}}'
GROUP BY d."Date"

