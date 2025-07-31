SELECT 
  date_trunc('{{time_grain}}', CAST("Date" AS TIMESTAMP)) AS "Date",
  COALESCE(SUM(sign_up_count), 0) AS "NSU",
  COALESCE(SUM(first_deposit_count), 0) AS "FTD",
  CAST(COALESCE(SUM(first_deposit_count), 0) AS DECIMAL(10, 3)) / 
    NULLIF(CAST(SUM(sign_up_count) AS DECIMAL(10, 3)), 0) AS "Conversion Rate",
  
  COALESCE(SUM(Unique_Depositors), 0) AS "UDW Unique Depositors",
  COALESCE(SUM(Deposit_Count), 0) AS "UDW Deposit Count",
  COALESCE(SUM(Deposit_amount), 0) AS "UDW Deposit Amount",

  COALESCE(SUM(Unique_Withdrawer), 0) AS "UDW Unique Withdrawer",
  COALESCE(SUM(Withdrawal_Count), 0) AS "UDW Withdrawal Count",
  COALESCE(SUM(Withdrawal_amount), 0) AS "UDW Withdrawal Amount",

  COALESCE(SUM(Deposit_amount), 0) - COALESCE(SUM(Withdrawal_amount), 0) AS "UDW Cash Inflow/(Outflow)",
  COALESCE(SUM(Unique_Depositors), 0) - COALESCE(SUM(first_deposit_count), 0) AS "UDW No. Unique Players W/ Returning Deposits",
  COALESCE(SUM(Deposit_Count), 0) - COALESCE(SUM(first_deposit_count), 0) AS "UDW Returning Deposits Count"

FROM (
  WITH deposit AS (
    SELECT 
      date_trunc('{{time_grain}}', CAST(from_unixtime((approved_time + 28800000)/1000) AS TIMESTAMP)) AS "Date",
      currency_type_name,
      COUNT(DISTINCT CASE WHEN d.status_name = 'Approved' THEN d.account_user_id END) AS Unique_Depositors,
      COUNT(CASE WHEN d.status_name = 'Approved' THEN d.account_user_id END) AS Deposit_Count,
      SUM(CASE WHEN d.status_name = 'Approved' THEN d.amount END) AS Deposit_amount
    FROM default.ads_mcd_ctn_deposit_transaction d
    WHERE d.status_name = 'Approved'
    GROUP BY 1, currency_type_name
  ),

  withdrawal AS (
    SELECT 
      date_trunc('{{time_grain}}', CAST(from_unixtime((approved_time + 28800000)/1000) AS TIMESTAMP)) AS "Date",
      currency_type_name,
      COUNT(DISTINCT CASE WHEN d.status_name = 'Approved' THEN d.account_user_id END) AS Unique_Withdrawer,
      COUNT(CASE WHEN d.status_name = 'Approved' THEN d.account_user_id END) AS Withdrawal_Count,
      SUM(CASE WHEN d.status_name = 'Approved' THEN d.amount END) AS Withdrawal_amount
    FROM ads_mcd_ctn_withdraw_transaction d
    WHERE d.status_name = 'Approved'
    GROUP BY 1, currency_type_name
  ),

  sign_up AS (
    SELECT 
      date_trunc('{{time_grain}}', CAST(from_unixtime((sign_up_time + 28800000)/1000) AS TIMESTAMP)) AS "Date",
      currency_type_name,
      COUNT(*) AS sign_up_count
    FROM default.ads_mcd_ctn_account_anon
    WHERE (sign_up_time + 28800000) >= 1577808000000
    GROUP BY 1, currency_type_name
  ),

  first_deposit AS (
    SELECT 
      date_trunc('{{time_grain}}', CAST(from_unixtime((first_deposit_time + 28800000)/1000) AS TIMESTAMP)) AS "Date",
      currency_type_name,
      COUNT(*) AS first_deposit_count
    FROM default.ads_mcd_ctn_account_anon
    WHERE (first_deposit_time + 28800000) >= 1577808000000
    GROUP BY 1, currency_type_name
  )

  SELECT 
    d.Date,
    d.currency_type_name,
    d.Unique_Depositors,
    d.Deposit_Count,
    d.Deposit_amount,
    w.Unique_Withdrawer,
    w.Withdrawal_Count,
    w.Withdrawal_amount,
    s.sign_up_count,
    fd.first_deposit_count

  FROM deposit d
  LEFT JOIN withdrawal w ON d.Date = w.Date AND d.currency_type_name = w.currency_type_name
  LEFT JOIN sign_up s     ON d.Date = s.Date AND d.currency_type_name = s.currency_type_name
  LEFT JOIN first_deposit fd ON d.Date = fd.Date AND d.currency_type_name = fd.currency_type_name
) AS virtual_table

WHERE "Date" >= TIMESTAMP '{{start_date}}'
  AND "Date" < TIMESTAMP '{{end_date}}'
  AND currency_type_name IN ('{{currency}}')

GROUP BY date_trunc('{{time_grain}}', CAST("Date" AS TIMESTAMP))
