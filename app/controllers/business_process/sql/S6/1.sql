WITH deposit AS (
    SELECT 
        date_trunc('{{time_grain}}', CAST(from_unixtime(((approved_time + 28800000) / 1000)) AS TIMESTAMP)) AS "Date",
        currency_type_name,
        COUNT(DISTINCT CASE WHEN d.status_name = 'Approved' THEN d.account_user_id END) AS Unique_Depositors,
        COUNT(CASE WHEN d.status_name = 'Approved' THEN d.account_user_id END) AS Deposit_Count,
        SUM(CASE WHEN d.status_name = 'Approved' THEN d.amount END) AS Deposit_amount
    FROM default.ads_mcd_s6_deposit_transaction d
    WHERE d.status_name = 'Approved'
    GROUP BY date_trunc('{{time_grain}}', CAST(from_unixtime(((d.approved_time + 28800000) / 1000)) AS TIMESTAMP)), currency_type_name
),
withdrawal AS (
    SELECT 
        date_trunc('{{time_grain}}', CAST(from_unixtime(((approved_time + 28800000) / 1000)) AS TIMESTAMP)) AS "Date",
        currency_type_name,
        COUNT(DISTINCT CASE WHEN d.status_name = 'Approved' THEN d.account_user_id END) AS Unique_Withdrawer,
        COUNT(CASE WHEN d.status_name = 'Approved' THEN d.account_user_id END) AS Withdrawal_Count,
        SUM(CASE WHEN d.status_name = 'Approved' THEN d.amount END) AS Withdrawal_amount
    FROM ads_mcd_s6_withdraw_transaction d
    WHERE d.status_name = 'Approved'
    GROUP BY date_trunc('{{time_grain}}', CAST(from_unixtime(((d.approved_time + 28800000) / 1000)) AS TIMESTAMP)), currency_type_name
),
sign_up AS (
    SELECT 
        date_trunc('{{time_grain}}', CAST(from_unixtime(((sign_up_time+28800000)/1000)) AS TIMESTAMP)) AS "Date",
        currency_type_name,
        COUNT(*) AS sign_up_count
    FROM default.ads_mcd_s6_account_anon
    WHERE (sign_up_time+28800000) >= 1577808000000
    GROUP BY date_trunc('{{time_grain}}', CAST(from_unixtime(((sign_up_time+28800000)/1000)) AS TIMESTAMP)), currency_type_name
),
first_deposit AS (
    SELECT 
        date_trunc('{{time_grain}}', CAST(from_unixtime(((first_deposit_time+28800000)/1000)) AS TIMESTAMP)) AS "Date",
        currency_type_name,
        COUNT(*) AS first_deposit_count
    FROM default.ads_mcd_s6_account_anon
    WHERE (first_deposit_time+28800000) >= 1577808000000
    GROUP BY date_trunc('{{time_grain}}', CAST(from_unixtime(((first_deposit_time+28800000)/1000)) AS TIMESTAMP)), currency_type_name
)

SELECT 
    d.Date AS "Date",
    COALESCE(s.sign_up_count, 0) AS "NSU",
    COALESCE(fd.first_deposit_count, 0) AS "FTD",
    COALESCE(CAST(fd.first_deposit_count AS DECIMAL(10, 3)), 0) / NULLIF(CAST(s.sign_up_count AS DECIMAL(10, 3)), 0) AS "Conversion Rate",
    d.Unique_Depositors AS "UDW Unique Depositors",
    d.Deposit_Count AS "UDW Deposit Count",
    d.Deposit_amount AS "UDW Deposit Amount",
    w.Unique_Withdrawer AS "UDW Unique Withdrawer",
    w.Withdrawal_Count AS "UDW Withdrawal Count",
    w.Withdrawal_amount AS "UDW Withdrawal Amount",
    COALESCE(d.Deposit_amount, 0) - COALESCE(w.Withdrawal_amount, 0) AS "UDW Cash Inflow/(Outflow)",
    COALESCE(d.Unique_Depositors, 0) - COALESCE(fd.first_deposit_count, 0) AS "UDW No. Unique Players W/ Returning Deposits",
    COALESCE(d.Deposit_Count, 0) - COALESCE(fd.first_deposit_count, 0) AS "UDW Returning Deposits Count"

FROM deposit d
LEFT JOIN withdrawal w ON d.currency_type_name = w.currency_type_name AND d.Date = w.Date
LEFT JOIN sign_up s ON d.Date = s.Date AND d.currency_type_name = s.currency_type_name
LEFT JOIN first_deposit fd ON d.Date = fd.Date AND d.currency_type_name = fd.currency_type_name
WHERE d.Date >= TIMESTAMP '{{start_date}}'
  AND d.Date < TIMESTAMP '{{end_date}}'
  AND d.currency_type_name IN ('{{currency}}')
GROUP BY 
    d.Date,
    s.sign_up_count,
    fd.first_deposit_count,
    d.Unique_Depositors,
    d.Deposit_Count,
    d.Deposit_amount,
    w.Unique_Withdrawer,
    w.Withdrawal_Count,
    w.Withdrawal_amount
ORDER BY d.Date DESC

