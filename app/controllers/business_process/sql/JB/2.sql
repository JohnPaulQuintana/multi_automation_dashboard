WITH 
-- Game Transaction (TBA)
GameTransactionData AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(from_unixtime(((settle_time + 28800000) / 1000)) AS TIMESTAMP)) AS "Date",
    currency_type_name AS Currency,
    COUNT(DISTINCT account_user_id) AS "TBA_Username",
    SUM(turnover) AS "TBA_Turnover",
    SUM(profit_loss * -1) AS "TBA_Company_WinLoss"
  FROM ads_mcd_jb_game_transaction_account_game_day_agg
  WHERE is_deleted = false
  GROUP BY 1, currency_type_name
),

-- Bonus Turnover (TBA)
BonusTurnoverData AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(FROM_UNIXTIME(((create_time + 28800000) / 1000)) AS TIMESTAMP)) AS "Date",
    currency_type_name AS Currency,
    SUM(bonus) AS "TBA_Bonus_Amount"
  FROM default.ads_mcd_jb_account_bonus_turnover
  WHERE bonus <> 0
    AND bonus_title <> 'Test Bonus'
  GROUP BY 1, currency_type_name
),

-- Adjustment (TBA)
AdjustmentData AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(from_unixtime(((create_time + 2880000) / 1000)) AS TIMESTAMP)) AS "Date",
    currency_type_name AS Currency,
    COALESCE(SUM(amount), 0) AS "TBA_Adjustment"
  FROM ads_mcd_jb_adjustment_transaction
  GROUP BY 1, currency_type_name
),

-- GTR Data
GTRData AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(settle_date_id AS TIMESTAMP)) AS "Date",
    currency_type_name AS Currency,
    COUNT(DISTINCT account_id) AS "GTR_Unique_Active_Players",
    SUM(turnover) AS "GTR_Turnover",
    SUM(profit_loss) * -1 AS "GTR_Profit_Loss"
  FROM default.ads_mcd_jb_game_transaction_account_game_day_agg
  WHERE currency_type_name = '{{currency}}'
    AND settle_date_id >= '{{start_date}}'
    AND settle_date_id < '{{end_date}}'
  GROUP BY 1, currency_type_name
)

-- Final Combined Metrics
SELECT 
  gtd.Date AS "Date",

  -- TBA metrics
  gtd.TBA_Turnover AS "TBA Total Turnover",
  gtd.TBA_Company_WinLoss AS "TBA Profit/Loss",
  gtd.TBA_Company_WinLoss / NULLIF(gtd.TBA_Turnover, 0) AS "TBA Gross Margin (%)",
  COALESCE(btd.TBA_Bonus_Amount, 0) AS "TBA (-) Bonus Cost",
  COALESCE(ad.TBA_Adjustment, 0) AS "TBA (-) Adjustment",
  gtd.TBA_Company_WinLoss - COALESCE(btd.TBA_Bonus_Amount, 0) - COALESCE(ad.TBA_Adjustment, 0) AS "TBA Net Gross Profit",

  -- Use Trino-compatible last-day-of-month workaround
  gtd.TBA_Turnover / NULLIF(DAY(date_add('day', -1, date_trunc('month', date_add('month', 1, gtd.Date)))), 0)
      AS "TBA Average Daily Turnover",
  gtd.TBA_Username / NULLIF(DAY(date_add('day', -1, date_trunc('month', date_add('month', 1, gtd.Date)))), 0)
      AS "TBA Average Daily Active Players",
  gtd.TBA_Company_WinLoss / NULLIF(DAY(date_add('day', -1, date_trunc('month', date_add('month', 1, gtd.Date)))), 0)
      AS "TBA Average Daily Profit/Loss",

  -- GTR metrics
  COALESCE(gtr.GTR_Unique_Active_Players, 0) AS "GTR Total Unique Active Players",
  COALESCE(gtr.GTR_Turnover, 0) AS "GTR Total Turnover",
  COALESCE(gtr.GTR_Profit_Loss, 0) AS "GTR Profit/Loss",
  COALESCE(gtr.GTR_Profit_Loss, 0) / NULLIF(gtr.GTR_Turnover, 0) AS "GTR Turnover Margin (%)"

FROM GameTransactionData gtd
LEFT JOIN BonusTurnoverData btd 
  ON gtd.Date = btd.Date AND gtd.Currency = btd.Currency
LEFT JOIN AdjustmentData ad 
  ON gtd.Date = ad.Date AND gtd.Currency = ad.Currency
LEFT JOIN GTRData gtr 
  ON gtd.Date = gtr.Date AND gtd.Currency = gtr.Currency

WHERE gtd.Date >= TIMESTAMP '{{start_date}}'
  AND gtd.Date < TIMESTAMP '{{end_date}}'
  AND gtd.Currency = '{{currency}}'
