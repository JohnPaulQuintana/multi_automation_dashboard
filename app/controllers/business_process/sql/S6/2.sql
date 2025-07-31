WITH 
-- First query components
GameTransactionData AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(from_unixtime(((settle_time + 28800000) / 1000)) AS TIMESTAMP)) AS "Date",
    currency_type_name AS Currency,
    COUNT(DISTINCT account_user_id) AS "Username",
    SUM(turnover) AS "Turnover",
    SUM(profit_loss * -1) AS "Company WinLoss",
    SUM(profit_loss * -1) / SUM(turnover) AS "Margin"
  FROM ads_mcd_s6_game_transaction_account_game_day_agg
  WHERE is_deleted = false
  GROUP BY date_trunc('{{time_grain}}', CAST(from_unixtime(((settle_time + 28800000) / 1000)) AS TIMESTAMP)),
           currency_type_name
),

BonusTurnoverData AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(FROM_UNIXTIME(((create_time + 28800000) / 1000)) AS TIMESTAMP)) AS "Date",
    currency_type_name AS "Currency",
    SUM(bonus) AS "Bonus_Amount",
    COUNT(bonus) AS "Bonus_Count",
    COUNT(DISTINCT account_user_id) AS "Uni_Depositors"
  FROM default.ads_mcd_s6_account_bonus_turnover
  WHERE bonus <> 0
    AND bonus_title <> 'Test Bonus'
  GROUP BY date_trunc('{{time_grain}}', CAST(FROM_UNIXTIME(((create_time + 28800000) / 1000)) AS TIMESTAMP)),
           currency_type_name
),

AdjustmentData AS (
  SELECT 
    date_trunc('{{time_grain}}', cast(from_unixtime(((create_time +2880000)/1000))AS timestamp)) AS Date,
    currency_type_name,
    COALESCE(sum(amount), 0) AS adjustment
  FROM ads_mcd_s6_adjustment_transaction
  GROUP BY date_trunc('{{time_grain}}', cast(from_unixtime(((create_time +2880000)/1000))AS timestamp)),
           currency_type_name
),

-- Second query components
DailyGameData AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(settle_date_id AS TIMESTAMP)) AS settle_date_id,
    COUNT(DISTINCT account_id) AS "GTR Total Unique Active Players",
    SUM(Turnover) AS "GTR Total Turnover",
    SUM(profit_loss) * -1 AS "GTR Profit/Loss",
    SUM(profit_loss)/SUM(turnover)*-1 AS "GTR Turnover Margin (%)"
  FROM default.ads_mcd_s6_game_transaction_account_game_day_agg
  WHERE settle_date_id >= '{{start_date}}'
    AND settle_date_id < '{{end_date}}'
    AND currency_type_name IN ('{{currency}}')
  GROUP BY date_trunc('{{time_grain}}', CAST(settle_date_id AS TIMESTAMP))
),

-- Combined data from first query
MonthlySummary AS (
  SELECT 
    g.Date,
    g.Currency,
    g.Username,
    g.Turnover,
    g."Company WinLoss",
    g.Margin,
    bt."Bonus_Amount",
    bt."Bonus_Count",
    bt."Uni_Depositors",
    a.adjustment
  FROM GameTransactionData g
  LEFT JOIN BonusTurnoverData bt ON g.Date = bt.Date AND g.Currency = bt.Currency
  LEFT JOIN AdjustmentData a ON g.Date = a.Date AND g.Currency = a.currency_type_name
  WHERE g.Date >= TIMESTAMP '{{start_date}}'
    AND g.Date < TIMESTAMP '{{end_date}}'
    AND g.Currency IN ('{{currency}}')
)

-- Final combined output
SELECT 
  m.Date AS "Date",
  -- Metrics from first query
  m.Turnover AS "TBA Total Turnover",
  m."Company WinLoss" AS "TBA Profit/Loss",
  m."Company WinLoss" / m.Turnover AS "TBA Gross Margin (%)",
  m."Bonus_Amount" AS "TBA (-) Bonus Cost",
  m.adjustment AS "TBA (-) Adjustment",
  m."Company WinLoss" - m."Bonus_Amount" - m.adjustment AS "TBA Net Gross Profit",
  m.Turnover / day(last_day_of_month(m.Date)) AS "TBA Average Daily Turnover",
  m.Username / day(last_day_of_month(m.Date)) AS "TBA Average Daily Active Players",
  m."Company WinLoss" / day(last_day_of_month(m.Date)) AS "TBA Average Daily Profit/Loss",
  
  -- Metrics from second query
  d."GTR Total Unique Active Players",
  d."GTR Total Turnover",
  d."GTR Profit/Loss",
  d."GTR Turnover Margin (%)"
  
FROM MonthlySummary m
LEFT JOIN DailyGameData d ON m.Date = d.settle_date_id