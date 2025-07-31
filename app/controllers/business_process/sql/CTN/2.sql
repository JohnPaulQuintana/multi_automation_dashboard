SELECT 
  date_trunc('{{time_grain}}', CAST("Date" AS TIMESTAMP)) AS "Date",
  
  -- TBA Metrics
  SUM(Turnover) AS "TBA Total Turnover",
  SUM("Company WinLoss") AS "TBA Profit/Loss",
  SUM("Company WinLoss") / NULLIF(SUM(Turnover), 0) AS "TBA Gross Margin (%)",
  SUM(Bonus_Amount) AS "TBA (-) Bonus Cost",
  SUM(adjustment) AS "TBA (-) Adjustment",
  COALESCE(SUM(vip_cash), 0) AS "TBA (-) VIP Cash",
  SUM("Company WinLoss") - SUM(Bonus_Amount) - SUM(adjustment) AS "TBA Net Gross Profit",
  SUM(Turnover) / SUM(day(last_day_of_month("Date"))) AS "TBA Average Daily Turnover",
  SUM(Username) / SUM(day(last_day_of_month("Date"))) AS "TBA Average Daily Active Players",
  SUM("Company WinLoss") / SUM(day(last_day_of_month("Date"))) AS "TBA Average Daily Profit/Loss",
  
  -- GTR Metrics
  SUM(UniqueActivePlayers) AS "GTR Total Unique Active Players",
  SUM(GTR_Turnover) AS "GTR Total Turnover",
  SUM(GTR_ProfitLoss) AS "GTR Profit/Loss",
  SUM(GTR_ProfitLoss) / NULLIF(SUM(GTR_Turnover), 0) AS "GTR Turnover Margin (%)"

FROM (
  WITH GameTransactionData AS (
    SELECT 
      date_trunc('{{time_grain}}', CAST(settle_date_id AS DATE)) AS "Date",
      currency_type_name AS Currency,
      COUNT(DISTINCT account_user_id) AS "Username",
      SUM(turnover) AS "Turnover",
      SUM(profit_loss * -1) AS "Company WinLoss",
      SUM(profit_loss * -1) / NULLIF(SUM(turnover), 0) AS "Margin"
    FROM ads_mcd_ctn_game_transaction_account_game_day_agg
    WHERE is_deleted = false
    GROUP BY 1, 2
  ),
  BonusTurnoverData AS (
    SELECT 
      date_trunc('{{time_grain}}', CAST(create_date_id AS DATE)) AS "Date",
      currency_type_name AS Currency,
      SUM(bonus) AS "Bonus_Amount",
      COUNT(bonus) AS "Bonus_Count",
      COUNT(DISTINCT account_user_id) AS "Uni_Depositors"
    FROM default.ads_mcd_ctn_account_bonus_turnover
    WHERE bonus <> 0
      AND bonus_title <> 'Test Bonus'
    GROUP BY 1, 2
  ),
  AdjustmentData AS (
    SELECT 
      date_trunc('{{time_grain}}', CAST(create_date_id AS DATE)) AS "Date",
      currency_type_name,
      COALESCE(SUM(amount), 0) AS adjustment
    FROM ads_mcd_ctn_adjustment_transaction
    GROUP BY 1, 2
  ),
  vip_redemption AS (
    SELECT 
      date_trunc('{{time_grain}}', CAST(create_date_id AS DATE)) AS "Date",
      currency_type_name,
      COALESCE(SUM(balance), 0) AS vip_cash
    FROM ads_mcd_ctn_vip_point_exchange_balance_record
    GROUP BY 1, 2
  ),
  GTRData AS (
    SELECT 
      date_trunc('{{time_grain}}', CAST(settle_date_id AS DATE)) AS "Date",
      currency_type_name,
      COUNT(DISTINCT account_id) AS UniqueActivePlayers,
      SUM(turnover) AS GTR_Turnover,
      SUM(profit_loss) * -1 AS GTR_ProfitLoss
    FROM default.ads_mcd_ctn_game_transaction_account_game_day_agg
    WHERE is_deleted = false
    GROUP BY 1, 2
  )

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
    a.adjustment,
    v.vip_cash,
    gtr.UniqueActivePlayers,
    gtr.GTR_Turnover,
    gtr.GTR_ProfitLoss

  FROM GameTransactionData g
  LEFT JOIN BonusTurnoverData bt ON g.Date = bt.Date AND g.Currency = bt.Currency
  LEFT JOIN AdjustmentData a ON g.Date = a.Date AND g.Currency = a.currency_type_name
  LEFT JOIN vip_redemption v ON g.Date = v.Date AND g.Currency = v.currency_type_name
  LEFT JOIN GTRData gtr ON g.Date = gtr.Date AND g.Currency = gtr.currency_type_name
) AS virtual_table

WHERE "Date" >= DATE '{{start_date}}'
  AND "Date" < DATE '{{end_date}}'
  AND Currency = '{{currency}}'

GROUP BY date_trunc('{{time_grain}}', CAST("Date" AS TIMESTAMP))
