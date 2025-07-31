-- Breakdown by Product Type
WITH GameTypeMapping AS (
  SELECT 
    date_trunc('{{time_grain}}', CAST(settle_date_id AS TIMESTAMP)) AS settle_date,
    CASE
      WHEN game_type_name = 'Sport' THEN 'Sport'
      WHEN game_type_name = 'CASINO' THEN 'CASINO'
      WHEN game_type_name = 'CRASH' THEN 'CRASH'
      WHEN game_type_name = 'SLOT' THEN 'SLOT'
      WHEN game_type_name = 'TABLE' THEN 'TABLE'
      WHEN game_type_name = 'FH' THEN 'FH'
      WHEN game_type_name = 'P2P' THEN 'P2P'
      WHEN game_type_name = 'LOTTERY' THEN 'LOTTERY'
      WHEN game_type_name = 'ARCADE' THEN 'ARCADE'
      WHEN game_type_name = 'ESport' THEN 'ESport'
      WHEN game_type_name = 'COCK_FIGHTING' THEN 'COCK_FIGHTING'
      WHEN game_type_name = 'NoValue' THEN 'OTHERS'
      ELSE NULL
    END AS product_type,
    account_id,
    turnover,
    profit_loss
  FROM default.ads_mcd_bj_game_transaction_account_game_day_agg
  WHERE settle_date_id >= '{{start_date}}'
    AND settle_date_id < '{{end_date}}'
    AND currency_type_name = '{{currency}}'
)

SELECT 
  settle_date AS "Date",
  product_type AS "PRD Product Type",
  COUNT(DISTINCT account_id) AS "PRD Number of Unique Player",
  SUM(turnover) AS "PRD Total Turnover",
  SUM(profit_loss) * -1 AS "PRD Profit/Loss",
  SUM(profit_loss) / NULLIF(SUM(turnover), 0) * -1 AS "PRD Margin"
FROM GameTypeMapping
GROUP BY settle_date, product_type