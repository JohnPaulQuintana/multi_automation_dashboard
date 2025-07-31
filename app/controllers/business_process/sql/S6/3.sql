SELECT game_type_name AS "PRD Product Type",
       date_trunc('{{time_grain}}', CAST(settle_date_id AS TIMESTAMP)) AS "Date",
       count(DISTINCT account_id) AS "PRD Number of Unique Player",
       sum(Turnover) AS "PRD Total Turnover",
       sum(profit_loss) * -1 AS "PRD Profit/Loss",
       sum(profit_loss)/sum(turnover)*-1 AS "PRD Margin"
FROM default.ads_mcd_s6_game_transaction_account_game_day_agg
WHERE settle_date_id >= '{{start_date}}'
  AND settle_date_id < '{{end_date}}'
  AND currency_type_name IN ('{{currency}}')
GROUP BY game_type_name,
         date_trunc('{{time_grain}}', CAST(settle_date_id AS TIMESTAMP))