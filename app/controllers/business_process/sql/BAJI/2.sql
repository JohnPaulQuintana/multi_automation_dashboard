-- Turnover,Bonus and Adjustment Report => Game Transaction Report
SELECT 
    Date("Date") AS "Date",
    SUM(game_Turnover) AS "TBA Total Turnover",
    SUM(game_Company_WinLoss) AS "TBA Profit/Loss",
    SUM(game_Company_WinLoss) / NULLIF(SUM(game_Turnover), 0) AS "TBA Gross Margin (%)",
    SUM(bonus_Bonus_Amount) AS "TBA (-) Bonus Cost",
    SUM(CASE WHEN adj_adjustment IS NOT NULL THEN adj_adjustment ELSE 0 END) AS "TBA (-) Adjustment",
    COALESCE(SUM(vip_vip_cash), 0) AS "TBA (-) VIP Cash",
    SUM(game_Company_WinLoss) - SUM(bonus_Bonus_Amount) - SUM(CASE WHEN adj_adjustment IS NOT NULL THEN adj_adjustment ELSE 0 END) AS "TBA Net Gross Profit",
    SUM(game_Turnover) / NULLIF(SUM(DAY(last_day_of_month("Date"))), 0) AS "TBA Average Daily Turnover",
    SUM(game_Username) / NULLIF(SUM(DAY(last_day_of_month("Date"))), 0) AS "TBA Average Daily Active Players",
    SUM(game_Company_WinLoss) / NULLIF(SUM(DAY(last_day_of_month("Date"))), 0) AS "TBA Average Daily Profit/Loss",
    COALESCE(SUM(agg_Total_Unique_Active_Players), 0) AS "GTR Total Unique Active Players",
    COALESCE(SUM(agg_Total_Turnover), 0) AS "GTR Total Turnover",
    COALESCE(SUM(agg_Profit_Loss), 0) AS "GTR Profit/Loss",
    COALESCE(SUM(agg_Turnover_Margin_Percentage), 0) AS "GTR Turnover Margin (%)"
FROM (
    WITH
    settings AS (
        SELECT '{{time_grain}}' AS time_grain
    ),
    GameTransactionData AS (
        SELECT 
        date_trunc((SELECT time_grain FROM settings), CAST(settle_date_id AS DATE)) AS "Date",
        currency_type_name AS "Currency",
        COUNT(DISTINCT account_user_id) AS game_Username,
        SUM(turnover) AS game_Turnover,
        SUM(profit_loss * -1) AS game_Company_WinLoss,
        SUM(profit_loss * -1) / NULLIF(SUM(turnover), 0) AS game_Margin
        FROM ads_mcd_bj_game_transaction_account_game_day_agg
        WHERE is_deleted = false
        AND settle_date_id >= '{{start_date}}' AND settle_date_id < '{{end_date}}'
        AND currency_type_name = '{{currency}}'
        GROUP BY 1, 2
    ),
    BonusTurnoverData AS (
        SELECT 
        date_trunc((SELECT time_grain FROM settings), CAST(create_date_id AS DATE)) AS "Date",
        currency_type_name AS "Currency",
        SUM(bonus) AS bonus_Bonus_Amount,
        COUNT(bonus) AS bonus_Bonus_Count,
        COUNT(DISTINCT account_user_id) AS bonus_Uni_Depositors
        FROM default.ads_mcd_bj_account_bonus_turnover
        WHERE bonus <> 0
        AND bonus_title <> 'Test Bonus'
        AND create_date_id >= '{{start_date}}' AND create_date_id < '{{end_date}}'
        AND currency_type_name = '{{currency}}'
        GROUP BY 1, 2
    ),
    AdjustmentData AS (
        SELECT 
        date_trunc((SELECT time_grain FROM settings), CAST(create_date_id AS DATE)) AS "Date",
        currency_type_name AS "Currency",
        SUM(amount) AS adj_adjustment
        FROM ads_mcd_bj_adjustment_transaction
        WHERE account_user_id NOT IN ('samdaykrw')
        AND create_date_id >= '{{start_date}}' AND create_date_id < '{{end_date}}'
        AND currency_type_name = '{{currency}}'
        GROUP BY 1, 2
    ),
    vip_redemption AS (
        SELECT 
        date_trunc((SELECT time_grain FROM settings), CAST(create_date_id AS DATE)) AS "Date",
        currency_type_name AS "Currency",
        SUM(balance) AS vip_vip_cash
        FROM ads_mcd_bj_vip_point_exchange_balance_record
        WHERE create_date_id >= '{{start_date}}' AND create_date_id < '{{end_date}}'
        AND currency_type_name = '{{currency}}'
        GROUP BY 1, 2
    ),
    GameTransactionMonthlyAgg AS (
        SELECT 
        date_trunc((SELECT time_grain FROM settings), CAST(settle_date_id AS TIMESTAMP)) AS "Date",
        currency_type_name AS "Currency",
        COUNT(DISTINCT account_id) AS agg_Total_Unique_Active_Players,
        SUM(Turnover) AS agg_Total_Turnover,
        SUM(profit_loss) * -1 AS agg_Profit_Loss,
        SUM(profit_loss) / NULLIF(SUM(turnover), 0) * -1 AS agg_Turnover_Margin_Percentage
        FROM default.ads_mcd_bj_game_transaction_account_game_day_agg
        WHERE settle_date_id >= '{{start_date}}' AND settle_date_id < '{{end_date}}'
        AND currency_type_name = '{{currency}}'
        GROUP BY 1, 2
    )
    SELECT
        g."Date",
        g."Currency",
        g.game_Username,
        g.game_Turnover,
        g.game_Company_WinLoss,
        g.game_Margin,
        b.bonus_Bonus_Amount,
        b.bonus_Bonus_Count,
        b.bonus_Uni_Depositors,
        a.adj_adjustment,
        v.vip_vip_cash,
        ga.agg_Total_Unique_Active_Players,
        ga.agg_Total_Turnover,
        ga.agg_Profit_Loss,
        ga.agg_Turnover_Margin_Percentage
    FROM GameTransactionData g
    LEFT JOIN BonusTurnoverData b ON g."Date" = b."Date" AND g."Currency" = b."Currency"
    LEFT JOIN AdjustmentData a ON g."Date" = a."Date" AND g."Currency" = a."Currency"
    LEFT JOIN vip_redemption v ON g."Date" = v."Date" AND g."Currency" = v."Currency"
    LEFT JOIN GameTransactionMonthlyAgg ga ON g."Date" = ga."Date" AND g."Currency" = ga."Currency"
) AS virtual_table
GROUP BY Date("Date")