WITH bonus_data AS (
    SELECT 
        date_trunc('{{time_grain}}', TRY_CAST(create_date_id AS TIMESTAMP)) AS month,
        currency_type_name,
        SUM(bonus) AS total_bonus_cost,
        COUNT(bonus) AS total_claimed,
        COUNT(DISTINCT account_user_id) AS total_unique_players
    FROM default.ads_mcd_bj_account_bonus_turnover
    WHERE 
        TRY_CAST(create_date_id AS DATE) >= DATE '{{start_date}}'
        AND TRY_CAST(create_date_id AS DATE) < DATE '{{end_date}}'
        AND bonus != 0
        AND bonus_title != 'Test Bonus'
        AND currency_type_name = '{{currency}}'
    GROUP BY 1, 2
),

vip_cash_data AS (
    SELECT 
        date_trunc('{{time_grain}}', TRY_CAST(create_date_id AS TIMESTAMP)) AS month,
        currency_type_name,
        COUNT(id) AS vip_claim_count,
        SUM(balance) AS vip_cash
    FROM default.ads_mcd_bj_vip_point_exchange_balance_record
    WHERE 
        TRY_CAST(create_date_id AS DATE) >= DATE '{{start_date}}'
        AND TRY_CAST(create_date_id AS DATE) < DATE '{{end_date}}'
        AND currency_type_name = '{{currency}}'
    GROUP BY 1, 2
),

raf_commission_data AS (
    SELECT 
        date_trunc('{{time_grain}}', TRY_CAST(received_date_id AS TIMESTAMP)) AS month,
        currency_type_name,
        COUNT(*) AS raf_count,
        SUM(real_amount) AS "RAF Commission"
    FROM ads_mcd_bj_semi_auto_raf_commission
    WHERE 
        TRY_CAST(received_date_id AS DATE) >= DATE '{{start_date}}'
        AND TRY_CAST(received_date_id AS DATE) < DATE '{{end_date}}'
        AND currency_type_name = '{{currency}}'
    GROUP BY 1, 2
)

SELECT 
    b.month AS "Date",
    b.total_bonus_cost AS "BC Total Bonus Cost",
    b.total_claimed AS "BC Total Claimed",
    b.total_unique_players AS "BC Total Unique Player Claimed",
    COALESCE(v.vip_claim_count, 0) AS "VIP Count",
    COALESCE(v.vip_cash, 0) AS "VIP Cash",
    COALESCE(r.raf_count, 0) AS "RAF Count",
    COALESCE(r."RAF Commission", 0) AS "RAF Commission"
FROM bonus_data b
LEFT JOIN vip_cash_data v
  ON b.month = v.month AND b.currency_type_name = v.currency_type_name
LEFT JOIN raf_commission_data r
  ON b.month = r.month AND b.currency_type_name = r.currency_type_name
