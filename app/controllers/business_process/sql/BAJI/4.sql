--Breakdown by Promotion Type
WITH 
aggregated_data AS (
    SELECT 
        date_trunc('{{time_grain}}', from_unixtime((create_time + 28800000) / 1000)) AS create_time,
        currency_type_name AS currency,
        promotion_purpose_type_name AS purpose,
        SUM(bonus) AS bonus_amount,
        COUNT(*) AS bonus_count,
        COUNT(DISTINCT account_user_id) AS uni_depositors
    FROM ads_mcd_bj_account_bonus_turnover
    WHERE bonus <> 0 AND bonus_title <> 'Test Bonus'
    GROUP BY 1, 2, 3
),

ranked_bonus_titles AS (
    SELECT 
        date_trunc('{{time_grain}}', from_unixtime((create_time + 28800000) / 1000)) AS create_time,
        bonus_title,
        promotion_purpose_type_name AS purpose,
        currency_type_name AS currency,
        COUNT(*) AS bonus_title_count,
        RANK() OVER (
            PARTITION BY 
                promotion_purpose_type_name, 
                currency_type_name, 
                date_trunc('{{time_grain}}', from_unixtime((create_time + 28800000) / 1000))
            ORDER BY COUNT(*) DESC
        ) AS bonus_title_rank
    FROM ads_mcd_bj_account_bonus_turnover
    WHERE bonus <> 0 AND bonus_title <> 'Test Bonus'
    GROUP BY 1, 2, 3, 4
),

joined_data AS (
    SELECT 
        ad.create_time,
        ad.currency,
        ad.purpose,
        ad.bonus_amount,
        ad.bonus_count,
        ad.uni_depositors,
        MAX(CASE WHEN rb.bonus_title_rank = 1 THEN rb.bonus_title END) AS rk1,
        MAX(CASE WHEN rb.bonus_title_rank = 2 THEN rb.bonus_title END) AS rk2,
        MAX(CASE WHEN rb.bonus_title_rank = 3 THEN rb.bonus_title END) AS rk3,
        MAX(CASE WHEN rb.bonus_title_rank = 4 THEN rb.bonus_title END) AS rk4,
        MAX(CASE WHEN rb.bonus_title_rank = 5 THEN rb.bonus_title END) AS rk5,
        MAX(CASE WHEN rb.bonus_title_rank = 6 THEN rb.bonus_title END) AS rk6,
        MAX(CASE WHEN rb.bonus_title_rank = 7 THEN rb.bonus_title END) AS rk7,
        MAX(CASE WHEN rb.bonus_title_rank = 8 THEN rb.bonus_title END) AS rk8,
        MAX(CASE WHEN rb.bonus_title_rank = 9 THEN rb.bonus_title END) AS rk9,
        MAX(CASE WHEN rb.bonus_title_rank = 10 THEN rb.bonus_title END) AS rk10
    FROM aggregated_data ad
    JOIN ranked_bonus_titles rb
        ON ad.currency = rb.currency
       AND ad.purpose = rb.purpose
       AND ad.create_time = rb.create_time
    WHERE rb.bonus_title_rank <= 10
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT 
    jd.create_time AS "Date",
    jd.purpose AS "PMT Purpose",
    COALESCE(NULLIF(jd.rk1, 'N/A'), '-') AS "Rank_1",
    COALESCE(NULLIF(jd.rk2, 'N/A'), '-') AS "Rank_2",
    COALESCE(NULLIF(jd.rk3, 'N/A'), '-') AS "Rank_3",
    COALESCE(NULLIF(jd.rk4, 'N/A'), '-') AS "Rank_4",
    COALESCE(NULLIF(jd.rk5, 'N/A'), '-') AS "Rank_5",
    COALESCE(NULLIF(jd.rk6, 'N/A'), '-') AS "Rank_6",
    COALESCE(NULLIF(jd.rk7, 'N/A'), '-') AS "Rank_7",
    COALESCE(NULLIF(jd.rk8, 'N/A'), '-') AS "Rank_8",
    COALESCE(NULLIF(jd.rk9, 'N/A'), '-') AS "Rank_9",
    COALESCE(NULLIF(jd.rk10, 'N/A'), '-') AS "Rank_10",
    jd.bonus_amount AS "PMT Bonus Cost",
    jd.bonus_count AS "PMT Total Claimed",
    jd.uni_depositors AS "PMT Total Unique Player Claimed"
FROM joined_data jd
WHERE jd.create_time >= DATE '{{start_date}}'
  AND jd.create_time < DATE '{{end_date}}'
  AND jd.currency = '{{currency}}'

