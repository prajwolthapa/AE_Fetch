WITH recent_users AS (
    -- Filter users created in the past 6 months
    SELECT
        user_id
    FROM
        users
    WHERE
        creation_date >= DATEADD(MONTH, -6, GETDATE()) --- Filter Created date to only get users that were created in the last 6 Months
),
spend_by_user_last_6_months AS (
    -- Calculate total spend for each brand by users created in the past 6 months 
    SELECT
        b.name AS BrandName,
        SUM(t.transaction_amount) AS TotalSpend
    FROM
        rewards_receipt rr
        INNER JOIN recent_users ru ON rr.user_id = ru.user_id
        INNER JOIN rewards_items_receipt rip ON rr.receipt_id = rip.receipt_id
        INNER JOIN brand br ON rip.barcode = br.barcode
    GROUP BY
        b.name
),
brand_ranking AS (
    -- Rank brands by total spend
    SELECT
        BrandName,
        TotalSpend,
        RANK() OVER (ORDER BY TotalSpend DESC) AS ranking
    FROM
        spend_by_user_last_6_months
)
-- Final Query to  Get the top brand by spend
SELECT
    BrandName,
    TotalSpend
FROM
    brand_ranking
WHERE
    ranking = 1;
