WITH cte_falg_previous_crecent_receipts AS (
    -- Flagging receipts for the lat two months and categorizing into two weather it was received in the current month vs the previous  month
    SELECT
        rr.receipt_id,
        br.name AS BrandName,
        CASE

                    WHEN rr.dateScanned >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 2, 0)
             AND rr.dateScanned < DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0) THEN 'Previous Month'


            WHEN rr.dateScanned >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0)
             AND rr.dateScanned < DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) THEN 'Most Recent Month'

        END AS Receipt_Moth_Grouping
    FROM
        rewards_receipts rr
        LEFT JOIN rewards_items_receipt rip ON rip.receipt_id = rr.receipt_id
        LEFT JOIN brand br ON br.barcode = rip.barcode

    WHERE --- Capturing 2 Months worth of Time Frame as we only need to look into the last two months
        rr.dateScanned >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 2, 0) 
        AND rr.dateScanned < DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) 
),
cte_receipt_count AS (
    -- Count receipts for each brand in each month
    SELECT
        BrandName,
        Receipt_Moth_Grouping,
        COUNT(receipt_id) AS Number_of_Receipts
    FROM
        cte_monthly_data
    GROUP BY
        BrandName, Receipt_Moth_Grouping
),
cte_ranking AS (
    -- Rank brands by receipts in each month
    SELECT
        BrandName,
        Receipt_Moth_Grouping,
        Number_of_Receipts,
        DENSE_RANK() OVER (PARTITION BY MonthCategory ORDER BY Number_of_Receipts DESC) AS ranking
    FROM
        cte_receipt_count
),

--- Getting Top 5 for the Most Current Month as this the baseline starting point
cte_desired_ranking_most_recent_Month as (
    Select 
    BrandName
    ,ranking
    ,Number_of_Receipts
     from cte_ranking

     where ranking <=5 and Receipt_Moth_Grouping = 'Most Recent Month'
)



-- Final Step to Compare Ranking between two Month Category
SELECT
    cm_rank.BrandName,
    cm_rank.Number_of_Receipts AS RecentMonth_Receipts,
    cm_rank.ranking AS RecentMonth_Rank,
    COALESCE(prvm_rank.Number_of_Receipts,'No Data for Previous Month') AS PreviousMonth_Receipts, -- Handling for scenario where if this a a new product that was just part of top five in the curret month and nothing existed in the prvious month
    COALESCE(prvm_rank.ranking,'No Data for Previous Month') AS PreviousMonth_Rank
FROM
    cte_desired_ranking_most_recent_Month cm_rank
    LEFT JOIN (Select BrandName,ranking,Number_of_Receipts from cte_ranking
    where Receipt_Moth_Grouping = 'Previous Month') prvm_rank on prvm_rank.BrandName = cm_rank.BrandName
ORDER BY
    t1.ranking;
