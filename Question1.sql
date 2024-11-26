--- I have used two different Approach 
--- 1. Counting recreipts based on the latest month available in the dataset
--- 2 . Counting recipts based on the latest calendar month . I have commneted this part out . I think this is closer to what was asked of .

WITH cte_branch_receipt AS (
    -- Get data for the most recent dateScanned and bring in the required dataset
    SELECT
        rr.receipt_id,
        br.name AS BrandName
    FROM
        rewards_receipts rr
        LEFT JOIN rewards_items_receipt rip ON rip.receipt_id = rr.receipt_id
        LEFT JOIN brand br ON br.barcode = rip.barcode
        
        -- Assumption 1 --- using most recent date based on the date available in the data Set
         WHERE rr.dateScanned = ( SELECT MAX(dateScanned) FROM rewards_receipts ) 

         --- Assumption 2 : Based on the Current Latest Month
        /*
          where   rr.dateScanned >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0) -- Start of the last month
        AND rr.dateScanned < DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) -- Start of the current month
        */
),
cte_receipt_count AS (
    -- Count receipts for each brand
    SELECT
        BrandName,
        COUNT(receipt_id) AS Number_of_Receipts
    FROM
        cte_branch_receipt
    GROUP BY
        BrandName
),
cte_rank_brand_final AS (
    -- Rank brands by number of receipts
    SELECT
        BrandName,
        DENSE_RANK() OVER (ORDER BY Number_of_Receipts DESC) AS ranking
    FROM
        cte_receipt_count
)
-- Select the top 5 brands
SELECT *
FROM cte_rank_brand_final
WHERE ranking <= 5;
