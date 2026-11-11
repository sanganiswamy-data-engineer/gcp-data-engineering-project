-- Run in BigQuery console after the PySpark + MERGE pipeline.
-- Exactly one current version per AccountID.
SELECT AccountID, COUNT(*) AS current_count
FROM `retail-omkar-cli-org-dev.bankeva_silver.accounts_clean`
WHERE cur_row_indicator = 'Y'
GROUP BY AccountID
HAVING COUNT(*) > 1;

-- Inspect account history.
SELECT AccountID, CurrentBalance, Status, source_day,
       cur_row_indicator, row_effective_date, row_expiry_date
FROM `retail-omkar-cli-org-dev.bankeva_silver.accounts_clean`
ORDER BY AccountID, row_effective_date;
