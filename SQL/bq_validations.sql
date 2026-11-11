-- Replace `project.gold` with your BigQuery project and Gold dataset.
-- fact_transaction grain: one unique TransactionID; amount is transaction value, NOT bank revenue.
-- Top 5 customers by total transaction value
SELECT a.CustomerID, COUNT(*) txn_count, SUM(t.Amount) transaction_value
FROM `project.gold.fact_transaction` t JOIN `project.gold.dim_account_current` a USING(AccountID)
GROUP BY 1 ORDER BY transaction_value DESC LIMIT 5;

-- Product-wise transaction value (not fee revenue)
SELECT p.ProductName, SUM(t.Amount) transaction_value
FROM `project.gold.fact_transaction` t JOIN `project.gold.dim_account_current` a USING(AccountID)
JOIN `project.gold.dim_product` p USING(ProductID)
GROUP BY 1 ORDER BY 2 DESC;

-- Inactive accounts; anchor to dataset as-of date for historical snapshots
DECLARE as_of_date DATE DEFAULT DATE '2026-10-08';
SELECT a.AccountID, MAX(t.TxnDate) last_transaction_date
FROM `project.gold.dim_account_current` a LEFT JOIN `project.gold.fact_transaction` t
ON a.AccountID=t.AccountID AND t.TxnDate<=as_of_date
GROUP BY 1 HAVING MAX(t.TxnDate) IS NULL OR MAX(t.TxnDate)<DATE_SUB(as_of_date,INTERVAL 3 MONTH);

-- High-value GBP transactions
SELECT * FROM `project.gold.fact_transaction`
WHERE Currency='GBP' AND Amount>100000;

-- SCD2 profile and account history: query dim_customer_scd2 and dim_account_scd2
-- Loan risk: Loans.OverdueDays > 30 (illustrative threshold)
-- Card adoption: Customers left join active Cards; non-holders are cross-sell candidates.
