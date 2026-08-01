-- ============================================================
-- 02_analytics.sql  — banking/card analytics on the Iceberg tables
-- Demonstrates joins, aggregation, and a couple of risk-flavoured
-- queries that read naturally on a card-issuing domain.
-- ============================================================

-- 1) Approved spend by merchant category (last 90 days)
SELECT
    merchant_category,
    COUNT(*)                              AS txn_count,
    ROUND(SUM(amount), 2)                 AS total_amount,
    ROUND(AVG(amount), 2)                 AS avg_amount
FROM lakehouse.cards.transactions
WHERE txn_status = 'APPROVED'
GROUP BY merchant_category
ORDER BY total_amount DESC;

-- 2) Decline rate by card network (data-quality / risk signal)
SELECT
    c.network,
    COUNT(*)                                                     AS total_txns,
    SUM(CASE WHEN t.txn_status = 'DECLINED' THEN 1 ELSE 0 END)   AS declines,
    ROUND(100.0 * SUM(CASE WHEN t.txn_status = 'DECLINED' THEN 1 ELSE 0 END)
          / COUNT(*), 2)                                         AS decline_rate_pct
FROM lakehouse.cards.transactions t
JOIN lakehouse.cards.cards c ON t.card_id = c.card_id
GROUP BY c.network
ORDER BY decline_rate_pct DESC;

-- 3) Top 20 spending accounts, joining all three tables
SELECT
    a.account_id,
    a.segment,
    a.country_code,
    COUNT(DISTINCT c.card_id)      AS num_cards,
    COUNT(t.transaction_id)        AS num_txns,
    ROUND(SUM(t.amount), 2)        AS total_spend
FROM lakehouse.cards.accounts a
JOIN lakehouse.cards.cards c        ON a.account_id = c.account_id
JOIN lakehouse.cards.transactions t ON c.card_id    = t.card_id
WHERE t.txn_status = 'APPROVED'
GROUP BY a.account_id, a.segment, a.country_code
ORDER BY total_spend DESC
LIMIT 20;

-- 4) International transactions on BLOCKED cards (fraud-style check)
SELECT
    t.transaction_id,
    t.txn_timestamp,
    t.amount,
    t.currency,
    c.card_id,
    c.card_status,
    a.country_code
FROM lakehouse.cards.transactions t
JOIN lakehouse.cards.cards c        ON t.card_id    = c.card_id
JOIN lakehouse.cards.accounts a     ON c.account_id = a.account_id
WHERE c.card_status = 'BLOCKED'
  AND t.is_international = TRUE
  AND t.txn_status = 'APPROVED'
ORDER BY t.amount DESC;

-- 5) Daily approved volume trend (feed a Dremio reflection / BI later)
SELECT
    CAST(txn_timestamp AS DATE)  AS txn_date,
    COUNT(*)                     AS approved_txns,
    ROUND(SUM(amount), 2)        AS approved_volume
FROM lakehouse.cards.transactions
WHERE txn_status = 'APPROVED'
GROUP BY CAST(txn_timestamp AS DATE)
ORDER BY txn_date;
