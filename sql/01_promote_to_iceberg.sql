-- ============================================================
-- 01_promote_to_iceberg.sql
-- Run these in the Dremio SQL Runner (http://localhost:9047).
--
-- Prereq (done once in the Dremio UI, see RUNBOOK.md step 4):
--   * Source "minio_landing" -> S3-compatible, points at MinIO `landing` bucket
--   * Source "lakehouse"     -> Nessie catalog, data stored in MinIO `warehouse`
--
-- Pattern: read raw parquet from the landing (bronze) zone and write
-- curated Apache Iceberg tables into the Nessie-managed warehouse (silver).
-- ============================================================

-- Optional: create a folder/namespace in the Nessie catalog
CREATE FOLDER IF NOT EXISTS lakehouse.cards;

-- ---- ACCOUNTS -------------------------------------------------
CREATE TABLE lakehouse.cards.accounts AS
SELECT
    account_id,
    customer_name,
    country_code,
    segment,
    CAST(opened_date AS DATE)  AS opened_date,
    is_active
FROM minio_landing.landing.accounts;

-- ---- CARDS ----------------------------------------------------
CREATE TABLE lakehouse.cards.cards AS
SELECT
    card_id,
    account_id,
    card_pan_masked,
    network,
    card_status,
    CAST(issued_date AS DATE)  AS issued_date,
    CAST(expiry_date AS DATE)  AS expiry_date,
    credit_limit
FROM minio_landing.landing.cards;

-- ---- TRANSACTIONS ---------------------------------------------
CREATE TABLE lakehouse.cards.transactions AS
SELECT
    transaction_id,
    card_id,
    CAST(txn_timestamp AS TIMESTAMP)     AS txn_timestamp,
    amount,
    currency,
    merchant_category,
    merchant_name,
    txn_status,
    is_international
FROM minio_landing.landing.transactions;

-- Iceberg maintenance you can show off (metadata that Hive tables can't do):
-- Time-travel query against a snapshot, table history, and compaction.
SELECT COUNT(*) AS row_count FROM lakehouse.cards.transactions;

-- Inspect Iceberg snapshots / history
SELECT * FROM TABLE(table_snapshot('lakehouse.cards.transactions'));

-- Compact small files (OPTIMIZE) — a real lakehouse operational task
OPTIMIZE TABLE lakehouse.cards.transactions;
