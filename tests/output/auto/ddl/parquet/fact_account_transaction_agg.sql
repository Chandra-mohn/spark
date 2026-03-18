CREATE TABLE IF NOT EXISTS fact_account_transaction_agg (
    account_id BIGINT NOT NULL,
    total_spend DECIMAL(15,2),
    transaction_count INT,
    avg_ticket_size DECIMAL(10,2),
    top_mcc_code STRING
)
USING PARQUET
PARTITIONED BY (report_date DATE)
CLUSTERED BY (account_id) 
SORTED BY (account_id, top_mcc_code)
INTO 32 BUCKETS
TBLPROPERTIES ('parquet.compression' = 'zstd', 'parquet.block.size' = '268435456');
