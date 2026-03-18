CREATE TABLE IF NOT EXISTS fact_account_transaction_agg (
    account_id BIGINT NOT NULL,
    report_date DATE NOT NULL,
    total_spend DECIMAL(15,2),
    transaction_count INT,
    avg_ticket_size DECIMAL(10,2),
    top_mcc_code STRING
)
USING ICEBERG
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd', 'write.parquet.row-group-size-bytes' = '268435456', 'format-version' = '2', 'write.distribution-mode' = 'range');
