CREATE TABLE IF NOT EXISTS dim_collections_status (
    account_id BIGINT NOT NULL,
    report_date DATE NOT NULL,
    collection_status STRING,
    promise_to_pay BOOLEAN,
    recovery_amount DECIMAL(15,2)
)
USING ICEBERG
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd', 'write.parquet.row-group-size-bytes' = '268435456', 'format-version' = '2', 'write.distribution-mode' = 'range');
