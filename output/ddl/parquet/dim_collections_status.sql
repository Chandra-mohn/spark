CREATE TABLE IF NOT EXISTS dim_collections_status (
    account_id BIGINT NOT NULL,
    collection_status STRING,
    promise_to_pay BOOLEAN,
    recovery_amount DECIMAL(15,2)
)
USING PARQUET
PARTITIONED BY (report_date DATE)
CLUSTERED BY (account_id) 
SORTED BY (account_id)
INTO 16 BUCKETS
TBLPROPERTIES ('parquet.compression' = 'zstd', 'parquet.block.size' = '268435456');
