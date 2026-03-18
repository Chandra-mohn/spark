CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_id BIGINT NOT NULL,
    merchant_name STRING,
    mcc_code STRING NOT NULL,
    merchant_state STRING
)
USING PARQUET
CLUSTERED BY (merchant_id) 
SORTED BY (merchant_id)
INTO 16 BUCKETS
TBLPROPERTIES ('parquet.compression' = 'zstd', 'parquet.block.size' = '268435456');
