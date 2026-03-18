CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_id BIGINT NOT NULL,
    merchant_name STRING,
    mcc_code STRING NOT NULL,
    merchant_state STRING
)
USING ICEBERG
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd', 'write.parquet.row-group-size-bytes' = '268435456', 'format-version' = '2', 'write.distribution-mode' = 'range');
