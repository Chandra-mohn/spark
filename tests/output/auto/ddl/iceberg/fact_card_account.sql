CREATE TABLE IF NOT EXISTS fact_card_account (
    account_id BIGINT NOT NULL,
    report_date DATE NOT NULL,
    product_id INT NOT NULL,
    customer_name STRING,
    credit_limit DECIMAL(15,2),
    open_date DATE,
    account_status STRING NOT NULL,
    state_code STRING,
    reward_program_id INT,
    card_product_product_name STRING NOT NULL,
    card_product_product_type STRING NOT NULL,
    card_product_annual_fee DECIMAL(10,2),
    reward_program_program_id INT NOT NULL,
    reward_program_program_name STRING NOT NULL,
    reward_program_points_multiplier DECIMAL(5,2)
)
USING ICEBERG
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd', 'write.parquet.row-group-size-bytes' = '268435456', 'format-version' = '2', 'write.distribution-mode' = 'range');
