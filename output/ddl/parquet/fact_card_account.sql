CREATE TABLE IF NOT EXISTS fact_card_account (
    account_id BIGINT NOT NULL,
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
USING PARQUET
PARTITIONED BY (report_date DATE)
CLUSTERED BY (account_id) 
SORTED BY (account_id, product_id)
INTO 64 BUCKETS
TBLPROPERTIES ('parquet.compression' = 'zstd', 'parquet.block.size' = '268435456');
