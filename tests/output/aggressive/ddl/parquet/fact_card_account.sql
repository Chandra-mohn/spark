CREATE TABLE IF NOT EXISTS fact_card_account (
    account_id BIGINT NOT NULL,
    product_id INT NOT NULL,
    customer_name STRING,
    credit_limit DECIMAL(15,2),
    reward_program_id INT,
    risk_score_fico_score INT,
    risk_score_behavioral_score DECIMAL(5,2),
    card_product_product_name STRING NOT NULL,
    card_product_product_type STRING NOT NULL,
    card_product_annual_fee DECIMAL(10,2),
    reward_program_program_id INT NOT NULL,
    reward_program_program_name STRING NOT NULL,
    reward_program_points_multiplier DECIMAL(5,2),
    merchant_merchant_id BIGINT NOT NULL,
    merchant_merchant_name STRING,
    merchant_mcc_code STRING NOT NULL
)
USING PARQUET
PARTITIONED BY (report_date DATE)
CLUSTERED BY (account_id) 
SORTED BY (account_id, product_id)
INTO 2048 BUCKETS
TBLPROPERTIES ('parquet.compression' = 'zstd', 'parquet.block.size' = '268435456');
