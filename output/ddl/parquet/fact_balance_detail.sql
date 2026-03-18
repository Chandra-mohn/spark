CREATE TABLE IF NOT EXISTS fact_balance_detail (
    account_id BIGINT NOT NULL,
    current_balance DECIMAL(15,2),
    statement_balance DECIMAL(15,2),
    minimum_payment_due DECIMAL(10,2),
    interest_accrued DECIMAL(10,2),
    payment_amount DECIMAL(15,2)
)
USING PARQUET
PARTITIONED BY (report_date DATE)
CLUSTERED BY (account_id) 
SORTED BY (account_id)
INTO 32 BUCKETS
TBLPROPERTIES ('parquet.compression' = 'zstd', 'parquet.block.size' = '268435456');
