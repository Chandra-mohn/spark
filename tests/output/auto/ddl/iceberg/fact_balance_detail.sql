CREATE TABLE IF NOT EXISTS fact_balance_detail (
    account_id BIGINT NOT NULL,
    report_date DATE NOT NULL,
    current_balance DECIMAL(15,2),
    statement_balance DECIMAL(15,2),
    minimum_payment_due DECIMAL(10,2),
    interest_accrued DECIMAL(10,2),
    payment_amount DECIMAL(15,2)
)
USING ICEBERG
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd', 'write.parquet.row-group-size-bytes' = '268435456', 'format-version' = '2', 'write.distribution-mode' = 'range');
