CREATE TABLE IF NOT EXISTS dim_risk_score (
    account_id BIGINT NOT NULL,
    scoring_date DATE NOT NULL,
    fico_score INT,
    behavioral_score DECIMAL(5,2),
    delinquency_bucket STRING,
    probability_of_default DECIMAL(7,6)
)
USING ICEBERG
PARTITIONED BY (days(scoring_date))
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd', 'write.parquet.row-group-size-bytes' = '268435456', 'format-version' = '2', 'write.distribution-mode' = 'range');
