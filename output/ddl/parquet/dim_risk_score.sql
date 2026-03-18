CREATE TABLE IF NOT EXISTS dim_risk_score (
    account_id BIGINT NOT NULL,
    fico_score INT,
    behavioral_score DECIMAL(5,2),
    delinquency_bucket STRING,
    probability_of_default DECIMAL(7,6)
)
USING PARQUET
PARTITIONED BY (scoring_date DATE)
CLUSTERED BY (account_id) 
SORTED BY (account_id)
INTO 16 BUCKETS
TBLPROPERTIES ('parquet.compression' = 'zstd', 'parquet.block.size' = '268435456');
