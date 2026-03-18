"""Default thresholds and heuristics for the transformation engine.

All thresholds are overridable via the config sheet in the input workbook.
"""

# Denormalization thresholds
SMALL_DIM_ROW_THRESHOLD = 100_000
DEFAULT_ESTIMATED_ROW_COUNT = 1_000_000

# Vertical split thresholds
COLUMN_THRESHOLD_FOR_VERTICAL_SPLIT = 500

# Partitioning
MAX_PARTITION_CARDINALITY = 10_000

# Bucketing
DEFAULT_BUCKET_COUNT = 2048
TARGET_FILE_SIZE_MB = 512
CLUSTER_PARALLELISM = 2048
MIN_BUCKET_COUNT = 16
MAX_BUCKET_COUNT = 16384

# Encoding
DICTIONARY_ENCODING_CARDINALITY_THRESHOLD = 10_000

# Row group and file sizing
ROW_GROUP_SIZE_MB = 256
MIN_FILE_SIZE_MB = 128
MAX_FILE_SIZE_MB = 2048

# Average row size estimation (bytes) when no stats available
DEFAULT_AVG_ROW_SIZE_BYTES = 500

# Type mapping: logical type -> (parquet_type, logical_type)
TYPE_MAP: dict[str, tuple[str, str]] = {
    "INTEGER": ("INT32", "INT(32, true)"),
    "INT": ("INT32", "INT(32, true)"),
    "SMALLINT": ("INT32", "INT(16, true)"),
    "TINYINT": ("INT32", "INT(8, true)"),
    "BIGINT": ("INT64", "INT(64, true)"),
    "LONG": ("INT64", "INT(64, true)"),
    "FLOAT": ("FLOAT", "FLOAT"),
    "REAL": ("FLOAT", "FLOAT"),
    "DOUBLE": ("DOUBLE", "DOUBLE"),
    "DECIMAL": ("FIXED_LEN_BYTE_ARRAY", "DECIMAL"),
    "NUMERIC": ("FIXED_LEN_BYTE_ARRAY", "DECIMAL"),
    "VARCHAR": ("BINARY", "STRING"),
    "CHAR": ("BINARY", "STRING"),
    "STRING": ("BINARY", "STRING"),
    "TEXT": ("BINARY", "STRING"),
    "NVARCHAR": ("BINARY", "STRING"),
    "CLOB": ("BINARY", "STRING"),
    "DATE": ("INT32", "DATE"),
    "TIMESTAMP": ("INT96", "TIMESTAMP"),
    "DATETIME": ("INT96", "TIMESTAMP"),
    "TIMESTAMP_NTZ": ("INT64", "TIMESTAMP_MICROS"),
    "BOOLEAN": ("BOOLEAN", "BOOLEAN"),
    "BIT": ("BOOLEAN", "BOOLEAN"),
    "BINARY": ("BINARY", "BINARY"),
    "VARBINARY": ("BINARY", "BINARY"),
    "BLOB": ("BINARY", "BINARY"),
}

# Default domain name
DEFAULT_DOMAIN = "general"

# Prefix conventions
FACT_PREFIX = "fact_"
DIMENSION_PREFIX = "dim_"
BRIDGE_PREFIX = "bridge_"
AGGREGATE_PREFIX = "agg_"
DOMAIN_SEPARATOR = "__"

# Override type constants
OVERRIDE_BLOCK_DENORMALIZE = "BLOCK_DENORMALIZE"
OVERRIDE_FORCE_DOMAIN_GROUP = "FORCE_DOMAIN_GROUP"
OVERRIDE_FORCE_PARTITION_COL = "FORCE_PARTITION_COL"
OVERRIDE_FORCE_BUCKET_COUNT = "FORCE_BUCKET_COUNT"
OVERRIDE_FORCE_COMPRESSION = "FORCE_COMPRESSION"
OVERRIDE_FORCE_ENCODING = "FORCE_ENCODING"
OVERRIDE_FORCE_ROW_GROUP_SIZE = "FORCE_ROW_GROUP_SIZE"
