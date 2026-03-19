# Physical Data Model (PDM) Generation Guide

How the Spark PDM Generator transforms a logical/OLTP data model into a
Spark/Parquet-optimized physical data model, and the decisions it makes
at each stage.

---

## Overview

The generator takes a logical data model (Excel workbook) as input and
produces a set of optimized physical artifacts:

- **Physical Model Excel** -- entities, attributes, relationships,
  transformation log, Spark config, warnings
- **Parquet DDL** -- CREATE TABLE statements for Hive Metastore
- **Iceberg DDL** -- CREATE TABLE statements for Iceberg catalog
- **ETL Scripts** -- SQL and PySpark load scripts with join logic

The transformation is a deterministic, rule-based pipeline with no network
calls or LLM involvement. Every decision is logged in the transformation
log for audit and iteration.

---

## Pipeline Stages

### Stage 0a: Estimate Record Lengths

**What it does**: Calculates the estimated byte size of each record by
summing storage sizes for each attribute based on its data type.

**Key decisions**:
- INTEGER = 4 bytes, BIGINT = 8 bytes, TIMESTAMP = 8 bytes
- VARCHAR/BINARY = max_length from schema (default 256 bytes)
- DECIMAL = 16 bytes (fixed-length byte array)
- Unknown types fall back to 256 bytes

**Why it matters**: Record length drives downstream bucketing calculations
(total data size = row_count * record_length).

---

### Stage 1: Build Entity-Relationship Graph

**What it does**: Constructs a directed graph (networkx) where nodes are
entities and edges are relationships (parent -> child).

**Key decisions**:
- Edge direction follows FK direction: parent (referenced) -> child (referencing)
- Composition relationships (MongoDB embedded subdocuments) are flagged
  with `is_identifying=True`
- Graph metrics (in-degree, out-degree, attribute count) feed into
  entity classification

---

### Stage 2: Classify Entities

**What it does**: Determines the role of each entity (FACT, DIMENSION,
BRIDGE, REFERENCE) using graph structure heuristics.

**Heuristics (in order)**:
1. **BRIDGE**: Involved in M:N relationships, has 2+ FK parents, mostly FK columns
2. **REFERENCE**: Row count < 1,000, no children, fewer than 20 attributes
3. **FACT**: 2+ FK parents AND > 20 attributes, OR row count > 10 million
4. **DIMENSION**: Has children (out_degree > 0), up to 2 parents
5. **Default**: DIMENSION

**Override**: Add an `EntityType` column to the Entities sheet with explicit
values (FACT, DIMENSION, BRIDGE, REFERENCE). The classifier skips any entity
with a user-specified type.

---

### Stage 0b: Fill Missing Row Counts

**What it does**: Assigns heuristic row count estimates to entities that
have no row count in the input.

**Defaults by type**:
- FACT = 100,000,000
- DIMENSION = 100,000
- BRIDGE = 10,000,000
- REFERENCE = 1,000

**Why it matters**: Row counts drive denormalization decisions (small vs
large dimension threshold) and bucketing calculations.

---

### Stage 3: Denormalize

**What it does**: Determines which logical entities merge into shared
physical tables, reducing the number of output tables and eliminating
joins at query time.

**Three phases**:

1. **1:1 Merges** (always): Parent entities with a 1:1 relationship to a
   FACT are absorbed into the FACT table. Always applied regardless of mode.

2. **Composition Absorption** (always): Entities linked by composition
   relationships (MongoDB embedded subdocuments) are absorbed into their
   parent FACT table. Use `--flip-composition` if the input data has the
   parent/child direction inverted.

3. **Mode-Specific Absorption**:
   - **AUTO** (default): Absorbs dimension parents with row count below
     `small_dim_row_threshold` (default 100,000). Large dimensions remain
     as separate tables.
   - **AGGRESSIVE**: Absorbs ALL dimension parents into their fact tables,
     regardless of size. Also follows transitive parent chains (parent of
     parent). Maximum flattening, fewest output tables.
   - **CONSERVATIVE**: Only the 1:1 merges and compositions from phases 1-2.
     All 1:N dimensions remain separate.

**Transitive absorption**: When entity A (which already absorbed entity C)
gets absorbed into entity B, entity C's attributes are automatically
carried over to entity B. No attributes are orphaned.

**Override**: Add `BLOCK_DENORMALIZE` entries to the rules_override sheet to
keep specific entities as separate tables even in AGGRESSIVE mode.

---

### Stage 4: Vertical Split (Lite: Skipped)

**What it does**: Splits physical entities wider than
`column_threshold_for_vertical_split` (default 500 columns) into
domain-aligned sub-tables.

**Note**: Skipped in the lite pipeline. Set threshold to 99999 in config
to effectively disable in the full pipeline.

---

### Stage 5: Select Partition Columns

**What it does**: Chooses the horizontal partition column for each physical
entity to enable partition pruning at query time.

**Selection strategy (in priority order)**:
1. Most common filter attribute from query patterns (if cardinality is
   acceptable -- below `max_partition_cardinality`, default 10,000)
2. Date-like columns (preferred if part of the primary key)
3. No partitioning (warning issued)

**Why it matters**: Partition pruning is the single biggest performance
lever for large Spark tables. A well-chosen partition column means queries
skip entire directory trees of data.

---

### Stage 6: Calculate Bucketing

**What it does**: Determines how many physical files (buckets) each table
should have, and which column to bucket on.

**Bucket count formula**:
```
estimated_total_bytes = row_count * record_length_bytes
bucket_count = ceil(total_bytes / target_file_size_mb)
```

**Constraints**:
- Rounded to nearest power of 2 (ensures even distribution)
- Minimum: 16, Maximum: 16,384
- Clamped by `cluster_parallelism` (default 2,048)
- Target file size: 512 MB (default)

**Bucket column selection**: First non-partition primary key column,
preferring non-date columns. This is the column used in JOIN conditions,
so co-locating rows by this column makes joins more efficient.

---

### Stage 7: Select Sort Keys

**What it does**: Chooses the sort order within each bucket file to
optimize join performance and filter efficiency.

**Selection**:
- Primary sort = bucket column (join colocation)
- Secondary sort = most common filter/group-by column from query patterns

**Why it matters**: Sorted data enables merge joins (faster than hash
joins) and improves predicate pushdown within Parquet row groups.

---

### Stage 8: Map Types and Encoding

**What it does**: Converts logical data types to Parquet physical types
and selects column-level encoding.

**Type mapping examples**:
- VARCHAR -> BINARY (STRING)
- INTEGER -> INT32
- BIGINT -> INT64
- DECIMAL -> FIXED_LEN_BYTE_ARRAY
- TIMESTAMP -> INT96
- DATE -> INT32 (DATE)
- BOOLEAN -> BOOLEAN

**Encoding selection**:
- BOOLEAN columns -> RLE (run-length encoding)
- Low-cardinality columns (< 10,000 distinct values) -> DICTIONARY
- Sorted integer/date columns -> DELTA_BINARY_PACKED
- Everything else -> PLAIN

---

### Stage 9: Build Physical Relationships

**What it does**: Translates logical relationships into physical
relationships, accounting for entity merges and absorptions. Maps join
columns to their physical attribute names (which may include prefixes
from absorbed entities).

---

### Stage 10: Apply Remaining Overrides

**What it does**: Processes any user overrides from the rules_override
sheet that were not handled in earlier stages.

**Available overrides**:
- `FORCE_PARTITION_COL` -- override partition column selection
- `FORCE_BUCKET_COUNT` -- override calculated bucket count
- `FORCE_COMPRESSION` -- override compression codec per entity
- `FORCE_ENCODING` -- override encoding per column
- `FORCE_ROW_GROUP_SIZE` -- override row group size per entity
- `FORCE_DOMAIN_GROUP` -- override domain group assignment (for vertical split)

---

### Stage 11: Generate Spark Config

**What it does**: Produces recommended Spark session configuration
parameters tuned to the physical model.

**Configurations generated**: Shuffle partitions, AQE (Adaptive Query
Execution) settings, broadcast threshold, compression codec, Parquet
format settings, and Iceberg-specific settings when applicable.

---

### Stage 12: Emit Outputs

**What it does**: Writes all output artifacts.

**Artifacts produced**:

| Artifact | Contents |
|----------|----------|
| physical_model.xlsx | 7 sheets: entities, attributes, relationships, transformation_log, spark_ddl, spark_config, warnings |
| ddl/parquet/*.sql | One CREATE TABLE per physical entity (Hive Metastore format) |
| ddl/iceberg/*.sql | One CREATE TABLE per physical entity (Iceberg format, if target_format=BOTH or ICEBERG) |
| etl/sql/*.sql | INSERT OVERWRITE ... SELECT with LEFT JOINs for denormalized sources |
| etl/pyspark/*.py | PySpark DataFrame load scripts with chained .join() calls |

---

## Configuration

All settings can be specified in an optional `config` sheet in the input
workbook (two columns: `key` and `value`).

| Setting | Default | Description |
|---------|---------|-------------|
| target_format | PARQUET | Output formats: PARQUET, ICEBERG, or BOTH |
| denormalization_mode | AUTO | AUTO, AGGRESSIVE, or CONSERVATIVE |
| compression | ZSTD | Parquet compression: ZSTD or SNAPPY |
| small_dim_row_threshold | 100,000 | Dimensions below this are absorbed (AUTO mode) |
| column_threshold_for_vertical_split | 500 | Split tables wider than this |
| cluster_parallelism | 2,048 | Max bucket count clamp |
| target_file_size_mb | 512 | Target size per Parquet file |
| row_group_size_mb | 256 | Parquet row group size |
| default_bucket_count | 2,048 | Fallback bucket count |
| max_partition_cardinality | 10,000 | Max distinct values for partition column |

---

## Attribute Coverage Validation

After pipeline execution, the tool reports how many logical attributes
were successfully mapped to physical attributes. This catches name
mismatches, failed absorptions, or parsing gaps:

```
Attribute coverage: 4914/4915 logical attributes mapped (100%)
Unmapped attributes: 1
    EntityName.ATTRIBUTE_NAME
```

A single unmapped attribute is typically a duplicated FK column that was
intentionally skipped during denormalization (the join key already exists
on the target table).

---

## Transformation Log

Every decision the pipeline makes is recorded in the transformation_log
sheet of the output Excel. Each entry includes:

- **Rule**: Which pipeline stage made the decision
- **Source/Target Entity**: What was affected
- **Description**: What happened
- **Rationale**: Why the decision was made

This log is the primary tool for reviewing, understanding, and iterating
on the physical model. If a decision seems wrong, use overrides or config
changes to adjust, then re-run.
