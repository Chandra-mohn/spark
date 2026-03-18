# Spark PDM Generator -- Design Document

## 1. Overview

### 1.1 What This Tool Does

The Spark PDM Generator is a deterministic Python rule engine that transforms a
logical or OLTP data model into a Spark/Parquet-optimized physical data model.

It accepts an Excel workbook describing entities, attributes, and relationships
(optionally with data distribution statistics and query patterns), and produces:

- A physical data model (Excel workbook)
- Spark SQL DDL statements (Parquet and Iceberg)
- ETL scripts (SQL primary, PySpark secondary)
- A transformation log explaining every decision

### 1.2 What This Tool Is Not

- **Not an LLM or AI tool** -- every decision is a deterministic rule applied to
  input data. Same input always produces the same output.
- **Not a data ingestion tool** -- it produces table definitions and ETL scripts,
  but does not move data.
- **Not a generic modeling tool** -- it is purpose-built for Spark with Parquet
  (and optionally Iceberg) as the storage layer.

### 1.3 Target Domain

Credit card issuer operational reporting at multi-terabyte scale, where a single
report may span 7+ TB with 8,000+ columns across transactional, risk, financial,
and regulatory attribute domains.

### 1.4 Design Constraints

| Constraint | Value |
|------------|-------|
| LLM usage | Zero -- fully offline, air-gap safe |
| Network access | None |
| Dependencies | openpyxl, networkx, pydantic, typer (no jinja2) |
| Max entities | ~50 |
| Max attributes | ~10,000 (8,000 per model) |
| Determinism | Same input + rules = same output, always |

---

## 2. Decision-Making Approach

The tool makes 7 sequential decisions when transforming a logical model into a
physical model. Each decision builds on the outputs of prior decisions.

### 2.1 Decision 1: Entity Classification

**Question**: What role does each entity play -- fact, dimension, bridge, or
reference?

**Why this matters**: The classification determines which transformation rules
apply. Facts get denormalized into. Dimensions get denormalized or kept separate.
Bridges are preserved. Reference tables become small broadcast dimensions.

**Decision logic**:

```
Is entity_type provided by the user?
|
|-- Yes --> Use it as-is. Log: "User-specified type preserved."
|
|-- No  --> Infer from graph structure:
      |
      |-- Has many FK parents (in_degree >= 2) AND wide (> 20 attrs)?
      |     --> FACT
      |         Rationale: sits at center of model, pulls context from
      |         surrounding entities.
      |
      |-- Very large row count (> 10M)?
      |     --> FACT
      |         Rationale: only facts grow to tens of millions of rows
      |         in reporting models.
      |
      |-- Only referenced by others (out_degree > 0), few parents?
      |     --> DIMENSION
      |         Rationale: provides descriptive context to facts.
      |
      |-- Entire PK composed of FKs to 2+ other entities,
      |   AND few non-FK attributes?
      |     --> BRIDGE
      |         Rationale: exists only to resolve M:N relationships.
      |
      |-- Very small (< 1,000 rows), only referenced, no outgoing FKs?
      |     --> REFERENCE
      |         Rationale: lookup/code table.
      |
      |-- None of the above?
            --> DIMENSION (safe default)
```

**Required input**: entity sheet (entity_name, relationships).
**Helpful input**: estimated_row_count improves accuracy.
**Logged**: Every classification with rationale (in_degree, out_degree,
attribute_count, row_count).
**Overridable**: User can specify entity_type directly in the input.

---

### 2.2 Decision 2: Denormalization

**Question**: For each relationship, should these two entities become one physical
table or remain separate?

**Why this matters**: This is the most consequential decision. It determines the
number of physical tables, their width, and whether queries require joins.
Denormalization eliminates joins at the cost of storage and rebuild complexity.

**Decision logic**:

```
For each relationship (parent -> child):
|
|-- Is this override-blocked? (BLOCK_DENORMALIZE for this entity)
|     --> KEEP SEPARATE regardless of size.
|         Log: "User override applied."
|
|-- 1:1 cardinality?
|     --> ALWAYS MERGE parent into child.
|         Rationale: same grain, zero benefit to keeping separate,
|         only adds a join. No data needed for this decision.
|
|-- 1:N cardinality (parent is the "one" side)?
|     |
|     |-- Parent estimated_row_count < threshold (default 100K)?
|     |     --> EMBED parent attributes into child (denormalize).
|     |         Rationale: join elimination benefit outweighs storage cost.
|     |
|     |         Example calculation:
|     |           card_product has 47 rows, 3 useful attributes.
|     |           Embedding into 50M-row fact adds ~30 bytes/row = 1.5GB.
|     |           Every query that touches product attributes avoids a join.
|     |           Clear win.
|     |
|     |-- Parent estimated_row_count >= threshold?
|     |     --> KEEP SEPARATE.
|     |         Rationale: storage bloat grows linearly with fact rows,
|     |         dimension changes require full fact rebuild,
|     |         join can be made efficient via bucketing.
|     |
|     |-- Parent estimated_row_count unknown?
|           --> KEEP SEPARATE with WARNING.
|               "Cannot determine denormalization viability without
|               estimated_row_count. Provide data_distribution."
|
|-- M:N cardinality?
      --> KEEP bridge table.
          Rationale: denormalization causes row explosion.
          Bridge table preserved as-is.
```

**Required input**: estimated_row_count from data_distribution sheet.
**Threshold**: Configurable via config sheet (`small_dim_row_threshold`,
default 100,000).
**Logged**: Every merge/keep decision with row count evidence.
**Overridable**: BLOCK_DENORMALIZE for specific entities.

**Why a user might override**:
- Dimension changes frequently (quarterly product refreshes)
- Dimension is shared across multiple facts (single source of truth)
- Downstream tools expect to query the dimension independently
- Regulatory requirement to keep certain data separate

---

### 2.3 Decision 3: Vertical Partitioning (Column Grouping)

**Question**: After denormalization, is any physical table too wide? If so, how
should its columns be split into domain-aligned groups?

**Why this matters**: Parquet stores data in columnar format. With 3,000+ columns:
- File footer metadata grows to 5-20MB per file (parsed on every open)
- Column chunks become too small for efficient compression
  (256MB row group / 3000 cols = 85KB per chunk -- should be 0.5-8MB)
- Schema deserialization overhead on every Spark task

**Decision logic**:

```
Count columns in this physical entity
|
|-- <= threshold (default 500)?
|     --> No split needed. Proceed.
|
|-- > threshold?
      |
      |-- Group columns by domain:
      |     1. Check domain_group tag on each attribute (most specific)
      |     2. Fall back to source entity's domain label
      |     3. Apply FORCE_DOMAIN_GROUP overrides
      |
      |-- How many groups resulted?
      |     |
      |     |-- Only 1 group?
      |     |     --> Cannot split meaningfully.
      |     |         WARN: "All columns belong to one domain.
      |     |         Add domain_group tags for finer splitting."
      |     |
      |     |-- 2+ groups?
      |           --> Split into separate physical tables.
      |               Each gets:
      |               - All primary key / join key columns (duplicated)
      |               - Its domain-specific columns
      |               - Same partition/bucket/sort scheme as parent
      |               Naming: fact_card_account__risk,
      |                       fact_card_account__financial, etc.
      |
      |-- With query_patterns (advanced):
            --> Analyze co-access affinity.
                Columns that always appear together in the same
                query patterns should stay in the same group,
                even if they belong to different domains.
```

**Threshold**: Configurable via `column_threshold_for_vertical_split`
(default 500).
**Required input**: domain_group tags on attributes, or domain on entities.
**Logged**: Each split decision with column counts per group.
**Overridable**: FORCE_DOMAIN_GROUP to move specific attributes between groups.

**Why the key columns must be duplicated**: Every domain group table needs the
join key (e.g., account_id, report_date) so that groups can be joined back
together. This is a small overhead (2-3 columns per group) that enables
independent querying of each group.

---

### 2.4 Decision 4: Horizontal Partitioning

**Question**: Which column determines the directory layout on storage (partition
pruning)?

**Why this matters**: Partition pruning is the single biggest I/O optimization in
Spark. If data is partitioned by report_date and the query filters by
report_date = '2024-06-15', Spark reads only that partition's files -- 1/365th
of total storage for daily partitions. At 7TB, this is the difference between
reading 7TB and reading 19GB.

**Decision logic (in priority order)**:

```
1. OVERRIDE present? (FORCE_PARTITION_COL)
   --> Use the specified column(s). Log override.

2. Query patterns available?
   |
   |-- Count filter attribute usage across all patterns.
   |   Weight by priority: HIGH=3x, MEDIUM=2x, LOW=1x.
   |
   |-- For the top candidate:
   |     |
   |     |-- Check cardinality from data_distribution:
   |     |     |
   |     |     |-- 1 < cardinality <= 10,000? --> ACCEPT
   |     |     |     Good partition count, good pruning.
   |     |     |
   |     |     |-- cardinality > 10,000? --> REJECT, try next candidate
   |     |     |     Too many partitions causes small file problem.
   |     |     |
   |     |     |-- cardinality = 1? --> REJECT
   |     |           No pruning benefit.
   |     |
   |     |-- Cardinality unknown but name is date-like?
   |           --> ACCEPT (dates have predictable cardinality)

3. No query patterns? Look for date columns:
   |
   |-- Column name contains: date, timestamp, _dt, _ts,
   |   report_date, effective_date, snapshot_date, etc.
   |
   |-- Prefer date columns that are part of the primary key
   |   (more likely to be the reporting grain date)

4. Nothing suitable?
   |
   --> WARN: "No partition column found. Every query reads all data.
       Add a date column or provide query_patterns."
```

**Required input**: query_patterns (for intelligent selection) or date columns
in the schema.
**Helpful input**: distinct_count from data_distribution (validates cardinality).
**Logged**: Selected column with cardinality evidence and usage count.
**Overridable**: FORCE_PARTITION_COL.

**Cardinality guidelines**:

| Cardinality | Example | Verdict |
|-------------|---------|---------|
| 1 | Single-value column | Reject -- no pruning |
| 12 | Monthly partitions | Good for monthly data |
| 365 | Daily partitions (1 year) | Ideal for daily reporting |
| 3,650 | Daily partitions (10 years) | Acceptable |
| 10,000+ | Hourly or high-cardinality | Too many -- reject |
| 50,000,000 | account_id | Never partition by this |

---

### 2.5 Decision 5: Bucketing and Sort Keys

**Question**: How should data be distributed across files within each partition
to optimize joins?

**Why this matters**: When two tables are bucketed by the same column with the
same bucket count, Spark joins them without shuffling data across the network.
Bucket 0 of Table A joins directly with Bucket 0 of Table B, all locally.
For TB-scale joins, this is the difference between 20 minutes and 2 hours.

**Bucket column selection**:

```
"What is the most common join key across tables?"
|
|-- Find primary key columns that are NOT the partition column
|     (partition column already handles row-level routing)
|
|-- Prefer non-date columns (account_id, customer_id)
|
|-- Avoid high-skew columns (one bucket would be overloaded)
|
|-- Fall back to any column with "_id" or "_key" suffix
```

**Bucket count calculation**:

```
total_data_bytes = estimated_row_count * avg_row_size_bytes
raw_count        = ceil(total_data_bytes / target_file_size_bytes)
bucket_count     = round_up_to_power_of_2(raw_count)
bucket_count     = clamp(bucket_count, min=16, max=16384)
bucket_count     = min(bucket_count, cluster_parallelism)

Example:
  50M rows * 500 bytes/row = 25 GB per daily partition
  25 GB / 512 MB target    = 49 raw buckets
  Next power of 2          = 64 buckets
```

**Why power of 2**: Spark's hash function distributes more evenly across
power-of-2 bucket counts, avoiding hot spots.

**The co-bucketing rule**: ALL tables that will be joined together MUST share
the same bucket count. If fact_card_account needs 64 buckets and dim_risk_score
needs 32, both get 64. Some of dim_risk_score's buckets will be small, but
this enables shuffle-free joins.

**Sort key selection**:

```
Primary sort:   bucket column (account_id)
                --> Enables merge join within buckets
Secondary sort: most common filter/group-by column from query patterns
                --> Enables predicate pushdown via min/max statistics
```

**Required input**: estimated_row_count for accurate bucket count.
**Configurable**: target_file_size_mb, cluster_parallelism, default_bucket_count.
**Logged**: Bucket count with calculation evidence.
**Overridable**: FORCE_BUCKET_COUNT.

---

### 2.6 Decision 6: Type Mapping and Encoding

**Question**: What Parquet data type and encoding should each column use?

**Why this matters**: The right encoding can reduce storage by 4-8x per column.
Dictionary encoding a 5-value status column across 50M rows saves ~350MB for
that single column.

**Type mapping** -- use the smallest faithful representation:

| OLTP Type | Parquet Physical | Spark SQL Type | Rationale |
|-----------|-----------------|----------------|-----------|
| VARCHAR(n) | BINARY | STRING | Variable length, no padding waste |
| CHAR(n) | BINARY | STRING | Same treatment as VARCHAR |
| DECIMAL(p,s) | FIXED_LEN_BYTE_ARRAY | DECIMAL(p,s) | Preserve exact precision |
| INTEGER | INT32 | INT | 4 bytes, covers +/- 2.1B |
| BIGINT | INT64 | BIGINT | 8 bytes, for large IDs |
| DATE | INT32 | DATE | Days since epoch |
| TIMESTAMP | INT64 | TIMESTAMP | Microseconds since epoch |
| BOOLEAN | BOOLEAN | BOOLEAN | 1 bit with RLE |
| FLOAT | FLOAT | FLOAT | IEEE 754 single |
| DOUBLE | DOUBLE | DOUBLE | IEEE 754 double |

**Key rules**:
- Never over-specify DECIMAL precision. DECIMAL(38,18) when you need (15,2)
  wastes bytes in every row.
- CHAR(1) columns with Y/N values should be mapped to BOOLEAN.
- TIMESTAMP_NTZ (no timezone) uses INT64 with TIMESTAMP_MICROS logical type,
  which is more efficient than INT96.

**Encoding selection per column**:

```
Is it BOOLEAN?
|-- Yes --> RLE (Run Length Encoding)
|           Long runs of same value (T,T,T,T,F,T,T) compress well.
|
Is it a sort key AND integer or date type?
|-- Yes --> DELTA_BINARY_PACKED
|           Store differences between consecutive values.
|           Sorted IDs: [1001, 1002, 1003] --> deltas: [1, 1, 1]
|           Extreme compression for sequential data.
|
Is cardinality known?
|-- Yes, cardinality <= 10,000 --> DICTIONARY
|     Store each distinct value once in a dictionary.
|     Each row stores only an index (1-2 bytes instead of 8-20).
|     Example: account_status has 5 values across 50M rows.
|     Without dictionary: 50M * 8 bytes = 400MB
|     With dictionary: 5 values + 50M * 1 byte = 50MB
|
|-- Yes, cardinality > 10,000 --> PLAIN
|     Dictionary too large, overhead not worth it.
|
Is it a STRING type with unknown cardinality?
|-- Yes --> DICTIONARY (Parquet auto-falls back to PLAIN if dictionary
|           exceeds page size, so this is safe)
|
Otherwise:
    --> PLAIN (store raw values, rely on compression)
```

**Required input**: distinct_count from data_distribution (for encoding).
**Threshold**: dictionary_encoding_cardinality_threshold (default 10,000).
**Logged**: Every type mapping and encoding decision per column.
**Overridable**: FORCE_ENCODING.

---

### 2.7 Decision 7: Join Strategy Between Physical Tables

**Question**: For each pair of physical tables that will be joined in queries,
what join strategy should Spark use?

**Why this matters**: The wrong join strategy on a 7TB table means hours of
unnecessary data shuffling.

**Decision logic**:

```
For each pair of remaining (non-absorbed) physical tables:
|
|-- Both bucketed by the same column with the same bucket count?
|     --> BUCKET_JOIN
|         Cost: LOW
|         No data movement. Bucket N from Table A joins with
|         Bucket N from Table B locally.
|
|-- One side has < 100K rows (small enough for executor memory)?
|     --> BROADCAST_JOIN
|         Cost: LOW
|         Small table is broadcast to all executors.
|         Large side doesn't move at all.
|
|-- Both sides large, different bucket schemes?
      --> SORT_MERGE_JOIN
          Cost: HIGH
          Both tables must be shuffled by the join key.
          This should be RARE in a well-designed physical model.
```

**The goal of the entire physical model design is to make as many joins as
possible fall into the BUCKET_JOIN or BROADCAST_JOIN categories.** The
denormalization decisions (eliminate joins), bucketing decisions (co-locate
join keys), and dimension sizing decisions (enable broadcast) all serve this
goal.

---

## 3. The Optimization Hierarchy

The 7 decisions form a layered optimization stack, from macro to micro:

```
Layer 1: DENORMALIZATION
         Eliminate joins entirely by merging entities.
         Impact: removes I/O for dimensions + join computation.

Layer 2: VERTICAL PARTITIONING
         Split wide tables so queries read fewer columns.
         Impact: 10x I/O reduction when query needs 1 of 10 domains.

Layer 3: HORIZONTAL PARTITIONING
         Divide rows into date-based directories.
         Impact: 365x I/O reduction for single-day queries.

Layer 4: BUCKETING
         Co-locate join keys across tables.
         Impact: eliminates network shuffle for joins (10x speedup).

Layer 5: SORT ORDER
         Order rows within files by join key + filter columns.
         Impact: enables predicate pushdown via min/max statistics.

Layer 6: ENCODING
         Exploit statistical properties of each column.
         Impact: 4-8x storage reduction for low-cardinality columns.

Layer 7: COMPRESSION
         Reduce bytes on disk after encoding.
         Impact: 20-30% additional reduction (ZSTD vs Snappy).
```

Decisions cascade downward: you cannot choose bucket count without knowing the
partition column (which determines per-partition data volume), and you cannot
choose encoding without knowing which column is the sort key.

---

## 4. Input Specification

### 4.1 Input Workbook Sheets

| # | Sheet | Required | Purpose |
|---|-------|----------|---------|
| 1 | entities | Yes | Entity definitions with type, domain, row counts |
| 2 | attributes | Yes | Column definitions with types, keys, domain groups |
| 3 | relationships | Yes | FK relationships with cardinality |
| 4 | data_distribution | Yes | Per-column cardinality, null %, skew |
| 5 | query_patterns | Recommended | Report filter/join/access patterns |
| 6 | config | Optional | Override default thresholds |
| 7 | rules_override | Iteration | Architect overrides from reviewing prior run |

### 4.2 Entities Sheet

| Column | Required | Default | Purpose |
|--------|----------|---------|---------|
| entity_name | Yes | -- | Unique identifier |
| entity_type | No | Inferred | FACT, DIMENSION, BRIDGE, REFERENCE |
| description | No | Empty | Documentation |
| grain_description | No | Empty | What one row represents |
| domain | No | "general" | Business domain for vertical partitioning |
| estimated_row_count | No | From distribution or 1M | Drives denormalization + bucket count |
| growth_rate | No | MODERATE | STATIC, SLOW, MODERATE, HIGH |
| update_frequency | No | DAILY | STATIC, DAILY, HOURLY, REAL_TIME |

### 4.3 Attributes Sheet

| Column | Required | Default | Purpose |
|--------|----------|---------|---------|
| entity_name | Yes | -- | FK to entities |
| attribute_name | Yes | -- | Column name |
| logical_data_type | Yes | -- | VARCHAR, DECIMAL(15,2), INTEGER, etc. |
| precision | No | From type string | For DECIMAL |
| scale | No | From type string | For DECIMAL |
| max_length | No | None | For VARCHAR |
| nullable | No | Y | Y/N |
| is_primary_key | No | Inferred from relationships | Y/N |
| is_foreign_key | No | Inferred from relationships | Y/N |
| fk_references | No | Inferred | entity.column format |
| description | No | Empty | Documentation |
| domain_group | No | Entity's domain | Sub-grouping for vertical split |

### 4.4 Relationships Sheet

| Column | Required | Default | Purpose |
|--------|----------|---------|---------|
| parent_entity | Yes | -- | The referenced entity |
| child_entity | Yes | -- | The referencing entity |
| cardinality | Yes | -- | 1:1, 1:N, M:N |
| parent_key_columns | Yes | -- | Comma-separated join columns |
| child_key_columns | Yes | -- | Comma-separated join columns |
| is_identifying | No | N | Is parent PK part of child PK? |
| description | No | Empty | Documentation |

### 4.5 Data Distribution Sheet

| Column | Required | Default | Purpose |
|--------|----------|---------|---------|
| entity_name | Yes | -- | FK to entities |
| attribute_name | Yes | -- | FK to attributes |
| distinct_count | No | None | Drives encoding selection |
| null_percentage | No | 0.0 | 0.0 to 100.0 -- identifies sparse columns |
| min_value | No | None | Range estimation |
| max_value | No | None | Range estimation |
| avg_length | No | None | Storage estimation for strings |
| skew_indicator | No | LOW | LOW, MODERATE, HIGH |

### 4.6 Query Patterns Sheet

| Column | Required | Default | Purpose |
|--------|----------|---------|---------|
| pattern_name | Yes | -- | Descriptive name |
| primary_entity | Yes | -- | Main entity being queried |
| filter_attributes | No | Empty | Comma-separated WHERE clause columns |
| group_by_attributes | No | Empty | Comma-separated GROUP BY columns |
| join_entities | No | Empty | Other entities joined |
| accessed_attributes | No | Empty | Columns read by this query |
| frequency | No | DAILY | DAILY, WEEKLY, MONTHLY, AD_HOC |
| priority | No | MEDIUM | HIGH, MEDIUM, LOW |

### 4.7 Rules Override Sheet (Iteration)

| Column | Required | Purpose |
|--------|----------|---------|
| rule_id | Yes | Unique identifier |
| override_type | Yes | The type of override |
| target | Yes | Entity or attribute being overridden |
| instruction | Yes | The override instruction |

**Supported override types**:

| Override Type | Target | Instruction | Effect |
|---------------|--------|-------------|--------|
| BLOCK_DENORMALIZE | entity_name | -- | Keep entity separate regardless of size |
| FORCE_DOMAIN_GROUP | attr1, attr2 | domain_name | Move attributes to specified domain group |
| FORCE_PARTITION_COL | entity_name | col1, col2 | Override partition column selection |
| FORCE_BUCKET_COUNT | entity_name or * | number | Override calculated bucket count |
| FORCE_COMPRESSION | entity_name | ZSTD/SNAPPY | Override compression for entity |
| FORCE_ENCODING | entity.attr | DICTIONARY/PLAIN | Override encoding for attribute |
| FORCE_ROW_GROUP_SIZE | entity_name | size_in_mb | Override row group size |

### 4.8 Column Mapper (JSON)

For users with existing workbooks whose column headers differ from the standard
names above, a JSON mapping file translates headers:

```json
{
  "sheet_names": {
    "entities": "Entity List",
    "attributes": "Columns",
    "relationships": "FK Relationships"
  },
  "entities": {
    "entity_name": "Entity Name",
    "entity_type": "Type",
    "estimated_row_count": "Est Rows"
  },
  "attributes": {
    "entity_name": "Parent Entity",
    "attribute_name": "Column Name",
    "logical_data_type": "Data Type"
  },
  "relationships": {
    "parent_entity": "Parent",
    "child_entity": "Child",
    "cardinality": "Cardinality"
  }
}
```

Fields set to `null` indicate "not present in my workbook" -- the tool applies
defaults or infers. Only the 3 user-owned sheets (entities, attributes,
relationships) are mapped. The 4 standard sheets use fixed column names.

---

## 5. Output Specification

### 5.1 Output Workbook Sheets

| Sheet | Content |
|-------|---------|
| physical_entities | Tables with partition, bucket, sort, compression specs |
| physical_attributes | Every column: Parquet type, encoding, source lineage |
| physical_relationships | Join strategies, co-bucketing flags, cost estimates |
| transformation_log | Numbered decisions with rule, rationale, evidence |
| spark_ddl | CREATE TABLE statements (inline) |
| spark_config | Recommended Spark session settings |
| warnings | Detected issues with recommendations |

### 5.2 Generated Files

```
output/
  physical_model.xlsx           -- Output workbook
  ddl/
    parquet/
      fact_card_account.sql     -- CREATE TABLE ... USING PARQUET
      dim_merchant.sql
      ...
    iceberg/                    -- If target_format includes ICEBERG
      fact_card_account.sql     -- CREATE TABLE ... USING ICEBERG
      ...
  etl/
    sql/                        -- PRIMARY output
      load_fact_card_account.sql
      ...
    pyspark/                    -- SECONDARY output
      load_fact_card_account.py
      ...
```

### 5.3 Transformation Log

The transformation log is the tool's audit trail. Each entry contains:

| Field | Content |
|-------|---------|
| log_id | Sequential number (used for override targeting) |
| rule_applied | Which rule fired (e.g., DENORMALIZE_SMALL_DIM) |
| source_entity | Entity being transformed |
| target_entity | Resulting physical entity |
| description | Human-readable: "Merged card_product into fact_card_account" |
| rationale | Evidence-based: "estimated_row_count=47 < threshold=100,000" |

This log serves two purposes:
1. **Auditability**: The architect can trace why every decision was made
2. **Iteration input**: Specific log_ids can be overridden in rules_override

---

## 6. Iteration Model

### 6.1 The Iteration Contract

```
Run 1: [model + stats + patterns]        --> [physical model + log]
                                                     |
                                          Architect reviews log
                                                     |
Run 2: [same input + rules_override]     --> [refined model]
```

### 6.2 What Changes Between Iterations

The tool is deterministic: same input produces same output. The ONLY source of
new information in iteration runs is the **rules_override sheet**, which encodes
architect judgment that the tool cannot automate:

- "Keep this dimension separate despite being small" (domain knowledge)
- "Move these attributes to a different domain group" (usage patterns)
- "Use this bucket count" (infrastructure constraints)
- "Don't partition by this column" (query pattern the tool missed)

### 6.3 Expected Iteration Count

A good first run should require **3-10 overrides**. If more than ~15 overrides
are needed, the tool's default heuristics should be tuned (adjust thresholds in
the config sheet) rather than overriding individual decisions.

---

## 7. Pipeline Architecture

### 7.1 Stage Sequence

```
Stage 1:  PARSE & VALIDATE
          Read Excel, apply column mapping, validate against Pydantic schemas.
          Output: LogicalModel (validated, normalized internal representation)

Stage 2:  BUILD ER GRAPH
          Construct networkx directed graph.
          Nodes = entities, edges = relationships.
          Output: ERGraph (traversable entity-relationship structure)

Stage 3:  CLASSIFY ENTITIES
          Detect or validate entity types (FACT/DIMENSION/BRIDGE/REFERENCE).
          Output: Updated entity types on LogicalModel

Stage 4:  DENORMALIZE
          Plan: identify 1:1 merges and small-dim embeds.
          Apply: create PhysicalEntity + PhysicalAttribute for each surviving entity.
          Output: PhysicalModel with denormalized entities and attributes

Stage 5:  VERTICAL SPLIT
          Break wide entities (> 500 cols) into domain groups.
          Output: Additional PhysicalEntities with domain-specific attributes

Stage 6:  SELECT PARTITIONS
          Choose horizontal partition columns.
          Output: partition_columns set on each PhysicalEntity

Stage 7:  CALCULATE BUCKETING
          Select bucket column, calculate bucket count.
          Output: bucket_column, bucket_count on each PhysicalEntity

Stage 8:  SELECT SORT KEYS
          Choose primary and secondary sort columns.
          Output: sort_columns on each PhysicalEntity

Stage 9:  MAP TYPES & ENCODING
          Logical types -> Parquet types + encoding per column.
          Output: parquet_type, logical_type, encoding on each PhysicalAttribute

Stage 10: BUILD RELATIONSHIPS
          Determine join strategies between physical tables.
          Output: PhysicalRelationship entries with join type and cost

Stage 11: APPLY OVERRIDES
          Apply any remaining rules_override entries.
          Output: Modified PhysicalModel

Stage 12: EMIT OUTPUTS
          Generate Excel workbook, DDL files, ETL files.
          Output: Files on disk
```

### 7.2 Module Structure

```
spark_pdm_generator/
  cli.py                        -- Typer CLI (generate + inspect commands)
  pipeline.py                   -- Orchestrates all stages
  models/
    logical.py                  -- Pydantic input models
    physical.py                 -- Pydantic output models
    graph.py                    -- networkx ER graph wrapper
  parsers/
    excel_parser.py             -- Reads + validates input workbook
    column_mapper.py            -- JSON mapping loader + header translation
    inspector.py                -- Workbook inspection + mapping template
  engine/
    classifier.py               -- Stage 3: entity type detection
    denormalizer.py             -- Stage 4: merge/embed/flatten logic
    partitioner.py              -- Stages 5-6: vertical split + partition selection
    optimizer.py                -- Stages 7-9: bucket, sort, type, encoding
    overrides.py                -- Stage 11: remaining override application
  emitters/
    excel_emitter.py            -- Output workbook generation
    ddl_builder.py              -- SQL DDL via f-string builders
    etl_builder.py              -- SQL + PySpark ETL via f-string builders
  rules/
    defaults.py                 -- Configurable thresholds and type mappings
```

---

## 8. CLI Usage

### 8.1 Generate Command

```bash
spark-pdm generate input.xlsx \
  --output output/physical_model.xlsx \
  --output-dir output/ \
  --mapping column_map.json
```

### 8.2 Inspect Command

```bash
spark-pdm inspect input.xlsx --output column_map.json
```

Reads the workbook, displays all sheet names and column headers, and generates
a best-guess mapping template JSON file for review.

---

## 9. Default Thresholds

All thresholds are configurable via the config sheet or rules_override.

| Threshold | Default | Purpose |
|-----------|---------|---------|
| small_dim_row_threshold | 100,000 | Denormalize dimensions smaller than this |
| column_threshold_for_vertical_split | 500 | Split entities wider than this |
| dictionary_encoding_cardinality_threshold | 10,000 | Use DICTIONARY below this |
| max_partition_cardinality | 10,000 | Reject partition columns above this |
| target_file_size_mb | 512 | Target Parquet file size |
| row_group_size_mb | 256 | Parquet row group size |
| default_bucket_count | 2,048 | Fallback when row count unknown |
| cluster_parallelism | 2,048 | Max bucket count ceiling |

---

## 10. Example Walkthrough

### Input: Credit Card OLTP Model

8 entities, 44 attributes, 6 relationships.

### Tool Decisions

| Decision | Action | Rationale |
|----------|--------|-----------|
| Classify | card_account = FACT | in_degree=2, 50M rows, 9 attributes |
| Classify | card_product = DIMENSION | out_degree=1, 47 rows, 4 attributes |
| Denormalize | Embed card_product into card_account | 47 rows < 100K threshold |
| Denormalize | Embed reward_program into card_account | 12 rows < 100K threshold |
| Keep separate | merchant (2M rows) | 2M >= 100K threshold |
| Keep separate | risk_score, balance_detail, collections_status | Large dimensions |
| Partition | report_date on all tables | Used in 3/4 query patterns as filter |
| Bucket | account_id, 64 buckets for card_account | 25GB / 512MB = 49, next power of 2 = 64 |
| Sort | Primary: account_id, Secondary: product_id | Most common filter after date |
| Encode | account_status: DICTIONARY | 5 distinct values < 10K threshold |
| Encode | credit_limit: PLAIN | High cardinality continuous value |
| Compress | ZSTD on all tables | I/O bound workload, best ratio |

### Output

6 physical tables, 43 attributes, 4 physical relationships, 72 logged decisions.
