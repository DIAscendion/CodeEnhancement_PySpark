_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Technical specification for adding SOURCE_TILE_METADATA table and extending target summary with tile category
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Technical Specification for Home Tile Reporting Enhancement - Add Tile Category Metadata

## Introduction

This technical specification outlines the implementation details for PCE-5: "Add New Source Table SOURCE_TILE_METADATA and Extend Target Summary Table with Tile Category Business Request". The enhancement aims to enrich the existing home tile reporting pipeline by adding tile-level metadata to enable business categorization and improved reporting capabilities.

### Business Context
Product Analytics has requested the addition of tile-level metadata to enrich reporting dashboards. Currently, all tile metrics are aggregated only at tile_id level with no business grouping. This enhancement will enable:
- Performance metrics by category (e.g., "Personal Finance Tiles vs Health Tiles")
- Category-level CTR comparisons
- Product manager tracking of feature adoption
- Improved dashboard drilldowns in Power BI/Tableau
- Future segmentation capabilities

## Code Changes Required for the Enhancement

### 1. ETL Pipeline Modifications (home_tile_reporting_etl.py)

#### 1.1 Configuration Updates
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

#### 1.2 Source Data Reading
```python
# Add metadata table reading
df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)
```

#### 1.3 Daily Summary Aggregation Enhancement
```python
# Modified daily summary with metadata join
df_daily_summary_enhanced = (
    df_daily_summary
    .join(df_metadata.select("tile_id", "tile_category", "tile_name"), "tile_id", "left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
    .withColumn("tile_name", F.coalesce(F.col("tile_name"), F.lit("UNKNOWN")))
    .select(
        "date",
        "tile_id",
        "tile_name",
        "tile_category",
        "unique_tile_views",
        "unique_tile_clicks",
        "unique_interstitial_views",
        "unique_interstitial_primary_clicks",
        "unique_interstitial_secondary_clicks"
    )
)
```

#### 1.4 Global KPIs Enhancement
```python
# Enhanced global KPIs with category-level aggregations
df_global_enhanced = (
    df_daily_summary_enhanced.groupBy("date")
    .agg(
        F.sum("unique_tile_views").alias("total_tile_views"),
        F.sum("unique_tile_clicks").alias("total_tile_clicks"),
        F.sum("unique_interstitial_views").alias("total_interstitial_views"),
        F.sum("unique_interstitial_primary_clicks").alias("total_primary_clicks"),
        F.sum("unique_interstitial_secondary_clicks").alias("total_secondary_clicks"),
        F.countDistinct("tile_category").alias("active_categories")
    )
    .withColumn(
        "overall_ctr",
        F.when(F.col("total_tile_views") > 0,
               F.col("total_tile_clicks") / F.col("total_tile_views")).otherwise(0.0)
    )
    .withColumn(
        "overall_primary_ctr",
        F.when(F.col("total_interstitial_views") > 0,
               F.col("total_primary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
    )
    .withColumn(
        "overall_secondary_ctr",
        F.when(F.col("total_interstitial_views") > 0,
               F.col("total_secondary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
    )
)
```

### 2. Error Handling and Validation
```python
# Add validation for metadata table
def validate_metadata_table():
    metadata_count = spark.table(SOURCE_TILE_METADATA).count()
    if metadata_count == 0:
        print("WARNING: SOURCE_TILE_METADATA table is empty. All tiles will have UNKNOWN category.")
    return metadata_count > 0

# Add schema validation
def validate_target_schema(df):
    required_columns = ["date", "tile_id", "tile_name", "tile_category", 
                       "unique_tile_views", "unique_tile_clicks"]
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    return True
```

## Data Model Updates

### 1. New Source Table: SOURCE_TILE_METADATA

**Table Definition:**
```sql
CREATE TABLE IF NOT EXISTS analytics_db.SOURCE_TILE_METADATA 
(
    tile_id        STRING    COMMENT 'Tile identifier',
    tile_name      STRING    COMMENT 'User-friendly tile name',
    tile_category  STRING    COMMENT 'Business or functional category of tile',
    is_active      BOOLEAN   COMMENT 'Indicates if tile is currently active',
    updated_ts     TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Master metadata for homepage tiles, used for business categorization and reporting enrichment';
```

**Sample Data:**
```sql
INSERT INTO analytics_db.SOURCE_TILE_METADATA VALUES
('TILE_001', 'Account Balance', 'Personal Finance', true, current_timestamp()),
('TILE_002', 'Health Check Reminder', 'Health & Wellness', true, current_timestamp()),
('TILE_003', 'Payment Due Alert', 'Payments', true, current_timestamp()),
('TILE_004', 'Investment Portfolio', 'Personal Finance', true, current_timestamp()),
('TILE_005', 'Fitness Goals', 'Health & Wellness', true, current_timestamp());
```

### 2. Target Table Updates

#### 2.1 TARGET_HOME_TILE_DAILY_SUMMARY Schema Enhancement
```sql
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMNS (
    tile_name STRING COMMENT 'User-friendly name of the tile',
    tile_category STRING COMMENT 'Functional category of the tile'
);
```

**Updated Schema:**
- date (DATE) - Partition key
- tile_id (STRING) - Tile identifier
- **tile_name (STRING)** - *NEW: User-friendly tile name*
- **tile_category (STRING)** - *NEW: Business category*
- unique_tile_views (BIGINT)
- unique_tile_clicks (BIGINT)
- unique_interstitial_views (BIGINT)
- unique_interstitial_primary_clicks (BIGINT)
- unique_interstitial_secondary_clicks (BIGINT)

#### 2.2 TARGET_HOME_TILE_GLOBAL_KPIS Schema Enhancement
```sql
ALTER TABLE reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS 
ADD COLUMNS (
    active_categories BIGINT COMMENT 'Number of distinct active tile categories'
);
```

## Source-to-Target Mapping

### 1. SOURCE_TILE_METADATA to TARGET_HOME_TILE_DAILY_SUMMARY Mapping

| Source Table | Source Column | Target Table | Target Column | Transformation Rule |
|--------------|---------------|--------------|---------------|--------------------|
| SOURCE_TILE_METADATA | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping (JOIN key) |
| SOURCE_TILE_METADATA | tile_name | TARGET_HOME_TILE_DAILY_SUMMARY | tile_name | COALESCE(tile_name, 'UNKNOWN') |
| SOURCE_TILE_METADATA | tile_category | TARGET_HOME_TILE_DAILY_SUMMARY | tile_category | COALESCE(tile_category, 'UNKNOWN') |
| SOURCE_TILE_METADATA | is_active | - | - | Filter condition (is_active = true) |

### 2. Enhanced Aggregation Mapping

| Source Calculation | Target Column | Transformation Rule |
|-------------------|---------------|--------------------|
| Existing tile metrics | All existing metrics | No change - maintain backward compatibility |
| COUNT(DISTINCT tile_category) | active_categories | Count distinct categories per date |

### 3. Join Logic and Transformation Rules

#### 3.1 Primary Join Strategy
```sql
-- Left join to ensure all tiles are preserved
LEFT JOIN SOURCE_TILE_METADATA metadata 
    ON daily_summary.tile_id = metadata.tile_id 
    AND metadata.is_active = true
```

#### 3.2 Default Value Handling
- **tile_category**: Default to "UNKNOWN" when no metadata exists
- **tile_name**: Default to "UNKNOWN" when no metadata exists
- **Backward Compatibility**: All existing metrics remain unchanged

#### 3.3 Data Quality Rules
- Ensure no NULL values in tile_category (use UNKNOWN default)
- Maintain referential integrity with existing tile_id values
- Preserve historical data without backfill requirement

## Implementation Steps

### Phase 1: Infrastructure Setup
1. Create SOURCE_TILE_METADATA table in analytics_db
2. Insert initial metadata for existing tiles
3. Update target table schemas with new columns

### Phase 2: ETL Pipeline Updates
1. Modify home_tile_reporting_etl.py with enhanced logic
2. Add metadata table reading and joining
3. Update target table writing with new columns
4. Implement validation and error handling

### Phase 3: Testing and Validation
1. Unit tests for metadata joining logic
2. Integration tests for end-to-end pipeline
3. Data quality validation
4. Backward compatibility verification

## Assumptions and Constraints

### Assumptions
- SOURCE_TILE_METADATA will be maintained by the product team
- Metadata updates will be infrequent (weekly/monthly)
- All existing tile_ids will eventually have metadata entries
- Default category "UNKNOWN" is acceptable for unmapped tiles

### Constraints
- No changes to historical data (no backfill)
- Maintain backward compatibility with existing reports
- No schema drift errors during deployment
- Performance impact should be minimal (<5% increase in runtime)

### Data Governance
- tile_category values should follow standardized naming convention
- Metadata updates require approval from product team
- Regular audits to ensure metadata completeness

## Testing Strategy

### Unit Tests
```python
def test_metadata_join_with_existing_tiles():
    # Test that existing tiles get enriched with metadata
    pass

def test_metadata_join_with_missing_tiles():
    # Test that missing tiles default to UNKNOWN
    pass

def test_backward_compatibility():
    # Test that existing metrics remain unchanged
    pass

def test_schema_validation():
    # Test that new schema is correctly applied
    pass
```

### Integration Tests
- End-to-end pipeline execution with sample data
- Partition overwrite functionality with new columns
- Performance benchmarking

## References

- **JIRA Story**: PCE-5 - Add New Source Table SOURCE_TILE_METADATA and Extend Target Summary Table with Tile Category Business Request
- **Source ETL Pipeline**: home_tile_reporting_etl.py
- **Source Metadata DDL**: SOURCE_TILE_METADATA.sql
- **Target Database**: reporting_db
- **Source Database**: analytics_db

## Deployment Checklist

- [ ] Create SOURCE_TILE_METADATA table
- [ ] Insert initial metadata records
- [ ] Update target table schemas
- [ ] Deploy updated ETL pipeline
- [ ] Execute unit and integration tests
- [ ] Validate data quality and completeness
- [ ] Monitor pipeline performance
- [ ] Update documentation and data dictionary
- [ ] Notify BI team for dashboard updates