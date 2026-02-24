_____________________________________________
## *Author*: AAVA
## *Created on*:  
## *Description*: Delta Model Changes for Home Tile Reporting - Add SOURCE_TILE_METADATA and extend summary with tile category
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Delta Model Changes SN for Home Tile Reporting Enhancement

## Summary
This document captures the data model changes required to support the addition of the SOURCE_TILE_METADATA table and the extension of the TARGET_HOME_TILE_DAILY_SUMMARY and TARGET_HOME_TILE_GLOBAL_KPIS tables with tile category and related metadata, as per the technical specification.

---

## 1. Model Ingestion
### Existing Model
- Source tables: `analytics_db.SOURCE_HOME_TILE_EVENTS`, `analytics_db.SOURCE_INTERSTITIAL_EVENTS`
- Target tables: `reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY`, `reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS`

### New Model (from Tech Spec)
- New source table: `analytics_db.SOURCE_TILE_METADATA`
- Extended target tables with new columns: `tile_name`, `tile_category`, `active_categories`

---

## 2. Spec Parsing & Mapping
### Additions
- New table: `SOURCE_TILE_METADATA` with columns:
    - tile_id STRING
    - tile_name STRING
    - tile_category STRING
    - is_active BOOLEAN
    - updated_ts TIMESTAMP
- TARGET_HOME_TILE_DAILY_SUMMARY: Add columns `tile_name`, `tile_category`
- TARGET_HOME_TILE_GLOBAL_KPIS: Add column `active_categories`

### Modifications
- Update ETL pipeline to join with metadata and enrich summary with tile_name and tile_category
- Default value for missing metadata: 'UNKNOWN'

### Deprecations
- None

---

## 3. Delta Computation
- **New Table:** `SOURCE_TILE_METADATA` (see DDL below)
- **Changed Table:** `TARGET_HOME_TILE_DAILY_SUMMARY`
    - Add columns: `tile_name` (STRING), `tile_category` (STRING)
- **Changed Table:** `TARGET_HOME_TILE_GLOBAL_KPIS`
    - Add column: `active_categories` (BIGINT)

---

## 4. Impact Assessment
- Downstream break detection: No breaking changes; new columns are additive.
- Data loss risk: None (no columns dropped or narrowed).
- Foreign key effects: Join on `tile_id` to enrich summary; referential integrity maintained.
- Platform caveats: Ensure schema evolution is supported in Delta Lake.

---

## 5. DDL/Alter Statement Generation
### Forward DDLs
```sql
-- Create new source table
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

-- Extend target summary table
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMNS (
    tile_name STRING COMMENT 'User-friendly name of the tile',
    tile_category STRING COMMENT 'Functional category of the tile'
);

-- Extend global KPIs table
ALTER TABLE reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS 
ADD COLUMNS (
    active_categories BIGINT COMMENT 'Number of distinct active tile categories'
);
```

### Rollback DDLs
```sql
-- Remove columns if rollback is required
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY DROP COLUMN tile_name;
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY DROP COLUMN tile_category;
ALTER TABLE reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS DROP COLUMN active_categories;
DROP TABLE IF EXISTS analytics_db.SOURCE_TILE_METADATA;
```

---

## 6. Documentation
### Side-by-Side Diff
| Table | Column | Before | After |
|-------|--------|--------|-------|
| TARGET_HOME_TILE_DAILY_SUMMARY | tile_name | (absent) | STRING |
| TARGET_HOME_TILE_DAILY_SUMMARY | tile_category | (absent) | STRING |
| TARGET_HOME_TILE_GLOBAL_KPIS | active_categories | (absent) | BIGINT |
| analytics_db | SOURCE_TILE_METADATA | (absent) | (new table) |

### Change Traceability Matrix
| Tech Spec Section | DDL Change | Reason |
|-------------------|------------|--------|
| Add SOURCE_TILE_METADATA | CREATE TABLE | Enable business categorization |
| Extend summary/global KPIs | ALTER TABLE ADD COLUMN | Support category-level metrics |

---

## Visual ERD Update
- SOURCE_TILE_METADATA (tile_id PK) → TARGET_HOME_TILE_DAILY_SUMMARY (tile_id FK)
- TARGET_HOME_TILE_DAILY_SUMMARY (enriched with tile_name, tile_category)
- TARGET_HOME_TILE_GLOBAL_KPIS (new active_categories metric)

---

## Versioning
- *Version*: 1
- *Changes*: Initial creation for tile metadata and category enrichment
- *Reason*: Support business request for enhanced reporting and categorization
