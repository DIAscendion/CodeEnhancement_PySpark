| Test case Id | Description | Expected output |
| ------------ | ----------- | --------------- |
| CTC_001 | Test successful Spark session initialization with active session | Spark session should be returned successfully without creating new instance |
| CTC_002 | Test Spark session creation when no active session exists | New Spark session should be created with correct configuration parameters |
| CTC_003 | Test Spark session initialization with exception handling | Exception should be raised and properly logged when Spark initialization fails |
| CTC_004 | Test sample data creation returns correct DataFrame structure | Three DataFrames should be returned with correct column names and schemas |
| CTC_005 | Test sample data creation contains expected content | DataFrames should contain expected tile_ids, event_types, and tile_categories |
| CTC_006 | Test successful source data reading with date filtering | Source data should be read and filtered correctly by process date |
| CTC_007 | Test source data reading with exception handling | Exception should be raised when data reading fails |
| CTC_008 | Test tile aggregations computation structure | Result DataFrame should have tile_id, unique_tile_views, and unique_tile_clicks columns |
| CTC_009 | Test tile aggregations computation values accuracy | Aggregated values should correctly count unique users for views and clicks per tile |
| CTC_010 | Test interstitial aggregations computation structure | Result DataFrame should have correct columns for interstitial metrics |
| CTC_011 | Test interstitial aggregations computation values accuracy | Aggregated values should correctly count unique users for interstitial events |
| CTC_012 | Test daily summary creation with metadata enrichment | Daily summary should include tile_category from metadata join |
| CTC_013 | Test daily summary with unknown tile category handling | Tiles without metadata should receive "UNKNOWN" as tile_category |
| CTC_014 | Test global KPIs computation structure | Result DataFrame should have correct columns for global metrics and CTR calculations |
| CTC_015 | Test global KPIs calculations accuracy | CTR calculations should be accurate: clicks/views and button_clicks/interstitial_views |
| CTC_016 | Test global KPIs with zero division scenarios | CTR should be 0.0 when denominator is zero to avoid division errors |
| CTC_017 | Test successful Delta table writing | Data should be written to Delta format with partition overwrite |
| CTC_018 | Test Delta table writing with exception handling | Exception should be raised and logged when write operation fails |
| CTC_019 | Test data quality validation with good data | All validation checks should pass with clean data |
| CTC_020 | Test data quality validation with bad data | Validation should detect and report data quality issues |
| CTC_021 | Test successful main ETL execution end-to-end | Complete ETL pipeline should execute successfully and return results |
| CTC_022 | Test main ETL execution with exception handling | Exception should be raised when ETL execution fails |
| CTC_023 | Test empty DataFrame handling in aggregations | Empty DataFrames should be handled gracefully without errors |
| CTC_024 | Test null value handling in data processing | Null values should be handled appropriately in aggregation functions |
| CTC_025 | Test large dataset performance | Processing should complete within reasonable time limits for large datasets |
| FTC_001 | Test duplicate tile_ids in metadata handling | System should handle duplicate metadata entries gracefully, typically using first match |
| FTC_002 | Test special characters in tile_category | Special characters, Unicode, and symbols should be preserved in tile_category |
| FTC_003 | Test very long tile_category values | Very long category names should be handled without truncation or errors |
| FTC_004 | Test LEFT JOIN functionality between tile data and metadata | JOIN should match records correctly and preserve unmatched records |
| FTC_005 | Test backward compatibility with default UNKNOWN category | Tiles without metadata should default to UNKNOWN category |
| FTC_006 | Test tile_category enrichment accuracy | Category values should match expected mappings from metadata table |
| FTC_007 | Test record count preservation during enrichment | No records should be lost or duplicated during metadata enrichment |
| FTC_008 | Test schema validation with new tile_category column | Schema should include tile_category column with correct data type |
| FTC_009 | Test inactive tile handling with is_active flag | Inactive tiles should still receive correct tile_category values |
| FTC_010 | Test error handling for malformed metadata | Pipeline should handle invalid metadata gracefully without failing |
| FTC_011 | Test performance impact of metadata enrichment | Performance impact should be minimal and within acceptable limits |
| FTC_012 | Test category-level performance metrics reporting | Metrics should be groupable and comparable by tile_category |
| FTC_013 | Test pipeline execution without schema drift errors | Complete pipeline should run without schema compatibility issues |
| FTC_014 | Test data lineage from source to target | Data should maintain integrity from source through all transformations |
| FTC_015 | Test concurrent processing scenarios | Pipeline should handle concurrent executions without data corruption |