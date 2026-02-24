| Test case Id | Description | Expected output |
| ------------ | ----------- | --------------- |
| TC_001 | Test ETL class initialization with valid Spark session | ETL instance created successfully with correct Spark session and logger |
| TC_002 | Test ETL class initialization without providing Spark session | ETL instance uses active Spark session from SparkSession.getActiveSession() |
| TC_003 | Test reading source tables with valid process date | Three DataFrames returned (tile events, interstitial events, metadata) filtered by process date |
| TC_004 | Test tile events aggregation with valid data | DataFrame with unique tile views and clicks aggregated by tile_id |
| TC_005 | Test interstitial events aggregation with valid data | DataFrame with unique interstitial views, primary clicks, and secondary clicks by tile_id |
| TC_006 | Test metadata enrichment with LEFT JOIN | DataFrame enriched with tile_category, unknown tiles get 'UNKNOWN' category |
| TC_007 | Test daily summary creation with tile and interstitial data | Combined DataFrame with all metrics, outer join preserves all tile_ids |
| TC_008 | Test global KPIs calculation with valid data | Aggregated metrics with correct CTR calculations |
| TC_009 | Test global KPIs calculation with zero values | CTR values default to 0.0 when division by zero occurs |
| TC_010 | Test Delta table writing functionality | Data written to Delta table with correct partition overwrite |
| TC_011 | Test complete ETL pipeline execution success | All pipeline steps execute successfully in correct order |
| TC_012 | Test ETL pipeline execution with failure | Exception raised and logged when pipeline step fails |
| TC_013 | Test handling of empty DataFrames | Empty results returned without errors when input is empty |
| TC_014 | Test handling of null values in data | Null values handled gracefully without causing pipeline failure |
| TC_015 | Test performance with large dataset | Pipeline completes within acceptable time limits for large data volumes |
| TC_016 | Test data type validation in results | Output DataFrames have correct schema and data types |
| TC_017 | Test tile events aggregation with multiple event types | Correctly counts distinct users for TILE_VIEW and TILE_CLICK events |
| TC_018 | Test interstitial events aggregation with boolean flags | Correctly counts distinct users based on true boolean flag values |
| TC_019 | Test metadata enrichment for tiles without metadata | Tiles not found in metadata table receive 'UNKNOWN' category |
| TC_020 | Test daily summary with missing interstitial data | Tiles with only tile events get zero values for interstitial metrics |
| TC_021 | Test daily summary with missing tile data | Tiles with only interstitial events get zero values for tile metrics |
| TC_022 | Test global KPIs aggregation across multiple tiles | Correct sum aggregation of all tile metrics |
| TC_023 | Test CTR calculation with non-zero denominators | Accurate click-through rate calculations |
| TC_024 | Test primary CTR calculation for interstitial events | Correct primary button CTR calculation |
| TC_025 | Test secondary CTR calculation for interstitial events | Correct secondary button CTR calculation |
| TC_026 | Test date filtering in source table reading | Only events matching process date are included |
| TC_027 | Test coalesce function in daily summary creation | Null values replaced with zero in final output |
| TC_028 | Test logging functionality throughout pipeline | Appropriate log messages generated at each pipeline step |
| TC_029 | Test error handling in write_delta method | Errors in Delta writing are caught and logged |
| TC_030 | Test pipeline execution with different process dates | Pipeline works correctly with various date inputs |