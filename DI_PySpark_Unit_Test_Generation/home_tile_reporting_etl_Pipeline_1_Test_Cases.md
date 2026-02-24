| Test case Id | Description | Expected output |
| ------------ | ----------- | --------------- |
| TC_ETL_001 | Test tile events aggregation with distinct user counts for TILE_VIEW and TILE_CLICK events | Aggregated DataFrame with unique_tile_views and unique_tile_clicks columns showing correct distinct user counts per tile_id |
| TC_ETL_002 | Test interstitial events aggregation with distinct user counts for view and click flags | Aggregated DataFrame with unique_interstitial_views, unique_interstitial_primary_clicks, and unique_interstitial_secondary_clicks columns |
| TC_ETL_003 | Test metadata enrichment using left join with tile_category defaulting to UNKNOWN for missing records | Enriched DataFrame with tile_category column populated from metadata, defaulting to UNKNOWN for unmatched tile_ids |
| TC_ETL_004 | Test daily summary combination of tile and interstitial data using outer join | Combined DataFrame with all metrics from both tile and interstitial events, missing values filled with 0 |
| TC_ETL_005 | Test global KPIs calculation with CTR metrics and aggregation across all tiles | Global summary DataFrame with total counts and calculated CTR values (overall_ctr, overall_primary_ctr, overall_secondary_ctr) |
| TC_ETL_006 | Test CTR calculation handles division by zero gracefully | CTR values should be 0.0 when denominator (views) is zero, no division errors |
| TC_ETL_007 | Test date filtering functionality filters events to specified process date | Filtered DataFrame containing only events matching the PROCESS_DATE (2025-12-01) |
| TC_ETL_008 | Test null handling in metadata join with coalesce function | DataFrame with null tile_category values replaced with UNKNOWN using coalesce function |
| TC_ETL_009 | Test empty DataFrame handling in aggregation operations | Empty result DataFrame when input is empty, no errors during aggregation |
| TC_ETL_010 | Test duplicate user events handling with countDistinct function | Aggregation correctly counts each user only once per tile_id regardless of multiple events |
| TC_ETL_011 | Test logging functionality is properly configured and accessible | Logger instance created successfully with correct name and logging level |
| TC_ETL_012 | Test schema validation for all source DataFrames | All DataFrames have expected column names and data types matching defined schemas |
| TC_ETL_013 | Test performance with large dataset (1000+ records) | Aggregation completes successfully with correct results for large data volumes |
| TC_ETL_014 | Test write_delta function with partition overwrite logic | Data written successfully to Delta table with correct partition replacement |
| TC_ETL_015 | Test end-to-end pipeline execution with all transformations | Complete pipeline runs successfully from source data to final target tables with correct data flow |