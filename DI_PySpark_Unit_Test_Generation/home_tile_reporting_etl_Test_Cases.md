| Test case Id | Description | Expected output |
| ------------ | ----------- | --------------- |
| TC_001 | Test tile events aggregation with unique user counts for TILE_VIEW and TILE_CLICK events | Correct unique counts per tile_id with distinct user aggregation |
| TC_002 | Test interstitial events aggregation for view, primary click, and secondary click flags | Accurate unique user counts for each interstitial event type per tile |
| TC_003 | Test LEFT JOIN enrichment between tile data and metadata table | Tile data enriched with correct tile_category from metadata |
| TC_004 | Test default UNKNOWN category assignment for tiles without metadata | Tiles not in metadata table receive "UNKNOWN" as tile_category |
| TC_005 | Test OUTER JOIN preservation of all tile and interstitial records | All records from both datasets preserved in combined output |
| TC_006 | Test coalesce function handling of NULL values in aggregated data | NULL values replaced with 0 for all numeric metrics |
| TC_007 | Test global KPIs calculation and CTR formula accuracy | Correct total sums and CTR calculations (clicks/views) for overall metrics |
| TC_008 | Test CTR calculation with zero denominator edge case | CTR returns 0.0 when total views is 0 to avoid division by zero |
| TC_009 | Test date filtering logic for process date parameter | Only records matching PROCESS_DATE are included in aggregations |
| TC_010 | Test output schema validation for daily summary table | Final output contains all required columns with correct data types |
| TC_011 | Test empty input data handling without pipeline failure | Pipeline completes successfully with empty result when no input data |
| TC_012 | Test duplicate user event handling with countDistinct function | Duplicate user events for same tile counted only once per user |
| TC_013 | Test logging functionality and error message generation | Appropriate log messages generated at INFO level during pipeline execution |
| TC_014 | Test data type consistency throughout transformation pipeline | Integer aggregations maintain IntegerType, strings remain StringType |
| TC_015 | Test performance characteristics with large dataset simulation | Pipeline completes within acceptable time limits for 1000+ records |
| TC_016 | Test tile_category enrichment accuracy with known mappings | Correct tile_category values assigned based on metadata mappings |
| TC_017 | Test backward compatibility with existing tile data | Existing tiles without metadata continue to process with UNKNOWN category |
| TC_018 | Test multiple tile categories in single pipeline execution | Different tile categories (Personal Finance, Health, Payments) processed correctly |
| TC_019 | Test interstitial flag combinations (view=true, primary=false, secondary=true) | Complex boolean flag combinations aggregated accurately per user |
| TC_020 | Test global aggregation across all tiles for daily summary | Sum aggregations across all tiles produce correct totals for KPIs |