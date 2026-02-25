| Test case Id | Description | Expected output |
| ------------ | ----------- | --------------- |
| FTC_001 | Validate Spark session initialization with correct configuration | Spark session created successfully with optimized configurations including adaptive query execution, coalesce partitions, and Kryo serializer |
| FTC_002 | Validate Spark session creation handles exceptions gracefully | Exception is properly caught and re-raised with appropriate error logging when Spark initialization fails |
| FTC_003 | Validate sample data creation returns correct DataFrame structure | Three DataFrames returned with correct schemas: tile events (user_id, tile_id, event_type, event_ts), interstitial events (user_id, tile_id, flags, event_ts), and metadata (tile_id, tile_name, tile_category, is_active, updated_ts) |
| FTC_004 | Validate sample data creation contains expected content and data types | DataFrames contain expected event types (TILE_VIEW, TILE_CLICK), tile categories, and proper timestamp data types |
| FTC_005 | Validate source data reading with date filtering | Source data is successfully read and filtered by process date, returning non-empty DataFrames for tile events, interstitial events, and metadata |
| FTC_006 | Validate source data reading handles exceptions properly | Exception is properly caught and re-raised when data reading fails, with appropriate error logging |
| FTC_007 | Validate tile aggregations compute correct unique metrics | Tile aggregations DataFrame contains tile_id, unique_tile_views, and unique_tile_clicks columns with non-negative values and correct distinct user counts |
| FTC_008 | Validate tile aggregations handle empty input DataFrame | Empty input DataFrame returns empty result DataFrame with correct schema structure |
| FTC_009 | Validate interstitial aggregations compute correct unique metrics | Interstitial aggregations DataFrame contains tile_id, unique_interstitial_views, unique_interstitial_primary_clicks, and unique_interstitial_secondary_clicks with non-negative values |
| FTC_010 | Validate interstitial aggregations handle empty input DataFrame | Empty input DataFrame returns empty result DataFrame with correct schema structure |
| FTC_011 | Validate daily summary creation with tile category enrichment | Daily summary DataFrame contains all required columns including tile_category enriched from metadata table via LEFT JOIN |
| FTC_012 | Validate daily summary handles unknown tiles with UNKNOWN category fallback | Tiles not found in metadata table receive "UNKNOWN" as tile_category value, maintaining backward compatibility |
| FTC_013 | Validate global KPIs computation with correct aggregation logic | Global KPIs DataFrame contains date, total metrics, and CTR calculations with values between 0 and 1 for CTR fields |
| FTC_014 | Validate global KPIs handle zero division in CTR calculations | CTR calculations return 0.0 when denominator is zero, preventing division by zero errors |
| FTC_015 | Validate Delta table writing with partition overwrite | Data is successfully written to Delta table with partition overwrite mode and proper configuration options |
| FTC_016 | Validate Delta table writing handles exceptions properly | Exception is properly caught and re-raised when Delta table writing fails, with appropriate error logging |
| FTC_017 | Validate data quality validation with all tests passing | Data quality validation returns list of validation results with test names, pass/fail status, and descriptive messages |
| FTC_018 | Validate data quality validation detects and reports failures | Data quality validation correctly identifies issues like null tile_ids, negative metrics, and invalid CTR values |
| FTC_019 | Validate main ETL function executes successfully end-to-end | Main function completes successfully, calling all pipeline stages and writing to both target tables |
| FTC_020 | Validate main ETL function handles exceptions properly | Main function catches and re-raises exceptions with proper error logging and status reporting |
| CTC_001 | Validate handling of null values in tile aggregations | Null values in user_id, tile_id, or event_type are handled gracefully without causing pipeline failures |
| CTC_002 | Validate handling of duplicate events in aggregations | Duplicate events are properly handled using countDistinct functions, ensuring accurate unique user counts |
| CTC_003 | Validate handling of very large numbers in metrics | System can process and aggregate large datasets (1000+ users) without performance degradation or overflow errors |
| CTC_004 | Validate performance characteristics with large datasets | Pipeline processes 10,000+ records within acceptable time limits (< 30 seconds) and maintains accuracy |
| CTC_005 | Validate end-to-end pipeline execution integration | Complete pipeline from data reading to Delta table writing executes successfully with consistent data flow |
| CTC_006 | Validate data lineage consistency across pipeline stages | All source tile_ids are preserved through pipeline stages, maintaining data lineage and consistency |
| CTC_007 | Validate tile category enrichment accuracy | Tile categories from metadata table are accurately mapped to corresponding tiles in final output |
| CTC_008 | Validate LEFT JOIN behavior between tile data and metadata | LEFT JOIN preserves all tile records while enriching with available metadata, handling unmatched records appropriately |
| CTC_009 | Validate schema validation and data type consistency | All DataFrames maintain expected schemas and data types throughout pipeline transformations |
| CTC_010 | Validate error handling for malformed input data | Pipeline handles malformed or invalid input data gracefully without complete failure |
| CTC_011 | Validate CTR calculation logic and boundary conditions | CTR calculations are mathematically correct and handle edge cases like zero denominators |
| CTC_012 | Validate partition overwrite logic for Delta tables | Partition overwrite correctly replaces data for specific date partitions without affecting other partitions |
| CTC_013 | Validate logging and monitoring throughout pipeline | Appropriate log messages are generated at each pipeline stage for monitoring and debugging purposes |
| CTC_014 | Validate memory management with large datasets | Pipeline manages memory efficiently when processing large datasets without out-of-memory errors |
| CTC_015 | Validate data quality metrics and thresholds | Data quality validation checks meet defined thresholds for completeness, accuracy, and consistency |