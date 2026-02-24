| Test case Id | Description | Expected output |
| ------------ | ----------- | --------------- |
| TC_001 | Test basic tile event aggregation with multiple users and event types | Correctly counts unique tile views (2) and unique tile clicks (1) for tile_1, and unique tile views (1) and unique tile clicks (0) for tile_2 |
| TC_002 | Test basic interstitial event aggregation with various flag combinations | Correctly counts unique interstitial views (3), primary clicks (1), and secondary clicks (1) for tile_1, and all zeros for tile_2 |
| TC_003 | Test metadata enrichment with known tile categories | Successfully joins tile data with metadata and assigns correct categories: tile_1 gets 'SPORTS', tile_2 gets 'NEWS' |
| TC_004 | Test metadata enrichment with missing tile categories | Successfully handles missing metadata by assigning 'UNKNOWN' category to tiles not found in metadata table |
| TC_005 | Test outer join logic for combining tile and interstitial data | Correctly performs outer join resulting in 3 records: tile_1 with both datasets, tile_2 with only tile data (interstitial values = 0), tile_3 with only interstitial data (tile values = 0) |
| TC_006 | Test global KPIs calculation including CTR calculations | Correctly calculates totals: 150 tile views, 30 tile clicks, 140 interstitial views, 28 primary clicks, 8 secondary clicks, and CTRs: 0.2, 0.2, 0.057 respectively |
| TC_007 | Test CTR calculation edge case with zero views | Correctly handles division by zero by returning 0.0 for all CTR calculations when total views are zero |
| TC_008 | Test handling of empty input dataframes | Gracefully handles empty input data by returning empty result sets without errors |
| TC_009 | Test duplicate user events deduplication | Correctly deduplicates multiple events from same user, counting unique users (1) rather than total events (2) for both views and clicks |
| TC_010 | Test write_delta function call with correct parameters | Verifies that write operation is called with correct format ('delta'), mode ('overwrite'), partition filter, and table name |
| TC_011 | Test date filtering functionality | Verifies that only events matching PROCESS_DATE are included in aggregations |
| TC_012 | Test coalesce functionality in daily summary | Ensures null values are properly replaced with 0 for all metric columns in the final output |
| TC_013 | Test tile category preservation in joins | Verifies that tile_category is correctly preserved during outer joins and not duplicated |
| TC_014 | Test aggregation with mixed boolean flags | Validates correct counting when interstitial flags have mixed true/false values across different users |
| TC_015 | Test performance with large datasets | Ensures aggregation logic performs efficiently with high-volume data scenarios |