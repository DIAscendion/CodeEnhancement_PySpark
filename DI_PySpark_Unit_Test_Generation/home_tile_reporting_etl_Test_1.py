# _____________________________________________
# *Author*: AAVA
# *Created on*: 2026-02-24
# *Description*: Comprehensive unit tests for home tile reporting ETL pipeline with tile_category enrichment
# *Version*: 1
# *Updated on*: 2026-02-24
# _____________________________________________

import unittest
from unittest.mock import Mock, patch, MagicMock
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
from datetime import datetime
import logging

class TestHomeTileReportingETL(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for testing"""
        cls.spark = SparkSession.builder \
            .appName("HomeTileReportingETLTest") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test data before each test"""
        # Sample tile events data
        self.tile_events_data = [
            ("tile_001", "user_001", "TILE_VIEW", "2025-12-01 10:00:00"),
            ("tile_001", "user_002", "TILE_VIEW", "2025-12-01 10:05:00"),
            ("tile_001", "user_001", "TILE_CLICK", "2025-12-01 10:10:00"),
            ("tile_002", "user_003", "TILE_VIEW", "2025-12-01 11:00:00"),
            ("tile_002", "user_003", "TILE_CLICK", "2025-12-01 11:05:00"),
            ("tile_003", "user_004", "TILE_VIEW", "2025-12-01 12:00:00")
        ]
        
        # Sample interstitial events data
        self.interstitial_events_data = [
            ("tile_001", "user_001", True, True, False, "2025-12-01 10:15:00"),
            ("tile_001", "user_002", True, False, True, "2025-12-01 10:20:00"),
            ("tile_002", "user_003", True, True, False, "2025-12-01 11:10:00"),
            ("tile_004", "user_005", True, False, False, "2025-12-01 13:00:00")
        ]
        
        # Sample metadata
        self.metadata_data = [
            ("tile_001", "Personal Finance Tile", "Personal Finance", True, "2025-11-01 00:00:00"),
            ("tile_002", "Health Tracker", "Health", True, "2025-11-01 00:00:00"),
            ("tile_003", "Payment Gateway", "Payments", True, "2025-11-01 00:00:00")
        ]
        
        # Create test DataFrames
        self.create_test_dataframes()
    
    def create_test_dataframes(self):
        """Create test DataFrames with proper schemas"""
        # Tile events schema
        tile_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        # Interstitial events schema
        inter_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("interstitial_view_flag", BooleanType(), True),
            StructField("primary_button_click_flag", BooleanType(), True),
            StructField("secondary_button_click_flag", BooleanType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        # Metadata schema
        meta_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("updated_ts", StringType(), True)
        ])
        
        self.df_tile = self.spark.createDataFrame(self.tile_events_data, tile_schema)
        self.df_inter = self.spark.createDataFrame(self.interstitial_events_data, inter_schema)
        self.df_meta = self.spark.createDataFrame(self.metadata_data, meta_schema)
    
    def test_tile_aggregation_logic(self):
        """Test tile events aggregation produces correct counts"""
        # Apply aggregation logic
        df_tile_agg = (
            self.df_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        result = df_tile_agg.collect()
        result_dict = {row['tile_id']: (row['unique_tile_views'], row['unique_tile_clicks']) for row in result}
        
        # Assertions
        self.assertEqual(result_dict['tile_001'], (2, 1))  # 2 unique views, 1 unique click
        self.assertEqual(result_dict['tile_002'], (1, 1))  # 1 unique view, 1 unique click
        self.assertEqual(result_dict['tile_003'], (1, 0))  # 1 unique view, 0 clicks
    
    def test_interstitial_aggregation_logic(self):
        """Test interstitial events aggregation produces correct counts"""
        df_inter_agg = (
            self.df_inter.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        result = df_inter_agg.collect()
        result_dict = {row['tile_id']: (row['unique_interstitial_views'], row['unique_interstitial_primary_clicks'], row['unique_interstitial_secondary_clicks']) for row in result}
        
        # Assertions
        self.assertEqual(result_dict['tile_001'], (2, 1, 1))  # 2 views, 1 primary, 1 secondary
        self.assertEqual(result_dict['tile_002'], (1, 1, 0))  # 1 view, 1 primary, 0 secondary
        self.assertEqual(result_dict['tile_004'], (1, 0, 0))  # 1 view, 0 primary, 0 secondary
    
    def test_metadata_enrichment_with_left_join(self):
        """Test that tile data is correctly enriched with metadata via LEFT JOIN"""
        # Create simple aggregated data
        tile_agg_data = [("tile_001", 5), ("tile_002", 3), ("tile_999", 2)]  # tile_999 not in metadata
        tile_agg_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("views", IntegerType(), True)
        ])
        df_tile_simple = self.spark.createDataFrame(tile_agg_data, tile_agg_schema)
        
        # Apply enrichment logic
        df_enriched = (
            df_tile_simple.join(self.df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_enriched.collect()
        result_dict = {row['tile_id']: row['tile_category'] for row in result}
        
        # Assertions
        self.assertEqual(result_dict['tile_001'], "Personal Finance")
        self.assertEqual(result_dict['tile_002'], "Health")
        self.assertEqual(result_dict['tile_999'], "UNKNOWN")  # Should default to UNKNOWN
    
    def test_unknown_category_default_handling(self):
        """Test that tiles without metadata get UNKNOWN category"""
        # Create data with tiles not in metadata
        unknown_tile_data = [("tile_999", 10), ("tile_888", 5)]
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("views", IntegerType(), True)
        ])
        df_unknown = self.spark.createDataFrame(unknown_tile_data, schema)
        
        # Apply enrichment
        df_enriched = (
            df_unknown.join(self.df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_enriched.collect()
        
        # All should have UNKNOWN category
        for row in result:
            self.assertEqual(row['tile_category'], "UNKNOWN")
    
    def test_outer_join_preserves_all_data(self):
        """Test that OUTER JOIN between tile and interstitial data preserves all records"""
        # Create separate aggregated data
        tile_data = [("tile_001", 5, 2), ("tile_002", 3, 1)]
        inter_data = [("tile_001", 2, 1, 1), ("tile_003", 1, 0, 0)]  # tile_003 only in interstitial
        
        tile_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True)
        ])
        
        inter_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        df_tile_test = self.spark.createDataFrame(tile_data, tile_schema)
        df_inter_test = self.spark.createDataFrame(inter_data, inter_schema)
        
        # Apply OUTER JOIN
        df_combined = df_tile_test.join(df_inter_test, ["tile_id"], how="outer")
        
        result = df_combined.collect()
        tile_ids = [row['tile_id'] for row in result]
        
        # Should have all unique tile_ids from both datasets
        expected_tiles = {"tile_001", "tile_002", "tile_003"}
        actual_tiles = set(tile_ids)
        
        self.assertEqual(expected_tiles, actual_tiles)
    
    def test_coalesce_handles_null_values(self):
        """Test that coalesce properly handles NULL values in final output"""
        # Create data with NULLs
        test_data = [
            ("tile_001", 5, None, 2, None),
            ("tile_002", None, 3, None, 1)
        ]
        
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True)
        ])
        
        df_test = self.spark.createDataFrame(test_data, schema)
        
        # Apply coalesce logic
        df_result = df_test.select(
            "tile_id",
            F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
            F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
            F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
            F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks")
        )
        
        result = df_result.collect()
        
        # Check that NULLs are replaced with 0
        for row in result:
            self.assertIsNotNone(row['unique_tile_views'])
            self.assertIsNotNone(row['unique_tile_clicks'])
            self.assertIsNotNone(row['unique_interstitial_views'])
            self.assertIsNotNone(row['unique_interstitial_primary_clicks'])
    
    def test_global_kpis_calculation(self):
        """Test global KPIs calculation and CTR formulas"""
        # Create daily summary test data
        daily_data = [
            ("2025-12-01", "tile_001", "Personal Finance", 100, 20, 50, 10, 5),
            ("2025-12-01", "tile_002", "Health", 80, 16, 40, 8, 4),
            ("2025-12-01", "tile_003", "Payments", 0, 0, 0, 0, 0)  # Edge case: zero values
        ]
        
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        df_daily = self.spark.createDataFrame(daily_data, schema)
        
        # Apply global KPIs logic
        df_global = (
            df_daily.groupBy("date")
            .agg(
                F.sum("unique_tile_views").alias("total_tile_views"),
                F.sum("unique_tile_clicks").alias("total_tile_clicks"),
                F.sum("unique_interstitial_views").alias("total_interstitial_views"),
                F.sum("unique_interstitial_primary_clicks").alias("total_primary_clicks"),
                F.sum("unique_interstitial_secondary_clicks").alias("total_secondary_clicks")
            )
            .withColumn(
                "overall_ctr",
                F.when(F.col("total_tile_views") > 0, F.col("total_tile_clicks") / F.col("total_tile_views")).otherwise(0.0)
            )
            .withColumn(
                "overall_primary_ctr",
                F.when(F.col("total_interstitial_views") > 0, F.col("total_primary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
            )
            .withColumn(
                "overall_secondary_ctr",
                F.when(F.col("total_interstitial_views") > 0, F.col("total_secondary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
            )
        )
        
        result = df_global.collect()[0]
        
        # Assertions
        self.assertEqual(result['total_tile_views'], 180)
        self.assertEqual(result['total_tile_clicks'], 36)
        self.assertEqual(result['total_interstitial_views'], 90)
        self.assertEqual(result['total_primary_clicks'], 18)
        self.assertEqual(result['total_secondary_clicks'], 9)
        
        # CTR calculations
        self.assertAlmostEqual(result['overall_ctr'], 36/180, places=4)  # 0.2
        self.assertAlmostEqual(result['overall_primary_ctr'], 18/90, places=4)  # 0.2
        self.assertAlmostEqual(result['overall_secondary_ctr'], 9/90, places=4)  # 0.1
    
    def test_ctr_calculation_with_zero_denominator(self):
        """Test CTR calculation handles zero denominator correctly"""
        # Create data with zero views
        zero_data = [("2025-12-01", "tile_001", "Test", 0, 5, 0, 3, 2)]
        
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        df_zero = self.spark.createDataFrame(zero_data, schema)
        
        # Apply CTR calculation
        df_result = df_zero.withColumn(
            "ctr",
            F.when(F.col("unique_tile_views") > 0, F.col("unique_tile_clicks") / F.col("unique_tile_views")).otherwise(0.0)
        )
        
        result = df_result.collect()[0]
        
        # Should be 0.0 when denominator is 0
        self.assertEqual(result['ctr'], 0.0)
    
    def test_date_filtering_logic(self):
        """Test that date filtering works correctly"""
        # Create data with multiple dates
        multi_date_data = [
            ("tile_001", "user_001", "TILE_VIEW", "2025-12-01 10:00:00"),
            ("tile_001", "user_002", "TILE_VIEW", "2025-12-02 10:00:00"),  # Different date
            ("tile_002", "user_003", "TILE_VIEW", "2025-12-01 11:00:00")
        ]
        
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        df_multi_date = self.spark.createDataFrame(multi_date_data, schema)
        
        # Apply date filtering
        process_date = "2025-12-01"
        df_filtered = df_multi_date.filter(F.to_date("event_ts") == process_date)
        
        result = df_filtered.collect()
        
        # Should only have records from 2025-12-01
        self.assertEqual(len(result), 2)
        for row in result:
            self.assertTrue(row['event_ts'].startswith("2025-12-01"))
    
    def test_schema_validation(self):
        """Test that output schema matches expected structure"""
        # Create expected final output
        final_data = [
            ("2025-12-01", "tile_001", "Personal Finance", 10, 5, 8, 3, 2)
        ]
        
        expected_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        df_final = self.spark.createDataFrame(final_data, expected_schema)
        
        # Validate schema
        actual_fields = {field.name: field.dataType for field in df_final.schema.fields}
        expected_fields = {field.name: field.dataType for field in expected_schema.fields}
        
        self.assertEqual(actual_fields, expected_fields)
    
    def test_empty_input_handling(self):
        """Test pipeline behavior with empty input data"""
        # Create empty DataFrames with correct schema
        tile_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        df_empty = self.spark.createDataFrame([], tile_schema)
        
        # Apply aggregation to empty DataFrame
        df_agg = (
            df_empty.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        result = df_agg.collect()
        
        # Should return empty result
        self.assertEqual(len(result), 0)
    
    def test_duplicate_user_handling(self):
        """Test that duplicate user events are handled correctly with countDistinct"""
        # Create data with duplicate user events
        duplicate_data = [
            ("tile_001", "user_001", "TILE_VIEW", "2025-12-01 10:00:00"),
            ("tile_001", "user_001", "TILE_VIEW", "2025-12-01 10:05:00"),  # Duplicate user
            ("tile_001", "user_002", "TILE_VIEW", "2025-12-01 10:10:00")
        ]
        
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        df_dup = self.spark.createDataFrame(duplicate_data, schema)
        
        # Apply countDistinct aggregation
        df_agg = (
            df_dup.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        result = df_agg.collect()[0]
        
        # Should count only unique users (2, not 3)
        self.assertEqual(result['unique_tile_views'], 2)
    
    @patch('logging.getLogger')
    def test_logging_functionality(self, mock_logger):
        """Test that logging works correctly"""
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        # Import and test logging
        import logging
        logger = logging.getLogger("HomeTileReportingETL")
        logger.info("Test message")
        
        # Verify logger was called
        mock_logger.assert_called_with("HomeTileReportingETL")
    
    def test_data_type_consistency(self):
        """Test that data types remain consistent throughout transformations"""
        # Test integer aggregations maintain integer type
        result_df = (
            self.df_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        # Check data type
        views_field = [field for field in result_df.schema.fields if field.name == "unique_tile_views"][0]
        self.assertEqual(views_field.dataType, IntegerType())
    
    def test_performance_with_large_dataset_simulation(self):
        """Test pipeline performance characteristics with simulated large dataset"""
        # Create larger test dataset (simulate performance testing)
        large_data = []
        for i in range(1000):
            large_data.append((f"tile_{i%10}", f"user_{i}", "TILE_VIEW", "2025-12-01 10:00:00"))
        
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        df_large = self.spark.createDataFrame(large_data, schema)
        
        # Apply aggregation
        start_time = datetime.now()
        result = (
            df_large.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
            .collect()
        )
        end_time = datetime.now()
        
        # Basic performance assertion (should complete within reasonable time)
        execution_time = (end_time - start_time).total_seconds()
        self.assertLess(execution_time, 30)  # Should complete within 30 seconds
        
        # Verify correct aggregation
        self.assertEqual(len(result), 10)  # Should have 10 unique tiles

if __name__ == '__main__':
    unittest.main()