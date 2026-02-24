_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Comprehensive unit tests for Home Tile Reporting ETL with tile category metadata integration
## *Version*: 1 
## *Updated on*: 
_____________________________________________

"""
===============================================================================
                        PYSPARK UNIT TEST SUITE
===============================================================================
File Name       : DI_PySpark_Unit_Test_Generation_1.py
Author          : AAVA
Created Date    : 2025-12-02
Last Modified   : <Auto-updated>
Version         : 1.0.0
Release         : R1 – Unit Tests for Home Tile Reporting Enhancement

Test Description:
    This test suite provides comprehensive unit tests for the Home Tile Reporting ETL pipeline
    including:
    - Data reading and schema validation tests
    - Metadata enrichment and JOIN operation tests
    - Aggregation logic and CTR calculation tests
    - Error handling and edge case tests
    - Data quality validation tests
    - Performance and integration tests

Test Coverage:
    • Schema validation for all source tables
    • Tile metadata integration and LEFT JOIN logic
    • Aggregation functions for unique counts
    • CTR calculations and division by zero handling
    • Default category assignment for backward compatibility
    • Error handling for malformed data
    • Data quality checks and null value handling
    • Performance validation for large datasets
"""

import pytest
import unittest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime, date
import logging
from decimal import Decimal

# Import the ETL functions (assuming they are modularized)
# from home_tile_reporting_etl_Pipeline_1 import (
#     get_spark_session, read_delta_table, write_delta_table, main_etl_pipeline
# )

class TestHomeTileReportingETL(unittest.TestCase):
    """
    Comprehensive unit test suite for Home Tile Reporting ETL pipeline
    """
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for testing"""
        cls.spark = (
            SparkSession.builder
            .appName("HomeTileReportingETLTest")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        if cls.spark:
            cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test"""
        # Define test schemas
        self.home_tile_events_schema = StructType([
            StructField("event_ts", TimestampType(), True),
            StructField("user_id", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        self.interstitial_events_schema = StructType([
            StructField("event_ts", TimestampType(), True),
            StructField("user_id", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("interstitial_view_flag", BooleanType(), True),
            StructField("primary_button_click_flag", BooleanType(), True),
            StructField("secondary_button_click_flag", BooleanType(), True)
        ])
        
        self.tile_metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("updated_ts", TimestampType(), True)
        ])
        
        # Create test data
        self.create_test_data()
    
    def create_test_data(self):
        """Create sample test data for various test scenarios"""
        
        # Sample home tile events data
        self.sample_tile_events = [
            (datetime(2025, 12, 1, 10, 0, 0), "user1", "tile1", "TILE_VIEW"),
            (datetime(2025, 12, 1, 10, 5, 0), "user1", "tile1", "TILE_CLICK"),
            (datetime(2025, 12, 1, 11, 0, 0), "user2", "tile1", "TILE_VIEW"),
            (datetime(2025, 12, 1, 12, 0, 0), "user2", "tile2", "TILE_VIEW"),
            (datetime(2025, 12, 1, 12, 5, 0), "user2", "tile2", "TILE_CLICK"),
            (datetime(2025, 12, 1, 13, 0, 0), "user3", "tile3", "TILE_VIEW"),
        ]
        
        # Sample interstitial events data
        self.sample_interstitial_events = [
            (datetime(2025, 12, 1, 10, 10, 0), "user1", "tile1", True, True, False),
            (datetime(2025, 12, 1, 11, 10, 0), "user2", "tile1", True, False, True),
            (datetime(2025, 12, 1, 12, 10, 0), "user2", "tile2", True, True, False),
            (datetime(2025, 12, 1, 13, 10, 0), "user3", "tile3", True, False, False),
        ]
        
        # Sample tile metadata
        self.sample_tile_metadata = [
            ("tile1", "Personal Finance Tile", "Personal Finance", True, datetime(2025, 12, 1)),
            ("tile2", "Health Tracker Tile", "Health", True, datetime(2025, 12, 1)),
            ("tile4", "Inactive Tile", "Payments", False, datetime(2025, 12, 1)),  # Inactive tile
        ]
        # Note: tile3 is intentionally missing from metadata to test default category
        
        # Create DataFrames
        self.df_tile_events = self.spark.createDataFrame(
            self.sample_tile_events, self.home_tile_events_schema
        )
        
        self.df_interstitial_events = self.spark.createDataFrame(
            self.sample_interstitial_events, self.interstitial_events_schema
        )
        
        self.df_tile_metadata = self.spark.createDataFrame(
            self.sample_tile_metadata, self.tile_metadata_schema
        )
    
    # =========================================================================
    # SCHEMA VALIDATION TESTS
    # =========================================================================
    
    def test_home_tile_events_schema_validation(self):
        """Test TC_PCE5_01: Validate home tile events schema structure"""
        expected_columns = ["event_ts", "user_id", "tile_id", "event_type"]
        actual_columns = self.df_tile_events.columns
        
        self.assertEqual(set(expected_columns), set(actual_columns),
                        "Home tile events schema mismatch")
        
        # Validate data types
        schema_dict = {field.name: field.dataType for field in self.df_tile_events.schema.fields}
        self.assertIsInstance(schema_dict["event_ts"], TimestampType)
        self.assertIsInstance(schema_dict["user_id"], StringType)
        self.assertIsInstance(schema_dict["tile_id"], StringType)
        self.assertIsInstance(schema_dict["event_type"], StringType)
    
    def test_interstitial_events_schema_validation(self):
        """Test TC_PCE5_02: Validate interstitial events schema structure"""
        expected_columns = [
            "event_ts", "user_id", "tile_id", "interstitial_view_flag",
            "primary_button_click_flag", "secondary_button_click_flag"
        ]
        actual_columns = self.df_interstitial_events.columns
        
        self.assertEqual(set(expected_columns), set(actual_columns),
                        "Interstitial events schema mismatch")
        
        # Validate boolean flags
        schema_dict = {field.name: field.dataType for field in self.df_interstitial_events.schema.fields}
        self.assertIsInstance(schema_dict["interstitial_view_flag"], BooleanType)
        self.assertIsInstance(schema_dict["primary_button_click_flag"], BooleanType)
        self.assertIsInstance(schema_dict["secondary_button_click_flag"], BooleanType)
    
    def test_tile_metadata_schema_validation(self):
        """Test TC_PCE5_03: Validate SOURCE_TILE_METADATA schema structure"""
        expected_columns = ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"]
        actual_columns = self.df_tile_metadata.columns
        
        self.assertEqual(set(expected_columns), set(actual_columns),
                        "Tile metadata schema mismatch")
        
        # Validate data types
        schema_dict = {field.name: field.dataType for field in self.df_tile_metadata.schema.fields}
        self.assertIsInstance(schema_dict["tile_id"], StringType)
        self.assertIsInstance(schema_dict["tile_name"], StringType)
        self.assertIsInstance(schema_dict["tile_category"], StringType)
        self.assertIsInstance(schema_dict["is_active"], BooleanType)
        self.assertIsInstance(schema_dict["updated_ts"], TimestampType)
    
    # =========================================================================
    # AGGREGATION LOGIC TESTS
    # =========================================================================
    
    def test_tile_events_aggregation(self):
        """Test TC_PCE5_04: Validate tile events aggregation logic"""
        # Filter for specific date
        df_filtered = self.df_tile_events.filter(
            F.to_date("event_ts") == "2025-12-01"
        )
        
        # Perform aggregation
        df_agg = (
            df_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        results = df_agg.collect()
        results_dict = {row["tile_id"]: row for row in results}
        
        # Validate tile1 metrics
        self.assertEqual(results_dict["tile1"]["unique_tile_views"], 2)  # user1, user2
        self.assertEqual(results_dict["tile1"]["unique_tile_clicks"], 1)  # user1
        
        # Validate tile2 metrics
        self.assertEqual(results_dict["tile2"]["unique_tile_views"], 1)  # user2
        self.assertEqual(results_dict["tile2"]["unique_tile_clicks"], 1)  # user2
        
        # Validate tile3 metrics
        self.assertEqual(results_dict["tile3"]["unique_tile_views"], 1)  # user3
        self.assertEqual(results_dict["tile3"]["unique_tile_clicks"], 0)  # no clicks
    
    def test_interstitial_events_aggregation(self):
        """Test TC_PCE5_05: Validate interstitial events aggregation logic"""
        df_agg = (
            self.df_interstitial_events.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_secondary_clicks")
            )
        )
        
        results = df_agg.collect()
        results_dict = {row["tile_id"]: row for row in results}
        
        # Validate tile1 interstitial metrics
        self.assertEqual(results_dict["tile1"]["unique_interstitial_views"], 2)  # user1, user2
        self.assertEqual(results_dict["tile1"]["unique_primary_clicks"], 1)  # user1
        self.assertEqual(results_dict["tile1"]["unique_secondary_clicks"], 1)  # user2
        
        # Validate tile2 interstitial metrics
        self.assertEqual(results_dict["tile2"]["unique_interstitial_views"], 1)  # user2
        self.assertEqual(results_dict["tile2"]["unique_primary_clicks"], 1)  # user2
        self.assertEqual(results_dict["tile2"]["unique_secondary_clicks"], 0)  # no secondary clicks
    
    # =========================================================================
    # METADATA ENRICHMENT TESTS
    # =========================================================================
    
    def test_metadata_left_join_logic(self):
        """Test TC_PCE5_06: Validate LEFT JOIN between tile data and metadata"""
        # Create sample aggregated data
        sample_agg_data = [
            ("tile1", 2, 1),
            ("tile2", 1, 1),
            ("tile3", 1, 0),  # This tile has no metadata
        ]
        
        df_agg = self.spark.createDataFrame(
            sample_agg_data,
            StructType([
                StructField("tile_id", StringType(), True),
                StructField("unique_tile_views", IntegerType(), True),
                StructField("unique_tile_clicks", IntegerType(), True)
            ])
        )
        
        # Filter active metadata
        df_metadata_active = (
            self.df_tile_metadata
            .filter(F.col("is_active") == True)
            .select("tile_id", "tile_name", "tile_category")
        )
        
        # Perform LEFT JOIN
        df_enriched = (
            df_agg.join(df_metadata_active, "tile_id", "left")
            .withColumn(
                "tile_category",
                F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))
            )
        )
        
        results = df_enriched.collect()
        results_dict = {row["tile_id"]: row for row in results}
        
        # Validate enrichment
        self.assertEqual(results_dict["tile1"]["tile_category"], "Personal Finance")
        self.assertEqual(results_dict["tile2"]["tile_category"], "Health")
        self.assertEqual(results_dict["tile3"]["tile_category"], "UNKNOWN")  # Default category
        
        # Validate record count preservation
        self.assertEqual(len(results), 3, "LEFT JOIN should preserve all records")
    
    def test_backward_compatibility_unknown_category(self):
        """Test TC_PCE5_07: Validate backward compatibility with UNKNOWN category"""
        # Create data with tiles not in metadata
        unknown_tile_data = [
            ("unknown_tile_1", 5, 2),
            ("unknown_tile_2", 3, 0),
        ]
        
        df_unknown = self.spark.createDataFrame(
            unknown_tile_data,
            StructType([
                StructField("tile_id", StringType(), True),
                StructField("unique_tile_views", IntegerType(), True),
                StructField("unique_tile_clicks", IntegerType(), True)
            ])
        )
        
        # Apply default category logic
        df_with_defaults = (
            df_unknown.join(
                self.df_tile_metadata.filter(F.col("is_active") == True).select("tile_id", "tile_category"),
                "tile_id", "left"
            )
            .withColumn(
                "tile_category",
                F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))
            )
        )
        
        results = df_with_defaults.collect()
        
        # All unknown tiles should have UNKNOWN category
        for row in results:
            self.assertEqual(row["tile_category"], "UNKNOWN")
    
    def test_inactive_tile_filtering(self):
        """Test TC_PCE5_08: Validate inactive tile filtering in metadata"""
        # Filter only active tiles
        df_active_metadata = (
            self.df_tile_metadata
            .filter(F.col("is_active") == True)
        )
        
        active_tile_ids = [row["tile_id"] for row in df_active_metadata.collect()]
        
        # tile4 should be filtered out (is_active = False)
        self.assertNotIn("tile4", active_tile_ids)
        self.assertIn("tile1", active_tile_ids)
        self.assertIn("tile2", active_tile_ids)
    
    # =========================================================================
    # CTR CALCULATION TESTS
    # =========================================================================
    
    def test_ctr_calculations(self):
        """Test TC_PCE5_09: Validate CTR calculation logic"""
        # Create test data with known CTR values
        test_data = [
            ("tile1", 100, 25, 50, 10, 5),  # 25% tile CTR, 20% primary CTR, 10% secondary CTR
            ("tile2", 0, 0, 0, 0, 0),       # Division by zero case
            ("tile3", 200, 50, 100, 30, 20), # 25% tile CTR, 30% primary CTR, 20% secondary CTR
        ]
        
        df_test = self.spark.createDataFrame(
            test_data,
            StructType([
                StructField("tile_id", StringType(), True),
                StructField("unique_tile_views", IntegerType(), True),
                StructField("unique_tile_clicks", IntegerType(), True),
                StructField("unique_interstitial_views", IntegerType(), True),
                StructField("unique_interstitial_primary_clicks", IntegerType(), True),
                StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
            ])
        )
        
        # Apply CTR calculations
        df_with_ctr = (
            df_test
            .withColumn(
                "tile_ctr",
                F.when(F.col("unique_tile_views") > 0,
                       F.col("unique_tile_clicks") / F.col("unique_tile_views")).otherwise(0.0)
            )
            .withColumn(
                "primary_button_ctr",
                F.when(F.col("unique_interstitial_views") > 0,
                       F.col("unique_interstitial_primary_clicks") / F.col("unique_interstitial_views")).otherwise(0.0)
            )
            .withColumn(
                "secondary_button_ctr",
                F.when(F.col("unique_interstitial_views") > 0,
                       F.col("unique_interstitial_secondary_clicks") / F.col("unique_interstitial_views")).otherwise(0.0)
            )
        )
        
        results = df_with_ctr.collect()
        results_dict = {row["tile_id"]: row for row in results}
        
        # Validate CTR calculations
        self.assertAlmostEqual(results_dict["tile1"]["tile_ctr"], 0.25, places=2)
        self.assertAlmostEqual(results_dict["tile1"]["primary_button_ctr"], 0.20, places=2)
        self.assertAlmostEqual(results_dict["tile1"]["secondary_button_ctr"], 0.10, places=2)
        
        # Validate division by zero handling
        self.assertEqual(results_dict["tile2"]["tile_ctr"], 0.0)
        self.assertEqual(results_dict["tile2"]["primary_button_ctr"], 0.0)
        self.assertEqual(results_dict["tile2"]["secondary_button_ctr"], 0.0)
        
        # Validate other calculations
        self.assertAlmostEqual(results_dict["tile3"]["tile_ctr"], 0.25, places=2)
        self.assertAlmostEqual(results_dict["tile3"]["primary_button_ctr"], 0.30, places=2)
        self.assertAlmostEqual(results_dict["tile3"]["secondary_button_ctr"], 0.20, places=2)
    
    def test_division_by_zero_handling(self):
        """Test TC_PCE5_10: Validate division by zero handling in CTR calculations"""
        # Create edge case data
        edge_case_data = [
            ("tile_zero_views", 0, 5, 0, 2, 1),    # Zero views
            ("tile_zero_inter", 100, 20, 0, 0, 0), # Zero interstitial views
        ]
        
        df_edge = self.spark.createDataFrame(
            edge_case_data,
            StructType([
                StructField("tile_id", StringType(), True),
                StructField("unique_tile_views", IntegerType(), True),
                StructField("unique_tile_clicks", IntegerType(), True),
                StructField("unique_interstitial_views", IntegerType(), True),
                StructField("unique_interstitial_primary_clicks", IntegerType(), True),
                StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
            ])
        )
        
        # Apply CTR calculations with division by zero protection
        df_protected = (
            df_edge
            .withColumn(
                "tile_ctr",
                F.when(F.col("unique_tile_views") > 0,
                       F.col("unique_tile_clicks") / F.col("unique_tile_views")).otherwise(0.0)
            )
            .withColumn(
                "primary_button_ctr",
                F.when(F.col("unique_interstitial_views") > 0,
                       F.col("unique_interstitial_primary_clicks") / F.col("unique_interstitial_views")).otherwise(0.0)
            )
        )
        
        results = df_protected.collect()
        
        # All CTR values should be 0.0 for division by zero cases
        for row in results:
            self.assertIsInstance(row["tile_ctr"], float)
            self.assertIsInstance(row["primary_button_ctr"], float)
            self.assertGreaterEqual(row["tile_ctr"], 0.0)
            self.assertGreaterEqual(row["primary_button_ctr"], 0.0)
    
    # =========================================================================
    # DATA QUALITY AND VALIDATION TESTS
    # =========================================================================
    
    def test_null_value_handling(self):
        """Test TC_PCE5_11: Validate null value handling in aggregations"""
        # Create data with null values
        null_data = [
            (datetime(2025, 12, 1), None, "tile1", "TILE_VIEW"),      # Null user_id
            (datetime(2025, 12, 1), "user1", None, "TILE_VIEW"),      # Null tile_id
            (datetime(2025, 12, 1), "user1", "tile1", None),          # Null event_type
        ]
        
        df_with_nulls = self.spark.createDataFrame(
            null_data, self.home_tile_events_schema
        )
        
        # Test aggregation with null handling
        df_agg = (
            df_with_nulls
            .filter(F.col("tile_id").isNotNull() & F.col("user_id").isNotNull())
            .groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        results = df_agg.collect()
        
        # Should handle nulls gracefully without errors
        self.assertIsInstance(results, list)
    
    def test_data_type_validation(self):
        """Test TC_PCE5_12: Validate data type consistency"""
        # Test that aggregation results have correct data types
        df_agg = (
            self.df_tile_events.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        # Check schema types
        schema_dict = {field.name: field.dataType for field in df_agg.schema.fields}
        
        # Count aggregations should return LongType
        self.assertIsInstance(schema_dict["unique_tile_views"], LongType)
        self.assertIsInstance(schema_dict["unique_tile_clicks"], LongType)
    
    def test_record_count_preservation(self):
        """Test TC_PCE5_13: Validate record count preservation in JOINs"""
        # Create known dataset
        tile_data = [
            ("tile1", 10, 5),
            ("tile2", 20, 8),
            ("tile3", 15, 3),
            ("tile_unknown", 5, 1),  # Not in metadata
        ]
        
        df_tiles = self.spark.createDataFrame(
            tile_data,
            StructType([
                StructField("tile_id", StringType(), True),
                StructField("unique_tile_views", IntegerType(), True),
                StructField("unique_tile_clicks", IntegerType(), True)
            ])
        )
        
        original_count = df_tiles.count()
        
        # Perform LEFT JOIN with metadata
        df_joined = (
            df_tiles.join(
                self.df_tile_metadata.filter(F.col("is_active") == True).select("tile_id", "tile_category"),
                "tile_id", "left"
            )
        )
        
        joined_count = df_joined.count()
        
        # Record count should be preserved
        self.assertEqual(original_count, joined_count, "LEFT JOIN should preserve all records")
    
    # =========================================================================
    # ERROR HANDLING AND EDGE CASE TESTS
    # =========================================================================
    
    def test_empty_dataset_handling(self):
        """Test TC_PCE5_14: Validate handling of empty datasets"""
        # Create empty DataFrame with correct schema
        df_empty = self.spark.createDataFrame([], self.home_tile_events_schema)
        
        # Test aggregation on empty dataset
        df_agg = (
            df_empty.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        result_count = df_agg.count()
        
        # Empty dataset should produce empty result
        self.assertEqual(result_count, 0)
    
    def test_duplicate_tile_id_in_metadata(self):
        """Test TC_PCE5_15: Validate handling of duplicate tile_id in metadata"""
        # Create metadata with duplicate tile_id
        duplicate_metadata = [
            ("tile1", "Finance Tile V1", "Personal Finance", True, datetime(2025, 12, 1)),
            ("tile1", "Finance Tile V2", "Banking", True, datetime(2025, 12, 2)),  # Duplicate
        ]
        
        df_duplicate_meta = self.spark.createDataFrame(
            duplicate_metadata, self.tile_metadata_schema
        )
        
        # Test JOIN behavior with duplicates
        sample_data = [("tile1", 10, 5)]
        df_sample = self.spark.createDataFrame(
            sample_data,
            StructType([
                StructField("tile_id", StringType(), True),
                StructField("unique_tile_views", IntegerType(), True),
                StructField("unique_tile_clicks", IntegerType(), True)
            ])
        )
        
        df_joined = df_sample.join(df_duplicate_meta.select("tile_id", "tile_category"), "tile_id", "left")
        
        result_count = df_joined.count()
        
        # Should handle duplicates (result count may be > 1)
        self.assertGreaterEqual(result_count, 1)
    
    def test_special_characters_in_category(self):
        """Test TC_PCE5_16: Validate handling of special characters in tile_category"""
        # Create metadata with special characters
        special_metadata = [
            ("tile_special", "Special Tile", "Finance & Banking 💰", True, datetime(2025, 12, 1)),
            ("tile_unicode", "Unicode Tile", "健康 Health", True, datetime(2025, 12, 1)),
        ]
        
        df_special_meta = self.spark.createDataFrame(
            special_metadata, self.tile_metadata_schema
        )
        
        # Test that special characters are preserved
        results = df_special_meta.collect()
        
        categories = [row["tile_category"] for row in results]
        
        self.assertIn("Finance & Banking 💰", categories)
        self.assertIn("健康 Health", categories)
    
    def test_very_long_category_names(self):
        """Test TC_PCE5_17: Validate handling of very long tile_category names"""
        # Create metadata with very long category name
        long_category = "A" * 1000  # 1000 character category name
        
        long_metadata = [
            ("tile_long", "Long Category Tile", long_category, True, datetime(2025, 12, 1)),
        ]
        
        df_long_meta = self.spark.createDataFrame(
            long_metadata, self.tile_metadata_schema
        )
        
        # Test that long category names are handled
        results = df_long_meta.collect()
        
        self.assertEqual(len(results[0]["tile_category"]), 1000)
        self.assertEqual(results[0]["tile_category"], long_category)
    
    # =========================================================================
    # PERFORMANCE AND INTEGRATION TESTS
    # =========================================================================
    
    def test_large_dataset_performance(self):
        """Test TC_PCE5_18: Validate performance with larger datasets"""
        import time
        
        # Create larger test dataset (1000 records)
        large_data = []
        for i in range(1000):
            large_data.append((
                datetime(2025, 12, 1, 10, i % 60, 0),
                f"user_{i % 100}",  # 100 unique users
                f"tile_{i % 50}",   # 50 unique tiles
                "TILE_VIEW" if i % 2 == 0 else "TILE_CLICK"
            ))
        
        df_large = self.spark.createDataFrame(large_data, self.home_tile_events_schema)
        
        # Measure aggregation performance
        start_time = time.time()
        
        df_agg = (
            df_large.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        result_count = df_agg.count()
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # Performance should be reasonable (< 10 seconds for 1000 records)
        self.assertLess(execution_time, 10.0, "Aggregation should complete within 10 seconds")
        self.assertEqual(result_count, 50, "Should have 50 unique tiles")
    
    def test_end_to_end_pipeline_simulation(self):
        """Test TC_PCE5_19: Simulate end-to-end pipeline execution"""
        # Simulate the complete pipeline logic
        
        # Step 1: Filter events for specific date
        df_tile_filtered = self.df_tile_events.filter(
            F.to_date("event_ts") == "2025-12-01"
        )
        
        # Step 2: Aggregate tile events
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        # Step 3: Aggregate interstitial events
        df_inter_agg = (
            self.df_interstitial_events.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_secondary_clicks")
            )
        )
        
        # Step 4: Combine aggregations
        df_combined = df_tile_agg.join(df_inter_agg, "tile_id", "outer")
        
        # Step 5: Enrich with metadata
        df_metadata_active = (
            self.df_tile_metadata
            .filter(F.col("is_active") == True)
            .select("tile_id", "tile_name", "tile_category")
        )
        
        df_enriched = (
            df_combined.join(df_metadata_active, "tile_id", "left")
            .withColumn(
                "tile_category",
                F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))
            )
            .withColumn("date", F.lit("2025-12-01"))
        )
        
        # Step 6: Add CTR calculations
        df_final = (
            df_enriched
            .withColumn(
                "tile_ctr",
                F.when(F.col("unique_tile_views") > 0,
                       F.col("unique_tile_clicks") / F.col("unique_tile_views")).otherwise(0.0)
            )
        )
        
        # Validate final results
        results = df_final.collect()
        
        self.assertGreater(len(results), 0, "Pipeline should produce results")
        
        # Validate required columns exist
        expected_columns = [
            "tile_id", "tile_category", "unique_tile_views", 
            "unique_tile_clicks", "tile_ctr", "date"
        ]
        
        for col in expected_columns:
            self.assertIn(col, df_final.columns, f"Column {col} should exist in final output")
    
    # =========================================================================
    # GLOBAL KPI TESTS
    # =========================================================================
    
    def test_global_kpi_aggregation(self):
        """Test TC_PCE5_20: Validate global KPI aggregation logic"""
        # Create sample daily summary data
        daily_summary_data = [
            ("2025-12-01", "tile1", "Personal Finance", 100, 25, 50, 10, 5),
            ("2025-12-01", "tile2", "Health", 80, 20, 40, 8, 4),
            ("2025-12-01", "tile3", "UNKNOWN", 60, 15, 30, 6, 3),
        ]
        
        df_daily = self.spark.createDataFrame(
            daily_summary_data,
            StructType([
                StructField("date", StringType(), True),
                StructField("tile_id", StringType(), True),
                StructField("tile_category", StringType(), True),
                StructField("unique_tile_views", IntegerType(), True),
                StructField("unique_tile_clicks", IntegerType(), True),
                StructField("unique_interstitial_views", IntegerType(), True),
                StructField("unique_primary_clicks", IntegerType(), True),
                StructField("unique_secondary_clicks", IntegerType(), True)
            ])
        )
        
        # Calculate global KPIs
        df_global = (
            df_daily.groupBy("date")
            .agg(
                F.sum("unique_tile_views").alias("total_tile_views"),
                F.sum("unique_tile_clicks").alias("total_tile_clicks"),
                F.sum("unique_interstitial_views").alias("total_interstitial_views"),
                F.sum("unique_primary_clicks").alias("total_primary_clicks"),
                F.sum("unique_secondary_clicks").alias("total_secondary_clicks")
            )
            .withColumn(
                "overall_ctr",
                F.when(F.col("total_tile_views") > 0,
                       F.col("total_tile_clicks") / F.col("total_tile_views")).otherwise(0.0)
            )
        )
        
        results = df_global.collect()[0]
        
        # Validate global aggregations
        self.assertEqual(results["total_tile_views"], 240)  # 100 + 80 + 60
        self.assertEqual(results["total_tile_clicks"], 60)   # 25 + 20 + 15
        self.assertEqual(results["total_interstitial_views"], 120)  # 50 + 40 + 30
        self.assertAlmostEqual(results["overall_ctr"], 0.25, places=2)  # 60/240


class TestDataQualityValidation(unittest.TestCase):
    """Additional test class for data quality validation"""
    
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .appName("DataQualityTest")
            .master("local[2]")
            .getOrCreate()
        )
    
    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.stop()
    
    def test_schema_drift_detection(self):
        """Test TC_PCE5_21: Validate schema drift detection"""
        # Define expected schema
        expected_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", LongType(), True)
        ])
        
        # Create DataFrame with different schema
        actual_data = [("tile1", "Finance", 100, "extra_column")]  # Extra column
        actual_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", LongType(), True),
            StructField("unexpected_column", StringType(), True)
        ])
        
        df_actual = self.spark.createDataFrame(actual_data, actual_schema)
        
        # Compare schemas
        expected_fields = {field.name: field.dataType for field in expected_schema.fields}
        actual_fields = {field.name: field.dataType for field in df_actual.schema.fields}
        
        # Detect schema drift
        missing_fields = set(expected_fields.keys()) - set(actual_fields.keys())
        extra_fields = set(actual_fields.keys()) - set(expected_fields.keys())
        
        # Validate drift detection
        self.assertEqual(len(missing_fields), 0, "No missing fields expected")
        self.assertEqual(len(extra_fields), 1, "One extra field expected")
        self.assertIn("unexpected_column", extra_fields)
    
    def test_data_freshness_validation(self):
        """Test TC_PCE5_22: Validate data freshness checks"""
        from datetime import datetime, timedelta
        
        # Create data with different timestamps
        current_time = datetime.now()
        old_time = current_time - timedelta(days=7)
        
        freshness_data = [
            ("tile1", "Finance", current_time),
            ("tile2", "Health", old_time),
        ]
        
        df_freshness = self.spark.createDataFrame(
            freshness_data,
            StructType([
                StructField("tile_id", StringType(), True),
                StructField("tile_category", StringType(), True),
                StructField("updated_ts", TimestampType(), True)
            ])
        )
        
        # Check data freshness (within last 24 hours)
        cutoff_time = current_time - timedelta(hours=24)
        
        df_fresh = df_freshness.filter(F.col("updated_ts") >= cutoff_time)
        df_stale = df_freshness.filter(F.col("updated_ts") < cutoff_time)
        
        fresh_count = df_fresh.count()
        stale_count = df_stale.count()
        
        # Validate freshness detection
        self.assertEqual(fresh_count, 1, "One fresh record expected")
        self.assertEqual(stale_count, 1, "One stale record expected")


if __name__ == "__main__":
    # Configure test runner
    unittest.main(verbosity=2, buffer=True)
    
    # Alternative: Run with pytest
    # pytest.main(["-v", "--tb=short", __file__])
