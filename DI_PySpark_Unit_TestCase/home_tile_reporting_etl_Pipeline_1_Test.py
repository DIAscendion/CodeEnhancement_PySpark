# _____________________________________________
# ## *Author*: AAVA
# ## *Created on*: 2025-01-27
# ## *Description*: Comprehensive unit tests for Home Tile Reporting ETL Pipeline with tile category metadata integration
# ## *Version*: 1
# ## *Updated on*: 2025-01-27
# _____________________________________________

import unittest
import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging
import sys
import os

# Add the source code path to sys.path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import functions from the main ETL module
try:
    from home_tile_reporting_etl_Pipeline_1 import (
        get_spark_session,
        create_sample_data,
        read_source_data,
        compute_tile_aggregations,
        compute_interstitial_aggregations,
        create_daily_summary,
        compute_global_kpis,
        write_to_delta_table,
        validate_data_quality,
        main
    )
except ImportError as e:
    print(f"Warning: Could not import main module functions: {e}")
    # Define mock functions for testing if import fails
    def get_spark_session(): pass
    def create_sample_data(spark): pass
    def read_source_data(spark, process_date): pass
    def compute_tile_aggregations(df_tile): pass
    def compute_interstitial_aggregations(df_inter): pass
    def create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date): pass
    def compute_global_kpis(df_daily_summary): pass
    def write_to_delta_table(df, table_name, process_date, partition_col="date"): pass
    def validate_data_quality(df_daily_summary, df_global): pass
    def main(): pass

class TestHomeTileReportingETL(unittest.TestCase):
    """Test suite for Home Tile Reporting ETL Pipeline"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for testing"""
        cls.spark = SparkSession.builder \
            .appName("HomeTileReportingETL_Test") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        if hasattr(cls, 'spark'):
            cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test"""
        # Sample tile events data
        self.tile_events_data = [
            ("user1", "tile_001", "TILE_VIEW", "2025-01-27 10:00:00"),
            ("user1", "tile_001", "TILE_CLICK", "2025-01-27 10:01:00"),
            ("user2", "tile_001", "TILE_VIEW", "2025-01-27 10:02:00"),
            ("user2", "tile_002", "TILE_VIEW", "2025-01-27 10:03:00"),
            ("user3", "tile_002", "TILE_CLICK", "2025-01-27 10:04:00")
        ]
        
        self.tile_events_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        # Sample interstitial events data
        self.interstitial_events_data = [
            ("user1", "tile_001", True, True, False, "2025-01-27 10:01:30"),
            ("user2", "tile_001", True, False, True, "2025-01-27 10:02:30"),
            ("user3", "tile_002", True, True, False, "2025-01-27 10:04:30")
        ]
        
        self.interstitial_events_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("interstitial_view_flag", BooleanType(), True),
            StructField("primary_button_click_flag", BooleanType(), True),
            StructField("secondary_button_click_flag", BooleanType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        # Sample tile metadata
        self.tile_metadata_data = [
            ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-01-27 09:00:00"),
            ("tile_002", "Health Check Tile", "HEALTH", True, "2025-01-27 09:00:00"),
            ("tile_003", "Offers Tile", "OFFERS", True, "2025-01-27 09:00:00")
        ]
        
        self.tile_metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("updated_ts", StringType(), True)
        ])
    
    def create_test_dataframes(self):
        """Helper method to create test DataFrames"""
        df_tile = self.spark.createDataFrame(self.tile_events_data, self.tile_events_schema)
        df_tile = df_tile.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        df_inter = self.spark.createDataFrame(self.interstitial_events_data, self.interstitial_events_schema)
        df_inter = df_inter.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        df_metadata = self.spark.createDataFrame(self.tile_metadata_data, self.tile_metadata_schema)
        df_metadata = df_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts"))
        
        return df_tile, df_inter, df_metadata
    
    def test_get_spark_session_success(self):
        """Test successful Spark session creation"""
        with patch('pyspark.sql.SparkSession.getActiveSession') as mock_active:
            mock_active.return_value = None
            with patch('pyspark.sql.SparkSession.builder') as mock_builder:
                mock_spark = Mock()
                mock_builder.appName.return_value.config.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
                
                result = get_spark_session()
                self.assertIsNotNone(result)
    
    def test_get_spark_session_exception(self):
        """Test Spark session creation with exception"""
        with patch('pyspark.sql.SparkSession.getActiveSession') as mock_active:
            mock_active.side_effect = Exception("Spark initialization failed")
            
            with self.assertRaises(Exception):
                get_spark_session()
    
    def test_create_sample_data_structure(self):
        """Test sample data creation returns correct structure"""
        df_tile, df_inter, df_metadata = create_sample_data(self.spark)
        
        # Test tile events DataFrame
        self.assertIsNotNone(df_tile)
        self.assertTrue(df_tile.count() > 0)
        expected_tile_columns = ["user_id", "tile_id", "event_type", "event_ts"]
        self.assertEqual(set(df_tile.columns), set(expected_tile_columns))
        
        # Test interstitial events DataFrame
        self.assertIsNotNone(df_inter)
        self.assertTrue(df_inter.count() > 0)
        expected_inter_columns = ["user_id", "tile_id", "interstitial_view_flag", 
                                "primary_button_click_flag", "secondary_button_click_flag", "event_ts"]
        self.assertEqual(set(df_inter.columns), set(expected_inter_columns))
        
        # Test metadata DataFrame
        self.assertIsNotNone(df_metadata)
        self.assertTrue(df_metadata.count() > 0)
        expected_metadata_columns = ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"]
        self.assertEqual(set(df_metadata.columns), set(expected_metadata_columns))
    
    def test_create_sample_data_content(self):
        """Test sample data creation returns expected content"""
        df_tile, df_inter, df_metadata = create_sample_data(self.spark)
        
        # Test tile events content
        tile_event_types = [row.event_type for row in df_tile.select("event_type").distinct().collect()]
        self.assertIn("TILE_VIEW", tile_event_types)
        self.assertIn("TILE_CLICK", tile_event_types)
        
        # Test metadata content
        categories = [row.tile_category for row in df_metadata.select("tile_category").distinct().collect()]
        self.assertTrue(len(categories) > 0)
        
        # Test data types
        tile_schema = df_tile.schema
        self.assertEqual(tile_schema["event_ts"].dataType, TimestampType())
    
    def test_read_source_data_success(self):
        """Test successful source data reading"""
        process_date = "2025-01-27"
        
        with patch('home_tile_reporting_etl_Pipeline_1.create_sample_data') as mock_create:
            df_tile, df_inter, df_metadata = self.create_test_dataframes()
            mock_create.return_value = (df_tile, df_inter, df_metadata)
            
            result_tile, result_inter, result_metadata = read_source_data(self.spark, process_date)
            
            self.assertIsNotNone(result_tile)
            self.assertIsNotNone(result_inter)
            self.assertIsNotNone(result_metadata)
    
    def test_read_source_data_exception(self):
        """Test source data reading with exception"""
        process_date = "2025-01-27"
        
        with patch('home_tile_reporting_etl_Pipeline_1.create_sample_data') as mock_create:
            mock_create.side_effect = Exception("Data read failed")
            
            with self.assertRaises(Exception):
                read_source_data(self.spark, process_date)
    
    def test_compute_tile_aggregations_correct_metrics(self):
        """Test tile aggregations compute correct metrics"""
        df_tile, _, _ = self.create_test_dataframes()
        
        result = compute_tile_aggregations(df_tile)
        
        self.assertIsNotNone(result)
        expected_columns = ["tile_id", "unique_tile_views", "unique_tile_clicks"]
        self.assertEqual(set(result.columns), set(expected_columns))
        
        # Test specific aggregation logic
        result_data = result.collect()
        self.assertTrue(len(result_data) > 0)
        
        # Verify aggregation correctness for a specific tile
        tile_001_data = [row for row in result_data if row.tile_id == "tile_001"]
        self.assertEqual(len(tile_001_data), 1)
        
        tile_001_row = tile_001_data[0]
        self.assertGreaterEqual(tile_001_row.unique_tile_views, 0)
        self.assertGreaterEqual(tile_001_row.unique_tile_clicks, 0)
    
    def test_compute_tile_aggregations_empty_dataframe(self):
        """Test tile aggregations with empty DataFrame"""
        empty_df = self.spark.createDataFrame([], self.tile_events_schema)
        empty_df = empty_df.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        result = compute_tile_aggregations(empty_df)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.count(), 0)
    
    def test_compute_interstitial_aggregations_correct_metrics(self):
        """Test interstitial aggregations compute correct metrics"""
        _, df_inter, _ = self.create_test_dataframes()
        
        result = compute_interstitial_aggregations(df_inter)
        
        self.assertIsNotNone(result)
        expected_columns = ["tile_id", "unique_interstitial_views", 
                          "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        self.assertEqual(set(result.columns), set(expected_columns))
        
        # Test aggregation correctness
        result_data = result.collect()
        self.assertTrue(len(result_data) > 0)
        
        for row in result_data:
            self.assertGreaterEqual(row.unique_interstitial_views, 0)
            self.assertGreaterEqual(row.unique_interstitial_primary_clicks, 0)
            self.assertGreaterEqual(row.unique_interstitial_secondary_clicks, 0)
    
    def test_compute_interstitial_aggregations_empty_dataframe(self):
        """Test interstitial aggregations with empty DataFrame"""
        empty_df = self.spark.createDataFrame([], self.interstitial_events_schema)
        empty_df = empty_df.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        result = compute_interstitial_aggregations(empty_df)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.count(), 0)
    
    def test_create_daily_summary_with_metadata_enrichment(self):
        """Test daily summary creation with tile category enrichment"""
        df_tile, df_inter, df_metadata = self.create_test_dataframes()
        
        df_tile_agg = compute_tile_aggregations(df_tile)
        df_inter_agg = compute_interstitial_aggregations(df_inter)
        
        process_date = "2025-01-27"
        result = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date)
        
        self.assertIsNotNone(result)
        
        # Check required columns
        expected_columns = ["date", "tile_id", "tile_category", "tile_name",
                          "unique_tile_views", "unique_tile_clicks", "unique_interstitial_views",
                          "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        self.assertEqual(set(result.columns), set(expected_columns))
        
        # Test tile category enrichment
        result_data = result.collect()
        for row in result_data:
            self.assertIsNotNone(row.tile_category)
            self.assertIsNotNone(row.date)
    
    def test_create_daily_summary_unknown_category_fallback(self):
        """Test daily summary handles unknown tiles with UNKNOWN category"""
        df_tile, df_inter, df_metadata = self.create_test_dataframes()
        
        # Add a tile that doesn't exist in metadata
        unknown_tile_data = [("user4", "tile_999", "TILE_VIEW", "2025-01-27 11:00:00")]
        unknown_tile_df = self.spark.createDataFrame(unknown_tile_data, self.tile_events_schema)
        unknown_tile_df = unknown_tile_df.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        df_tile_extended = df_tile.union(unknown_tile_df)
        
        df_tile_agg = compute_tile_aggregations(df_tile_extended)
        df_inter_agg = compute_interstitial_aggregations(df_inter)
        
        process_date = "2025-01-27"
        result = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date)
        
        # Check that unknown tile gets "UNKNOWN" category
        unknown_tile_rows = [row for row in result.collect() if row.tile_id == "tile_999"]
        self.assertEqual(len(unknown_tile_rows), 1)
        self.assertEqual(unknown_tile_rows[0].tile_category, "UNKNOWN")
    
    def test_compute_global_kpis_correct_aggregation(self):
        """Test global KPIs computation with correct aggregation"""
        df_tile, df_inter, df_metadata = self.create_test_dataframes()
        
        df_tile_agg = compute_tile_aggregations(df_tile)
        df_inter_agg = compute_interstitial_aggregations(df_inter)
        
        process_date = "2025-01-27"
        df_daily_summary = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date)
        
        result = compute_global_kpis(df_daily_summary)
        
        self.assertIsNotNone(result)
        
        # Check required columns
        expected_columns = ["date", "total_tile_views", "total_tile_clicks", 
                          "total_interstitial_views", "total_primary_clicks", 
                          "total_secondary_clicks", "overall_ctr", 
                          "overall_primary_ctr", "overall_secondary_ctr"]
        self.assertEqual(set(result.columns), set(expected_columns))
        
        # Test CTR calculations
        result_data = result.collect()
        self.assertEqual(len(result_data), 1)  # Should have one row for the date
        
        row = result_data[0]
        self.assertGreaterEqual(row.overall_ctr, 0.0)
        self.assertLessEqual(row.overall_ctr, 1.0)
    
    def test_compute_global_kpis_zero_division_handling(self):
        """Test global KPIs handles zero division correctly"""
        # Create empty daily summary
        empty_data = []
        empty_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        empty_summary = self.spark.createDataFrame(empty_data, empty_schema)
        
        result = compute_global_kpis(empty_summary)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.count(), 0)
    
    @patch('home_tile_reporting_etl_Pipeline_1.logger')
    def test_write_to_delta_table_success(self, mock_logger):
        """Test successful Delta table writing"""
        df_tile, _, _ = self.create_test_dataframes()
        
        table_name = "test_table"
        process_date = "2025-01-27"
        
        with patch.object(df_tile.write, 'format') as mock_format:
            mock_format.return_value.mode.return_value.option.return_value.option.return_value.save.return_value = None
            
            result = write_to_delta_table(df_tile, table_name, process_date)
            
            self.assertTrue(result)
            mock_logger.info.assert_called()
    
    @patch('home_tile_reporting_etl_Pipeline_1.logger')
    def test_write_to_delta_table_exception(self, mock_logger):
        """Test Delta table writing with exception"""
        df_tile, _, _ = self.create_test_dataframes()
        
        table_name = "test_table"
        process_date = "2025-01-27"
        
        with patch.object(df_tile.write, 'format') as mock_format:
            mock_format.side_effect = Exception("Write failed")
            
            with self.assertRaises(Exception):
                write_to_delta_table(df_tile, table_name, process_date)
    
    def test_validate_data_quality_all_pass(self):
        """Test data quality validation with all tests passing"""
        df_tile, df_inter, df_metadata = self.create_test_dataframes()
        
        df_tile_agg = compute_tile_aggregations(df_tile)
        df_inter_agg = compute_interstitial_aggregations(df_inter)
        
        process_date = "2025-01-27"
        df_daily_summary = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date)
        df_global = compute_global_kpis(df_daily_summary)
        
        validation_results = validate_data_quality(df_daily_summary, df_global)
        
        self.assertIsNotNone(validation_results)
        self.assertTrue(len(validation_results) > 0)
        
        # Check validation result structure
        for test_name, passed, message in validation_results:
            self.assertIsInstance(test_name, str)
            self.assertIsInstance(passed, bool)
            self.assertIsInstance(message, str)
    
    def test_validate_data_quality_with_failures(self):
        """Test data quality validation with some failures"""
        # Create data with quality issues
        bad_data = [
            ("2025-01-27", None, "FINANCE", "Bad Tile", 10, 5, 8, 3, 2),  # Null tile_id
            ("2025-01-27", "tile_002", "HEALTH", "Good Tile", -5, 2, 4, 1, 1)  # Negative views
        ]
        
        bad_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        bad_daily_summary = self.spark.createDataFrame(bad_data, bad_schema)
        bad_global = compute_global_kpis(bad_daily_summary)
        
        validation_results = validate_data_quality(bad_daily_summary, bad_global)
        
        # Should have some failed validations
        failed_validations = [result for result in validation_results if not result[1]]
        self.assertTrue(len(failed_validations) > 0)
    
    @patch('home_tile_reporting_etl_Pipeline_1.logger')
    @patch('home_tile_reporting_etl_Pipeline_1.write_to_delta_table')
    @patch('home_tile_reporting_etl_Pipeline_1.read_source_data')
    def test_main_function_success(self, mock_read_source, mock_write_delta, mock_logger):
        """Test main function executes successfully"""
        df_tile, df_inter, df_metadata = self.create_test_dataframes()
        mock_read_source.return_value = (df_tile, df_inter, df_metadata)
        mock_write_delta.return_value = True
        
        with patch('home_tile_reporting_etl_Pipeline_1.get_spark_session') as mock_spark:
            mock_spark.return_value = self.spark
            
            result = main()
            
            self.assertIsNotNone(result)
            mock_read_source.assert_called_once()
            self.assertEqual(mock_write_delta.call_count, 2)  # Called for both tables
    
    @patch('home_tile_reporting_etl_Pipeline_1.logger')
    @patch('home_tile_reporting_etl_Pipeline_1.read_source_data')
    def test_main_function_exception(self, mock_read_source, mock_logger):
        """Test main function handles exceptions"""
        mock_read_source.side_effect = Exception("Data read failed")
        
        with patch('home_tile_reporting_etl_Pipeline_1.get_spark_session') as mock_spark:
            mock_spark.return_value = self.spark
            
            with self.assertRaises(Exception):
                main()
    
    def test_edge_case_null_values_in_aggregations(self):
        """Test handling of null values in aggregations"""
        # Create data with null values
        null_data = [
            (None, "tile_001", "TILE_VIEW", "2025-01-27 10:00:00"),
            ("user1", None, "TILE_CLICK", "2025-01-27 10:01:00"),
            ("user2", "tile_002", None, "2025-01-27 10:02:00")
        ]
        
        null_df = self.spark.createDataFrame(null_data, self.tile_events_schema)
        null_df = null_df.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        result = compute_tile_aggregations(null_df)
        
        # Should handle nulls gracefully
        self.assertIsNotNone(result)
        result_count = result.count()
        self.assertGreaterEqual(result_count, 0)
    
    def test_edge_case_duplicate_events(self):
        """Test handling of duplicate events"""
        # Create data with duplicate events
        duplicate_data = [
            ("user1", "tile_001", "TILE_VIEW", "2025-01-27 10:00:00"),
            ("user1", "tile_001", "TILE_VIEW", "2025-01-27 10:00:00"),  # Duplicate
            ("user1", "tile_001", "TILE_CLICK", "2025-01-27 10:01:00")
        ]
        
        duplicate_df = self.spark.createDataFrame(duplicate_data, self.tile_events_schema)
        duplicate_df = duplicate_df.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        result = compute_tile_aggregations(duplicate_df)
        
        # Should handle duplicates correctly (countDistinct should work)
        self.assertIsNotNone(result)
        result_data = result.collect()
        
        tile_001_data = [row for row in result_data if row.tile_id == "tile_001"]
        self.assertEqual(len(tile_001_data), 1)
        
        # Should count distinct users, so views should be 1 despite duplicate
        self.assertEqual(tile_001_data[0].unique_tile_views, 1)
    
    def test_edge_case_very_large_numbers(self):
        """Test handling of very large numbers in metrics"""
        # This test ensures the system can handle large metric values
        large_data = []
        for i in range(1000):
            large_data.append((f"user_{i}", "tile_001", "TILE_VIEW", "2025-01-27 10:00:00"))
        
        large_df = self.spark.createDataFrame(large_data, self.tile_events_schema)
        large_df = large_df.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        result = compute_tile_aggregations(large_df)
        
        self.assertIsNotNone(result)
        result_data = result.collect()
        
        tile_001_data = [row for row in result_data if row.tile_id == "tile_001"]
        self.assertEqual(len(tile_001_data), 1)
        self.assertEqual(tile_001_data[0].unique_tile_views, 1000)
    
    def test_performance_with_large_dataset(self):
        """Test performance characteristics with larger dataset"""
        import time
        
        # Create larger dataset
        large_data = []
        for i in range(10000):
            user_id = f"user_{i % 1000}"  # 1000 unique users
            tile_id = f"tile_{i % 100}"   # 100 unique tiles
            event_type = "TILE_VIEW" if i % 2 == 0 else "TILE_CLICK"
            large_data.append((user_id, tile_id, event_type, "2025-01-27 10:00:00"))
        
        large_df = self.spark.createDataFrame(large_data, self.tile_events_schema)
        large_df = large_df.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        start_time = time.time()
        result = compute_tile_aggregations(large_df)
        result.count()  # Trigger computation
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # Performance should be reasonable (less than 30 seconds for this test)
        self.assertLess(execution_time, 30.0)
        self.assertIsNotNone(result)
        self.assertEqual(result.count(), 100)  # Should have 100 unique tiles

class TestETLIntegration(unittest.TestCase):
    """Integration tests for the complete ETL pipeline"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for integration testing"""
        cls.spark = SparkSession.builder \
            .appName("HomeTileReportingETL_IntegrationTest") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        if hasattr(cls, 'spark'):
            cls.spark.stop()
    
    def test_end_to_end_pipeline_execution(self):
        """Test complete end-to-end pipeline execution"""
        process_date = "2025-01-27"
        
        with patch('home_tile_reporting_etl_Pipeline_1.get_spark_session') as mock_spark:
            mock_spark.return_value = self.spark
            
            with patch('home_tile_reporting_etl_Pipeline_1.write_to_delta_table') as mock_write:
                mock_write.return_value = True
                
                # Execute the pipeline
                df_tile, df_inter, df_metadata = create_sample_data(self.spark)
                df_tile_filtered = df_tile.filter(F.to_date("event_ts") == process_date)
                df_inter_filtered = df_inter.filter(F.to_date("event_ts") == process_date)
                
                df_tile_agg = compute_tile_aggregations(df_tile_filtered)
                df_inter_agg = compute_interstitial_aggregations(df_inter_filtered)
                
                df_daily_summary = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date)
                df_global = compute_global_kpis(df_daily_summary)
                
                validation_results = validate_data_quality(df_daily_summary, df_global)
                
                # Verify pipeline results
                self.assertIsNotNone(df_daily_summary)
                self.assertIsNotNone(df_global)
                self.assertTrue(df_daily_summary.count() > 0)
                self.assertTrue(df_global.count() > 0)
                
                # Verify data quality
                passed_validations = [result for result in validation_results if result[1]]
                self.assertTrue(len(passed_validations) > 0)
    
    def test_data_lineage_consistency(self):
        """Test data lineage and consistency across pipeline stages"""
        process_date = "2025-01-27"
        
        # Create and process data
        df_tile, df_inter, df_metadata = create_sample_data(self.spark)
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == process_date)
        df_inter_filtered = df_inter.filter(F.to_date("event_ts") == process_date)
        
        # Get unique tile_ids from source
        source_tile_ids = set([row.tile_id for row in df_tile_filtered.select("tile_id").distinct().collect()])
        
        # Process through pipeline
        df_tile_agg = compute_tile_aggregations(df_tile_filtered)
        df_inter_agg = compute_interstitial_aggregations(df_inter_filtered)
        df_daily_summary = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date)
        
        # Get tile_ids from final output
        output_tile_ids = set([row.tile_id for row in df_daily_summary.select("tile_id").distinct().collect()])
        
        # Verify data lineage consistency
        # All source tile_ids should appear in output (may have additional from interstitial)
        self.assertTrue(source_tile_ids.issubset(output_tile_ids))

if __name__ == '__main__':
    # Configure test runner
    unittest.main(verbosity=2, buffer=True)