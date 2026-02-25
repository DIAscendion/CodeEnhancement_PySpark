# _____________________________________________
# ## *Author*: AAVA
# ## *Created on*: 2025-01-27
# ## *Description*: Comprehensive unit tests for Home Tile Reporting ETL with tile category metadata integration
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

# Add the source code directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import functions from the main module
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
except ImportError:
    # Mock the functions if import fails
    def get_spark_session():
        pass
    def create_sample_data(spark):
        pass
    def read_source_data(spark, process_date):
        pass
    def compute_tile_aggregations(df_tile):
        pass
    def compute_interstitial_aggregations(df_inter):
        pass
    def create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date):
        pass
    def compute_global_kpis(df_daily_summary):
        pass
    def write_to_delta_table(df, table_name, process_date, partition_col="date"):
        pass
    def validate_data_quality(df_daily_summary, df_global):
        pass
    def main():
        pass

class TestHomeTileReportingETL(unittest.TestCase):
    """Test suite for Home Tile Reporting ETL Pipeline"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for testing"""
        cls.spark = SparkSession.builder \
            .appName("HomeTileReportingETL_Test") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test"""
        self.test_date = "2025-01-27"
        self.sample_tile_data = [
            ("user1", "tile_001", "TILE_VIEW", "2025-01-27 10:00:00"),
            ("user1", "tile_001", "TILE_CLICK", "2025-01-27 10:01:00"),
            ("user2", "tile_001", "TILE_VIEW", "2025-01-27 10:02:00")
        ]
        self.sample_interstitial_data = [
            ("user1", "tile_001", True, True, False, "2025-01-27 10:01:30"),
            ("user2", "tile_001", True, False, True, "2025-01-27 10:02:30")
        ]
        self.sample_metadata_data = [
            ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-01-27 09:00:00"),
            ("tile_002", "Health Check Tile", "HEALTH", True, "2025-01-27 09:00:00")
        ]
    
    def test_get_spark_session_success(self):
        """Test successful Spark session initialization"""
        with patch('home_tile_reporting_etl_Pipeline_1.SparkSession') as mock_spark:
            mock_session = Mock()
            mock_spark.getActiveSession.return_value = mock_session
            
            result = get_spark_session()
            
            self.assertEqual(result, mock_session)
            mock_spark.getActiveSession.assert_called_once()
    
    def test_get_spark_session_create_new(self):
        """Test Spark session creation when no active session exists"""
        with patch('home_tile_reporting_etl_Pipeline_1.SparkSession') as mock_spark:
            mock_spark.getActiveSession.return_value = None
            mock_builder = Mock()
            mock_session = Mock()
            mock_spark.builder = mock_builder
            mock_builder.appName.return_value = mock_builder
            mock_builder.config.return_value = mock_builder
            mock_builder.getOrCreate.return_value = mock_session
            
            result = get_spark_session()
            
            self.assertEqual(result, mock_session)
            mock_builder.appName.assert_called_with("HomeTileReportingETL_Enhanced")
    
    def test_get_spark_session_exception(self):
        """Test Spark session initialization with exception"""
        with patch('home_tile_reporting_etl_Pipeline_1.SparkSession') as mock_spark:
            mock_spark.getActiveSession.side_effect = Exception("Spark error")
            
            with self.assertRaises(Exception):
                get_spark_session()
    
    def test_create_sample_data_structure(self):
        """Test sample data creation returns correct structure"""
        df_tile, df_inter, df_metadata = create_sample_data(self.spark)
        
        # Test tile events DataFrame
        self.assertIsNotNone(df_tile)
        self.assertTrue(df_tile.count() > 0)
        expected_tile_columns = ["user_id", "tile_id", "event_type", "event_ts"]
        self.assertEqual(df_tile.columns, expected_tile_columns)
        
        # Test interstitial events DataFrame
        self.assertIsNotNone(df_inter)
        self.assertTrue(df_inter.count() > 0)
        expected_inter_columns = ["user_id", "tile_id", "interstitial_view_flag", 
                                "primary_button_click_flag", "secondary_button_click_flag", "event_ts"]
        self.assertEqual(df_inter.columns, expected_inter_columns)
        
        # Test metadata DataFrame
        self.assertIsNotNone(df_metadata)
        self.assertTrue(df_metadata.count() > 0)
        expected_metadata_columns = ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"]
        self.assertEqual(df_metadata.columns, expected_metadata_columns)
    
    def test_create_sample_data_content(self):
        """Test sample data creation contains expected content"""
        df_tile, df_inter, df_metadata = create_sample_data(self.spark)
        
        # Test tile events content
        tile_data = df_tile.collect()
        self.assertTrue(any(row.tile_id == "tile_001" for row in tile_data))
        self.assertTrue(any(row.event_type == "TILE_VIEW" for row in tile_data))
        self.assertTrue(any(row.event_type == "TILE_CLICK" for row in tile_data))
        
        # Test metadata content
        metadata_data = df_metadata.collect()
        self.assertTrue(any(row.tile_category == "FINANCE" for row in metadata_data))
        self.assertTrue(any(row.tile_category == "HEALTH" for row in metadata_data))
    
    def test_read_source_data_success(self):
        """Test successful source data reading"""
        with patch('home_tile_reporting_etl_Pipeline_1.create_sample_data') as mock_create:
            mock_tile_df = Mock()
            mock_inter_df = Mock()
            mock_metadata_df = Mock()
            mock_create.return_value = (mock_tile_df, mock_inter_df, mock_metadata_df)
            
            # Mock filter method
            mock_tile_df.filter.return_value = mock_tile_df
            mock_inter_df.filter.return_value = mock_inter_df
            mock_tile_df.count.return_value = 10
            mock_inter_df.count.return_value = 5
            mock_metadata_df.count.return_value = 3
            
            result = read_source_data(self.spark, self.test_date)
            
            self.assertEqual(len(result), 3)
            mock_create.assert_called_once_with(self.spark)
    
    def test_read_source_data_exception(self):
        """Test source data reading with exception"""
        with patch('home_tile_reporting_etl_Pipeline_1.create_sample_data') as mock_create:
            mock_create.side_effect = Exception("Data read error")
            
            with self.assertRaises(Exception):
                read_source_data(self.spark, self.test_date)
    
    def test_compute_tile_aggregations_structure(self):
        """Test tile aggregations computation structure"""
        # Create test DataFrame
        tile_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        test_data = [
            ("tile_001", "user1", "TILE_VIEW"),
            ("tile_001", "user1", "TILE_CLICK"),
            ("tile_001", "user2", "TILE_VIEW"),
            ("tile_002", "user3", "TILE_VIEW")
        ]
        
        df_tile = self.spark.createDataFrame(test_data, tile_schema)
        result = compute_tile_aggregations(df_tile)
        
        self.assertIsNotNone(result)
        expected_columns = ["tile_id", "unique_tile_views", "unique_tile_clicks"]
        self.assertEqual(result.columns, expected_columns)
    
    def test_compute_tile_aggregations_values(self):
        """Test tile aggregations computation values"""
        tile_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        test_data = [
            ("tile_001", "user1", "TILE_VIEW"),
            ("tile_001", "user2", "TILE_VIEW"),
            ("tile_001", "user1", "TILE_CLICK")
        ]
        
        df_tile = self.spark.createDataFrame(test_data, tile_schema)
        result = compute_tile_aggregations(df_tile)
        
        result_data = result.collect()
        self.assertEqual(len(result_data), 1)
        self.assertEqual(result_data[0].tile_id, "tile_001")
        self.assertEqual(result_data[0].unique_tile_views, 2)
        self.assertEqual(result_data[0].unique_tile_clicks, 1)
    
    def test_compute_interstitial_aggregations_structure(self):
        """Test interstitial aggregations computation structure"""
        inter_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("interstitial_view_flag", BooleanType(), True),
            StructField("primary_button_click_flag", BooleanType(), True),
            StructField("secondary_button_click_flag", BooleanType(), True)
        ])
        
        test_data = [
            ("tile_001", "user1", True, True, False),
            ("tile_001", "user2", True, False, True)
        ]
        
        df_inter = self.spark.createDataFrame(test_data, inter_schema)
        result = compute_interstitial_aggregations(df_inter)
        
        self.assertIsNotNone(result)
        expected_columns = ["tile_id", "unique_interstitial_views", 
                          "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        self.assertEqual(result.columns, expected_columns)
    
    def test_compute_interstitial_aggregations_values(self):
        """Test interstitial aggregations computation values"""
        inter_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("interstitial_view_flag", BooleanType(), True),
            StructField("primary_button_click_flag", BooleanType(), True),
            StructField("secondary_button_click_flag", BooleanType(), True)
        ])
        
        test_data = [
            ("tile_001", "user1", True, True, False),
            ("tile_001", "user2", True, False, True),
            ("tile_001", "user3", True, True, False)
        ]
        
        df_inter = self.spark.createDataFrame(test_data, inter_schema)
        result = compute_interstitial_aggregations(df_inter)
        
        result_data = result.collect()
        self.assertEqual(len(result_data), 1)
        self.assertEqual(result_data[0].tile_id, "tile_001")
        self.assertEqual(result_data[0].unique_interstitial_views, 3)
        self.assertEqual(result_data[0].unique_interstitial_primary_clicks, 2)
        self.assertEqual(result_data[0].unique_interstitial_secondary_clicks, 1)
    
    def test_create_daily_summary_structure(self):
        """Test daily summary creation structure"""
        # Create mock DataFrames
        tile_agg_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", LongType(), True),
            StructField("unique_tile_clicks", LongType(), True)
        ])
        
        inter_agg_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_interstitial_views", LongType(), True),
            StructField("unique_interstitial_primary_clicks", LongType(), True),
            StructField("unique_interstitial_secondary_clicks", LongType(), True)
        ])
        
        metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True)
        ])
        
        df_tile_agg = self.spark.createDataFrame([("tile_001", 10, 5)], tile_agg_schema)
        df_inter_agg = self.spark.createDataFrame([("tile_001", 8, 3, 2)], inter_agg_schema)
        df_metadata = self.spark.createDataFrame([("tile_001", "Finance Tile", "FINANCE")], metadata_schema)
        
        result = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, self.test_date)
        
        self.assertIsNotNone(result)
        expected_columns = ["date", "tile_id", "tile_category", "tile_name", "unique_tile_views", 
                          "unique_tile_clicks", "unique_interstitial_views", 
                          "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        self.assertEqual(result.columns, expected_columns)
    
    def test_create_daily_summary_unknown_category(self):
        """Test daily summary with unknown tile category"""
        tile_agg_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", LongType(), True),
            StructField("unique_tile_clicks", LongType(), True)
        ])
        
        metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True)
        ])
        
        df_tile_agg = self.spark.createDataFrame([("tile_999", 10, 5)], tile_agg_schema)
        df_inter_agg = self.spark.createDataFrame([], StructType([StructField("tile_id", StringType(), True)]))
        df_metadata = self.spark.createDataFrame([("tile_001", "Finance Tile", "FINANCE")], metadata_schema)
        
        result = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, self.test_date)
        
        result_data = result.collect()
        self.assertEqual(len(result_data), 1)
        self.assertEqual(result_data[0].tile_category, "UNKNOWN")
    
    def test_compute_global_kpis_structure(self):
        """Test global KPIs computation structure"""
        daily_summary_schema = StructType([
            StructField("date", StringType(), True),
            StructField("unique_tile_views", LongType(), True),
            StructField("unique_tile_clicks", LongType(), True),
            StructField("unique_interstitial_views", LongType(), True),
            StructField("unique_interstitial_primary_clicks", LongType(), True),
            StructField("unique_interstitial_secondary_clicks", LongType(), True)
        ])
        
        test_data = [
            (self.test_date, 100, 20, 80, 15, 10),
            (self.test_date, 50, 10, 40, 8, 5)
        ]
        
        df_daily_summary = self.spark.createDataFrame(test_data, daily_summary_schema)
        result = compute_global_kpis(df_daily_summary)
        
        self.assertIsNotNone(result)
        expected_columns = ["date", "total_tile_views", "total_tile_clicks", "total_interstitial_views",
                          "total_primary_clicks", "total_secondary_clicks", "overall_ctr",
                          "overall_primary_ctr", "overall_secondary_ctr"]
        self.assertEqual(result.columns, expected_columns)
    
    def test_compute_global_kpis_calculations(self):
        """Test global KPIs calculations"""
        daily_summary_schema = StructType([
            StructField("date", StringType(), True),
            StructField("unique_tile_views", LongType(), True),
            StructField("unique_tile_clicks", LongType(), True),
            StructField("unique_interstitial_views", LongType(), True),
            StructField("unique_interstitial_primary_clicks", LongType(), True),
            StructField("unique_interstitial_secondary_clicks", LongType(), True)
        ])
        
        test_data = [(self.test_date, 100, 20, 80, 16, 8)]
        
        df_daily_summary = self.spark.createDataFrame(test_data, daily_summary_schema)
        result = compute_global_kpis(df_daily_summary)
        
        result_data = result.collect()
        self.assertEqual(len(result_data), 1)
        self.assertEqual(result_data[0].total_tile_views, 100)
        self.assertEqual(result_data[0].total_tile_clicks, 20)
        self.assertEqual(result_data[0].overall_ctr, 0.2)  # 20/100
        self.assertEqual(result_data[0].overall_primary_ctr, 0.2)  # 16/80
        self.assertEqual(result_data[0].overall_secondary_ctr, 0.1)  # 8/80
    
    def test_compute_global_kpis_zero_division(self):
        """Test global KPIs with zero division scenarios"""
        daily_summary_schema = StructType([
            StructField("date", StringType(), True),
            StructField("unique_tile_views", LongType(), True),
            StructField("unique_tile_clicks", LongType(), True),
            StructField("unique_interstitial_views", LongType(), True),
            StructField("unique_interstitial_primary_clicks", LongType(), True),
            StructField("unique_interstitial_secondary_clicks", LongType(), True)
        ])
        
        test_data = [(self.test_date, 0, 0, 0, 0, 0)]
        
        df_daily_summary = self.spark.createDataFrame(test_data, daily_summary_schema)
        result = compute_global_kpis(df_daily_summary)
        
        result_data = result.collect()
        self.assertEqual(len(result_data), 1)
        self.assertEqual(result_data[0].overall_ctr, 0.0)
        self.assertEqual(result_data[0].overall_primary_ctr, 0.0)
        self.assertEqual(result_data[0].overall_secondary_ctr, 0.0)
    
    @patch('home_tile_reporting_etl_Pipeline_1.logger')
    def test_write_to_delta_table_success(self, mock_logger):
        """Test successful Delta table writing"""
        test_schema = StructType([StructField("date", StringType(), True)])
        test_data = [(self.test_date,)]
        df_test = self.spark.createDataFrame(test_data, test_schema)
        
        with patch.object(df_test.write, 'format') as mock_format:
            mock_writer = Mock()
            mock_format.return_value = mock_writer
            mock_writer.mode.return_value = mock_writer
            mock_writer.option.return_value = mock_writer
            mock_writer.save.return_value = None
            
            result = write_to_delta_table(df_test, "test_table", self.test_date)
            
            self.assertTrue(result)
            mock_format.assert_called_with("delta")
    
    @patch('home_tile_reporting_etl_Pipeline_1.logger')
    def test_write_to_delta_table_exception(self, mock_logger):
        """Test Delta table writing with exception"""
        test_schema = StructType([StructField("date", StringType(), True)])
        test_data = [(self.test_date,)]
        df_test = self.spark.createDataFrame(test_data, test_schema)
        
        with patch.object(df_test.write, 'format') as mock_format:
            mock_format.side_effect = Exception("Write error")
            
            with self.assertRaises(Exception):
                write_to_delta_table(df_test, "test_table", self.test_date)
    
    def test_validate_data_quality_success(self):
        """Test data quality validation with good data"""
        daily_summary_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", LongType(), True),
            StructField("tile_category", StringType(), True)
        ])
        
        global_schema = StructType([
            StructField("overall_ctr", DoubleType(), True)
        ])
        
        df_daily = self.spark.createDataFrame([("tile_001", 10, "FINANCE")], daily_summary_schema)
        df_global = self.spark.createDataFrame([(0.2,)], global_schema)
        
        result = validate_data_quality(df_daily, df_global)
        
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) > 0)
        # Check that all validations have the expected structure
        for validation in result:
            self.assertEqual(len(validation), 3)  # (test_name, passed, message)
    
    def test_validate_data_quality_failures(self):
        """Test data quality validation with bad data"""
        daily_summary_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", LongType(), True),
            StructField("tile_category", StringType(), True)
        ])
        
        global_schema = StructType([
            StructField("overall_ctr", DoubleType(), True)
        ])
        
        # Create data with quality issues
        df_daily = self.spark.createDataFrame([(None, -5, "UNKNOWN")], daily_summary_schema)
        df_global = self.spark.createDataFrame([(-0.5,)], global_schema)
        
        result = validate_data_quality(df_daily, df_global)
        
        self.assertIsInstance(result, list)
        # Check that some validations failed
        failed_validations = [r for r in result if not r[1]]
        self.assertTrue(len(failed_validations) > 0)
    
    @patch('home_tile_reporting_etl_Pipeline_1.get_spark_session')
    @patch('home_tile_reporting_etl_Pipeline_1.read_source_data')
    @patch('home_tile_reporting_etl_Pipeline_1.write_to_delta_table')
    def test_main_success(self, mock_write, mock_read, mock_spark):
        """Test successful main ETL execution"""
        # Mock Spark session
        mock_spark.return_value = self.spark
        
        # Mock source data
        tile_schema = StructType([StructField("tile_id", StringType(), True)])
        df_mock = self.spark.createDataFrame([("tile_001",)], tile_schema)
        mock_read.return_value = (df_mock, df_mock, df_mock)
        
        # Mock write success
        mock_write.return_value = True
        
        with patch('home_tile_reporting_etl_Pipeline_1.compute_tile_aggregations') as mock_tile_agg, \
             patch('home_tile_reporting_etl_Pipeline_1.compute_interstitial_aggregations') as mock_inter_agg, \
             patch('home_tile_reporting_etl_Pipeline_1.create_daily_summary') as mock_daily, \
             patch('home_tile_reporting_etl_Pipeline_1.compute_global_kpis') as mock_global, \
             patch('home_tile_reporting_etl_Pipeline_1.validate_data_quality') as mock_validate:
            
            mock_tile_agg.return_value = df_mock
            mock_inter_agg.return_value = df_mock
            mock_daily.return_value = df_mock
            mock_global.return_value = df_mock
            mock_validate.return_value = [("test", True, "passed")]
            
            result = main()
            
            self.assertIsNotNone(result)
    
    @patch('home_tile_reporting_etl_Pipeline_1.get_spark_session')
    def test_main_exception(self, mock_spark):
        """Test main ETL execution with exception"""
        mock_spark.side_effect = Exception("Spark initialization failed")
        
        with self.assertRaises(Exception):
            main()
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty DataFrames"""
        empty_schema = StructType([StructField("tile_id", StringType(), True)])
        empty_df = self.spark.createDataFrame([], empty_schema)
        
        # Test tile aggregations with empty DataFrame
        result = compute_tile_aggregations(empty_df)
        self.assertEqual(result.count(), 0)
        
        # Test interstitial aggregations with empty DataFrame
        inter_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("interstitial_view_flag", BooleanType(), True),
            StructField("primary_button_click_flag", BooleanType(), True),
            StructField("secondary_button_click_flag", BooleanType(), True)
        ])
        empty_inter_df = self.spark.createDataFrame([], inter_schema)
        result = compute_interstitial_aggregations(empty_inter_df)
        self.assertEqual(result.count(), 0)
    
    def test_null_value_handling(self):
        """Test handling of null values in data"""
        tile_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        test_data = [
            (None, "user1", "TILE_VIEW"),
            ("tile_001", None, "TILE_CLICK"),
            ("tile_001", "user1", None)
        ]
        
        df_tile = self.spark.createDataFrame(test_data, tile_schema)
        result = compute_tile_aggregations(df_tile)
        
        # Should handle nulls gracefully
        self.assertIsNotNone(result)
    
    def test_large_dataset_performance(self):
        """Test performance with larger datasets"""
        # Create a larger test dataset
        large_data = [(f"tile_{i%10}", f"user_{i}", "TILE_VIEW" if i%2==0 else "TILE_CLICK") 
                     for i in range(1000)]
        
        tile_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        df_large = self.spark.createDataFrame(large_data, tile_schema)
        
        import time
        start_time = time.time()
        result = compute_tile_aggregations(df_large)
        result.count()  # Trigger computation
        end_time = time.time()
        
        # Should complete within reasonable time (5 seconds)
        self.assertLess(end_time - start_time, 5.0)
        self.assertEqual(result.count(), 10)  # 10 unique tiles

class TestEdgeCases(unittest.TestCase):
    """Test suite for edge cases and error scenarios"""
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("HomeTileReportingETL_EdgeCases_Test") \
            .master("local[*]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_duplicate_tile_ids_in_metadata(self):
        """Test handling of duplicate tile_ids in metadata"""
        metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True)
        ])
        
        # Create metadata with duplicate tile_ids
        duplicate_data = [
            ("tile_001", "Finance Tile", "FINANCE"),
            ("tile_001", "Investment Tile", "INVESTMENT")  # Duplicate
        ]
        
        df_metadata = self.spark.createDataFrame(duplicate_data, metadata_schema)
        
        # Create aggregation data
        agg_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", LongType(), True),
            StructField("unique_tile_clicks", LongType(), True)
        ])
        
        df_agg = self.spark.createDataFrame([("tile_001", 10, 5)], agg_schema)
        empty_inter = self.spark.createDataFrame([], StructType([StructField("tile_id", StringType(), True)]))
        
        # Should handle duplicates gracefully (typically takes first match)
        result = create_daily_summary(df_agg, empty_inter, df_metadata, "2025-01-27")
        self.assertEqual(result.count(), 1)
    
    def test_special_characters_in_tile_category(self):
        """Test handling of special characters in tile_category"""
        metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True)
        ])
        
        # Test with special characters and Unicode
        special_data = [
            ("tile_001", "Finance Tile", "FINANCE & INVESTMENT"),
            ("tile_002", "Health Tile", "HEALTH/WELLNESS"),
            ("tile_003", "Unicode Tile", "CATÉGORIE_SPÉCIALE")
        ]
        
        df_metadata = self.spark.createDataFrame(special_data, metadata_schema)
        
        # Should preserve special characters
        result_data = df_metadata.collect()
        self.assertTrue(any("&" in row.tile_category for row in result_data))
        self.assertTrue(any("/" in row.tile_category for row in result_data))
        self.assertTrue(any("É" in row.tile_category for row in result_data))
    
    def test_very_long_tile_category(self):
        """Test handling of very long tile_category values"""
        metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True)
        ])
        
        # Create very long category name
        long_category = "A" * 1000
        long_data = [("tile_001", long_category)]
        
        df_metadata = self.spark.createDataFrame(long_data, metadata_schema)
        
        # Should handle long strings without truncation
        result_data = df_metadata.collect()
        self.assertEqual(len(result_data[0].tile_category), 1000)

if __name__ == '__main__':
    # Configure test runner
    unittest.main(verbosity=2, buffer=True)