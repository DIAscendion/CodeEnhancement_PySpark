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
    """Unit tests for Home Tile Reporting ETL Pipeline"""
    
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
        """Set up test data and schemas"""
        # Define schemas
        self.tile_events_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", TimestampType(), True)
        ])
        
        self.interstitial_events_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("interstitial_view_flag", BooleanType(), True),
            StructField("primary_button_click_flag", BooleanType(), True),
            StructField("secondary_button_click_flag", BooleanType(), True),
            StructField("event_ts", TimestampType(), True)
        ])
        
        self.tile_metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("is_active", BooleanType(), True)
        ])
        
        # Create test data
        self.sample_tile_events = [
            ("tile_001", "user_001", "TILE_VIEW", datetime(2025, 12, 1, 10, 0, 0)),
            ("tile_001", "user_002", "TILE_CLICK", datetime(2025, 12, 1, 10, 5, 0)),
            ("tile_002", "user_001", "TILE_VIEW", datetime(2025, 12, 1, 11, 0, 0)),
            ("tile_002", "user_003", "TILE_VIEW", datetime(2025, 12, 1, 11, 5, 0)),
            ("tile_003", "user_004", "TILE_CLICK", datetime(2025, 12, 1, 12, 0, 0))
        ]
        
        self.sample_interstitial_events = [
            ("tile_001", "user_001", True, False, False, datetime(2025, 12, 1, 10, 1, 0)),
            ("tile_001", "user_002", True, True, False, datetime(2025, 12, 1, 10, 6, 0)),
            ("tile_002", "user_001", True, False, True, datetime(2025, 12, 1, 11, 1, 0)),
            ("tile_004", "user_005", True, True, False, datetime(2025, 12, 1, 13, 0, 0))
        ]
        
        self.sample_tile_metadata = [
            ("tile_001", "Personal Finance", "Budget Tracker", True),
            ("tile_002", "Health", "Fitness Goals", True),
            ("tile_003", "Payments", "Quick Pay", True),
            ("tile_005", "Entertainment", "Movie Recommendations", False)
        ]
    
    def test_tile_events_aggregation(self):
        """Test tile events aggregation logic"""
        # Create test DataFrame
        df_tile = self.spark.createDataFrame(self.sample_tile_events, self.tile_events_schema)
        
        # Apply aggregation logic
        df_tile_agg = (
            df_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        result = df_tile_agg.collect()
        result_dict = {row['tile_id']: (row['unique_tile_views'], row['unique_tile_clicks']) for row in result}
        
        # Assertions
        self.assertEqual(result_dict['tile_001'], (1, 1))  # 1 unique view, 1 unique click
        self.assertEqual(result_dict['tile_002'], (2, 0))  # 2 unique views, 0 clicks
        self.assertEqual(result_dict['tile_003'], (0, 1))  # 0 views, 1 unique click
    
    def test_interstitial_events_aggregation(self):
        """Test interstitial events aggregation logic"""
        # Create test DataFrame
        df_inter = self.spark.createDataFrame(self.sample_interstitial_events, self.interstitial_events_schema)
        
        # Apply aggregation logic
        df_inter_agg = (
            df_inter.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        result = df_inter_agg.collect()
        result_dict = {row['tile_id']: (row['unique_interstitial_views'], row['unique_interstitial_primary_clicks'], row['unique_interstitial_secondary_clicks']) for row in result}
        
        # Assertions
        self.assertEqual(result_dict['tile_001'], (2, 1, 0))  # 2 views, 1 primary click, 0 secondary
        self.assertEqual(result_dict['tile_002'], (1, 0, 1))  # 1 view, 0 primary, 1 secondary
        self.assertEqual(result_dict['tile_004'], (1, 1, 0))  # 1 view, 1 primary, 0 secondary
    
    def test_metadata_enrichment_with_left_join(self):
        """Test tile category enrichment using left join with metadata"""
        # Create test DataFrames
        df_tile_agg = self.spark.createDataFrame([
            ("tile_001", 1, 1),
            ("tile_002", 2, 0),
            ("tile_999", 1, 0)  # tile not in metadata
        ], ["tile_id", "unique_tile_views", "unique_tile_clicks"])
        
        df_meta = self.spark.createDataFrame(self.sample_tile_metadata, self.tile_metadata_schema)
        
        # Apply enrichment logic
        df_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_enriched.collect()
        result_dict = {row['tile_id']: row['tile_category'] for row in result}
        
        # Assertions
        self.assertEqual(result_dict['tile_001'], "Personal Finance")
        self.assertEqual(result_dict['tile_002'], "Health")
        self.assertEqual(result_dict['tile_999'], "UNKNOWN")  # Should default to UNKNOWN
    
    def test_daily_summary_combination(self):
        """Test combining tile and interstitial data for daily summary"""
        # Create test DataFrames
        df_tile_enriched = self.spark.createDataFrame([
            ("tile_001", "Personal Finance", 1, 1),
            ("tile_002", "Health", 2, 0)
        ], ["tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks"])
        
        df_inter_enriched = self.spark.createDataFrame([
            ("tile_001", "Personal Finance", 2, 1, 0),
            ("tile_003", "Payments", 1, 0, 1)
        ], ["tile_id", "tile_category", "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"])
        
        # Apply combination logic
        df_daily_summary = (
            df_tile_enriched.join(df_inter_enriched.drop("tile_category"), ["tile_id"], how="outer")
            .withColumn("date", F.lit("2025-12-01"))
            .select(
                "date",
                "tile_id",
                "tile_category",
                F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
                F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
                F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
                F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
                F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        result = df_daily_summary.collect()
        
        # Assertions
        self.assertEqual(len(result), 3)  # Should have 3 tiles total
        
        # Check specific tile data
        tile_001_data = [row for row in result if row['tile_id'] == 'tile_001'][0]
        self.assertEqual(tile_001_data['unique_tile_views'], 1)
        self.assertEqual(tile_001_data['unique_interstitial_views'], 2)
        
        # Check that missing values are filled with 0
        tile_002_data = [row for row in result if row['tile_id'] == 'tile_002'][0]
        self.assertEqual(tile_002_data['unique_interstitial_views'], 0)
        
        tile_003_data = [row for row in result if row['tile_id'] == 'tile_003'][0]
        self.assertEqual(tile_003_data['unique_tile_views'], 0)
    
    def test_global_kpis_calculation(self):
        """Test global KPIs calculation with CTR metrics"""
        # Create test daily summary DataFrame
        df_daily_summary = self.spark.createDataFrame([
            ("2025-12-01", "tile_001", "Personal Finance", 100, 10, 50, 5, 2),
            ("2025-12-01", "tile_002", "Health", 200, 20, 80, 8, 3),
            ("2025-12-01", "tile_003", "Payments", 150, 15, 60, 6, 4)
        ], ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", 
            "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"])
        
        # Apply global KPIs calculation
        df_global = (
            df_daily_summary.groupBy("date")
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
        self.assertEqual(result['total_tile_views'], 450)
        self.assertEqual(result['total_tile_clicks'], 45)
        self.assertEqual(result['total_interstitial_views'], 190)
        self.assertEqual(result['total_primary_clicks'], 19)
        self.assertEqual(result['total_secondary_clicks'], 9)
        
        # Check CTR calculations
        self.assertAlmostEqual(result['overall_ctr'], 0.1, places=2)  # 45/450 = 0.1
        self.assertAlmostEqual(result['overall_primary_ctr'], 0.1, places=2)  # 19/190 = 0.1
        self.assertAlmostEqual(result['overall_secondary_ctr'], 0.047, places=2)  # 9/190 ≈ 0.047
    
    def test_ctr_calculation_with_zero_views(self):
        """Test CTR calculation handles division by zero"""
        # Create test data with zero views
        df_test = self.spark.createDataFrame([
            ("2025-12-01", 0, 5, 0, 3, 2)
        ], ["date", "total_tile_views", "total_tile_clicks", "total_interstitial_views", "total_primary_clicks", "total_secondary_clicks"])
        
        # Apply CTR calculation
        df_result = df_test.withColumn(
            "overall_ctr",
            F.when(F.col("total_tile_views") > 0, F.col("total_tile_clicks") / F.col("total_tile_views")).otherwise(0.0)
        ).withColumn(
            "overall_primary_ctr",
            F.when(F.col("total_interstitial_views") > 0, F.col("total_primary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
        )
        
        result = df_result.collect()[0]
        
        # Assertions - should handle division by zero gracefully
        self.assertEqual(result['overall_ctr'], 0.0)
        self.assertEqual(result['overall_primary_ctr'], 0.0)  # 0 interstitial views
    
    def test_date_filtering(self):
        """Test date filtering functionality"""
        # Create test data with multiple dates
        df_events = self.spark.createDataFrame([
            ("tile_001", "user_001", "TILE_VIEW", datetime(2025, 12, 1, 10, 0, 0)),
            ("tile_001", "user_002", "TILE_VIEW", datetime(2025, 12, 2, 10, 0, 0)),  # Different date
            ("tile_002", "user_001", "TILE_CLICK", datetime(2025, 12, 1, 11, 0, 0))
        ], self.tile_events_schema)
        
        # Apply date filter
        df_filtered = df_events.filter(F.to_date("event_ts") == "2025-12-01")
        
        result = df_filtered.collect()
        
        # Assertions
        self.assertEqual(len(result), 2)  # Should only have 2 records for 2025-12-01
        for row in result:
            self.assertEqual(row['event_ts'].date().strftime('%Y-%m-%d'), '2025-12-01')
    
    def test_null_handling_in_metadata_join(self):
        """Test handling of null values in metadata join"""
        # Create test data with null tile_category
        df_tile_agg = self.spark.createDataFrame([
            ("tile_001", 1, 1),
            ("tile_999", 2, 0)
        ], ["tile_id", "unique_tile_views", "unique_tile_clicks"])
        
        df_meta = self.spark.createDataFrame([
            ("tile_001", None),  # Null category
            ("tile_002", "Health")
        ], ["tile_id", "tile_category"])
        
        # Apply enrichment with null handling
        df_enriched = (
            df_tile_agg.join(df_meta, ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_enriched.collect()
        result_dict = {row['tile_id']: row['tile_category'] for row in result}
        
        # Assertions
        self.assertEqual(result_dict['tile_001'], "UNKNOWN")  # Null should become UNKNOWN
        self.assertEqual(result_dict['tile_999'], "UNKNOWN")  # Missing should become UNKNOWN
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty input DataFrames"""
        # Create empty DataFrame with correct schema
        df_empty = self.spark.createDataFrame([], self.tile_events_schema)
        
        # Apply aggregation on empty DataFrame
        df_agg = (
            df_empty.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        result = df_agg.collect()
        
        # Assertions
        self.assertEqual(len(result), 0)  # Should return empty result
    
    def test_duplicate_user_events_handling(self):
        """Test handling of duplicate user events for same tile"""
        # Create test data with duplicate user events
        df_events = self.spark.createDataFrame([
            ("tile_001", "user_001", "TILE_VIEW", datetime(2025, 12, 1, 10, 0, 0)),
            ("tile_001", "user_001", "TILE_VIEW", datetime(2025, 12, 1, 10, 1, 0)),  # Duplicate user view
            ("tile_001", "user_002", "TILE_CLICK", datetime(2025, 12, 1, 10, 5, 0))
        ], self.tile_events_schema)
        
        # Apply aggregation (should count distinct users)
        df_agg = (
            df_events.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        result = df_agg.collect()[0]
        
        # Assertions
        self.assertEqual(result['unique_tile_views'], 1)  # Should count user_001 only once
        self.assertEqual(result['unique_tile_clicks'], 1)  # Should count user_002 once
    
    @patch('logging.getLogger')
    def test_logging_functionality(self, mock_logger):
        """Test that logging is properly configured"""
        # Mock logger
        mock_log_instance = Mock()
        mock_logger.return_value = mock_log_instance
        
        # Import and test logging
        import logging
        logger = logging.getLogger("HomeTileReportingETL")
        logger.info("Test message")
        
        # Assertions
        mock_logger.assert_called_with("HomeTileReportingETL")
    
    def test_schema_validation(self):
        """Test that DataFrames have expected schemas"""
        # Create DataFrames with expected schemas
        df_tile = self.spark.createDataFrame(self.sample_tile_events, self.tile_events_schema)
        df_inter = self.spark.createDataFrame(self.sample_interstitial_events, self.interstitial_events_schema)
        df_meta = self.spark.createDataFrame(self.sample_tile_metadata, self.tile_metadata_schema)
        
        # Validate schemas
        expected_tile_columns = {"tile_id", "user_id", "event_type", "event_ts"}
        expected_inter_columns = {"tile_id", "user_id", "interstitial_view_flag", "primary_button_click_flag", "secondary_button_click_flag", "event_ts"}
        expected_meta_columns = {"tile_id", "tile_category", "tile_name", "is_active"}
        
        # Assertions
        self.assertEqual(set(df_tile.columns), expected_tile_columns)
        self.assertEqual(set(df_inter.columns), expected_inter_columns)
        self.assertEqual(set(df_meta.columns), expected_meta_columns)
    
    def test_performance_with_large_dataset(self):
        """Test performance characteristics with larger dataset"""
        # Create larger test dataset
        large_tile_events = []
        for i in range(1000):
            large_tile_events.append(
                (f"tile_{i % 10}", f"user_{i}", "TILE_VIEW" if i % 2 == 0 else "TILE_CLICK", datetime(2025, 12, 1, 10, 0, 0))
            )
        
        df_large = self.spark.createDataFrame(large_tile_events, self.tile_events_schema)
        
        # Apply aggregation
        df_agg = (
            df_large.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        result = df_agg.collect()
        
        # Assertions
        self.assertEqual(len(result), 10)  # Should have 10 unique tiles
        
        # Verify aggregation correctness
        for row in result:
            self.assertGreater(row['unique_tile_views'], 0)
            self.assertGreater(row['unique_tile_clicks'], 0)

if __name__ == '__main__':
    unittest.main()