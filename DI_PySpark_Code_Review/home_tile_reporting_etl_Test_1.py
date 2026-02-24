# _____________________________________________
# *Author*: AAVA
# *Created on*: 2025-01-08
# *Description*: Comprehensive unit tests for home tile reporting ETL pipeline
# *Version*: 1
# *Updated on*: 2025-01-08
# _____________________________________________

import unittest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
import pytest
from datetime import datetime

class TestHomeTileReportingETL(unittest.TestCase):
    
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
        """Set up test data schemas"""
        # Home tile events schema
        self.tile_events_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", TimestampType(), True)
        ])
        
        # Interstitial events schema
        self.interstitial_events_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("interstitial_view_flag", BooleanType(), True),
            StructField("primary_button_click_flag", BooleanType(), True),
            StructField("secondary_button_click_flag", BooleanType(), True),
            StructField("event_ts", TimestampType(), True)
        ])
        
        # Tile metadata schema
        self.tile_metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True)
        ])
    
    def test_tile_aggregation_basic_functionality(self):
        """Test basic tile event aggregation"""
        # Create test data
        tile_data = [
            ("tile_1", "user_1", "TILE_VIEW", datetime(2025, 12, 1, 10, 0, 0)),
            ("tile_1", "user_2", "TILE_VIEW", datetime(2025, 12, 1, 10, 5, 0)),
            ("tile_1", "user_1", "TILE_CLICK", datetime(2025, 12, 1, 10, 10, 0)),
            ("tile_2", "user_3", "TILE_VIEW", datetime(2025, 12, 1, 11, 0, 0))
        ]
        
        df_tile = self.spark.createDataFrame(tile_data, self.tile_events_schema)
        
        # Apply aggregation logic
        df_tile_agg = (
            df_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        result = df_tile_agg.collect()
        
        # Assertions
        self.assertEqual(len(result), 2)
        
        # Check tile_1 results
        tile_1_result = [r for r in result if r['tile_id'] == 'tile_1'][0]
        self.assertEqual(tile_1_result['unique_tile_views'], 2)
        self.assertEqual(tile_1_result['unique_tile_clicks'], 1)
        
        # Check tile_2 results
        tile_2_result = [r for r in result if r['tile_id'] == 'tile_2'][0]
        self.assertEqual(tile_2_result['unique_tile_views'], 1)
        self.assertEqual(tile_2_result['unique_tile_clicks'], 0)
    
    def test_interstitial_aggregation_basic_functionality(self):
        """Test basic interstitial event aggregation"""
        # Create test data
        interstitial_data = [
            ("tile_1", "user_1", True, False, False, datetime(2025, 12, 1, 10, 0, 0)),
            ("tile_1", "user_2", True, True, False, datetime(2025, 12, 1, 10, 5, 0)),
            ("tile_1", "user_3", True, False, True, datetime(2025, 12, 1, 10, 10, 0)),
            ("tile_2", "user_4", False, False, False, datetime(2025, 12, 1, 11, 0, 0))
        ]
        
        df_inter = self.spark.createDataFrame(interstitial_data, self.interstitial_events_schema)
        
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
        
        # Assertions
        self.assertEqual(len(result), 2)
        
        # Check tile_1 results
        tile_1_result = [r for r in result if r['tile_id'] == 'tile_1'][0]
        self.assertEqual(tile_1_result['unique_interstitial_views'], 3)
        self.assertEqual(tile_1_result['unique_interstitial_primary_clicks'], 1)
        self.assertEqual(tile_1_result['unique_interstitial_secondary_clicks'], 1)
        
        # Check tile_2 results
        tile_2_result = [r for r in result if r['tile_id'] == 'tile_2'][0]
        self.assertEqual(tile_2_result['unique_interstitial_views'], 0)
        self.assertEqual(tile_2_result['unique_interstitial_primary_clicks'], 0)
        self.assertEqual(tile_2_result['unique_interstitial_secondary_clicks'], 0)
    
    def test_metadata_enrichment_with_known_categories(self):
        """Test tile category enrichment with known categories"""
        # Create test data
        tile_agg_data = [("tile_1", 10, 5), ("tile_2", 8, 3)]
        tile_agg_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True)
        ])
        
        metadata_data = [("tile_1", "SPORTS"), ("tile_2", "NEWS")]
        
        df_tile_agg = self.spark.createDataFrame(tile_agg_data, tile_agg_schema)
        df_meta = self.spark.createDataFrame(metadata_data, self.tile_metadata_schema)
        
        # Apply enrichment logic
        df_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_enriched.collect()
        
        # Assertions
        self.assertEqual(len(result), 2)
        
        tile_1_result = [r for r in result if r['tile_id'] == 'tile_1'][0]
        self.assertEqual(tile_1_result['tile_category'], 'SPORTS')
        
        tile_2_result = [r for r in result if r['tile_id'] == 'tile_2'][0]
        self.assertEqual(tile_2_result['tile_category'], 'NEWS')
    
    def test_metadata_enrichment_with_unknown_categories(self):
        """Test tile category enrichment with missing metadata"""
        # Create test data
        tile_agg_data = [("tile_1", 10, 5), ("tile_3", 8, 3)]
        tile_agg_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True)
        ])
        
        metadata_data = [("tile_1", "SPORTS")]
        
        df_tile_agg = self.spark.createDataFrame(tile_agg_data, tile_agg_schema)
        df_meta = self.spark.createDataFrame(metadata_data, self.tile_metadata_schema)
        
        # Apply enrichment logic
        df_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_enriched.collect()
        
        # Assertions
        self.assertEqual(len(result), 2)
        
        tile_1_result = [r for r in result if r['tile_id'] == 'tile_1'][0]
        self.assertEqual(tile_1_result['tile_category'], 'SPORTS')
        
        tile_3_result = [r for r in result if r['tile_id'] == 'tile_3'][0]
        self.assertEqual(tile_3_result['tile_category'], 'UNKNOWN')
    
    def test_daily_summary_outer_join_logic(self):
        """Test outer join logic for combining tile and interstitial data"""
        # Create test data
        tile_enriched_data = [("tile_1", "SPORTS", 10, 5), ("tile_2", "NEWS", 8, 3)]
        tile_enriched_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True)
        ])
        
        inter_enriched_data = [("tile_1", "SPORTS", 15, 7, 2), ("tile_3", "UNKNOWN", 5, 1, 0)]
        inter_enriched_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        df_tile_enriched = self.spark.createDataFrame(tile_enriched_data, tile_enriched_schema)
        df_inter_enriched = self.spark.createDataFrame(inter_enriched_data, inter_enriched_schema)
        
        # Apply join logic
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
        self.assertEqual(len(result), 3)  # tile_1, tile_2, tile_3
        
        # Check tile_1 (has both tile and interstitial data)
        tile_1_result = [r for r in result if r['tile_id'] == 'tile_1'][0]
        self.assertEqual(tile_1_result['unique_tile_views'], 10)
        self.assertEqual(tile_1_result['unique_interstitial_views'], 15)
        
        # Check tile_2 (has only tile data)
        tile_2_result = [r for r in result if r['tile_id'] == 'tile_2'][0]
        self.assertEqual(tile_2_result['unique_tile_views'], 8)
        self.assertEqual(tile_2_result['unique_interstitial_views'], 0)
        
        # Check tile_3 (has only interstitial data)
        tile_3_result = [r for r in result if r['tile_id'] == 'tile_3'][0]
        self.assertEqual(tile_3_result['unique_tile_views'], 0)
        self.assertEqual(tile_3_result['unique_interstitial_views'], 5)
    
    def test_global_kpis_calculation(self):
        """Test global KPIs calculation including CTR calculations"""
        # Create test data
        daily_summary_data = [
            ("2025-12-01", "tile_1", "SPORTS", 100, 20, 80, 15, 5),
            ("2025-12-01", "tile_2", "NEWS", 50, 10, 40, 8, 2),
            ("2025-12-01", "tile_3", "UNKNOWN", 0, 0, 20, 5, 1)
        ]
        
        daily_summary_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        df_daily_summary = self.spark.createDataFrame(daily_summary_data, daily_summary_schema)
        
        # Apply global KPIs logic
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
        self.assertEqual(result['total_tile_views'], 150)
        self.assertEqual(result['total_tile_clicks'], 30)
        self.assertEqual(result['total_interstitial_views'], 140)
        self.assertEqual(result['total_primary_clicks'], 28)
        self.assertEqual(result['total_secondary_clicks'], 8)
        
        # CTR calculations
        self.assertAlmostEqual(result['overall_ctr'], 0.2, places=2)  # 30/150
        self.assertAlmostEqual(result['overall_primary_ctr'], 0.2, places=2)  # 28/140
        self.assertAlmostEqual(result['overall_secondary_ctr'], 0.057, places=2)  # 8/140
    
    def test_ctr_calculation_with_zero_views(self):
        """Test CTR calculation edge case with zero views"""
        # Create test data with zero views
        daily_summary_data = [
            ("2025-12-01", "tile_1", "SPORTS", 0, 0, 0, 0, 0)
        ]
        
        daily_summary_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        df_daily_summary = self.spark.createDataFrame(daily_summary_data, daily_summary_schema)
        
        # Apply global KPIs logic
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
        
        # Assertions - all CTRs should be 0.0 when views are 0
        self.assertEqual(result['overall_ctr'], 0.0)
        self.assertEqual(result['overall_primary_ctr'], 0.0)
        self.assertEqual(result['overall_secondary_ctr'], 0.0)
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty input dataframes"""
        # Create empty dataframes
        df_tile_empty = self.spark.createDataFrame([], self.tile_events_schema)
        df_inter_empty = self.spark.createDataFrame([], self.interstitial_events_schema)
        
        # Apply aggregation logic
        df_tile_agg = (
            df_tile_empty.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        df_inter_agg = (
            df_inter_empty.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        # Assertions
        self.assertEqual(df_tile_agg.count(), 0)
        self.assertEqual(df_inter_agg.count(), 0)
    
    def test_duplicate_user_events_deduplication(self):
        """Test that duplicate user events are properly deduplicated"""
        # Create test data with duplicate user events
        tile_data = [
            ("tile_1", "user_1", "TILE_VIEW", datetime(2025, 12, 1, 10, 0, 0)),
            ("tile_1", "user_1", "TILE_VIEW", datetime(2025, 12, 1, 10, 5, 0)),  # Duplicate
            ("tile_1", "user_1", "TILE_CLICK", datetime(2025, 12, 1, 10, 10, 0)),
            ("tile_1", "user_1", "TILE_CLICK", datetime(2025, 12, 1, 10, 15, 0))  # Duplicate
        ]
        
        df_tile = self.spark.createDataFrame(tile_data, self.tile_events_schema)
        
        # Apply aggregation logic
        df_tile_agg = (
            df_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        result = df_tile_agg.collect()[0]
        
        # Assertions - should count unique users, not events
        self.assertEqual(result['unique_tile_views'], 1)
        self.assertEqual(result['unique_tile_clicks'], 1)
    
    @patch('pyspark.sql.DataFrame.write')
    def test_write_delta_function_call(self, mock_write):
        """Test that write_delta function is called with correct parameters"""
        # Mock the write chain
        mock_write_chain = Mock()
        mock_write.return_value = mock_write_chain
        mock_write_chain.format.return_value = mock_write_chain
        mock_write_chain.mode.return_value = mock_write_chain
        mock_write_chain.option.return_value = mock_write_chain
        
        # Create test dataframe
        test_data = [("2025-12-01", "tile_1", "SPORTS", 10, 5, 8, 3, 1)]
        test_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        df_test = self.spark.createDataFrame(test_data, test_schema)
        
        # Define write_delta function
        def write_delta(df: DataFrame, table: str, partition_col: str = "date"):
            df.write.format("delta").mode("overwrite").option("replaceWhere", f"{partition_col} = '2025-12-01'").saveAsTable(table)
        
        # Call the function
        write_delta(df_test, "test_table")
        
        # Assertions
        mock_write_chain.format.assert_called_with("delta")
        mock_write_chain.mode.assert_called_with("overwrite")
        mock_write_chain.option.assert_called_with("replaceWhere", "date = '2025-12-01'")
        mock_write_chain.saveAsTable.assert_called_with("test_table")

if __name__ == '__main__':
    unittest.main()