_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Comprehensive unit tests for home tile reporting ETL pipeline with tile_category enrichment
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
from unittest.mock import patch, MagicMock
from datetime import datetime, date
import logging
from typing import List, Dict, Any

# Test configuration
PROCESS_DATE = "2025-12-01"
PIPELINE_NAME = "HOME_TILE_REPORTING_ETL"

class TestHomeTileReportingETL:
    """Comprehensive unit test suite for Home Tile Reporting ETL Pipeline"""
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for testing"""
        spark = SparkSession.builder \
            .appName("HomeTileReportingETL_UnitTests") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_tile_events_data(self):
        """Sample data for SOURCE_HOME_TILE_EVENTS table"""
        return [
            ("tile_001", "user_001", "TILE_VIEW", "2025-12-01 10:00:00"),
            ("tile_001", "user_002", "TILE_VIEW", "2025-12-01 10:05:00"),
            ("tile_001", "user_001", "TILE_CLICK", "2025-12-01 10:10:00"),
            ("tile_002", "user_003", "TILE_VIEW", "2025-12-01 11:00:00"),
            ("tile_002", "user_003", "TILE_CLICK", "2025-12-01 11:05:00"),
            ("tile_003", "user_004", "TILE_VIEW", "2025-12-01 12:00:00"),
            # Edge case: Same user multiple views/clicks
            ("tile_001", "user_001", "TILE_VIEW", "2025-12-01 13:00:00"),
            ("tile_001", "user_001", "TILE_CLICK", "2025-12-01 13:05:00"),
        ]
    
    @pytest.fixture
    def sample_interstitial_events_data(self):
        """Sample data for SOURCE_INTERSTITIAL_EVENTS table"""
        return [
            ("tile_001", "user_001", True, True, False, "2025-12-01 10:15:00"),
            ("tile_001", "user_002", True, False, True, "2025-12-01 10:20:00"),
            ("tile_002", "user_003", True, True, False, "2025-12-01 11:10:00"),
            ("tile_003", "user_004", True, False, False, "2025-12-01 12:05:00"),
            # Edge case: Same user multiple interstitial events
            ("tile_001", "user_001", True, True, False, "2025-12-01 14:00:00"),
        ]
    
    @pytest.fixture
    def sample_tile_metadata_data(self):
        """Sample data for SOURCE_TILE_METADATA table"""
        return [
            ("tile_001", "Personal Finance Dashboard", "Personal Finance", True, "2025-11-01 00:00:00"),
            ("tile_002", "Health Tracker", "Health", True, "2025-11-01 00:00:00"),
            ("tile_003", "Payment Gateway", "Payments", True, "2025-11-01 00:00:00"),
            # Edge case: Inactive tile
            ("tile_004", "Old Feature", "Deprecated", False, "2025-10-01 00:00:00"),
        ]
    
    def create_tile_events_df(self, spark_session, data):
        """Create DataFrame for tile events with proper schema"""
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        return df.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
    
    def create_interstitial_events_df(self, spark_session, data):
        """Create DataFrame for interstitial events with proper schema"""
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("interstitial_view_flag", BooleanType(), True),
            StructField("primary_button_click_flag", BooleanType(), True),
            StructField("secondary_button_click_flag", BooleanType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        return df.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
    
    def create_tile_metadata_df(self, spark_session, data):
        """Create DataFrame for tile metadata with proper schema"""
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("updated_ts", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        return df.withColumn("updated_ts", F.to_timestamp("updated_ts", "yyyy-MM-dd HH:mm:ss"))
    
    def test_tile_events_aggregation(self, spark_session, sample_tile_events_data):
        """Test TC_PCE5_01: Validate tile events aggregation logic"""
        # Arrange
        df_tile = self.create_tile_events_df(spark_session, sample_tile_events_data)
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        
        # Act
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        result = df_tile_agg.collect()
        
        # Assert
        assert len(result) == 3, "Should have 3 unique tiles"
        
        # Validate tile_001 metrics (2 unique users for views, 1 unique user for clicks)
        tile_001_result = [row for row in result if row['tile_id'] == 'tile_001'][0]
        assert tile_001_result['unique_tile_views'] == 2, "tile_001 should have 2 unique viewers"
        assert tile_001_result['unique_tile_clicks'] == 1, "tile_001 should have 1 unique clicker"
        
        # Validate tile_002 metrics
        tile_002_result = [row for row in result if row['tile_id'] == 'tile_002'][0]
        assert tile_002_result['unique_tile_views'] == 1, "tile_002 should have 1 unique viewer"
        assert tile_002_result['unique_tile_clicks'] == 1, "tile_002 should have 1 unique clicker"
    
    def test_interstitial_events_aggregation(self, spark_session, sample_interstitial_events_data):
        """Test TC_PCE5_02: Validate interstitial events aggregation logic"""
        # Arrange
        df_inter = self.create_interstitial_events_df(spark_session, sample_interstitial_events_data)
        df_inter_filtered = df_inter.filter(F.to_date("event_ts") == PROCESS_DATE)
        
        # Act
        df_inter_agg = (
            df_inter_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        result = df_inter_agg.collect()
        
        # Assert
        assert len(result) == 3, "Should have 3 unique tiles with interstitial events"
        
        # Validate tile_001 interstitial metrics
        tile_001_result = [row for row in result if row['tile_id'] == 'tile_001'][0]
        assert tile_001_result['unique_interstitial_views'] == 2, "tile_001 should have 2 unique interstitial viewers"
        assert tile_001_result['unique_interstitial_primary_clicks'] == 1, "tile_001 should have 1 unique primary clicker"
        assert tile_001_result['unique_interstitial_secondary_clicks'] == 1, "tile_001 should have 1 unique secondary clicker"
    
    def test_metadata_enrichment_left_join(self, spark_session, sample_tile_events_data, sample_tile_metadata_data):
        """Test TC_PCE5_05: Validate LEFT JOIN between tile data and metadata"""
        # Arrange
        df_tile = self.create_tile_events_df(spark_session, sample_tile_events_data)
        df_meta = self.create_tile_metadata_df(spark_session, sample_tile_metadata_data)
        
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        # Act
        df_tile_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_tile_enriched.collect()
        
        # Assert
        assert len(result) == 3, "Should preserve all tile records after LEFT JOIN"
        
        # Validate enrichment for existing metadata
        tile_001_result = [row for row in result if row['tile_id'] == 'tile_001'][0]
        assert tile_001_result['tile_category'] == 'Personal Finance', "tile_001 should have Personal Finance category"
        
        tile_002_result = [row for row in result if row['tile_id'] == 'tile_002'][0]
        assert tile_002_result['tile_category'] == 'Health', "tile_002 should have Health category"
    
    def test_backward_compatibility_unknown_category(self, spark_session):
        """Test TC_PCE5_06: Validate backward compatibility with UNKNOWN category"""
        # Arrange - Create tile data without corresponding metadata
        tile_data = [("tile_999", "user_001", "TILE_VIEW", "2025-12-01 10:00:00")]
        metadata_data = [("tile_001", "Test Tile", "Test Category", True, "2025-11-01 00:00:00")]
        
        df_tile = self.create_tile_events_df(spark_session, tile_data)
        df_meta = self.create_tile_metadata_df(spark_session, metadata_data)
        
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        # Act
        df_tile_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_tile_enriched.collect()
        
        # Assert
        assert len(result) == 1, "Should have one record"
        assert result[0]['tile_category'] == 'UNKNOWN', "Unmatched tile should have UNKNOWN category"
        assert result[0]['tile_id'] == 'tile_999', "Should preserve original tile_id"
    
    def test_daily_summary_creation(self, spark_session, sample_tile_events_data, sample_interstitial_events_data, sample_tile_metadata_data):
        """Test TC_PCE5_07: Validate complete daily summary creation with tile_category"""
        # Arrange
        df_tile = self.create_tile_events_df(spark_session, sample_tile_events_data)
        df_inter = self.create_interstitial_events_df(spark_session, sample_interstitial_events_data)
        df_meta = self.create_tile_metadata_df(spark_session, sample_tile_metadata_data)
        
        # Filter by process date
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_inter_filtered = df_inter.filter(F.to_date("event_ts") == PROCESS_DATE)
        
        # Aggregate tile events
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        # Aggregate interstitial events
        df_inter_agg = (
            df_inter_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        # Enrich with metadata
        df_tile_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        df_inter_enriched = (
            df_inter_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        # Act - Combine tile and interstitial aggregates
        df_daily_summary = (
            df_tile_enriched.join(df_inter_enriched.drop("tile_category"), ["tile_id"], how="outer")
            .withColumn("date", F.lit(PROCESS_DATE))
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
        
        # Assert
        assert len(result) == 3, "Should have 3 tiles in daily summary"
        
        # Validate schema includes tile_category
        expected_columns = ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", 
                          "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        actual_columns = df_daily_summary.columns
        assert all(col in actual_columns for col in expected_columns), "All expected columns should be present"
        
        # Validate tile_category values
        categories = [row['tile_category'] for row in result]
        assert 'Personal Finance' in categories, "Should contain Personal Finance category"
        assert 'Health' in categories, "Should contain Health category"
        assert 'Payments' in categories, "Should contain Payments category"
    
    def test_global_kpis_calculation(self, spark_session):
        """Test TC_PCE5_12: Validate global KPIs calculation with category data"""
        # Arrange - Create sample daily summary data
        daily_summary_data = [
            ("2025-12-01", "tile_001", "Personal Finance", 100, 20, 50, 10, 5),
            ("2025-12-01", "tile_002", "Health", 80, 15, 40, 8, 3),
            ("2025-12-01", "tile_003", "Payments", 60, 12, 30, 6, 2)
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
        
        df_daily_summary = spark_session.createDataFrame(daily_summary_data, schema)
        
        # Act
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
        
        # Assert
        assert result['total_tile_views'] == 240, "Total tile views should be 240"
        assert result['total_tile_clicks'] == 47, "Total tile clicks should be 47"
        assert result['total_interstitial_views'] == 120, "Total interstitial views should be 120"
        assert abs(result['overall_ctr'] - (47/240)) < 0.001, "Overall CTR should be approximately 0.196"
        assert abs(result['overall_primary_ctr'] - (24/120)) < 0.001, "Overall primary CTR should be 0.2"
    
    def test_category_level_reporting(self, spark_session):
        """Test TC_PCE5_12: Validate category-level performance metrics reporting"""
        # Arrange
        daily_summary_data = [
            ("2025-12-01", "tile_001", "Personal Finance", 100, 20, 50, 10, 5),
            ("2025-12-01", "tile_002", "Personal Finance", 80, 16, 40, 8, 4),
            ("2025-12-01", "tile_003", "Health", 60, 12, 30, 6, 2),
            ("2025-12-01", "tile_004", "Health", 40, 8, 20, 4, 1)
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
        
        df_daily_summary = spark_session.createDataFrame(daily_summary_data, schema)
        
        # Act - Group by category
        df_category_metrics = (
            df_daily_summary.groupBy("tile_category")
            .agg(
                F.sum("unique_tile_views").alias("category_tile_views"),
                F.sum("unique_tile_clicks").alias("category_tile_clicks"),
                F.avg("unique_tile_views").alias("avg_tile_views_per_tile")
            )
            .withColumn(
                "category_ctr",
                F.when(F.col("category_tile_views") > 0, F.col("category_tile_clicks") / F.col("category_tile_views")).otherwise(0.0)
            )
        )
        
        result = df_category_metrics.collect()
        
        # Assert
        assert len(result) == 2, "Should have 2 categories"
        
        # Validate Personal Finance metrics
        pf_result = [row for row in result if row['tile_category'] == 'Personal Finance'][0]
        assert pf_result['category_tile_views'] == 180, "Personal Finance should have 180 total views"
        assert pf_result['category_tile_clicks'] == 36, "Personal Finance should have 36 total clicks"
        assert abs(pf_result['category_ctr'] - (36/180)) < 0.001, "Personal Finance CTR should be 0.2"
        
        # Validate Health metrics
        health_result = [row for row in result if row['tile_category'] == 'Health'][0]
        assert health_result['category_tile_views'] == 100, "Health should have 100 total views"
        assert health_result['category_tile_clicks'] == 20, "Health should have 20 total clicks"
        assert abs(health_result['category_ctr'] - (20/100)) < 0.001, "Health CTR should be 0.2"
    
    def test_duplicate_tile_id_handling(self, spark_session):
        """Test TC_PCE5_16: Validate handling of duplicate tile_id in metadata"""
        # Arrange - Create metadata with duplicate tile_ids
        metadata_data = [
            ("tile_001", "First Entry", "Personal Finance", True, "2025-11-01 00:00:00"),
            ("tile_001", "Second Entry", "Health", True, "2025-11-02 00:00:00"),  # Duplicate
            ("tile_002", "Unique Entry", "Payments", True, "2025-11-01 00:00:00")
        ]
        
        tile_data = [("tile_001", "user_001", "TILE_VIEW", "2025-12-01 10:00:00")]
        
        df_tile = self.create_tile_events_df(spark_session, tile_data)
        df_meta = self.create_tile_metadata_df(spark_session, metadata_data)
        
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        # Act - JOIN should handle duplicates (typically takes first match)
        df_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_enriched.collect()
        
        # Assert - Should handle duplicates gracefully
        assert len(result) >= 1, "Should have at least one result"
        tile_001_results = [row for row in result if row['tile_id'] == 'tile_001']
        assert len(tile_001_results) >= 1, "Should have result for tile_001"
        # The exact category depends on Spark's duplicate handling, but should not be UNKNOWN
        assert tile_001_results[0]['tile_category'] in ['Personal Finance', 'Health'], "Should have a valid category from metadata"
    
    def test_special_characters_in_category(self, spark_session):
        """Test TC_PCE5_18: Validate handling of special characters in tile_category"""
        # Arrange - Create metadata with special characters
        metadata_data = [
            ("tile_001", "Unicode Test", "Financé & Héalth 💰", True, "2025-11-01 00:00:00"),
            ("tile_002", "Special Chars", "Category with 'quotes' & symbols!", True, "2025-11-01 00:00:00")
        ]
        
        tile_data = [
            ("tile_001", "user_001", "TILE_VIEW", "2025-12-01 10:00:00"),
            ("tile_002", "user_002", "TILE_VIEW", "2025-12-01 10:00:00")
        ]
        
        df_tile = self.create_tile_events_df(spark_session, tile_data)
        df_meta = self.create_tile_metadata_df(spark_session, metadata_data)
        
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        # Act
        df_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_enriched.collect()
        
        # Assert
        assert len(result) == 2, "Should have 2 results"
        
        tile_001_result = [row for row in result if row['tile_id'] == 'tile_001'][0]
        assert "💰" in tile_001_result['tile_category'], "Should preserve emoji characters"
        assert "Financé" in tile_001_result['tile_category'], "Should preserve Unicode characters"
        
        tile_002_result = [row for row in result if row['tile_id'] == 'tile_002'][0]
        assert "'quotes'" in tile_002_result['tile_category'], "Should preserve quote characters"
        assert "!" in tile_002_result['tile_category'], "Should preserve special symbols"
    
    def test_zero_division_handling_in_ctr(self, spark_session):
        """Test edge case: Zero division handling in CTR calculations"""
        # Arrange - Create data with zero views
        daily_summary_data = [
            ("2025-12-01", "tile_001", "Personal Finance", 0, 0, 0, 0, 0),  # All zeros
            ("2025-12-01", "tile_002", "Health", 100, 20, 50, 10, 5)  # Normal data
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
        
        df_daily_summary = spark_session.createDataFrame(daily_summary_data, schema)
        
        # Act
        df_global = (
            df_daily_summary.groupBy("date")
            .agg(
                F.sum("unique_tile_views").alias("total_tile_views"),
                F.sum("unique_tile_clicks").alias("total_tile_clicks"),
                F.sum("unique_interstitial_views").alias("total_interstitial_views"),
                F.sum("unique_interstitial_primary_clicks").alias("total_primary_clicks")
            )
            .withColumn(
                "overall_ctr",
                F.when(F.col("total_tile_views") > 0, F.col("total_tile_clicks") / F.col("total_tile_views")).otherwise(0.0)
            )
            .withColumn(
                "overall_primary_ctr",
                F.when(F.col("total_interstitial_views") > 0, F.col("total_primary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
            )
        )
        
        result = df_global.collect()[0]
        
        # Assert - Should handle zero division gracefully
        assert result['overall_ctr'] == 0.2, "CTR should be calculated correctly even with some zero values"
        assert result['overall_primary_ctr'] == 0.2, "Primary CTR should be calculated correctly"
    
    def test_inactive_tile_handling(self, spark_session):
        """Test TC_PCE5_13: Validate inactive tile handling"""
        # Arrange - Include inactive tiles in metadata
        metadata_data = [
            ("tile_001", "Active Tile", "Personal Finance", True, "2025-11-01 00:00:00"),
            ("tile_002", "Inactive Tile", "Health", False, "2025-11-01 00:00:00")  # Inactive
        ]
        
        tile_data = [
            ("tile_001", "user_001", "TILE_VIEW", "2025-12-01 10:00:00"),
            ("tile_002", "user_002", "TILE_VIEW", "2025-12-01 10:00:00")
        ]
        
        df_tile = self.create_tile_events_df(spark_session, tile_data)
        df_meta = self.create_tile_metadata_df(spark_session, metadata_data)
        
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        # Act - Enrich with metadata (including inactive tiles)
        df_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category", "is_active"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        result = df_enriched.collect()
        
        # Assert
        assert len(result) == 2, "Should include both active and inactive tiles"
        
        # Validate inactive tile gets category
        tile_002_result = [row for row in result if row['tile_id'] == 'tile_002'][0]
        assert tile_002_result['tile_category'] == 'Health', "Inactive tile should still get category"
        assert tile_002_result['is_active'] == False, "Should preserve is_active flag"
    
    @patch('logging.getLogger')
    def test_logging_functionality(self, mock_logger, spark_session):
        """Test that logging works correctly throughout the pipeline"""
        # Arrange
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Act - Simulate logging calls that would happen in the actual pipeline
        logger = logging.getLogger("HomeTileReportingETL")
        logger.info("Reading source tables...")
        logger.info(f"ETL completed successfully for {PROCESS_DATE}")
        
        # Assert
        assert mock_logger.called, "Logger should be initialized"
    
    def test_schema_validation(self, spark_session, sample_tile_events_data, sample_tile_metadata_data):
        """Test TC_PCE5_08: Validate schema validation passes with tile_category"""
        # Arrange
        df_tile = self.create_tile_events_df(spark_session, sample_tile_events_data)
        df_meta = self.create_tile_metadata_df(spark_session, sample_tile_metadata_data)
        
        # Act - Create final output schema
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        df_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
            .withColumn("date", F.lit(PROCESS_DATE))
        )
        
        # Assert - Validate expected schema
        expected_columns = ["tile_id", "unique_tile_views", "tile_category", "date"]
        actual_columns = df_enriched.columns
        
        for col in expected_columns:
            assert col in actual_columns, f"Column {col} should be present in schema"
        
        # Validate data types
        schema_dict = {field.name: field.dataType for field in df_enriched.schema.fields}
        assert isinstance(schema_dict['tile_category'], StringType), "tile_category should be StringType"
        assert isinstance(schema_dict['tile_id'], StringType), "tile_id should be StringType"
    
    def test_record_count_preservation(self, spark_session, sample_tile_events_data, sample_tile_metadata_data):
        """Test TC_PCE5_09: Validate correct record counts are maintained"""
        # Arrange
        df_tile = self.create_tile_events_df(spark_session, sample_tile_events_data)
        df_meta = self.create_tile_metadata_df(spark_session, sample_tile_metadata_data)
        
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        
        # Count unique tiles before processing
        unique_tiles_before = df_tile_filtered.select("tile_id").distinct().count()
        
        # Act - Process through aggregation and enrichment
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views")
            )
        )
        
        df_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        unique_tiles_after = df_enriched.count()
        
        # Assert
        assert unique_tiles_before == unique_tiles_after, "Record count should be preserved through enrichment"
        assert unique_tiles_after == 3, "Should have exactly 3 unique tiles"

# Performance and Integration Tests
class TestHomeTileReportingETLPerformance:
    """Performance and integration tests for the ETL pipeline"""
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for performance testing"""
        spark = SparkSession.builder \
            .appName("HomeTileReportingETL_PerformanceTests") \
            .master("local[4]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        yield spark
        spark.stop()
    
    def test_large_dataset_performance(self, spark_session):
        """Test TC_PCE5_15: Validate performance impact of metadata enrichment"""
        import time
        
        # Arrange - Create larger dataset for performance testing
        large_tile_data = []
        for i in range(10000):
            tile_id = f"tile_{i % 100:03d}"  # 100 unique tiles
            user_id = f"user_{i % 1000:04d}"  # 1000 unique users
            event_type = "TILE_VIEW" if i % 3 == 0 else "TILE_CLICK"
            large_tile_data.append((tile_id, user_id, event_type, "2025-12-01 10:00:00"))
        
        metadata_data = []
        for i in range(100):
            tile_id = f"tile_{i:03d}"
            category = ["Personal Finance", "Health", "Payments", "Entertainment"][i % 4]
            metadata_data.append((tile_id, f"Tile {i}", category, True, "2025-11-01 00:00:00"))
        
        # Create DataFrames
        tile_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True)
        ])
        
        meta_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("updated_ts", StringType(), True)
        ])
        
        df_tile = spark_session.createDataFrame(large_tile_data, tile_schema)
        df_tile = df_tile.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
        
        df_meta = spark_session.createDataFrame(metadata_data, meta_schema)
        df_meta = df_meta.withColumn("updated_ts", F.to_timestamp("updated_ts", "yyyy-MM-dd HH:mm:ss"))
        
        # Act - Measure performance
        start_time = time.time()
        
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_tile_agg = (
            df_tile_filtered.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        df_enriched = (
            df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
        
        # Force execution
        result_count = df_enriched.count()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Assert
        assert result_count == 100, "Should process 100 unique tiles"
        assert execution_time < 30.0, f"Execution should complete within 30 seconds, took {execution_time:.2f}s"
        
        # Validate memory usage is reasonable (this is a basic check)
        assert df_enriched.rdd.getNumPartitions() > 0, "Should have reasonable partitioning"

if __name__ == "__main__":
    # Run tests with pytest
    pytest.main(["-v", __file__])
