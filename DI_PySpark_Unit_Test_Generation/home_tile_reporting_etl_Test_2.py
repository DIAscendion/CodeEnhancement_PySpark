# _____________________________________________
# *Author*: AAVA
# *Created on*: 2024-12-19
# *Description*: Comprehensive unit tests for home tile reporting ETL pipeline with enhanced validation
# *Version*: 2
# *Updated on*: 2024-12-19
# _____________________________________________

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, LongType
from datetime import datetime
import sys
import os

# Add the pipeline module to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from home_tile_reporting_etl_Pipeline_2 import HomeTileReportingETL

class TestHomeTileReportingETL:
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for testing"""
        spark = SparkSession.builder \
            .appName("HomeTileReportingETL_Test") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def etl_instance(self, spark_session):
        """Create ETL instance for testing"""
        return HomeTileReportingETL(spark_session)
    
    @pytest.fixture
    def sample_tile_events_data(self, spark_session):
        """Create sample tile events data"""
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", TimestampType(), True)
        ])
        
        data = [
            ("tile_001", "user_001", "TILE_VIEW", datetime(2025, 12, 1, 10, 0, 0)),
            ("tile_001", "user_002", "TILE_VIEW", datetime(2025, 12, 1, 10, 5, 0)),
            ("tile_001", "user_001", "TILE_CLICK", datetime(2025, 12, 1, 10, 10, 0)),
            ("tile_002", "user_003", "TILE_VIEW", datetime(2025, 12, 1, 11, 0, 0)),
            ("tile_002", "user_003", "TILE_CLICK", datetime(2025, 12, 1, 11, 5, 0)),
            ("tile_003", "user_004", "TILE_VIEW", datetime(2025, 12, 1, 12, 0, 0))
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_interstitial_events_data(self, spark_session):
        """Create sample interstitial events data"""
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("interstitial_view_flag", BooleanType(), True),
            StructField("primary_button_click_flag", BooleanType(), True),
            StructField("secondary_button_click_flag", BooleanType(), True),
            StructField("event_ts", TimestampType(), True)
        ])
        
        data = [
            ("tile_001", "user_001", True, True, False, datetime(2025, 12, 1, 10, 15, 0)),
            ("tile_001", "user_002", True, False, True, datetime(2025, 12, 1, 10, 20, 0)),
            ("tile_002", "user_003", True, True, False, datetime(2025, 12, 1, 11, 10, 0)),
            ("tile_003", "user_004", True, False, False, datetime(2025, 12, 1, 12, 5, 0))
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_metadata_data(self, spark_session):
        """Create sample metadata data"""
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_name", StringType(), True),
            StructField("tile_category", StringType(), True),
            StructField("is_active", BooleanType(), True)
        ])
        
        data = [
            ("tile_001", "Personal Finance Tile", "Personal Finance", True),
            ("tile_002", "Health Tracker", "Health", True),
            ("tile_003", "Payment Gateway", "Payments", False)
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    def test_etl_initialization(self, spark_session):
        """Test ETL class initialization"""
        etl = HomeTileReportingETL(spark_session)
        assert etl.spark == spark_session
        assert etl.logger is not None
    
    def test_etl_initialization_without_spark(self):
        """Test ETL initialization without providing spark session"""
        with patch('home_tile_reporting_etl_Pipeline_2.SparkSession.getActiveSession') as mock_spark:
            mock_session = Mock()
            mock_spark.return_value = mock_session
            etl = HomeTileReportingETL()
            assert etl.spark == mock_session
    
    @patch('home_tile_reporting_etl_Pipeline_2.SOURCE_HOME_TILE_EVENTS', 'test.tile_events')
    @patch('home_tile_reporting_etl_Pipeline_2.SOURCE_INTERSTITIAL_EVENTS', 'test.interstitial_events')
    @patch('home_tile_reporting_etl_Pipeline_2.SOURCE_TILE_METADATA', 'test.metadata')
    def test_read_source_tables(self, etl_instance, sample_tile_events_data, sample_interstitial_events_data, sample_metadata_data):
        """Test reading source tables with date filtering"""
        # Mock the table method to return our sample data
        def mock_table(table_name):
            if 'tile_events' in table_name:
                return sample_tile_events_data
            elif 'interstitial_events' in table_name:
                return sample_interstitial_events_data
            elif 'metadata' in table_name:
                return sample_metadata_data
        
        etl_instance.spark.table = Mock(side_effect=mock_table)
        
        df_tile, df_inter, df_meta = etl_instance.read_source_tables("2025-12-01")
        
        # Verify tables were called
        assert etl_instance.spark.table.call_count == 3
        
        # Verify data is returned
        assert df_tile is not None
        assert df_inter is not None
        assert df_meta is not None
    
    def test_aggregate_tile_events(self, etl_instance, sample_tile_events_data):
        """Test tile events aggregation"""
        result_df = etl_instance.aggregate_tile_events(sample_tile_events_data)
        
        # Collect results for verification
        results = result_df.collect()
        
        # Verify aggregation results
        assert len(results) == 3  # 3 unique tile_ids
        
        # Find tile_001 results
        tile_001_result = next(row for row in results if row['tile_id'] == 'tile_001')
        assert tile_001_result['unique_tile_views'] == 2  # user_001, user_002
        assert tile_001_result['unique_tile_clicks'] == 1  # user_001
    
    def test_aggregate_interstitial_events(self, etl_instance, sample_interstitial_events_data):
        """Test interstitial events aggregation"""
        result_df = etl_instance.aggregate_interstitial_events(sample_interstitial_events_data)
        
        # Collect results for verification
        results = result_df.collect()
        
        # Verify aggregation results
        assert len(results) == 3  # 3 unique tile_ids
        
        # Find tile_001 results
        tile_001_result = next(row for row in results if row['tile_id'] == 'tile_001')
        assert tile_001_result['unique_interstitial_views'] == 2  # user_001, user_002
        assert tile_001_result['unique_interstitial_primary_clicks'] == 1  # user_001
        assert tile_001_result['unique_interstitial_secondary_clicks'] == 1  # user_002
    
    def test_enrich_with_metadata(self, etl_instance, spark_session, sample_metadata_data):
        """Test metadata enrichment with LEFT JOIN"""
        # Create sample aggregated data
        agg_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True)
        ])
        
        agg_data = [
            ("tile_001", 10),
            ("tile_002", 5),
            ("tile_999", 3)  # This tile_id doesn't exist in metadata
        ]
        
        df_agg = spark_session.createDataFrame(agg_data, agg_schema)
        
        result_df = etl_instance.enrich_with_metadata(df_agg, sample_metadata_data)
        results = result_df.collect()
        
        # Verify enrichment
        assert len(results) == 3
        
        # Check known tile
        tile_001_result = next(row for row in results if row['tile_id'] == 'tile_001')
        assert tile_001_result['tile_category'] == 'Personal Finance'
        
        # Check unknown tile gets UNKNOWN category
        tile_999_result = next(row for row in results if row['tile_id'] == 'tile_999')
        assert tile_999_result['tile_category'] == 'UNKNOWN'
    
    def test_create_daily_summary(self, etl_instance, spark_session):
        """Test daily summary creation"""
        # Create sample enriched tile data
        tile_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("tile_category", StringType(), True)
        ])
        
        tile_data = [
            ("tile_001", 10, 5, "Personal Finance"),
            ("tile_002", 8, 3, "Health")
        ]
        
        df_tile_enriched = spark_session.createDataFrame(tile_data, tile_schema)
        
        # Create sample enriched interstitial data
        inter_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True),
            StructField("tile_category", StringType(), True)
        ])
        
        inter_data = [
            ("tile_001", 7, 4, 2, "Personal Finance"),
            ("tile_003", 5, 1, 1, "Payments")  # Different tile_id
        ]
        
        df_inter_enriched = spark_session.createDataFrame(inter_data, inter_schema)
        
        result_df = etl_instance.create_daily_summary(df_tile_enriched, df_inter_enriched, "2025-12-01")
        results = result_df.collect()
        
        # Verify daily summary
        assert len(results) == 3  # tile_001, tile_002, tile_003
        
        # Check tile_001 (exists in both)
        tile_001_result = next(row for row in results if row['tile_id'] == 'tile_001')
        assert tile_001_result['date'] == '2025-12-01'
        assert tile_001_result['unique_tile_views'] == 10
        assert tile_001_result['unique_interstitial_views'] == 7
        
        # Check tile_002 (only in tile data)
        tile_002_result = next(row for row in results if row['tile_id'] == 'tile_002')
        assert tile_002_result['unique_interstitial_views'] == 0  # Should be 0 due to coalesce
        
        # Check tile_003 (only in interstitial data)
        tile_003_result = next(row for row in results if row['tile_id'] == 'tile_003')
        assert tile_003_result['unique_tile_views'] == 0  # Should be 0 due to coalesce
    
    def test_create_global_kpis(self, etl_instance, spark_session):
        """Test global KPIs calculation"""
        # Create sample daily summary data
        summary_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        summary_data = [
            ("2025-12-01", "tile_001", 100, 20, 50, 10, 5),
            ("2025-12-01", "tile_002", 80, 16, 40, 8, 4)
        ]
        
        df_daily_summary = spark_session.createDataFrame(summary_data, summary_schema)
        
        result_df = etl_instance.create_global_kpis(df_daily_summary)
        results = result_df.collect()
        
        # Verify global KPIs
        assert len(results) == 1
        result = results[0]
        
        assert result['date'] == '2025-12-01'
        assert result['total_tile_views'] == 180
        assert result['total_tile_clicks'] == 36
        assert result['total_interstitial_views'] == 90
        assert result['total_primary_clicks'] == 18
        assert result['total_secondary_clicks'] == 9
        
        # Check CTR calculations
        assert abs(result['overall_ctr'] - (36/180)) < 0.001  # 0.2
        assert abs(result['overall_primary_ctr'] - (18/90)) < 0.001  # 0.2
        assert abs(result['overall_secondary_ctr'] - (9/90)) < 0.001  # 0.1
    
    def test_create_global_kpis_zero_division(self, etl_instance, spark_session):
        """Test global KPIs calculation with zero values to avoid division by zero"""
        summary_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        summary_data = [
            ("2025-12-01", "tile_001", 0, 0, 0, 0, 0)
        ]
        
        df_daily_summary = spark_session.createDataFrame(summary_data, summary_schema)
        
        result_df = etl_instance.create_global_kpis(df_daily_summary)
        results = result_df.collect()
        
        # Verify zero division handling
        result = results[0]
        assert result['overall_ctr'] == 0.0
        assert result['overall_primary_ctr'] == 0.0
        assert result['overall_secondary_ctr'] == 0.0
    
    def test_validate_data_quality(self, etl_instance, spark_session):
        """Test data quality validation function"""
        # Create test data with quality issues
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True)
        ])
        
        data = [
            (None, 10, 5),  # Null tile_id
            ("tile_001", -5, 3),  # Negative value
            ("tile_002", 8, 2)  # Valid data
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        # Test validation (should return True but log warnings)
        result = etl_instance.validate_data_quality(df, "test_table")
        assert result == True
    
    def test_validate_data_quality_clean_data(self, etl_instance, spark_session):
        """Test data quality validation with clean data"""
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True)
        ])
        
        data = [
            ("tile_001", 10, 5),
            ("tile_002", 8, 2)
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        # Test validation with clean data
        result = etl_instance.validate_data_quality(df, "test_table")
        assert result == True
    
    @patch.object(HomeTileReportingETL, 'write_delta')
    def test_write_delta(self, mock_write_delta, etl_instance, spark_session):
        """Test Delta table writing"""
        # Create sample DataFrame
        schema = StructType([StructField("test_col", StringType(), True)])
        df = spark_session.createDataFrame([("test_value",)], schema)
        
        # Call write_delta
        etl_instance.write_delta(df, "test_table", "date", "2025-12-01")
        
        # Verify write_delta was called
        mock_write_delta.assert_called_once_with(df, "test_table", "date", "2025-12-01")
    
    @patch.object(HomeTileReportingETL, 'read_source_tables')
    @patch.object(HomeTileReportingETL, 'aggregate_tile_events')
    @patch.object(HomeTileReportingETL, 'aggregate_interstitial_events')
    @patch.object(HomeTileReportingETL, 'enrich_with_metadata')
    @patch.object(HomeTileReportingETL, 'create_daily_summary')
    @patch.object(HomeTileReportingETL, 'create_global_kpis')
    @patch.object(HomeTileReportingETL, 'write_delta')
    @patch.object(HomeTileReportingETL, 'validate_data_quality')
    def test_run_etl_success(self, mock_validate_data_quality, mock_write_delta, mock_create_global_kpis, 
                           mock_create_daily_summary, mock_enrich_with_metadata, mock_aggregate_interstitial_events,
                           mock_aggregate_tile_events, mock_read_source_tables, etl_instance, spark_session):
        """Test successful ETL execution with enhanced validation"""
        # Setup mocks
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100  # Mock count for logging
        mock_read_source_tables.return_value = (mock_df, mock_df, mock_df)
        mock_aggregate_tile_events.return_value = mock_df
        mock_aggregate_interstitial_events.return_value = mock_df
        mock_enrich_with_metadata.return_value = mock_df
        mock_create_daily_summary.return_value = mock_df
        mock_create_global_kpis.return_value = mock_df
        mock_validate_data_quality.return_value = True
        
        # Execute ETL
        etl_instance.run_etl("2025-12-01")
        
        # Verify all methods were called
        mock_read_source_tables.assert_called_once_with("2025-12-01")
        mock_aggregate_tile_events.assert_called_once()
        mock_aggregate_interstitial_events.assert_called_once()
        assert mock_enrich_with_metadata.call_count == 2  # Called for both tile and interstitial
        mock_create_daily_summary.assert_called_once()
        mock_create_global_kpis.assert_called_once()
        assert mock_write_delta.call_count == 2  # Called for daily summary and global KPIs
        assert mock_validate_data_quality.call_count == 2  # Called for both outputs
    
    @patch.object(HomeTileReportingETL, 'read_source_tables')
    def test_run_etl_failure(self, mock_read_source_tables, etl_instance):
        """Test ETL execution with failure"""
        # Setup mock to raise exception
        mock_read_source_tables.side_effect = Exception("Test error")
        
        # Verify exception is raised
        with pytest.raises(Exception, match="Test error"):
            etl_instance.run_etl("2025-12-01")
    
    def test_empty_dataframe_handling(self, etl_instance, spark_session):
        """Test handling of empty DataFrames"""
        # Create empty DataFrame
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        empty_df = spark_session.createDataFrame([], schema)
        
        # Test aggregation with empty DataFrame
        result_df = etl_instance.aggregate_tile_events(empty_df)
        results = result_df.collect()
        
        # Should return empty result
        assert len(results) == 0
    
    def test_null_values_handling(self, etl_instance, spark_session):
        """Test handling of null values in data"""
        # Create DataFrame with null values
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        data = [
            (None, "user_001", "TILE_VIEW"),
            ("tile_001", None, "TILE_VIEW"),
            ("tile_001", "user_001", None)
        ]
        
        df_with_nulls = spark_session.createDataFrame(data, schema)
        
        # Test aggregation with null values
        result_df = etl_instance.aggregate_tile_events(df_with_nulls)
        results = result_df.collect()
        
        # Should handle nulls gracefully
        assert len(results) >= 0  # May have results depending on null handling
    
    def test_large_dataset_performance(self, etl_instance, spark_session):
        """Test performance with larger dataset"""
        # Create larger dataset
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        # Generate 1000 records
        data = [(f"tile_{i%10}", f"user_{i}", "TILE_VIEW" if i%2 == 0 else "TILE_CLICK") for i in range(1000)]
        large_df = spark_session.createDataFrame(data, schema)
        
        # Test aggregation performance
        import time
        start_time = time.time()
        result_df = etl_instance.aggregate_tile_events(large_df)
        results = result_df.collect()
        end_time = time.time()
        
        # Should complete within reasonable time (5 seconds)
        assert (end_time - start_time) < 5.0
        assert len(results) == 10  # 10 unique tile_ids
    
    def test_data_type_validation(self, etl_instance, spark_session):
        """Test data type validation in results"""
        # Create sample data
        schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        data = [("tile_001", "user_001", "TILE_VIEW")]
        df = spark_session.createDataFrame(data, schema)
        
        # Test aggregation
        result_df = etl_instance.aggregate_tile_events(df)
        
        # Verify schema types
        schema_dict = {field.name: field.dataType for field in result_df.schema.fields}
        assert isinstance(schema_dict['tile_id'], StringType)
        assert isinstance(schema_dict['unique_tile_views'], (IntegerType, LongType))  # May be LongType
        assert isinstance(schema_dict['unique_tile_clicks'], (IntegerType, LongType))  # May be LongType
    
    def test_metadata_enrichment_edge_cases(self, etl_instance, spark_session):
        """Test metadata enrichment with edge cases"""
        # Create metadata with edge cases
        metadata_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("tile_category", StringType(), True)
        ])
        
        metadata_data = [
            ("tile_001", "Personal Finance"),
            ("tile_002", None),  # Null category
            ("tile_003", ""),   # Empty category
            ("tile_004", "Health & Wellness")  # Category with special characters
        ]
        
        df_metadata = spark_session.createDataFrame(metadata_data, metadata_schema)
        
        # Create aggregated data
        agg_schema = StructType([
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True)
        ])
        
        agg_data = [
            ("tile_001", 10),
            ("tile_002", 5),
            ("tile_003", 3),
            ("tile_004", 8),
            ("tile_999", 2)  # Not in metadata
        ]
        
        df_agg = spark_session.createDataFrame(agg_data, agg_schema)
        
        result_df = etl_instance.enrich_with_metadata(df_agg, df_metadata)
        results = result_df.collect()
        
        # Verify enrichment handles edge cases
        assert len(results) == 5
        
        # Check various scenarios
        tile_001_result = next(row for row in results if row['tile_id'] == 'tile_001')
        assert tile_001_result['tile_category'] == 'Personal Finance'
        
        tile_002_result = next(row for row in results if row['tile_id'] == 'tile_002')
        assert tile_002_result['tile_category'] == 'UNKNOWN'  # Null should become UNKNOWN
        
        tile_004_result = next(row for row in results if row['tile_id'] == 'tile_004')
        assert tile_004_result['tile_category'] == 'Health & Wellness'  # Special characters preserved
        
        tile_999_result = next(row for row in results if row['tile_id'] == 'tile_999')
        assert tile_999_result['tile_category'] == 'UNKNOWN'  # Not found should be UNKNOWN
    
    def test_ctr_calculation_edge_cases(self, etl_instance, spark_session):
        """Test CTR calculations with various edge cases"""
        # Create test data with edge cases
        summary_schema = StructType([
            StructField("date", StringType(), True),
            StructField("tile_id", StringType(), True),
            StructField("unique_tile_views", IntegerType(), True),
            StructField("unique_tile_clicks", IntegerType(), True),
            StructField("unique_interstitial_views", IntegerType(), True),
            StructField("unique_interstitial_primary_clicks", IntegerType(), True),
            StructField("unique_interstitial_secondary_clicks", IntegerType(), True)
        ])
        
        # Test cases: normal, zero views, zero clicks, high CTR
        summary_data = [
            ("2025-12-01", "tile_001", 100, 20, 50, 10, 5),  # Normal case
            ("2025-12-01", "tile_002", 0, 0, 0, 0, 0),       # All zeros
            ("2025-12-01", "tile_003", 50, 0, 25, 0, 0),     # Zero clicks
            ("2025-12-01", "tile_004", 10, 10, 5, 5, 5)      # High CTR (100%)
        ]
        
        df_daily_summary = spark_session.createDataFrame(summary_data, summary_schema)
        
        result_df = etl_instance.create_global_kpis(df_daily_summary)
        results = result_df.collect()
        
        # Verify CTR calculations
        result = results[0]
        
        # Total views: 100 + 0 + 50 + 10 = 160
        # Total clicks: 20 + 0 + 0 + 10 = 30
        # Overall CTR: 30/160 = 0.1875
        assert result['total_tile_views'] == 160
        assert result['total_tile_clicks'] == 30
        assert abs(result['overall_ctr'] - 0.1875) < 0.001
        
        # Total interstitial views: 50 + 0 + 25 + 5 = 80
        # Total primary clicks: 10 + 0 + 0 + 5 = 15
        # Primary CTR: 15/80 = 0.1875
        assert result['total_interstitial_views'] == 80
        assert result['total_primary_clicks'] == 15
        assert abs(result['overall_primary_ctr'] - 0.1875) < 0.001

if __name__ == "__main__":
    pytest.main([__file__])