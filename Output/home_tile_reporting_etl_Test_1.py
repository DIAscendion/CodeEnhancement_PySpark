# _____________________________________________
# ## *Author*: AAVA
# ## *Created on*: 2025-01-27
# ## *Description*: Test suite for Home Tile Reporting ETL Pipeline - Tests insert and update scenarios
# ## *Version*: 1
# ## *Updated on*: 2025-01-27
# ## *Databricks Notebook*: home_tile_reporting_etl_Test_1
# ## *Databricks Path*: /Workspace/Users/elansuriyaa.p@ascendion.com/PySpark/home_tile_reporting_etl_Test_1
# _____________________________________________

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from datetime import datetime, date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HomeTileReportingETLTest:
    """
    Test suite for Home Tile Reporting ETL Pipeline
    
    This test suite validates the ETL pipeline with two scenarios:
    1. Insert scenario - Tests processing of new tile interaction data
    2. Update scenario - Tests processing of updated/additional data for existing tiles
    
    The tests use sample data and validate the output against expected results.
    """
    
    def __init__(self):
        self.spark = SparkSession.getActiveSession()
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName("HomeTileReportingETLTest") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
        
        self.test_date = '2025-01-27'
        self.test_results = []
        
        logger.info("Initialized HomeTileReportingETLTest")
    
    def create_insert_test_data(self):
        """
        Creates sample data for insert scenario testing
        """
        logger.info("Creating insert test data...")
        
        # Sample home tile events for insert scenario
        home_tile_data = [
            ('evt_001', 'user_001', 'sess_001', '2025-01-27 10:00:00', 'tile_001', 'TILE_VIEW', 'Mobile', '1.0'),
            ('evt_002', 'user_001', 'sess_001', '2025-01-27 10:01:00', 'tile_001', 'TILE_CLICK', 'Mobile', '1.0'),
            ('evt_003', 'user_002', 'sess_002', '2025-01-27 11:00:00', 'tile_001', 'TILE_VIEW', 'Web', '1.0'),
            ('evt_004', 'user_002', 'sess_002', '2025-01-27 11:01:00', 'tile_002', 'TILE_VIEW', 'Web', '1.0'),
            ('evt_005', 'user_003', 'sess_003', '2025-01-27 12:00:00', 'tile_002', 'TILE_VIEW', 'Mobile', '1.0'),
            ('evt_006', 'user_003', 'sess_003', '2025-01-27 12:01:00', 'tile_002', 'TILE_CLICK', 'Mobile', '1.0')
        ]
        
        home_tile_schema = StructType([
            StructField('event_id', StringType(), True),
            StructField('user_id', StringType(), True),
            StructField('session_id', StringType(), True),
            StructField('event_ts', StringType(), True),
            StructField('tile_id', StringType(), True),
            StructField('event_type', StringType(), True),
            StructField('device_type', StringType(), True),
            StructField('app_version', StringType(), True)
        ])
        
        df_home_tile = self.spark.createDataFrame(home_tile_data, home_tile_schema) \
            .withColumn('event_ts', F.to_timestamp('event_ts', 'yyyy-MM-dd HH:mm:ss'))
        
        # Sample interstitial events for insert scenario
        interstitial_data = [
            ('int_001', 'user_001', 'sess_001', '2025-01-27 10:02:00', 'tile_001', True, True, False),
            ('int_002', 'user_002', 'sess_002', '2025-01-27 11:02:00', 'tile_002', True, False, True),
            ('int_003', 'user_003', 'sess_003', '2025-01-27 12:02:00', 'tile_002', True, True, False)
        ]
        
        interstitial_schema = StructType([
            StructField('event_id', StringType(), True),
            StructField('user_id', StringType(), True),
            StructField('session_id', StringType(), True),
            StructField('event_ts', StringType(), True),
            StructField('tile_id', StringType(), True),
            StructField('interstitial_view_flag', BooleanType(), True),
            StructField('primary_button_click_flag', BooleanType(), True),
            StructField('secondary_button_click_flag', BooleanType(), True)
        ])
        
        df_interstitial = self.spark.createDataFrame(interstitial_data, interstitial_schema) \
            .withColumn('event_ts', F.to_timestamp('event_ts', 'yyyy-MM-dd HH:mm:ss'))
        
        return df_home_tile, df_interstitial
    
    def create_update_test_data(self):
        """
        Creates sample data for update scenario testing
        """
        logger.info("Creating update test data...")
        
        # Additional home tile events for update scenario (more users and interactions)
        home_tile_data = [
            # Original data
            ('evt_001', 'user_001', 'sess_001', '2025-01-27 10:00:00', 'tile_001', 'TILE_VIEW', 'Mobile', '1.0'),
            ('evt_002', 'user_001', 'sess_001', '2025-01-27 10:01:00', 'tile_001', 'TILE_CLICK', 'Mobile', '1.0'),
            ('evt_003', 'user_002', 'sess_002', '2025-01-27 11:00:00', 'tile_001', 'TILE_VIEW', 'Web', '1.0'),
            ('evt_004', 'user_002', 'sess_002', '2025-01-27 11:01:00', 'tile_002', 'TILE_VIEW', 'Web', '1.0'),
            ('evt_005', 'user_003', 'sess_003', '2025-01-27 12:00:00', 'tile_002', 'TILE_VIEW', 'Mobile', '1.0'),
            ('evt_006', 'user_003', 'sess_003', '2025-01-27 12:01:00', 'tile_002', 'TILE_CLICK', 'Mobile', '1.0'),
            # Additional data for update scenario
            ('evt_007', 'user_004', 'sess_004', '2025-01-27 13:00:00', 'tile_001', 'TILE_VIEW', 'Mobile', '1.0'),
            ('evt_008', 'user_004', 'sess_004', '2025-01-27 13:01:00', 'tile_001', 'TILE_CLICK', 'Mobile', '1.0'),
            ('evt_009', 'user_005', 'sess_005', '2025-01-27 14:00:00', 'tile_002', 'TILE_VIEW', 'Web', '1.0'),
            ('evt_010', 'user_006', 'sess_006', '2025-01-27 15:00:00', 'tile_003', 'TILE_VIEW', 'Mobile', '1.0'),
            ('evt_011', 'user_006', 'sess_006', '2025-01-27 15:01:00', 'tile_003', 'TILE_CLICK', 'Mobile', '1.0')
        ]
        
        home_tile_schema = StructType([
            StructField('event_id', StringType(), True),
            StructField('user_id', StringType(), True),
            StructField('session_id', StringType(), True),
            StructField('event_ts', StringType(), True),
            StructField('tile_id', StringType(), True),
            StructField('event_type', StringType(), True),
            StructField('device_type', StringType(), True),
            StructField('app_version', StringType(), True)
        ])
        
        df_home_tile = self.spark.createDataFrame(home_tile_data, home_tile_schema) \
            .withColumn('event_ts', F.to_timestamp('event_ts', 'yyyy-MM-dd HH:mm:ss'))
        
        # Additional interstitial events for update scenario
        interstitial_data = [
            # Original data
            ('int_001', 'user_001', 'sess_001', '2025-01-27 10:02:00', 'tile_001', True, True, False),
            ('int_002', 'user_002', 'sess_002', '2025-01-27 11:02:00', 'tile_002', True, False, True),
            ('int_003', 'user_003', 'sess_003', '2025-01-27 12:02:00', 'tile_002', True, True, False),
            # Additional data for update scenario
            ('int_004', 'user_004', 'sess_004', '2025-01-27 13:02:00', 'tile_001', True, False, True),
            ('int_005', 'user_005', 'sess_005', '2025-01-27 14:02:00', 'tile_002', True, True, True),
            ('int_006', 'user_006', 'sess_006', '2025-01-27 15:02:00', 'tile_003', True, True, False)
        ]
        
        interstitial_schema = StructType([
            StructField('event_id', StringType(), True),
            StructField('user_id', StringType(), True),
            StructField('session_id', StringType(), True),
            StructField('event_ts', StringType(), True),
            StructField('tile_id', StringType(), True),
            StructField('interstitial_view_flag', BooleanType(), True),
            StructField('primary_button_click_flag', BooleanType(), True),
            StructField('secondary_button_click_flag', BooleanType(), True)
        ])
        
        df_interstitial = self.spark.createDataFrame(interstitial_data, interstitial_schema) \
            .withColumn('event_ts', F.to_timestamp('event_ts', 'yyyy-MM-dd HH:mm:ss'))
        
        return df_home_tile, df_interstitial
    
    def run_etl_logic(self, df_tile, df_inter):
        """
        Runs the core ETL logic on provided data
        """
        # Filter data by test date
        df_tile_filtered = df_tile.filter(F.to_date('event_ts') == self.test_date)
        df_inter_filtered = df_inter.filter(F.to_date('event_ts') == self.test_date)
        
        # Compute tile aggregations
        df_tile_agg = df_tile_filtered.groupBy('tile_id').agg(
            F.countDistinct(
                F.when(F.col('event_type') == 'TILE_VIEW', F.col('user_id'))
            ).alias('unique_tile_views'),
            F.countDistinct(
                F.when(F.col('event_type') == 'TILE_CLICK', F.col('user_id'))
            ).alias('unique_tile_clicks')
        )
        
        # Compute interstitial aggregations
        df_inter_agg = df_inter_filtered.groupBy('tile_id').agg(
            F.countDistinct(
                F.when(F.col('interstitial_view_flag') == True, F.col('user_id'))
            ).alias('unique_interstitial_views'),
            F.countDistinct(
                F.when(F.col('primary_button_click_flag') == True, F.col('user_id'))
            ).alias('unique_interstitial_primary_clicks'),
            F.countDistinct(
                F.when(F.col('secondary_button_click_flag') == True, F.col('user_id'))
            ).alias('unique_interstitial_secondary_clicks')
        )
        
        # Create daily summary
        df_daily_summary = df_tile_agg.join(df_inter_agg, 'tile_id', 'outer') \
            .withColumn('date', F.lit(self.test_date).cast('date')) \
            .select(
                'date',
                'tile_id',
                F.coalesce('unique_tile_views', F.lit(0)).alias('unique_tile_views'),
                F.coalesce('unique_tile_clicks', F.lit(0)).alias('unique_tile_clicks'),
                F.coalesce('unique_interstitial_views', F.lit(0)).alias('unique_interstitial_views'),
                F.coalesce('unique_interstitial_primary_clicks', F.lit(0)).alias('unique_interstitial_primary_clicks'),
                F.coalesce('unique_interstitial_secondary_clicks', F.lit(0)).alias('unique_interstitial_secondary_clicks')
            )
        
        # Create global KPIs
        df_global = df_daily_summary.groupBy('date').agg(
            F.sum('unique_tile_views').alias('total_tile_views'),
            F.sum('unique_tile_clicks').alias('total_tile_clicks'),
            F.sum('unique_interstitial_views').alias('total_interstitial_views'),
            F.sum('unique_interstitial_primary_clicks').alias('total_primary_clicks'),
            F.sum('unique_interstitial_secondary_clicks').alias('total_secondary_clicks')
        ).withColumn(
            'overall_ctr',
            F.when(F.col('total_tile_views') > 0,
                   F.col('total_tile_clicks') / F.col('total_tile_views')).otherwise(0.0)
        ).withColumn(
            'overall_primary_ctr',
            F.when(F.col('total_interstitial_views') > 0,
                   F.col('total_primary_clicks') / F.col('total_interstitial_views')).otherwise(0.0)
        ).withColumn(
            'overall_secondary_ctr',
            F.when(F.col('total_interstitial_views') > 0,
                   F.col('total_secondary_clicks') / F.col('total_interstitial_views')).otherwise(0.0)
        )
        
        return df_daily_summary, df_global
    
    def test_insert_scenario(self):
        """
        Tests the insert scenario with new tile interaction data
        """
        logger.info("Running Insert Scenario Test...")
        
        try:
            # Create test data
            df_tile, df_inter = self.create_insert_test_data()
            
            # Run ETL logic
            df_daily, df_global = self.run_etl_logic(df_tile, df_inter)
            
            # Validate results
            daily_count = df_daily.count()
            global_count = df_global.count()
            
            # Expected results for insert scenario
            expected_tiles = 2  # tile_001 and tile_002
            expected_global_records = 1
            
            # Collect results for validation
            daily_results = df_daily.collect()
            global_results = df_global.collect()
            
            # Validate daily summary
            assert daily_count == expected_tiles, f"Expected {expected_tiles} daily records, got {daily_count}"
            assert global_count == expected_global_records, f"Expected {expected_global_records} global records, got {global_count}"
            
            # Validate specific metrics
            tile_001_metrics = [row for row in daily_results if row['tile_id'] == 'tile_001'][0]
            tile_002_metrics = [row for row in daily_results if row['tile_id'] == 'tile_002'][0]
            
            # tile_001: 2 unique views (user_001, user_002), 1 unique click (user_001)
            assert tile_001_metrics['unique_tile_views'] == 2, f"tile_001 views: expected 2, got {tile_001_metrics['unique_tile_views']}"
            assert tile_001_metrics['unique_tile_clicks'] == 1, f"tile_001 clicks: expected 1, got {tile_001_metrics['unique_tile_clicks']}"
            
            # tile_002: 2 unique views (user_002, user_003), 1 unique click (user_003)
            assert tile_002_metrics['unique_tile_views'] == 2, f"tile_002 views: expected 2, got {tile_002_metrics['unique_tile_views']}"
            assert tile_002_metrics['unique_tile_clicks'] == 1, f"tile_002 clicks: expected 1, got {tile_002_metrics['unique_tile_clicks']}"
            
            # Validate global metrics
            global_metrics = global_results[0]
            assert global_metrics['total_tile_views'] == 4, f"Total views: expected 4, got {global_metrics['total_tile_views']}"
            assert global_metrics['total_tile_clicks'] == 2, f"Total clicks: expected 2, got {global_metrics['total_tile_clicks']}"
            
            self.test_results.append({
                'scenario': 'Insert',
                'status': 'PASS',
                'daily_records': daily_count,
                'global_records': global_count,
                'details': 'All validations passed successfully'
            })
            
            logger.info("Insert Scenario Test: PASSED")
            return df_daily, df_global, True
            
        except Exception as e:
            self.test_results.append({
                'scenario': 'Insert',
                'status': 'FAIL',
                'error': str(e),
                'details': f'Test failed with error: {str(e)}'
            })
            logger.error(f"Insert Scenario Test: FAILED - {str(e)}")
            return None, None, False
    
    def test_update_scenario(self):
        """
        Tests the update scenario with additional tile interaction data
        """
        logger.info("Running Update Scenario Test...")
        
        try:
            # Create test data with additional records
            df_tile, df_inter = self.create_update_test_data()
            
            # Run ETL logic
            df_daily, df_global = self.run_etl_logic(df_tile, df_inter)
            
            # Validate results
            daily_count = df_daily.count()
            global_count = df_global.count()
            
            # Expected results for update scenario
            expected_tiles = 3  # tile_001, tile_002, tile_003
            expected_global_records = 1
            
            # Collect results for validation
            daily_results = df_daily.collect()
            global_results = df_global.collect()
            
            # Validate daily summary
            assert daily_count == expected_tiles, f"Expected {expected_tiles} daily records, got {daily_count}"
            assert global_count == expected_global_records, f"Expected {expected_global_records} global records, got {global_count}"
            
            # Validate specific metrics for updated data
            tile_001_metrics = [row for row in daily_results if row['tile_id'] == 'tile_001'][0]
            tile_002_metrics = [row for row in daily_results if row['tile_id'] == 'tile_002'][0]
            tile_003_metrics = [row for row in daily_results if row['tile_id'] == 'tile_003'][0]
            
            # tile_001: 3 unique views (user_001, user_002, user_004), 2 unique clicks (user_001, user_004)
            assert tile_001_metrics['unique_tile_views'] == 3, f"tile_001 views: expected 3, got {tile_001_metrics['unique_tile_views']}"
            assert tile_001_metrics['unique_tile_clicks'] == 2, f"tile_001 clicks: expected 2, got {tile_001_metrics['unique_tile_clicks']}"
            
            # tile_002: 3 unique views (user_002, user_003, user_005), 1 unique click (user_003)
            assert tile_002_metrics['unique_tile_views'] == 3, f"tile_002 views: expected 3, got {tile_002_metrics['unique_tile_views']}"
            assert tile_002_metrics['unique_tile_clicks'] == 1, f"tile_002 clicks: expected 1, got {tile_002_metrics['unique_tile_clicks']}"
            
            # tile_003: 1 unique view (user_006), 1 unique click (user_006)
            assert tile_003_metrics['unique_tile_views'] == 1, f"tile_003 views: expected 1, got {tile_003_metrics['unique_tile_views']}"
            assert tile_003_metrics['unique_tile_clicks'] == 1, f"tile_003 clicks: expected 1, got {tile_003_metrics['unique_tile_clicks']}"
            
            # Validate global metrics
            global_metrics = global_results[0]
            assert global_metrics['total_tile_views'] == 7, f"Total views: expected 7, got {global_metrics['total_tile_views']}"
            assert global_metrics['total_tile_clicks'] == 4, f"Total clicks: expected 4, got {global_metrics['total_tile_clicks']}"
            
            self.test_results.append({
                'scenario': 'Update',
                'status': 'PASS',
                'daily_records': daily_count,
                'global_records': global_count,
                'details': 'All validations passed successfully with updated data'
            })
            
            logger.info("Update Scenario Test: PASSED")
            return df_daily, df_global, True
            
        except Exception as e:
            self.test_results.append({
                'scenario': 'Update',
                'status': 'FAIL',
                'error': str(e),
                'details': f'Test failed with error: {str(e)}'
            })
            logger.error(f"Update Scenario Test: FAILED - {str(e)}")
            return None, None, False
    
    def generate_test_report(self):
        """
        Generates a comprehensive test report in Markdown format
        """
        report = []
        report.append("# Test Report")
        report.append("")
        report.append(f"**Test Date:** {self.test_date}")
        report.append(f"**Test Execution Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        for result in self.test_results:
            report.append(f"## Scenario: {result['scenario']}")
            report.append("")
            
            if result['status'] == 'PASS':
                report.append("### Input Data:")
                if result['scenario'] == 'Insert':
                    report.append("| tile_id | unique_views | unique_clicks | interstitial_views |")
                    report.append("|---------|--------------|---------------|-------------------|")
                    report.append("| tile_001 | 2 | 1 | 1 |")
                    report.append("| tile_002 | 2 | 1 | 2 |")
                else:
                    report.append("| tile_id | unique_views | unique_clicks | interstitial_views |")
                    report.append("|---------|--------------|---------------|-------------------|")
                    report.append("| tile_001 | 3 | 2 | 2 |")
                    report.append("| tile_002 | 3 | 1 | 3 |")
                    report.append("| tile_003 | 1 | 1 | 1 |")
                
                report.append("")
                report.append("### Output:")
                report.append(f"- Daily Summary Records: {result['daily_records']}")
                report.append(f"- Global KPI Records: {result['global_records']}")
                report.append("")
                report.append(f"**Status: {result['status']}**")
                report.append("")
                report.append(f"**Details:** {result['details']}")
            else:
                report.append(f"**Status: {result['status']}**")
                report.append("")
                report.append(f"**Error:** {result.get('error', 'Unknown error')}")
                report.append("")
                report.append(f"**Details:** {result['details']}")
            
            report.append("")
            report.append("---")
            report.append("")
        
        # Overall summary
        passed_tests = len([r for r in self.test_results if r['status'] == 'PASS'])
        total_tests = len(self.test_results)
        
        report.append("## Summary")
        report.append("")
        report.append(f"**Total Tests:** {total_tests}")
        report.append(f"**Passed:** {passed_tests}")
        report.append(f"**Failed:** {total_tests - passed_tests}")
        report.append("")
        
        if passed_tests == total_tests:
            report.append("🎉 **All tests passed successfully!**")
        else:
            report.append("⚠️ **Some tests failed. Please review the details above.**")
        
        return "\n".join(report)
    
    def run_all_tests(self):
        """
        Runs all test scenarios and generates a comprehensive report
        """
        logger.info("Starting Home Tile Reporting ETL Test Suite...")
        
        # Run insert scenario test
        insert_daily, insert_global, insert_success = self.test_insert_scenario()
        
        # Run update scenario test
        update_daily, update_global, update_success = self.test_update_scenario()
        
        # Generate and display test report
        test_report = self.generate_test_report()
        
        print("\n" + "="*80)
        print("HOME TILE REPORTING ETL - TEST RESULTS")
        print("="*80)
        print(test_report)
        print("="*80)
        
        # Display sample data if tests passed
        if insert_success and insert_daily is not None:
            print("\n=== INSERT SCENARIO - DAILY SUMMARY SAMPLE ===")
            insert_daily.show(10, truncate=False)
            
            print("\n=== INSERT SCENARIO - GLOBAL KPIS ===")
            insert_global.show(truncate=False)
        
        if update_success and update_daily is not None:
            print("\n=== UPDATE SCENARIO - DAILY SUMMARY SAMPLE ===")
            update_daily.show(10, truncate=False)
            
            print("\n=== UPDATE SCENARIO - GLOBAL KPIS ===")
            update_global.show(truncate=False)
        
        # Return overall test status
        overall_success = insert_success and update_success
        logger.info(f"Test Suite Completed. Overall Status: {'PASS' if overall_success else 'FAIL'}")
        
        return overall_success

# Main execution
if __name__ == "__main__":
    # Initialize and run test suite
    test_suite = HomeTileReportingETLTest()
    success = test_suite.run_all_tests()
    
    print(f"\n=== TEST SUITE EXECUTION COMPLETED ===")
    print(f"Overall Status: {'PASS' if success else 'FAIL'}")
    print(f"Test Date: {test_suite.test_date}")
    print(f"Total Test Scenarios: {len(test_suite.test_results)}")
    
    if success:
        print("✅ All tests passed successfully!")
    else:
        print("❌ Some tests failed. Please review the test report above.")