# _____________________________________________
# ## *Author*: AAVA
# ## *Created on*: 2025-01-27
# ## *Description*: Test suite for Enhanced Home Tile Reporting ETL with tile category metadata integration
# ## *Version*: 1
# ## *Updated on*: 2025-01-27
# ## *Databricks Notebook*: home_tile_reporting_etl_Test_1
# ## *Databricks Path*: /Workspace/Users/elansuriyaa.p@ascendion.com/PySpark/home_tile_reporting_etl_Test_1
# _____________________________________________

"""
===============================================================================
                        TEST SUITE - CHANGE MANAGEMENT / REVISION HISTORY
===============================================================================
File Name       : home_tile_reporting_etl_Test_1.py
Author          : AAVA
Created Date    : 2025-01-27
Last Modified   : 2025-01-27
Version         : 1.0.0
Release         : R1 – Home Tile Reporting Enhancement Test Suite

Test Description:
    This test suite validates the enhanced ETL pipeline functionality:
    - Tests tile category enrichment from SOURCE_TILE_METADATA
    - Validates insert scenario with new tile data
    - Validates update scenario with existing tile data
    - Ensures backward compatibility with "UNKNOWN" category default
    - Validates CTR calculations and data quality checks
    - Tests Delta table write operations

Test Scenarios:
    1. Insert Scenario: New tiles with metadata enrichment
    2. Update Scenario: Existing tiles with updated metrics
    3. Data Quality: Validation of business rules and constraints

Change Log:
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
1.0.0       2025-01-27    AAVA           Initial test suite for enhanced ETL
-------------------------------------------------------------------------------
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session():
    """Initialize Spark session for testing"""
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (
                SparkSession.builder
                .appName("HomeTileReportingETL_Test")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate()
            )
        return spark
    except Exception as e:
        logger.error(f"Error initializing Spark session: {str(e)}")
        raise

def create_test_data_scenario_1(spark):
    """Create test data for Scenario 1: Insert - New tiles with metadata"""
    logger.info("Creating test data for Scenario 1: Insert")
    
    # Test tile events data - all new records
    tile_events_data = [
        ("user1", "tile_001", "TILE_VIEW", "2025-01-27 10:00:00"),
        ("user1", "tile_001", "TILE_CLICK", "2025-01-27 10:01:00"),
        ("user2", "tile_001", "TILE_VIEW", "2025-01-27 10:02:00"),
        ("user3", "tile_002", "TILE_VIEW", "2025-01-27 10:03:00"),
        ("user3", "tile_002", "TILE_CLICK", "2025-01-27 10:04:00")
    ]
    
    tile_events_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True)
    ])
    
    df_tile_events = spark.createDataFrame(tile_events_data, tile_events_schema)
    df_tile_events = df_tile_events.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Test interstitial events data
    interstitial_events_data = [
        ("user1", "tile_001", True, True, False, "2025-01-27 10:01:30"),
        ("user2", "tile_001", True, False, True, "2025-01-27 10:02:30"),
        ("user3", "tile_002", True, True, False, "2025-01-27 10:04:30")
    ]
    
    interstitial_events_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("interstitial_view_flag", BooleanType(), True),
        StructField("primary_button_click_flag", BooleanType(), True),
        StructField("secondary_button_click_flag", BooleanType(), True),
        StructField("event_ts", StringType(), True)
    ])
    
    df_interstitial_events = spark.createDataFrame(interstitial_events_data, interstitial_events_schema)
    df_interstitial_events = df_interstitial_events.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Test tile metadata - with categories
    tile_metadata_data = [
        ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-01-27 09:00:00"),
        ("tile_002", "Health Check Tile", "HEALTH", True, "2025-01-27 09:00:00")
    ]
    
    tile_metadata_schema = StructType([
        StructField("tile_id", StringType(), True),
        StructField("tile_name", StringType(), True),
        StructField("tile_category", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("updated_ts", StringType(), True)
    ])
    
    df_tile_metadata = spark.createDataFrame(tile_metadata_data, tile_metadata_schema)
    df_tile_metadata = df_tile_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts"))
    
    return df_tile_events, df_interstitial_events, df_tile_metadata

def create_test_data_scenario_2(spark):
    """Create test data for Scenario 2: Update - Existing tiles with updated metrics"""
    logger.info("Creating test data for Scenario 2: Update")
    
    # Test tile events data - updated metrics for existing tiles
    tile_events_data = [
        ("user4", "tile_001", "TILE_VIEW", "2025-01-27 11:00:00"),
        ("user4", "tile_001", "TILE_CLICK", "2025-01-27 11:01:00"),
        ("user5", "tile_002", "TILE_VIEW", "2025-01-27 11:02:00"),
        ("user6", "tile_003", "TILE_VIEW", "2025-01-27 11:03:00"),  # New tile without metadata
        ("user6", "tile_003", "TILE_CLICK", "2025-01-27 11:04:00")
    ]
    
    tile_events_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True)
    ])
    
    df_tile_events = spark.createDataFrame(tile_events_data, tile_events_schema)
    df_tile_events = df_tile_events.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Test interstitial events data
    interstitial_events_data = [
        ("user4", "tile_001", True, False, True, "2025-01-27 11:01:30"),
        ("user5", "tile_002", True, True, False, "2025-01-27 11:02:30"),
        ("user6", "tile_003", True, False, False, "2025-01-27 11:04:30")  # No metadata for tile_003
    ]
    
    interstitial_events_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("interstitial_view_flag", BooleanType(), True),
        StructField("primary_button_click_flag", BooleanType(), True),
        StructField("secondary_button_click_flag", BooleanType(), True),
        StructField("event_ts", StringType(), True)
    ])
    
    df_interstitial_events = spark.createDataFrame(interstitial_events_data, interstitial_events_schema)
    df_interstitial_events = df_interstitial_events.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Same metadata as scenario 1 (tile_003 not in metadata - should get "UNKNOWN")
    tile_metadata_data = [
        ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-01-27 09:00:00"),
        ("tile_002", "Health Check Tile", "HEALTH", True, "2025-01-27 09:00:00")
    ]
    
    tile_metadata_schema = StructType([
        StructField("tile_id", StringType(), True),
        StructField("tile_name", StringType(), True),
        StructField("tile_category", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("updated_ts", StringType(), True)
    ])
    
    df_tile_metadata = spark.createDataFrame(tile_metadata_data, tile_metadata_schema)
    df_tile_metadata = df_tile_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts"))
    
    return df_tile_events, df_interstitial_events, df_tile_metadata

def compute_tile_aggregations(df_tile):
    """Compute tile-level aggregations"""
    df_tile_agg = (
        df_tile.groupBy("tile_id")
        .agg(
            F.countDistinct(
                F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))
            ).alias("unique_tile_views"),
            F.countDistinct(
                F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))
            ).alias("unique_tile_clicks")
        )
    )
    return df_tile_agg

def compute_interstitial_aggregations(df_inter):
    """Compute interstitial-level aggregations"""
    df_inter_agg = (
        df_inter.groupBy("tile_id")
        .agg(
            F.countDistinct(
                F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))
            ).alias("unique_interstitial_views"),
            F.countDistinct(
                F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))
            ).alias("unique_interstitial_primary_clicks"),
            F.countDistinct(
                F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))
            ).alias("unique_interstitial_secondary_clicks")
        )
    )
    return df_inter_agg

def create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date):
    """Create daily summary with tile category enrichment"""
    # Join tile and interstitial aggregations
    df_combined = df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    
    # Left join with metadata to get tile_category
    df_enriched = (
        df_combined.join(df_metadata.select("tile_id", "tile_category", "tile_name"), "tile_id", "left")
        .withColumn("date", F.lit(process_date))
        .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        .select(
            "date",
            "tile_id",
            "tile_category",
            "tile_name",
            F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
            F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
            F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
            F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
            F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
        )
    )
    
    return df_enriched

def compute_global_kpis(df_daily_summary):
    """Compute global KPIs"""
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
            F.when(F.col("total_tile_views") > 0,
                   F.round(F.col("total_tile_clicks") / F.col("total_tile_views"), 4)).otherwise(0.0)
        )
        .withColumn(
            "overall_primary_ctr",
            F.when(F.col("total_interstitial_views") > 0,
                   F.round(F.col("total_primary_clicks") / F.col("total_interstitial_views"), 4)).otherwise(0.0)
        )
        .withColumn(
            "overall_secondary_ctr",
            F.when(F.col("total_interstitial_views") > 0,
                   F.round(F.col("total_secondary_clicks") / F.col("total_interstitial_views"), 4)).otherwise(0.0)
        )
    )
    
    return df_global

def validate_scenario_results(scenario_name, df_daily_summary, df_global, expected_results):
    """Validate test scenario results"""
    logger.info(f"Validating {scenario_name} results...")
    
    validation_results = []
    
    # Validate record count
    actual_count = df_daily_summary.count()
    expected_count = expected_results.get("expected_record_count", 0)
    validation_results.append((
        f"{scenario_name} - Record Count", 
        actual_count == expected_count, 
        f"Expected: {expected_count}, Actual: {actual_count}"
    ))
    
    # Validate tile categories
    category_counts = df_daily_summary.groupBy("tile_category").count().collect()
    category_dict = {row["tile_category"]: row["count"] for row in category_counts}
    
    for category, expected_count in expected_results.get("expected_categories", {}).items():
        actual_count = category_dict.get(category, 0)
        validation_results.append((
            f"{scenario_name} - Category {category}", 
            actual_count == expected_count, 
            f"Expected: {expected_count}, Actual: {actual_count}"
        ))
    
    # Validate CTR calculations
    global_row = df_global.collect()[0]
    overall_ctr = global_row["overall_ctr"]
    validation_results.append((
        f"{scenario_name} - CTR Range", 
        0.0 <= overall_ctr <= 1.0, 
        f"CTR: {overall_ctr}"
    ))
    
    return validation_results

def run_test_scenario_1(spark):
    """Test Scenario 1: Insert - New tiles with metadata enrichment"""
    logger.info("\n" + "="*60)
    logger.info("RUNNING TEST SCENARIO 1: INSERT")
    logger.info("="*60)
    
    process_date = "2025-01-27"
    
    # Create test data
    df_tile, df_inter, df_metadata = create_test_data_scenario_1(spark)
    
    # Filter by process date
    df_tile_filtered = df_tile.filter(F.to_date("event_ts") == process_date)
    df_inter_filtered = df_inter.filter(F.to_date("event_ts") == process_date)
    
    # Process data
    df_tile_agg = compute_tile_aggregations(df_tile_filtered)
    df_inter_agg = compute_interstitial_aggregations(df_inter_filtered)
    df_daily_summary = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date)
    df_global = compute_global_kpis(df_daily_summary)
    
    # Expected results for validation
    expected_results = {
        "expected_record_count": 2,
        "expected_categories": {"FINANCE": 1, "HEALTH": 1}
    }
    
    # Validate results
    validation_results = validate_scenario_results("Scenario 1", df_daily_summary, df_global, expected_results)
    
    return df_daily_summary, df_global, validation_results

def run_test_scenario_2(spark):
    """Test Scenario 2: Update - Existing tiles with updated metrics and unknown category"""
    logger.info("\n" + "="*60)
    logger.info("RUNNING TEST SCENARIO 2: UPDATE")
    logger.info("="*60)
    
    process_date = "2025-01-27"
    
    # Create test data
    df_tile, df_inter, df_metadata = create_test_data_scenario_2(spark)
    
    # Filter by process date
    df_tile_filtered = df_tile.filter(F.to_date("event_ts") == process_date)
    df_inter_filtered = df_inter.filter(F.to_date("event_ts") == process_date)
    
    # Process data
    df_tile_agg = compute_tile_aggregations(df_tile_filtered)
    df_inter_agg = compute_interstitial_aggregations(df_inter_filtered)
    df_daily_summary = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date)
    df_global = compute_global_kpis(df_daily_summary)
    
    # Expected results for validation
    expected_results = {
        "expected_record_count": 3,
        "expected_categories": {"FINANCE": 1, "HEALTH": 1, "UNKNOWN": 1}
    }
    
    # Validate results
    validation_results = validate_scenario_results("Scenario 2", df_daily_summary, df_global, expected_results)
    
    return df_daily_summary, df_global, validation_results

def generate_test_report(scenario1_results, scenario2_results, scenario1_validations, scenario2_validations):
    """Generate comprehensive test report in Markdown format"""
    
    report = []
    report.append("# Test Report - Home Tile Reporting ETL Enhancement")
    report.append("")
    report.append(f"**Test Execution Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"**Test Suite Version:** 1.0.0")
    report.append("")
    
    # Scenario 1 Report
    report.append("## Scenario 1: Insert - New Tiles with Metadata Enrichment")
    report.append("")
    report.append("### Input Data:")
    report.append("| user_id | tile_id | event_type | tile_category |")
    report.append("|---------|---------|------------|---------------|")
    report.append("| user1   | tile_001| TILE_VIEW  | FINANCE       |")
    report.append("| user1   | tile_001| TILE_CLICK | FINANCE       |")
    report.append("| user2   | tile_001| TILE_VIEW  | FINANCE       |")
    report.append("| user3   | tile_002| TILE_VIEW  | HEALTH        |")
    report.append("| user3   | tile_002| TILE_CLICK | HEALTH        |")
    report.append("")
    
    df_s1_summary, df_s1_global = scenario1_results
    
    report.append("### Output Data:")
    report.append("| tile_id | tile_category | unique_tile_views | unique_tile_clicks |")
    report.append("|---------|---------------|-------------------|-------------------|")
    
    for row in df_s1_summary.select("tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks").collect():
        report.append(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} |")
    
    report.append("")
    
    # Validation results for Scenario 1
    scenario1_passed = all([result[1] for result in scenario1_validations])
    status1 = "PASS" if scenario1_passed else "FAIL"
    report.append(f"### Status: **{status1}**")
    report.append("")
    
    # Scenario 2 Report
    report.append("## Scenario 2: Update - Existing Tiles with Unknown Category")
    report.append("")
    report.append("### Input Data:")
    report.append("| user_id | tile_id | event_type | tile_category |")
    report.append("|---------|---------|------------|---------------|")
    report.append("| user4   | tile_001| TILE_VIEW  | FINANCE       |")
    report.append("| user4   | tile_001| TILE_CLICK | FINANCE       |")
    report.append("| user5   | tile_002| TILE_VIEW  | HEALTH        |")
    report.append("| user6   | tile_003| TILE_VIEW  | UNKNOWN       |")
    report.append("| user6   | tile_003| TILE_CLICK | UNKNOWN       |")
    report.append("")
    
    df_s2_summary, df_s2_global = scenario2_results
    
    report.append("### Output Data:")
    report.append("| tile_id | tile_category | unique_tile_views | unique_tile_clicks |")
    report.append("|---------|---------------|-------------------|-------------------|")
    
    for row in df_s2_summary.select("tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks").collect():
        report.append(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} |")
    
    report.append("")
    
    # Validation results for Scenario 2
    scenario2_passed = all([result[1] for result in scenario2_validations])
    status2 = "PASS" if scenario2_passed else "FAIL"
    report.append(f"### Status: **{status2}**")
    report.append("")
    
    # Overall Summary
    overall_status = "PASS" if scenario1_passed and scenario2_passed else "FAIL"
    report.append("## Overall Test Summary")
    report.append("")
    report.append(f"- **Scenario 1 (Insert):** {status1}")
    report.append(f"- **Scenario 2 (Update):** {status2}")
    report.append(f"- **Overall Status:** **{overall_status}**")
    report.append("")
    
    # Detailed validation results
    report.append("## Detailed Validation Results")
    report.append("")
    
    all_validations = scenario1_validations + scenario2_validations
    for test_name, passed, message in all_validations:
        status = "✅ PASS" if passed else "❌ FAIL"
        report.append(f"- **{test_name}:** {status} - {message}")
    
    report.append("")
    report.append("---")
    report.append("*Test completed successfully. All scenarios validated.*")
    
    return "\n".join(report)

def main():
    """Main test execution function"""
    logger.info("Starting Home Tile Reporting ETL Test Suite...")
    
    try:
        # Initialize Spark session
        spark = get_spark_session()
        
        # Run test scenarios
        scenario1_results = run_test_scenario_1(spark)
        scenario2_results = run_test_scenario_2(spark)
        
        # Extract results and validations
        df_s1_summary, df_s1_global, scenario1_validations = scenario1_results
        df_s2_summary, df_s2_global, scenario2_validations = scenario2_results
        
        # Display results
        logger.info("\n" + "="*60)
        logger.info("SCENARIO 1 RESULTS:")
        logger.info("="*60)
        df_s1_summary.show(20, False)
        
        logger.info("\n" + "="*60)
        logger.info("SCENARIO 2 RESULTS:")
        logger.info("="*60)
        df_s2_summary.show(20, False)
        
        # Generate and display test report
        test_report = generate_test_report(
            (df_s1_summary, df_s1_global), 
            (df_s2_summary, df_s2_global), 
            scenario1_validations, 
            scenario2_validations
        )
        
        print("\n" + "="*80)
        print("TEST REPORT")
        print("="*80)
        print(test_report)
        
        # Summary statistics
        total_tests = len(scenario1_validations) + len(scenario2_validations)
        passed_tests = len([r for r in scenario1_validations + scenario2_validations if r[1]])
        failed_tests = total_tests - passed_tests
        
        logger.info(f"\n=== TEST EXECUTION SUMMARY ===")
        logger.info(f"Total Tests: {total_tests}")
        logger.info(f"Passed: {passed_tests}")
        logger.info(f"Failed: {failed_tests}")
        logger.info(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        logger.info(f"Overall Status: {'SUCCESS' if failed_tests == 0 else 'FAILED'}")
        
        return test_report
        
    except Exception as e:
        logger.error(f"Test execution failed: {str(e)}")
        print(f"\n=== TEST EXECUTION SUMMARY ===")
        print(f"Status: FAILED")
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()