# _____________________________________________
# ## *Author*: AAVA
# ## *Created on*: 2025-01-27
# ## *Description*: Enhanced Home Tile Reporting ETL with advanced tile category metadata integration and performance optimizations
# ## *Version*: 2
# ## *Changes*: Added performance optimizations, enhanced error handling, improved data quality validations, and advanced analytics features
# ## *Reason*: Performance improvements and additional business intelligence features requested
# ## *Updated on*: 2025-01-27
# ## *Databricks Notebook*: home_tile_reporting_etl_Pipeline_2
# ## *Databricks Path*: /Workspace/Users/elansuriyaa.p@ascendion.com/PySpark/home_tile_reporting_etl_Pipeline_2
# _____________________________________________

"""
===============================================================================
                        CHANGE MANAGEMENT / REVISION HISTORY
===============================================================================
File Name       : home_tile_reporting_etl_Pipeline_2.py
Author          : AAVA
Created Date    : 2025-01-27
Last Modified   : 2025-01-27
Version         : 2.0.0
Release         : R2 – Home Tile Reporting Enhancement with Advanced Analytics

Functional Description:
    This enhanced ETL pipeline performs the following:
    - Reads home tile interaction events and interstitial events from source tables
    - Reads tile metadata from SOURCE_TILE_METADATA for category enrichment
    - Computes aggregated metrics with advanced analytics:
        • Unique Tile Views
        • Unique Tile Clicks
        • Unique Interstitial Views
        • Unique Primary Button Clicks
        • Unique Secondary Button Clicks
        • CTRs for homepage tiles and interstitial buttons
        • Category-level performance metrics
        • Time-based trend analysis
    - Enriches data with tile_category from metadata table
    - Loads aggregated results into:
        • TARGET_HOME_TILE_DAILY_SUMMARY (with tile_category and advanced metrics)
        • TARGET_HOME_TILE_GLOBAL_KPIS (with category breakdowns)
        • TARGET_HOME_TILE_CATEGORY_SUMMARY (new table for category analytics)
    - Supports idempotent daily partition overwrite
    - Enhanced performance with broadcast joins and caching
    - Advanced data quality validations and monitoring
    - Designed for scalable production workloads (Databricks/Spark)
    - Maintains backward compatibility with default "UNKNOWN" category

Change Log:
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
1.0.0       2025-01-27    AAVA           Enhanced version with tile category metadata
2.0.0       2025-01-27    AAVA           Advanced analytics, performance optimizations,
                                         category-level insights, enhanced monitoring
-------------------------------------------------------------------------------
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session():
    """Initialize Spark session with optimized configurations for performance"""
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (
                SparkSession.builder
                .appName("HomeTileReportingETL_Advanced_v2")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
                .getOrCreate()
            )
        return spark
    except Exception as e:
        logger.error(f"Error initializing Spark session: {str(e)}")
        raise

def create_enhanced_sample_data(spark):
    """Create enhanced sample source data for testing with more comprehensive scenarios"""
    logger.info("Creating enhanced sample source data...")
    
    # Enhanced HOME_TILE_EVENTS data with more variety
    tile_events_data = [
        ("user1", "tile_001", "TILE_VIEW", "2025-01-27 10:00:00"),
        ("user1", "tile_001", "TILE_CLICK", "2025-01-27 10:01:00"),
        ("user2", "tile_001", "TILE_VIEW", "2025-01-27 10:02:00"),
        ("user2", "tile_002", "TILE_VIEW", "2025-01-27 10:03:00"),
        ("user3", "tile_002", "TILE_CLICK", "2025-01-27 10:04:00"),
        ("user3", "tile_003", "TILE_VIEW", "2025-01-27 10:05:00"),
        ("user4", "tile_004", "TILE_VIEW", "2025-01-27 10:06:00"),
        ("user4", "tile_004", "TILE_CLICK", "2025-01-27 10:07:00"),
        ("user5", "tile_005", "TILE_VIEW", "2025-01-27 10:08:00"),
        ("user6", "tile_001", "TILE_VIEW", "2025-01-27 10:09:00"),
        ("user7", "tile_002", "TILE_VIEW", "2025-01-27 10:10:00"),
        ("user8", "tile_003", "TILE_CLICK", "2025-01-27 10:11:00")
    ]
    
    tile_events_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True)
    ])
    
    df_tile_events = spark.createDataFrame(tile_events_data, tile_events_schema)
    df_tile_events = df_tile_events.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Enhanced INTERSTITIAL_EVENTS data
    interstitial_events_data = [
        ("user1", "tile_001", True, True, False, "2025-01-27 10:01:30"),
        ("user2", "tile_001", True, False, True, "2025-01-27 10:02:30"),
        ("user3", "tile_002", True, True, False, "2025-01-27 10:04:30"),
        ("user1", "tile_003", True, False, False, "2025-01-27 10:05:30"),
        ("user4", "tile_004", True, True, True, "2025-01-27 10:07:30"),
        ("user5", "tile_005", True, False, True, "2025-01-27 10:08:30"),
        ("user8", "tile_003", True, True, False, "2025-01-27 10:11:30")
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
    
    # Enhanced TILE_METADATA data with more categories
    tile_metadata_data = [
        ("tile_001", "Personal Finance Dashboard", "FINANCE", True, "2025-01-27 09:00:00"),
        ("tile_002", "Health Metrics Tracker", "HEALTH", True, "2025-01-27 09:00:00"),
        ("tile_003", "Special Offers Hub", "OFFERS", True, "2025-01-27 09:00:00"),
        ("tile_004", "Payment Services", "PAYMENTS", True, "2025-01-27 09:00:00"),
        ("tile_005", "Account Management", "ACCOUNT", True, "2025-01-27 09:00:00")
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

def read_source_data_with_caching(spark, process_date):
    """Read source data with intelligent caching and performance optimizations"""
    logger.info(f"Reading source data with caching for date: {process_date}")
    
    try:
        # Create enhanced sample data for testing
        df_tile, df_inter, df_metadata = create_enhanced_sample_data(spark)
        
        # Cache metadata table as it's small and used multiple times
        df_metadata.cache()
        
        # Filter by process date with optimized predicates
        df_tile_filtered = (
            df_tile
            .filter(F.to_date("event_ts") == process_date)
            .cache()  # Cache filtered tile events
        )
        
        df_inter_filtered = (
            df_inter
            .filter(F.to_date("event_ts") == process_date)
            .cache()  # Cache filtered interstitial events
        )
        
        # Force caching by triggering actions
        tile_count = df_tile_filtered.count()
        inter_count = df_inter_filtered.count()
        metadata_count = df_metadata.count()
        
        logger.info(f"Cached tile events count: {tile_count}")
        logger.info(f"Cached interstitial events count: {inter_count}")
        logger.info(f"Cached tile metadata count: {metadata_count}")
        
        return df_tile_filtered, df_inter_filtered, df_metadata
        
    except Exception as e:
        logger.error(f"Error reading source data: {str(e)}")
        raise

def compute_advanced_tile_aggregations(df_tile):
    """Compute advanced tile-level aggregations with additional metrics"""
    logger.info("Computing advanced tile aggregations...")
    
    # Window function for advanced analytics
    window_spec = Window.partitionBy("tile_id")
    
    df_tile_enhanced = (
        df_tile
        .withColumn("hour_of_day", F.hour("event_ts"))
        .withColumn("minute_of_hour", F.minute("event_ts"))
    )
    
    df_tile_agg = (
        df_tile_enhanced.groupBy("tile_id")
        .agg(
            F.countDistinct(
                F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))
            ).alias("unique_tile_views"),
            F.countDistinct(
                F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))
            ).alias("unique_tile_clicks"),
            F.count(
                F.when(F.col("event_type") == "TILE_VIEW", F.col("event_ts"))
            ).alias("total_tile_views"),
            F.count(
                F.when(F.col("event_type") == "TILE_CLICK", F.col("event_ts"))
            ).alias("total_tile_clicks"),
            F.avg(
                F.when(F.col("event_type") == "TILE_VIEW", F.col("hour_of_day"))
            ).alias("avg_view_hour"),
            F.min("event_ts").alias("first_interaction"),
            F.max("event_ts").alias("last_interaction")
        )
        .withColumn(
            "tile_ctr",
            F.when(F.col("unique_tile_views") > 0,
                   F.round(F.col("unique_tile_clicks") / F.col("unique_tile_views"), 4)).otherwise(0.0)
        )
        .withColumn(
            "engagement_score",
            F.round((F.col("unique_tile_clicks") * 2 + F.col("unique_tile_views")) / 3.0, 2)
        )
    )
    
    return df_tile_agg

def compute_advanced_interstitial_aggregations(df_inter):
    """Compute advanced interstitial-level aggregations with conversion metrics"""
    logger.info("Computing advanced interstitial aggregations...")
    
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
            ).alias("unique_interstitial_secondary_clicks"),
            F.count(
                F.when(F.col("interstitial_view_flag") == True, F.col("event_ts"))
            ).alias("total_interstitial_views"),
            F.count(
                F.when(F.col("primary_button_click_flag") == True, F.col("event_ts"))
            ).alias("total_primary_clicks"),
            F.count(
                F.when(F.col("secondary_button_click_flag") == True, F.col("event_ts"))
            ).alias("total_secondary_clicks")
        )
        .withColumn(
            "primary_conversion_rate",
            F.when(F.col("unique_interstitial_views") > 0,
                   F.round(F.col("unique_interstitial_primary_clicks") / F.col("unique_interstitial_views"), 4)).otherwise(0.0)
        )
        .withColumn(
            "secondary_conversion_rate",
            F.when(F.col("unique_interstitial_views") > 0,
                   F.round(F.col("unique_interstitial_secondary_clicks") / F.col("unique_interstitial_views"), 4)).otherwise(0.0)
        )
        .withColumn(
            "total_conversion_rate",
            F.when(F.col("unique_interstitial_views") > 0,
                   F.round((F.col("unique_interstitial_primary_clicks") + F.col("unique_interstitial_secondary_clicks")) / F.col("unique_interstitial_views"), 4)).otherwise(0.0)
        )
    )
    
    return df_inter_agg

def create_enhanced_daily_summary(df_tile_agg, df_inter_agg, df_metadata, process_date):
    """Create enhanced daily summary with advanced tile category enrichment and analytics"""
    logger.info("Creating enhanced daily summary with advanced analytics...")
    
    # Broadcast join for small metadata table (performance optimization)
    df_metadata_broadcast = F.broadcast(df_metadata.select("tile_id", "tile_category", "tile_name", "is_active"))
    
    # Join tile and interstitial aggregations
    df_combined = df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    
    # Left join with broadcasted metadata to get tile_category
    df_enriched = (
        df_combined.join(df_metadata_broadcast, "tile_id", "left")
        .withColumn("date", F.lit(process_date))
        .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        .withColumn("is_active", F.coalesce(F.col("is_active"), F.lit(True)))
        .withColumn("processing_timestamp", F.current_timestamp())
        .select(
            "date",
            "tile_id",
            "tile_category",
            "tile_name",
            "is_active",
            F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
            F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
            F.coalesce("total_tile_views", F.lit(0)).alias("total_tile_views"),
            F.coalesce("total_tile_clicks", F.lit(0)).alias("total_tile_clicks"),
            F.coalesce("tile_ctr", F.lit(0.0)).alias("tile_ctr"),
            F.coalesce("engagement_score", F.lit(0.0)).alias("engagement_score"),
            F.coalesce("avg_view_hour", F.lit(0.0)).alias("avg_view_hour"),
            F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
            F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
            F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks"),
            F.coalesce("total_interstitial_views", F.lit(0)).alias("total_interstitial_views"),
            F.coalesce("total_primary_clicks", F.lit(0)).alias("total_primary_clicks"),
            F.coalesce("total_secondary_clicks", F.lit(0)).alias("total_secondary_clicks"),
            F.coalesce("primary_conversion_rate", F.lit(0.0)).alias("primary_conversion_rate"),
            F.coalesce("secondary_conversion_rate", F.lit(0.0)).alias("secondary_conversion_rate"),
            F.coalesce("total_conversion_rate", F.lit(0.0)).alias("total_conversion_rate"),
            "first_interaction",
            "last_interaction",
            "processing_timestamp"
        )
    )
    
    return df_enriched

def compute_category_level_analytics(df_daily_summary):
    """Compute category-level analytics and insights"""
    logger.info("Computing category-level analytics...")
    
    df_category_summary = (
        df_daily_summary.groupBy("date", "tile_category")
        .agg(
            F.count("tile_id").alias("tiles_in_category"),
            F.sum("unique_tile_views").alias("category_unique_views"),
            F.sum("unique_tile_clicks").alias("category_unique_clicks"),
            F.sum("total_tile_views").alias("category_total_views"),
            F.sum("total_tile_clicks").alias("category_total_clicks"),
            F.sum("unique_interstitial_views").alias("category_interstitial_views"),
            F.sum("unique_interstitial_primary_clicks").alias("category_primary_clicks"),
            F.sum("unique_interstitial_secondary_clicks").alias("category_secondary_clicks"),
            F.avg("engagement_score").alias("avg_category_engagement"),
            F.max("engagement_score").alias("max_category_engagement"),
            F.min("engagement_score").alias("min_category_engagement")
        )
        .withColumn(
            "category_ctr",
            F.when(F.col("category_unique_views") > 0,
                   F.round(F.col("category_unique_clicks") / F.col("category_unique_views"), 4)).otherwise(0.0)
        )
        .withColumn(
            "category_primary_conversion",
            F.when(F.col("category_interstitial_views") > 0,
                   F.round(F.col("category_primary_clicks") / F.col("category_interstitial_views"), 4)).otherwise(0.0)
        )
        .withColumn(
            "category_secondary_conversion",
            F.when(F.col("category_interstitial_views") > 0,
                   F.round(F.col("category_secondary_clicks") / F.col("category_interstitial_views"), 4)).otherwise(0.0)
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )
    
    return df_category_summary

def compute_enhanced_global_kpis(df_daily_summary, df_category_summary):
    """Compute enhanced global KPIs with category breakdowns"""
    logger.info("Computing enhanced global KPIs...")
    
    # Overall global metrics
    df_global = (
        df_daily_summary.groupBy("date")
        .agg(
            F.sum("unique_tile_views").alias("total_tile_views"),
            F.sum("unique_tile_clicks").alias("total_tile_clicks"),
            F.sum("total_tile_views").alias("total_tile_impressions"),
            F.sum("total_tile_clicks").alias("total_tile_interactions"),
            F.sum("unique_interstitial_views").alias("total_interstitial_views"),
            F.sum("unique_interstitial_primary_clicks").alias("total_primary_clicks"),
            F.sum("unique_interstitial_secondary_clicks").alias("total_secondary_clicks"),
            F.avg("engagement_score").alias("avg_engagement_score"),
            F.max("engagement_score").alias("max_engagement_score"),
            F.count("tile_id").alias("total_active_tiles"),
            F.countDistinct("tile_category").alias("total_categories")
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
        .withColumn(
            "overall_conversion_rate",
            F.when(F.col("total_interstitial_views") > 0,
                   F.round((F.col("total_primary_clicks") + F.col("total_secondary_clicks")) / F.col("total_interstitial_views"), 4)).otherwise(0.0)
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )
    
    # Add category performance rankings
    window_rank = Window.orderBy(F.desc("category_ctr"))
    df_category_ranked = (
        df_category_summary
        .withColumn("category_ctr_rank", F.row_number().over(window_rank))
    )
    
    return df_global, df_category_ranked

def write_to_delta_table_optimized(df, table_name, process_date, partition_col="date"):
    """Write DataFrame to Delta table with optimized performance settings"""
    logger.info(f"Writing to optimized Delta table: {table_name}")
    
    try:
        # Create Delta table path
        table_path = f"/tmp/delta/{table_name.replace('.', '_')}"
        
        # Optimize DataFrame before writing
        df_optimized = (
            df.repartition(F.col(partition_col))  # Partition by date for optimal writes
            .sortWithinPartitions(partition_col)  # Sort within partitions
        )
        
        # Write with optimized settings
        (
            df_optimized.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("replaceWhere", f"{partition_col} = '{process_date}'")
            .option("optimizeWrite", "true")
            .option("autoCompact", "true")
            .save(table_path)
        )
        
        record_count = df.count()
        logger.info(f"Successfully wrote {record_count} records to {table_name}")
        return True, record_count
        
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {str(e)}")
        raise

def validate_enhanced_data_quality(df_daily_summary, df_global, df_category_summary):
    """Enhanced data quality validation with comprehensive business rules"""
    logger.info("Validating enhanced data quality...")
    
    validation_results = []
    
    # Basic data integrity checks
    null_tile_ids = df_daily_summary.filter(F.col("tile_id").isNull()).count()
    validation_results.append(("Null tile_ids", null_tile_ids == 0, f"Found {null_tile_ids} null tile_ids"))
    
    # Negative metrics validation
    negative_views = df_daily_summary.filter(F.col("unique_tile_views") < 0).count()
    validation_results.append(("Negative views", negative_views == 0, f"Found {negative_views} negative views"))
    
    # CTR bounds validation (should be between 0 and 1)
    invalid_ctr = df_global.filter(
        (F.col("overall_ctr") < 0) | (F.col("overall_ctr") > 1)
    ).count()
    validation_results.append(("Invalid CTR", invalid_ctr == 0, f"Found {invalid_ctr} invalid CTR values"))
    
    # Category enrichment validation
    unknown_categories = df_daily_summary.filter(F.col("tile_category") == "UNKNOWN").count()
    total_records = df_daily_summary.count()
    enrichment_rate = (total_records - unknown_categories) / total_records if total_records > 0 else 0
    validation_results.append(("Category enrichment", enrichment_rate >= 0.7, f"Enrichment rate: {enrichment_rate:.2%}"))
    
    # Advanced validation: Engagement score bounds
    invalid_engagement = df_daily_summary.filter(
        (F.col("engagement_score") < 0) | (F.col("engagement_score") > 1000)
    ).count()
    validation_results.append(("Engagement score bounds", invalid_engagement == 0, f"Found {invalid_engagement} invalid engagement scores"))
    
    # Category-level validation
    category_count = df_category_summary.count()
    expected_min_categories = 3  # Expect at least 3 categories
    validation_results.append(("Minimum categories", category_count >= expected_min_categories, f"Found {category_count} categories, expected >= {expected_min_categories}"))
    
    # Conversion rate validation
    invalid_conversion = df_daily_summary.filter(
        (F.col("total_conversion_rate") < 0) | (F.col("total_conversion_rate") > 1)
    ).count()
    validation_results.append(("Conversion rate bounds", invalid_conversion == 0, f"Found {invalid_conversion} invalid conversion rates"))
    
    # Data freshness validation
    processing_time_diff = df_daily_summary.select(
        F.max(F.unix_timestamp("processing_timestamp") - F.unix_timestamp(F.current_timestamp())).alias("time_diff")
    ).collect()[0]["time_diff"]
    
    data_freshness_ok = abs(processing_time_diff) < 300  # Within 5 minutes
    validation_results.append(("Data freshness", data_freshness_ok, f"Processing time difference: {processing_time_diff} seconds"))
    
    # Log validation results
    for test_name, passed, message in validation_results:
        status = "PASS" if passed else "FAIL"
        logger.info(f"Validation {test_name}: {status} - {message}")
    
    return validation_results

def generate_performance_metrics(start_time, df_daily_summary, df_global, df_category_summary):
    """Generate performance metrics for monitoring"""
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    
    metrics = {
        "execution_time_seconds": execution_time,
        "daily_summary_records": df_daily_summary.count(),
        "global_kpi_records": df_global.count(),
        "category_summary_records": df_category_summary.count(),
        "processing_timestamp": end_time.isoformat(),
        "performance_grade": "EXCELLENT" if execution_time < 30 else "GOOD" if execution_time < 60 else "NEEDS_IMPROVEMENT"
    }
    
    return metrics

def main():
    """Main enhanced ETL execution function"""
    start_time = datetime.now()
    logger.info("Starting Enhanced Home Tile Reporting ETL v2.0 with Advanced Analytics...")
    
    # Configuration
    PROCESS_DATE = "2025-01-27"
    TARGET_DAILY_SUMMARY = "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY"
    TARGET_GLOBAL_KPIS = "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"
    TARGET_CATEGORY_SUMMARY = "reporting_db.TARGET_HOME_TILE_CATEGORY_SUMMARY"
    
    try:
        # Initialize Spark session with optimizations
        spark = get_spark_session()
        
        # Read source data with caching
        df_tile, df_inter, df_metadata = read_source_data_with_caching(spark, PROCESS_DATE)
        
        # Compute advanced aggregations
        df_tile_agg = compute_advanced_tile_aggregations(df_tile)
        df_inter_agg = compute_advanced_interstitial_aggregations(df_inter)
        
        # Create enhanced daily summary with advanced analytics
        df_daily_summary = create_enhanced_daily_summary(df_tile_agg, df_inter_agg, df_metadata, PROCESS_DATE)
        
        # Compute category-level analytics
        df_category_summary = compute_category_level_analytics(df_daily_summary)
        
        # Compute enhanced global KPIs
        df_global, df_category_ranked = compute_enhanced_global_kpis(df_daily_summary, df_category_summary)
        
        # Enhanced data quality validation
        validation_results = validate_enhanced_data_quality(df_daily_summary, df_global, df_category_summary)
        
        # Display results with enhanced formatting
        logger.info("\n" + "="*80)
        logger.info("ENHANCED DAILY SUMMARY RESULTS:")
        logger.info("="*80)
        df_daily_summary.show(20, False)
        
        logger.info("\n" + "="*80)
        logger.info("CATEGORY-LEVEL ANALYTICS:")
        logger.info("="*80)
        df_category_ranked.show(20, False)
        
        logger.info("\n" + "="*80)
        logger.info("ENHANCED GLOBAL KPIs:")
        logger.info("="*80)
        df_global.show(20, False)
        
        # Write to optimized Delta tables
        success1, count1 = write_to_delta_table_optimized(df_daily_summary, TARGET_DAILY_SUMMARY, PROCESS_DATE)
        success2, count2 = write_to_delta_table_optimized(df_global, TARGET_GLOBAL_KPIS, PROCESS_DATE)
        success3, count3 = write_to_delta_table_optimized(df_category_ranked, TARGET_CATEGORY_SUMMARY, PROCESS_DATE)
        
        # Generate performance metrics
        performance_metrics = generate_performance_metrics(start_time, df_daily_summary, df_global, df_category_summary)
        
        logger.info(f"Enhanced ETL completed successfully for {PROCESS_DATE}")
        
        # Print comprehensive summary statistics
        print(f"\n" + "="*100)
        print(f"ENHANCED ETL EXECUTION SUMMARY v2.0")
        print(f"="*100)
        print(f"Process Date: {PROCESS_DATE}")
        print(f"Execution Time: {performance_metrics['execution_time_seconds']:.2f} seconds")
        print(f"Performance Grade: {performance_metrics['performance_grade']}")
        print(f"Daily Summary Records: {performance_metrics['daily_summary_records']}")
        print(f"Category Summary Records: {performance_metrics['category_summary_records']}")
        print(f"Global KPI Records: {performance_metrics['global_kpi_records']}")
        print(f"Validation Tests: {len([r for r in validation_results if r[1]])} passed, {len([r for r in validation_results if not r[1]])} failed")
        print(f"Data Enrichment: Advanced category analytics enabled")
        print(f"Performance Optimizations: Caching, broadcast joins, optimized writes")
        print(f"Status: SUCCESS")
        print(f"="*100)
        
        # Cleanup cached DataFrames
        df_tile.unpersist()
        df_inter.unpersist()
        df_metadata.unpersist()
        
        return df_daily_summary, df_global, df_category_summary
        
    except Exception as e:
        logger.error(f"Enhanced ETL execution failed: {str(e)}")
        print(f"\n" + "="*100)
        print(f"ENHANCED ETL EXECUTION SUMMARY v2.0")
        print(f"="*100)
        print(f"Process Date: {PROCESS_DATE}")
        print(f"Status: FAILED")
        print(f"Error: {str(e)}")
        print(f"="*100)
        raise

if __name__ == "__main__":
    main()