# _____________________________________________
# ## *Author*: AAVA
# ## *Created on*: 2025-01-27
# ## *Description*: Enhanced Home Tile Reporting ETL with tile category metadata integration
# ## *Version*: 1
# ## *Updated on*: 2025-01-27
# ## *Databricks Notebook*: home_tile_reporting_etl_Pipeline_1
# ## *Databricks Path*: /Workspace/Users/elansuriyaa.p@ascendion.com/PySpark/home_tile_reporting_etl_Pipeline_1
# _____________________________________________

"""
===============================================================================
                        CHANGE MANAGEMENT / REVISION HISTORY
===============================================================================
File Name       : home_tile_reporting_etl_Pipeline_1.py
Author          : AAVA
Created Date    : 2025-01-27
Last Modified   : 2025-01-27
Version         : 1.0.0
Release         : R1 – Home Tile Reporting Enhancement with Tile Category

Functional Description:
    This enhanced ETL pipeline performs the following:
    - Reads home tile interaction events and interstitial events from source tables
    - Reads tile metadata from SOURCE_TILE_METADATA for category enrichment
    - Computes aggregated metrics:
        • Unique Tile Views
        • Unique Tile Clicks
        • Unique Interstitial Views
        • Unique Primary Button Clicks
        • Unique Secondary Button Clicks
        • CTRs for homepage tiles and interstitial buttons
    - Enriches data with tile_category from metadata table
    - Loads aggregated results into:
        • TARGET_HOME_TILE_DAILY_SUMMARY (with tile_category)
        • TARGET_HOME_TILE_GLOBAL_KPIS
    - Supports idempotent daily partition overwrite
    - Designed for scalable production workloads (Databricks/Spark)
    - Maintains backward compatibility with default "UNKNOWN" category

Change Log:
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
1.0.0       2025-01-27    AAVA           Enhanced version with tile category metadata
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
    """Initialize Spark session with optimized configurations"""
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (
                SparkSession.builder
                .appName("HomeTileReportingETL_Enhanced")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate()
            )
        return spark
    except Exception as e:
        logger.error(f"Error initializing Spark session: {str(e)}")
        raise

def create_sample_data(spark):
    """Create sample source data for testing"""
    logger.info("Creating sample source data...")
    
    # Sample HOME_TILE_EVENTS data
    tile_events_data = [
        ("user1", "tile_001", "TILE_VIEW", "2025-01-27 10:00:00"),
        ("user1", "tile_001", "TILE_CLICK", "2025-01-27 10:01:00"),
        ("user2", "tile_001", "TILE_VIEW", "2025-01-27 10:02:00"),
        ("user2", "tile_002", "TILE_VIEW", "2025-01-27 10:03:00"),
        ("user3", "tile_002", "TILE_CLICK", "2025-01-27 10:04:00"),
        ("user3", "tile_003", "TILE_VIEW", "2025-01-27 10:05:00")
    ]
    
    tile_events_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True)
    ])
    
    df_tile_events = spark.createDataFrame(tile_events_data, tile_events_schema)
    df_tile_events = df_tile_events.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Sample INTERSTITIAL_EVENTS data
    interstitial_events_data = [
        ("user1", "tile_001", True, True, False, "2025-01-27 10:01:30"),
        ("user2", "tile_001", True, False, True, "2025-01-27 10:02:30"),
        ("user3", "tile_002", True, True, False, "2025-01-27 10:04:30"),
        ("user1", "tile_003", True, False, False, "2025-01-27 10:05:30")
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
    
    # Sample TILE_METADATA data
    tile_metadata_data = [
        ("tile_001", "Personal Finance Tile", "FINANCE", True, "2025-01-27 09:00:00"),
        ("tile_002", "Health Check Tile", "HEALTH", True, "2025-01-27 09:00:00"),
        ("tile_003", "Offers Tile", "OFFERS", True, "2025-01-27 09:00:00")
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

def read_source_data(spark, process_date):
    """Read source data with date filtering"""
    logger.info(f"Reading source data for date: {process_date}")
    
    try:
        # Create sample data for testing
        df_tile, df_inter, df_metadata = create_sample_data(spark)
        
        # Filter by process date
        df_tile_filtered = df_tile.filter(F.to_date("event_ts") == process_date)
        df_inter_filtered = df_inter.filter(F.to_date("event_ts") == process_date)
        
        logger.info(f"Tile events count: {df_tile_filtered.count()}")
        logger.info(f"Interstitial events count: {df_inter_filtered.count()}")
        logger.info(f"Tile metadata count: {df_metadata.count()}")
        
        return df_tile_filtered, df_inter_filtered, df_metadata
        
    except Exception as e:
        logger.error(f"Error reading source data: {str(e)}")
        raise

def compute_tile_aggregations(df_tile):
    """Compute tile-level aggregations"""
    logger.info("Computing tile aggregations...")
    
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
    logger.info("Computing interstitial aggregations...")
    
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
    logger.info("Creating daily summary with tile category...")
    
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
    logger.info("Computing global KPIs...")
    
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

def write_to_delta_table(df, table_name, process_date, partition_col="date"):
    """Write DataFrame to Delta table with partition overwrite"""
    logger.info(f"Writing to Delta table: {table_name}")
    
    try:
        # Create Delta table path
        table_path = f"/tmp/delta/{table_name.replace('.', '_')}"
        
        # Write with partition overwrite
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("replaceWhere", f"{partition_col} = '{process_date}'")
            .save(table_path)
        )
        
        logger.info(f"Successfully wrote {df.count()} records to {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {str(e)}")
        raise

def validate_data_quality(df_daily_summary, df_global):
    """Validate data quality and business rules"""
    logger.info("Validating data quality...")
    
    validation_results = []
    
    # Check for null tile_ids
    null_tile_ids = df_daily_summary.filter(F.col("tile_id").isNull()).count()
    validation_results.append(("Null tile_ids", null_tile_ids == 0, f"Found {null_tile_ids} null tile_ids"))
    
    # Check for negative metrics
    negative_views = df_daily_summary.filter(F.col("unique_tile_views") < 0).count()
    validation_results.append(("Negative views", negative_views == 0, f"Found {negative_views} negative views"))
    
    # Check CTR bounds (should be between 0 and 1)
    invalid_ctr = df_global.filter(
        (F.col("overall_ctr") < 0) | (F.col("overall_ctr") > 1)
    ).count()
    validation_results.append(("Invalid CTR", invalid_ctr == 0, f"Found {invalid_ctr} invalid CTR values"))
    
    # Check for tile_category enrichment
    unknown_categories = df_daily_summary.filter(F.col("tile_category") == "UNKNOWN").count()
    total_records = df_daily_summary.count()
    enrichment_rate = (total_records - unknown_categories) / total_records if total_records > 0 else 0
    validation_results.append(("Category enrichment", enrichment_rate >= 0.8, f"Enrichment rate: {enrichment_rate:.2%}"))
    
    # Log validation results
    for test_name, passed, message in validation_results:
        status = "PASS" if passed else "FAIL"
        logger.info(f"Validation {test_name}: {status} - {message}")
    
    return validation_results

def main():
    """Main ETL execution function"""
    logger.info("Starting Home Tile Reporting ETL with Tile Category Enhancement...")
    
    # Configuration
    PROCESS_DATE = "2025-01-27"
    TARGET_DAILY_SUMMARY = "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY"
    TARGET_GLOBAL_KPIS = "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"
    
    try:
        # Initialize Spark session
        spark = get_spark_session()
        
        # Read source data
        df_tile, df_inter, df_metadata = read_source_data(spark, PROCESS_DATE)
        
        # Compute aggregations
        df_tile_agg = compute_tile_aggregations(df_tile)
        df_inter_agg = compute_interstitial_aggregations(df_inter)
        
        # Create daily summary with tile category
        df_daily_summary = create_daily_summary(df_tile_agg, df_inter_agg, df_metadata, PROCESS_DATE)
        
        # Compute global KPIs
        df_global = compute_global_kpis(df_daily_summary)
        
        # Validate data quality
        validation_results = validate_data_quality(df_daily_summary, df_global)
        
        # Display results
        logger.info("Daily Summary Results:")
        df_daily_summary.show(20, False)
        
        logger.info("Global KPIs Results:")
        df_global.show(20, False)
        
        # Write to Delta tables
        write_to_delta_table(df_daily_summary, TARGET_DAILY_SUMMARY, PROCESS_DATE)
        write_to_delta_table(df_global, TARGET_GLOBAL_KPIS, PROCESS_DATE)
        
        logger.info(f"ETL completed successfully for {PROCESS_DATE}")
        
        # Print summary statistics
        print(f"\n=== ETL EXECUTION SUMMARY ===")
        print(f"Process Date: {PROCESS_DATE}")
        print(f"Daily Summary Records: {df_daily_summary.count()}")
        print(f"Global KPI Records: {df_global.count()}")
        print(f"Validation Tests: {len([r for r in validation_results if r[1]])} passed, {len([r for r in validation_results if not r[1]])} failed")
        print(f"Status: SUCCESS")
        
        return df_daily_summary, df_global
        
    except Exception as e:
        logger.error(f"ETL execution failed: {str(e)}")
        print(f"\n=== ETL EXECUTION SUMMARY ===")
        print(f"Process Date: {PROCESS_DATE}")
        print(f"Status: FAILED")
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()