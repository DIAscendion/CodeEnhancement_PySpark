_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Enhanced Home Tile Reporting ETL with tile category metadata integration
## *Version*: 1 
## *Updated on*: 
_____________________________________________

"""
===============================================================================
                        CHANGE MANAGEMENT / REVISION HISTORY
===============================================================================
File Name       : home_tile_reporting_etl_Pipeline_1.py
Author          : AAVA
Created Date    : 2025-12-02
Last Modified   : <Auto-updated>
Version         : 1.0.0
Release         : R1 – Home Tile Reporting Enhancement with Metadata Integration

Functional Description:
    This ETL pipeline performs the following:
    - Reads home tile interaction events and interstitial events from source tables
    - Integrates with SOURCE_TILE_METADATA for tile categorization
    - Computes aggregated metrics:
        • Unique Tile Views
        • Unique Tile Clicks
        • Unique Interstitial Views
        • Unique Primary Button Clicks
        • Unique Secondary Button Clicks
        • CTRs for homepage tiles and interstitial buttons
    - Enriches data with tile_category for business reporting
    - Loads aggregated results into:
        • TARGET_HOME_TILE_DAILY_SUMMARY (with tile_category)
        • TARGET_HOME_TILE_GLOBAL_KPIS
    - Supports idempotent daily partition overwrite
    - Designed for scalable production workloads (Databricks/Spark)

Change Log:
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
1.0.0       2025-12-02    AAVA           Enhanced version with tile metadata integration
-------------------------------------------------------------------------------
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
PIPELINE_NAME = "HOME_TILE_REPORTING_ETL_ENHANCED"

# Source Tables
SOURCE_HOME_TILE_EVENTS = "analytics_db.SOURCE_HOME_TILE_EVENTS"
SOURCE_INTERSTITIAL_EVENTS = "analytics_db.SOURCE_INTERSTITIAL_EVENTS"
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"  # New metadata table

# Target Tables
TARGET_DAILY_SUMMARY = "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY"
TARGET_GLOBAL_KPIS = "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"

PROCESS_DATE = "2025-12-01"  # pass dynamically from ADF/Airflow if needed
DEFAULT_CATEGORY = "UNKNOWN"  # Default category for backward compatibility

# Initialize Spark Session with Spark Connect compatibility
def get_spark_session():
    try:
        # Try to get active session first (Spark Connect compatible)
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (
                SparkSession.builder
                .appName("HomeTileReportingETLEnhanced")
                .enableHiveSupport()
                .getOrCreate()
            )
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

spark = get_spark_session()

# ------------------------------------------------------------------------------
# SCHEMA DEFINITIONS
# ------------------------------------------------------------------------------

# Define schemas for validation
home_tile_events_schema = StructType([
    StructField("event_ts", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("tile_id", StringType(), True),
    StructField("event_type", StringType(), True)
])

interstitial_events_schema = StructType([
    StructField("event_ts", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("tile_id", StringType(), True),
    StructField("interstitial_view_flag", BooleanType(), True),
    StructField("primary_button_click_flag", BooleanType(), True),
    StructField("secondary_button_click_flag", BooleanType(), True)
])

tile_metadata_schema = StructType([
    StructField("tile_id", StringType(), True),
    StructField("tile_name", StringType(), True),
    StructField("tile_category", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("updated_ts", TimestampType(), True)
])

# ------------------------------------------------------------------------------
# DATA READING FUNCTIONS
# ------------------------------------------------------------------------------

def read_delta_table(table_name, schema=None):
    """Read Delta table with error handling and validation"""
    try:
        logger.info(f"Reading table: {table_name}")
        df = spark.table(table_name)
        
        if schema:
            # Validate schema compatibility
            for field in schema.fields:
                if field.name not in df.columns:
                    logger.warning(f"Expected column {field.name} not found in {table_name}")
        
        logger.info(f"Successfully read {df.count()} records from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error reading table {table_name}: {e}")
        raise

def write_delta_table(df, table_name, partition_col="date", process_date=PROCESS_DATE):
    """Write DataFrame to Delta table with partition overwrite"""
    try:
        logger.info(f"Writing {df.count()} records to {table_name}")
        
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("replaceWhere", f"{partition_col} = '{process_date}'")
              .saveAsTable(table_name)
        )
        
        logger.info(f"Successfully wrote data to {table_name}")
    except Exception as e:
        logger.error(f"Error writing to table {table_name}: {e}")
        raise

# ------------------------------------------------------------------------------
# MAIN ETL LOGIC
# ------------------------------------------------------------------------------

def main_etl_pipeline():
    """Main ETL pipeline execution"""
    try:
        logger.info(f"Starting ETL pipeline for date: {PROCESS_DATE}")
        
        # ------------------------------------------------------------------------------
        # READ SOURCE TABLES
        # ------------------------------------------------------------------------------
        
        # Read home tile events
        df_tile = (
            read_delta_table(SOURCE_HOME_TILE_EVENTS, home_tile_events_schema)
            .filter(F.to_date("event_ts") == PROCESS_DATE)
        )
        
        # Read interstitial events
        df_inter = (
            read_delta_table(SOURCE_INTERSTITIAL_EVENTS, interstitial_events_schema)
            .filter(F.to_date("event_ts") == PROCESS_DATE)
        )
        
        # Read tile metadata (new enhancement)
        df_metadata = (
            read_delta_table(SOURCE_TILE_METADATA, tile_metadata_schema)
            .filter(F.col("is_active") == True)  # Only active tiles
            .select("tile_id", "tile_name", "tile_category")
        )
        
        # ------------------------------------------------------------------------------
        # DAILY TILE SUMMARY AGGREGATION
        # ------------------------------------------------------------------------------
        
        # Aggregate tile events
        df_tile_agg = (
            df_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
        
        # Aggregate interstitial events
        df_inter_agg = (
            df_inter.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        # Join tile and interstitial aggregations
        df_combined_agg = (
            df_tile_agg.join(df_inter_agg, "tile_id", "outer")
        )
        
        # ------------------------------------------------------------------------------
        # METADATA ENRICHMENT (NEW ENHANCEMENT)
        # ------------------------------------------------------------------------------
        
        # Left join with metadata to enrich with tile_category
        df_enriched = (
            df_combined_agg.join(df_metadata, "tile_id", "left")
            .withColumn(
                "tile_category", 
                F.coalesce(F.col("tile_category"), F.lit(DEFAULT_CATEGORY))
            )
            .withColumn("date", F.lit(PROCESS_DATE))
            .select(
                "date",
                "tile_id",
                F.coalesce("tile_name", F.lit("Unknown Tile")).alias("tile_name"),
                "tile_category",  # New column for business reporting
                F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
                F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
                F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
                F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
                F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
            )
        )
        
        # Add calculated CTR metrics
        df_daily_summary = (
            df_enriched
            .withColumn(
                "tile_ctr",
                F.when(F.col("unique_tile_views") > 0,
                       F.col("unique_tile_clicks") / F.col("unique_tile_views")).otherwise(0.0)
            )
            .withColumn(
                "primary_button_ctr",
                F.when(F.col("unique_interstitial_views") > 0,
                       F.col("unique_interstitial_primary_clicks") / F.col("unique_interstitial_views")).otherwise(0.0)
            )
            .withColumn(
                "secondary_button_ctr",
                F.when(F.col("unique_interstitial_views") > 0,
                       F.col("unique_interstitial_secondary_clicks") / F.col("unique_interstitial_views")).otherwise(0.0)
            )
        )
        
        # ------------------------------------------------------------------------------
        # GLOBAL KPIs (UNCHANGED)
        # ------------------------------------------------------------------------------
        
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
                       F.col("total_tile_clicks") / F.col("total_tile_views")).otherwise(0.0)
            )
            .withColumn(
                "overall_primary_ctr",
                F.when(F.col("total_interstitial_views") > 0,
                       F.col("total_primary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
            )
            .withColumn(
                "overall_secondary_ctr",
                F.when(F.col("total_interstitial_views") > 0,
                       F.col("total_secondary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
            )
        )
        
        # ------------------------------------------------------------------------------
        # DATA VALIDATION
        # ------------------------------------------------------------------------------
        
        # Validate data quality
        daily_count = df_daily_summary.count()
        global_count = df_global.count()
        
        logger.info(f"Daily summary records: {daily_count}")
        logger.info(f"Global KPI records: {global_count}")
        
        if daily_count == 0:
            logger.warning("No daily summary records generated")
        if global_count == 0:
            logger.warning("No global KPI records generated")
        
        # Check for null tile_ids
        null_tile_count = df_daily_summary.filter(F.col("tile_id").isNull()).count()
        if null_tile_count > 0:
            logger.warning(f"Found {null_tile_count} records with null tile_id")
        
        # ------------------------------------------------------------------------------
        # WRITE TARGET TABLES
        # ------------------------------------------------------------------------------
        
        # Write daily summary with enhanced schema
        write_delta_table(df_daily_summary, TARGET_DAILY_SUMMARY)
        
        # Write global KPIs (unchanged)
        write_delta_table(df_global, TARGET_GLOBAL_KPIS)
        
        logger.info(f"ETL completed successfully for {PROCESS_DATE}")
        
        return df_daily_summary, df_global
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

# ------------------------------------------------------------------------------
# EXECUTION
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    # Execute the main ETL pipeline
    daily_summary_df, global_kpis_df = main_etl_pipeline()
    
    # Display sample results for verification
    print("\n=== DAILY SUMMARY SAMPLE ===")
    daily_summary_df.show(10, truncate=False)
    
    print("\n=== GLOBAL KPIS SAMPLE ===")
    global_kpis_df.show(truncate=False)
    
    print(f"\nETL Pipeline completed successfully for {PROCESS_DATE}")
    print(f"Enhanced with tile_category metadata integration")
