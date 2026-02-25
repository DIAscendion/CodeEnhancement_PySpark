# _____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Enhanced home tile reporting ETL pipeline with SOURCE_TILE_METADATA integration and extended target summary
## *Version*: 2
## *Changes*: Added SOURCE_TILE_METADATA table integration, enhanced tile category enrichment, added business grouping support
## *Reason*: PCE-5 - Add New Source Table SOURCE_TILE_METADATA and Extend Target Summary Table with Tile Category Business Request
## *Updated on*: 2024-12-19
## *Databricks Notebook*: home_tile_reporting_etl_Pipeline_2
## *Databricks Path*: /Workspace/Users/elansuriyaa.p@ascendion.com/PySpark/home_tile_reporting_etl_Pipeline_2
# _____________________________________________

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import logging
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("HomeTileReportingETL_Enhanced").getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sample_source_data():
    """
    Create sample source data for home tile events and interstitial events
    Enhanced to match the original ETL structure from PCE-5
    """
    logger.info("Creating sample source home tile events data")
    
    # Sample home tile events data
    home_tile_events_data = [
        ("user_001", "tile_001", "TILE_VIEW", "2024-12-19 10:00:00"),
        ("user_002", "tile_001", "TILE_VIEW", "2024-12-19 10:01:00"),
        ("user_001", "tile_001", "TILE_CLICK", "2024-12-19 10:02:00"),
        ("user_003", "tile_002", "TILE_VIEW", "2024-12-19 10:03:00"),
        ("user_003", "tile_002", "TILE_CLICK", "2024-12-19 10:04:00"),
        ("user_004", "tile_003", "TILE_VIEW", "2024-12-19 10:05:00"),
        ("user_005", "tile_004", "TILE_VIEW", "2024-12-19 10:06:00"),
        ("user_005", "tile_004", "TILE_CLICK", "2024-12-19 10:07:00"),
        ("user_006", "tile_005", "TILE_VIEW", "2024-12-19 10:08:00")
    ]
    
    home_tile_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True)
    ])
    
    home_tile_df = spark.createDataFrame(home_tile_events_data, home_tile_schema)
    home_tile_df = home_tile_df.withColumn("event_ts", to_timestamp(col("event_ts"), "yyyy-MM-dd HH:mm:ss"))
    
    logger.info("Creating sample source interstitial events data")
    
    # Sample interstitial events data
    interstitial_events_data = [
        ("user_001", "tile_001", True, False, False, "2024-12-19 10:10:00"),
        ("user_002", "tile_001", True, True, False, "2024-12-19 10:11:00"),
        ("user_003", "tile_002", True, False, True, "2024-12-19 10:12:00"),
        ("user_004", "tile_003", True, True, False, "2024-12-19 10:13:00"),
        ("user_005", "tile_004", True, False, False, "2024-12-19 10:14:00")
    ]
    
    interstitial_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("interstitial_view_flag", BooleanType(), True),
        StructField("primary_button_click_flag", BooleanType(), True),
        StructField("secondary_button_click_flag", BooleanType(), True),
        StructField("event_ts", StringType(), True)
    ])
    
    interstitial_df = spark.createDataFrame(interstitial_events_data, interstitial_schema)
    interstitial_df = interstitial_df.withColumn("event_ts", to_timestamp(col("event_ts"), "yyyy-MM-dd HH:mm:ss"))
    
    return home_tile_df, interstitial_df

def create_source_tile_metadata():
    """
    Create SOURCE_TILE_METADATA table as per PCE-5 requirements
    Enhanced with business grouping and additional metadata fields
    """
    logger.info("Creating SOURCE_TILE_METADATA table")
    
    metadata_data = [
        ("tile_001", "Personal Finance Overview", "Personal Finance", True, "2024-12-19 09:00:00"),
        ("tile_002", "Health Check Reminder", "Health", True, "2024-12-19 09:00:00"),
        ("tile_003", "Payment Due Alert", "Payments", True, "2024-12-19 09:00:00"),
        ("tile_004", "Offers & Promotions", "Offers", True, "2024-12-19 09:00:00"),
        ("tile_005", "Account Summary", "Personal Finance", False, "2024-12-19 09:00:00")  # Inactive tile
    ]
    
    metadata_schema = StructType([
        StructField("tile_id", StringType(), True),
        StructField("tile_name", StringType(), True),
        StructField("tile_category", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("updated_ts", StringType(), True)
    ])
    
    metadata_df = spark.createDataFrame(metadata_data, metadata_schema)
    metadata_df = metadata_df.withColumn("updated_ts", to_timestamp(col("updated_ts"), "yyyy-MM-dd HH:mm:ss"))
    
    return metadata_df

def compute_daily_tile_summary(home_tile_df, interstitial_df, process_date="2024-12-19"):
    """
    Compute daily tile summary aggregations as per original ETL logic
    Enhanced with proper date filtering and aggregation logic
    """
    logger.info(f"Computing daily tile summary for {process_date}")
    
    # Filter data for process date
    df_tile = home_tile_df.filter(to_date(col("event_ts")) == process_date)
    df_inter = interstitial_df.filter(to_date(col("event_ts")) == process_date)
    
    # Daily tile summary aggregation
    df_tile_agg = (
        df_tile.groupBy("tile_id")
        .agg(
            countDistinct(when(col("event_type") == "TILE_VIEW", col("user_id"))).alias("unique_tile_views"),
            countDistinct(when(col("event_type") == "TILE_CLICK", col("user_id"))).alias("unique_tile_clicks")
        )
    )
    
    # Interstitial events aggregation
    df_inter_agg = (
        df_inter.groupBy("tile_id")
        .agg(
            countDistinct(when(col("interstitial_view_flag") == True, col("user_id"))).alias("unique_interstitial_views"),
            countDistinct(when(col("primary_button_click_flag") == True, col("user_id"))).alias("unique_interstitial_primary_clicks"),
            countDistinct(when(col("secondary_button_click_flag") == True, col("user_id"))).alias("unique_interstitial_secondary_clicks")
        )
    )
    
    # Combine tile and interstitial aggregations
    df_daily_summary = (
        df_tile_agg.join(df_inter_agg, "tile_id", "outer")
        .withColumn("date", lit(process_date))
        .select(
            "date",
            "tile_id",
            coalesce("unique_tile_views", lit(0)).alias("unique_tile_views"),
            coalesce("unique_tile_clicks", lit(0)).alias("unique_tile_clicks"),
            coalesce("unique_interstitial_views", lit(0)).alias("unique_interstitial_views"),
            coalesce("unique_interstitial_primary_clicks", lit(0)).alias("unique_interstitial_primary_clicks"),
            coalesce("unique_interstitial_secondary_clicks", lit(0)).alias("unique_interstitial_secondary_clicks")
        )
    )
    
    return df_daily_summary

def enrich_with_tile_metadata(daily_summary_df, metadata_df):
    """
    Enrich daily summary with tile metadata as per PCE-5 requirements
    Enhanced with backward compatibility and business grouping
    """
    logger.info("Enriching daily summary with tile metadata")
    
    # Left join to enrich with tile category, default to "UNKNOWN" if no match
    enriched_df = daily_summary_df.alias("summary").join(
        metadata_df.alias("meta"),
        col("summary.tile_id") == col("meta.tile_id"),
        "left"
    ).select(
        col("summary.date"),
        col("summary.tile_id"),
        col("summary.unique_tile_views"),
        col("summary.unique_tile_clicks"),
        col("summary.unique_interstitial_views"),
        col("summary.unique_interstitial_primary_clicks"),
        col("summary.unique_interstitial_secondary_clicks"),
        coalesce(col("meta.tile_name"), lit("Unknown Tile")).alias("tile_name"),
        coalesce(col("meta.tile_category"), lit("UNKNOWN")).alias("tile_category"),
        coalesce(col("meta.is_active"), lit(True)).alias("is_active")
    )
    
    # Add calculated CTR metrics
    enriched_df = enriched_df.withColumn(
        "tile_ctr",
        when(col("unique_tile_views") > 0,
             round(col("unique_tile_clicks") / col("unique_tile_views"), 4)).otherwise(0.0)
    ).withColumn(
        "primary_button_ctr",
        when(col("unique_interstitial_views") > 0,
             round(col("unique_interstitial_primary_clicks") / col("unique_interstitial_views"), 4)).otherwise(0.0)
    ).withColumn(
        "secondary_button_ctr",
        when(col("unique_interstitial_views") > 0,
             round(col("unique_interstitial_secondary_clicks") / col("unique_interstitial_views"), 4)).otherwise(0.0)
    ).withColumn(
        "processed_timestamp",
        current_timestamp()
    )
    
    return enriched_df

def compute_global_kpis(enriched_summary_df):
    """
    Compute global KPIs as per original ETL logic
    Enhanced with category-level aggregations
    """
    logger.info("Computing global KPIs")
    
    # Overall global KPIs
    df_global = (
        enriched_summary_df.groupBy("date")
        .agg(
            sum("unique_tile_views").alias("total_tile_views"),
            sum("unique_tile_clicks").alias("total_tile_clicks"),
            sum("unique_interstitial_views").alias("total_interstitial_views"),
            sum("unique_interstitial_primary_clicks").alias("total_primary_clicks"),
            sum("unique_interstitial_secondary_clicks").alias("total_secondary_clicks")
        )
        .withColumn(
            "overall_ctr",
            when(col("total_tile_views") > 0,
                 round(col("total_tile_clicks") / col("total_tile_views"), 4)).otherwise(0.0)
        )
        .withColumn(
            "overall_primary_ctr",
            when(col("total_interstitial_views") > 0,
                 round(col("total_primary_clicks") / col("total_interstitial_views"), 4)).otherwise(0.0)
        )
        .withColumn(
            "overall_secondary_ctr",
            when(col("total_interstitial_views") > 0,
                 round(col("total_secondary_clicks") / col("total_interstitial_views"), 4)).otherwise(0.0)
        )
    )
    
    return df_global

def validate_data(df, stage_name):
    """
    Enhanced data validation with tile category specific checks
    """
    logger.info(f"Validating data for stage: {stage_name}")
    
    record_count = df.count()
    logger.info(f"Record count for {stage_name}: {record_count}")
    
    # Check for null values in critical columns
    null_checks = {
        "tile_id": df.filter(col("tile_id").isNull()).count(),
        "date": df.filter(col("date").isNull()).count() if "date" in df.columns else 0
    }
    
    for column, null_count in null_checks.items():
        if null_count > 0:
            logger.warning(f"Found {null_count} null values in {column}")
        else:
            logger.info(f"No null values found in {column}")
    
    # Validate tile_category enrichment
    if "tile_category" in df.columns:
        category_counts = df.groupBy("tile_category").count().collect()
        logger.info("Tile category distribution:")
        for row in category_counts:
            logger.info(f"  {row['tile_category']}: {row['count']} tiles")
        
        unknown_count = df.filter(col("tile_category") == "UNKNOWN").count()
        if unknown_count > 0:
            logger.warning(f"Found {unknown_count} tiles with UNKNOWN category - ensure metadata is complete")
    
    return record_count > 0

def write_to_delta(df, table_path, mode="overwrite", partition_col=None, partition_value=None):
    """
    Enhanced Delta write with partition overwrite support
    """
    logger.info(f"Writing data to Delta table: {table_path}")
    
    try:
        writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
        
        # Add partition overwrite if specified
        if partition_col and partition_value:
            writer = writer.option("replaceWhere", f"{partition_col} = '{partition_value}'")
        
        writer.save(table_path)
        
        logger.info(f"Successfully wrote {df.count()} records to {table_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write to Delta table: {str(e)}")
        return False

def read_from_delta(table_path):
    """
    Read DataFrame from Delta table with error handling
    """
    logger.info(f"Reading data from Delta table: {table_path}")
    
    try:
        df = spark.read.format("delta").load(table_path)
        logger.info(f"Successfully read {df.count()} records from {table_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read from Delta table: {str(e)}")
        return None

def main():
    """
    Enhanced main ETL pipeline execution with SOURCE_TILE_METADATA integration
    """
    logger.info("Starting Enhanced Home Tile Reporting ETL Pipeline v2.0")
    
    try:
        process_date = "2024-12-19"
        
        # Step 1: Create sample source data
        home_tile_df, interstitial_df = create_sample_source_data()
        validate_data(home_tile_df, "Source Home Tile Events")
        validate_data(interstitial_df, "Source Interstitial Events")
        
        # Step 2: Create SOURCE_TILE_METADATA
        metadata_df = create_source_tile_metadata()
        validate_data(metadata_df, "SOURCE_TILE_METADATA")
        
        # Step 3: Compute daily tile summary
        daily_summary_df = compute_daily_tile_summary(home_tile_df, interstitial_df, process_date)
        validate_data(daily_summary_df, "Daily Tile Summary")
        
        # Step 4: Enrich with tile metadata (PCE-5 enhancement)
        enriched_summary_df = enrich_with_tile_metadata(daily_summary_df, metadata_df)
        validate_data(enriched_summary_df, "Enriched Daily Summary")
        
        # Step 5: Compute global KPIs
        global_kpis_df = compute_global_kpis(enriched_summary_df)
        validate_data(global_kpis_df, "Global KPIs")
        
        # Step 6: Write to Delta tables
        # Write source data
        home_tile_table_path = "/tmp/delta/analytics_db/source_home_tile_events"
        write_to_delta(home_tile_df, home_tile_table_path)
        
        interstitial_table_path = "/tmp/delta/analytics_db/source_interstitial_events"
        write_to_delta(interstitial_df, interstitial_table_path)
        
        # Write SOURCE_TILE_METADATA
        metadata_table_path = "/tmp/delta/analytics_db/source_tile_metadata"
        write_to_delta(metadata_df, metadata_table_path)
        
        # Write enriched target data with partition overwrite
        target_daily_summary_path = "/tmp/delta/reporting_db/target_home_tile_daily_summary"
        write_to_delta(enriched_summary_df, target_daily_summary_path, 
                      mode="overwrite", partition_col="date", partition_value=process_date)
        
        # Write global KPIs
        target_global_kpis_path = "/tmp/delta/reporting_db/target_home_tile_global_kpis"
        write_to_delta(global_kpis_df, target_global_kpis_path,
                      mode="overwrite", partition_col="date", partition_value=process_date)
        
        # Step 7: Verify final output and generate business insights
        final_summary_df = read_from_delta(target_daily_summary_path)
        if final_summary_df:
            logger.info("Enhanced TARGET_HOME_TILE_DAILY_SUMMARY schema:")
            final_summary_df.printSchema()
            
            logger.info("Sample enriched output data:")
            final_summary_df.show(10, truncate=False)
            
            # Category-wise business insights (PCE-5 requirement)
            category_performance = final_summary_df.groupBy("tile_category") \
                .agg(
                    count("tile_id").alias("tile_count"),
                    sum("unique_tile_views").alias("total_views"),
                    sum("unique_tile_clicks").alias("total_clicks"),
                    avg("tile_ctr").alias("avg_ctr"),
                    sum("unique_interstitial_views").alias("total_interstitial_views")
                ).orderBy(desc("total_views"))
            
            logger.info("Category-wise Performance Analysis (PCE-5 Enhancement):")
            category_performance.show()
            
            # Active vs Inactive tile analysis
            active_analysis = final_summary_df.groupBy("is_active") \
                .agg(
                    count("tile_id").alias("tile_count"),
                    sum("unique_tile_views").alias("total_views")
                )
            
            logger.info("Active vs Inactive Tile Analysis:")
            active_analysis.show()
        
        # Verify global KPIs
        final_kpis_df = read_from_delta(target_global_kpis_path)
        if final_kpis_df:
            logger.info("Global KPIs output:")
            final_kpis_df.show()
        
        logger.info("Enhanced Home Tile Reporting ETL Pipeline v2.0 completed successfully")
        logger.info("PCE-5 enhancements applied: SOURCE_TILE_METADATA integration, tile category enrichment, business grouping")
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ Enhanced Pipeline execution completed successfully")
        print("🎯 PCE-5 Requirements Implemented:")
        print("   ✓ SOURCE_TILE_METADATA table integration")
        print("   ✓ Tile category enrichment in target summary")
        print("   ✓ Backward compatibility with UNKNOWN default")
        print("   ✓ Business grouping and category-level reporting")
        print("   ✓ Enhanced CTR calculations and performance metrics")
    else:
        print("❌ Enhanced Pipeline execution failed")