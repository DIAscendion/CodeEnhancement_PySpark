# _____________________________________________
## *Author*: AAVA
## *Created on*: 2024-12-19
## *Description*: Home tile reporting ETL pipeline with tile category metadata enrichment
## *Version*: 1
## *Updated on*: 2024-12-19
## *Databricks Notebook*: home_tile_reporting_etl_Pipeline_1
## *Databricks Path*: /Workspace/Users/elansuriyaa.p@ascendion.com/PySpark/home_tile_reporting_etl_Pipeline_1
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
    spark = SparkSession.builder.appName("HomeTileReportingETL").getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sample_source_data():
    """
    Create sample source data for home tile metrics
    """
    logger.info("Creating sample source tile data")
    
    # Sample tile metrics data
    tile_metrics_data = [
        ("tile_001", "2024-12-19", 1500, 120, 0.08, 45),
        ("tile_002", "2024-12-19", 2300, 180, 0.078, 67),
        ("tile_003", "2024-12-19", 890, 45, 0.051, 23),
        ("tile_004", "2024-12-19", 1200, 95, 0.079, 38),
        ("tile_005", "2024-12-19", 3400, 275, 0.081, 102)
    ]
    
    tile_metrics_schema = StructType([
        StructField("tile_id", StringType(), True),
        StructField("report_date", StringType(), True),
        StructField("tile_views", IntegerType(), True),
        StructField("tile_clicks", IntegerType(), True),
        StructField("ctr", DoubleType(), True),
        StructField("interstitial_interactions", IntegerType(), True)
    ])
    
    tile_metrics_df = spark.createDataFrame(tile_metrics_data, tile_metrics_schema)
    
    # Convert report_date to date type
    tile_metrics_df = tile_metrics_df.withColumn("report_date", to_date(col("report_date"), "yyyy-MM-dd"))
    
    return tile_metrics_df

def create_tile_metadata():
    """
    Create SOURCE_TILE_METADATA table with tile category information
    """
    logger.info("Creating tile metadata")
    
    metadata_data = [
        ("tile_001", "Personal Finance Overview", "Personal Finance"),
        ("tile_002", "Health Check Reminder", "Health"),
        ("tile_003", "Payment Due Alert", "Payments"),
        ("tile_004", "Offers & Promotions", "Offers"),
        ("tile_005", "Account Summary", "Personal Finance")
    ]
    
    metadata_schema = StructType([
        StructField("tile_id", StringType(), True),
        StructField("tile_name", StringType(), True),
        StructField("tile_category", StringType(), True)
    ])
    
    metadata_df = spark.createDataFrame(metadata_data, metadata_schema)
    
    return metadata_df

def transform_tile_data(tile_metrics_df, metadata_df):
    """
    Transform and enrich tile data with metadata
    """
    logger.info("Transforming tile data with metadata enrichment")
    
    # Left join to enrich with tile category, default to "UNKNOWN" if no match
    enriched_df = tile_metrics_df.alias("metrics").join(
        metadata_df.alias("meta"),
        col("metrics.tile_id") == col("meta.tile_id"),
        "left"
    ).select(
        col("metrics.tile_id"),
        col("metrics.report_date"),
        col("metrics.tile_views"),
        col("metrics.tile_clicks"),
        col("metrics.ctr"),
        col("metrics.interstitial_interactions"),
        coalesce(col("meta.tile_name"), lit("Unknown Tile")).alias("tile_name"),
        coalesce(col("meta.tile_category"), lit("UNKNOWN")).alias("tile_category")
    )
    
    # Add calculated metrics
    enriched_df = enriched_df.withColumn(
        "total_interactions", 
        col("tile_clicks") + col("interstitial_interactions")
    ).withColumn(
        "engagement_rate",
        round(col("total_interactions") / col("tile_views"), 4)
    ).withColumn(
        "processed_timestamp",
        current_timestamp()
    )
    
    return enriched_df

def validate_data(df, stage_name):
    """
    Validate data quality and completeness
    """
    logger.info(f"Validating data for stage: {stage_name}")
    
    record_count = df.count()
    logger.info(f"Record count for {stage_name}: {record_count}")
    
    # Check for null values in critical columns
    null_checks = {
        "tile_id": df.filter(col("tile_id").isNull()).count(),
        "report_date": df.filter(col("report_date").isNull()).count(),
        "tile_views": df.filter(col("tile_views").isNull()).count()
    }
    
    for column, null_count in null_checks.items():
        if null_count > 0:
            logger.warning(f"Found {null_count} null values in {column}")
        else:
            logger.info(f"No null values found in {column}")
    
    # Validate tile_category enrichment
    if "tile_category" in df.columns:
        unknown_count = df.filter(col("tile_category") == "UNKNOWN").count()
        logger.info(f"Tiles with UNKNOWN category: {unknown_count}")
    
    return record_count > 0

def write_to_delta(df, table_path, mode="overwrite"):
    """
    Write DataFrame to Delta table
    """
    logger.info(f"Writing data to Delta table: {table_path}")
    
    try:
        df.write \n            .format("delta") \n            .mode(mode) \n            .option("mergeSchema", "true") \n            .save(table_path)
        
        logger.info(f"Successfully wrote {df.count()} records to {table_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write to Delta table: {str(e)}")
        return False

def read_from_delta(table_path):
    """
    Read DataFrame from Delta table
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
    Main ETL pipeline execution
    """
    logger.info("Starting Home Tile Reporting ETL Pipeline")
    
    try:
        # Step 1: Create sample source data
        tile_metrics_df = create_sample_source_data()
        validate_data(tile_metrics_df, "Source Tile Metrics")
        
        # Step 2: Create tile metadata
        metadata_df = create_tile_metadata()
        validate_data(metadata_df, "Tile Metadata")
        
        # Step 3: Transform and enrich data
        enriched_df = transform_tile_data(tile_metrics_df, metadata_df)
        validate_data(enriched_df, "Enriched Tile Data")
        
        # Step 4: Write to Delta tables
        # Write source data
        source_table_path = "/tmp/delta/analytics_db/source_tile_metrics"
        write_to_delta(tile_metrics_df, source_table_path)
        
        # Write metadata
        metadata_table_path = "/tmp/delta/analytics_db/source_tile_metadata"
        write_to_delta(metadata_df, metadata_table_path)
        
        # Write enriched target data
        target_table_path = "/tmp/delta/analytics_db/home_tile_daily_summary"
        write_to_delta(enriched_df, target_table_path)
        
        # Step 5: Verify final output
        final_df = read_from_delta(target_table_path)
        if final_df:
            logger.info("Final output schema:")
            final_df.printSchema()
            
            logger.info("Sample output data:")
            final_df.show(10, truncate=False)
            
            # Category-wise summary
            category_summary = final_df.groupBy("tile_category") \n                .agg(
                    count("tile_id").alias("tile_count"),
                    sum("tile_views").alias("total_views"),
                    sum("tile_clicks").alias("total_clicks"),
                    avg("ctr").alias("avg_ctr")
                )
            
            logger.info("Category-wise performance summary:")
            category_summary.show()
        
        logger.info("Home Tile Reporting ETL Pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ Pipeline execution completed successfully")
    else:
        print("❌ Pipeline execution failed")