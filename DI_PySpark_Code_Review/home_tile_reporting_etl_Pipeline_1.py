# _____________________________________________
# *Author*: AAVA
# *Created on*: 2025-01-08
# *Description*: ETL pipeline for home tile reporting with tile_category enrichment
# *Version*: 1
# *Updated on*: 2025-01-08
# _____________________________________________

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from typing import Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HomeTileReportingETL")

PIPELINE_NAME = "HOME_TILE_REPORTING_ETL"

# Table names
SOURCE_HOME_TILE_EVENTS = "analytics_db.SOURCE_HOME_TILE_EVENTS"
SOURCE_INTERSTITIAL_EVENTS = "analytics_db.SOURCE_INTERSTITIAL_EVENTS"
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"

TARGET_DAILY_SUMMARY = "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY"
TARGET_GLOBAL_KPIS = "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"

PROCESS_DATE = "2025-12-01"

# Use Spark Connect compatible session
spark = SparkSession.getActiveSession()

# Read source tables
logger.info("Reading source tables...")
df_tile = spark.table(SOURCE_HOME_TILE_EVENTS).filter(F.to_date("event_ts") == PROCESS_DATE)
df_inter = spark.table(SOURCE_INTERSTITIAL_EVENTS).filter(F.to_date("event_ts") == PROCESS_DATE)
df_meta = spark.table(SOURCE_TILE_METADATA)

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

# Join with metadata for tile_category
df_tile_enriched = (
    df_tile_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
)

df_inter_enriched = (
    df_inter_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
)

# Combine tile and interstitial aggregates
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

# Write daily summary to Delta
def write_delta(df: DataFrame, table: str, partition_col: str = "date"):
    logger.info(f"Writing to {table} for {partition_col}={PROCESS_DATE}")
    df.write.format("delta").mode("overwrite").option("replaceWhere", f"{partition_col} = '{PROCESS_DATE}'").saveAsTable(table)

write_delta(df_daily_summary, TARGET_DAILY_SUMMARY)

# Global KPIs (unchanged as per Jira)
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

write_delta(df_global, TARGET_GLOBAL_KPIS)

logger.info(f"ETL completed successfully for {PROCESS_DATE}")