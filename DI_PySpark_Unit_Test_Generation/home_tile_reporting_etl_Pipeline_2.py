# _____________________________________________
# *Author*: AAVA
# *Created on*: 2024-12-19
# *Description*: ETL pipeline for home tile reporting with tile_category enrichment
# *Version*: 2
# *Updated on*: 2024-12-19
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

class HomeTileReportingETL:
    def __init__(self, spark_session: SparkSession = None):
        self.spark = spark_session or SparkSession.getActiveSession()
        self.logger = logging.getLogger("HomeTileReportingETL")
    
    def read_source_tables(self, process_date: str = PROCESS_DATE) -> tuple:
        """Read and filter source tables for the given process date"""
        self.logger.info("Reading source tables...")
        
        df_tile = self.spark.table(SOURCE_HOME_TILE_EVENTS).filter(
            F.to_date("event_ts") == process_date
        )
        df_inter = self.spark.table(SOURCE_INTERSTITIAL_EVENTS).filter(
            F.to_date("event_ts") == process_date
        )
        df_meta = self.spark.table(SOURCE_TILE_METADATA)
        
        return df_tile, df_inter, df_meta
    
    def aggregate_tile_events(self, df_tile: DataFrame) -> DataFrame:
        """Aggregate tile events by tile_id"""
        return (
            df_tile.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
                F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
            )
        )
    
    def aggregate_interstitial_events(self, df_inter: DataFrame) -> DataFrame:
        """Aggregate interstitial events by tile_id"""
        return (
            df_inter.groupBy("tile_id")
            .agg(
                F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
                F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
                F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
            )
        )
    
    def enrich_with_metadata(self, df_agg: DataFrame, df_meta: DataFrame) -> DataFrame:
        """Enrich aggregated data with tile metadata"""
        return (
            df_agg.join(df_meta.select("tile_id", "tile_category"), ["tile_id"], how="left")
            .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        )
    
    def create_daily_summary(self, df_tile_enriched: DataFrame, df_inter_enriched: DataFrame, process_date: str) -> DataFrame:
        """Create daily summary by combining tile and interstitial data"""
        return (
            df_tile_enriched.join(df_inter_enriched.drop("tile_category"), ["tile_id"], how="outer")
            .withColumn("date", F.lit(process_date))
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
    
    def create_global_kpis(self, df_daily_summary: DataFrame) -> DataFrame:
        """Create global KPIs from daily summary"""
        return (
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
    
    def write_delta(self, df: DataFrame, table: str, partition_col: str = "date", process_date: str = PROCESS_DATE):
        """Write DataFrame to Delta table with partition overwrite"""
        self.logger.info(f"Writing to {table} for {partition_col}={process_date}")
        df.write.format("delta").mode("overwrite").option("replaceWhere", f"{partition_col} = '{process_date}'").saveAsTable(table)
    
    def validate_data_quality(self, df: DataFrame, table_name: str) -> bool:
        """Validate data quality before writing to target tables"""
        self.logger.info(f"Validating data quality for {table_name}")
        
        # Check for null tile_ids
        null_tile_ids = df.filter(F.col("tile_id").isNull()).count()
        if null_tile_ids > 0:
            self.logger.warning(f"Found {null_tile_ids} records with null tile_id in {table_name}")
        
        # Check for negative values in metrics
        numeric_cols = [col for col in df.columns if "unique_" in col or "total_" in col]
        for col in numeric_cols:
            if col in df.columns:
                negative_count = df.filter(F.col(col) < 0).count()
                if negative_count > 0:
                    self.logger.warning(f"Found {negative_count} negative values in column {col}")
        
        return True
    
    def run_etl(self, process_date: str = PROCESS_DATE):
        """Execute the complete ETL pipeline with enhanced error handling and data validation"""
        try:
            # Read source tables
            df_tile, df_inter, df_meta = self.read_source_tables(process_date)
            
            # Validate source data
            self.logger.info(f"Source data counts - Tiles: {df_tile.count()}, Interstitials: {df_inter.count()}, Metadata: {df_meta.count()}")
            
            # Aggregate events
            df_tile_agg = self.aggregate_tile_events(df_tile)
            df_inter_agg = self.aggregate_interstitial_events(df_inter)
            
            # Enrich with metadata
            df_tile_enriched = self.enrich_with_metadata(df_tile_agg, df_meta)
            df_inter_enriched = self.enrich_with_metadata(df_inter_agg, df_meta)
            
            # Create daily summary
            df_daily_summary = self.create_daily_summary(df_tile_enriched, df_inter_enriched, process_date)
            
            # Validate daily summary data quality
            self.validate_data_quality(df_daily_summary, "daily_summary")
            
            # Write daily summary
            self.write_delta(df_daily_summary, TARGET_DAILY_SUMMARY, "date", process_date)
            
            # Create and write global KPIs
            df_global = self.create_global_kpis(df_daily_summary)
            
            # Validate global KPIs data quality
            self.validate_data_quality(df_global, "global_kpis")
            
            self.write_delta(df_global, TARGET_GLOBAL_KPIS, "date", process_date)
            
            self.logger.info(f"ETL completed successfully for {process_date}")
            
        except Exception as e:
            self.logger.error(f"ETL failed: {str(e)}")
            raise

# Main execution
if __name__ == "__main__":
    spark = SparkSession.getActiveSession()
    etl = HomeTileReportingETL(spark)
    etl.run_etl()